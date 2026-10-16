// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

// Bounded, byte-for-byte validation against a real URMA peer. Both sides
// reject TCP fallback; this is a correctness check, not a benchmark.
#include <algorithm>
#include <atomic>
#include <cstdint>
#include <cstring>
#include <sstream>
#include <string>
#include <thread>
#include <vector>

#include <gflags/gflags.h>
#include "butil/logging.h"
#include "butil/time.h"
#include "brpc/channel.h"
#include "brpc/closure_guard.h"
#include "brpc/controller.h"
#include "brpc/details/controller_private_accessor.h"
#include "brpc/server.h"
#include "brpc/socket.h"
#include "brpc/urma/urma_helper.h"
#include "test.pb.h"

DEFINE_string(mode, "client", "client or server");
DEFINE_string(server, "127.0.0.1:18083", "Peer TCP control address");
DEFINE_int32(port, 18083, "Server TCP control port");
DEFINE_int32(requests, 100, "Total number of RPCs across all workers");
DEFINE_int32(threads, 1, "Concurrent synchronous RPC workers");
DEFINE_string(connection_type, "single", "single, pooled, or short");
DEFINE_string(sizes, "0,1,63,64,511,512,4096,8159,8160,8161,16384,65536",
              "Comma-separated attachment sizes cycled by request id");
DEFINE_int32(timeout_ms, 5000, "Per-RPC timeout; retries are disabled");
DEFINE_int32(fragment_bytes, 0,
             "Use separate registered attachment blocks of this size; 0 disables fragmentation");

class VerifyService : public test::PerfTestService {
public:
    void Test(google::protobuf::RpcController* controller,
              const test::PerfTestRequest* request,
              test::PerfTestResponse* response,
              google::protobuf::Closure* done) override {
        brpc::ClosureGuard guard(done);
        auto* cntl = static_cast<brpc::Controller*>(controller);
        brpc::Socket* socket =
            brpc::ControllerPrivateAccessor(cntl).get_sending_socket();
        std::ostringstream debug;
        if (socket) { brpc::Socket::DebugSocket(debug, socket->id()); }
        const std::string state = debug.str();
        if (!socket || socket->socket_mode() != brpc::SOCKET_MODE_URMA ||
            state.find("state=ESTABLISHED") == std::string::npos) {
            cntl->SetFailed("Verification requires established URMA, TCP fallback rejected");
            return;
        }
        response->set_cpu_usage("URMA_ESTABLISHED");
        if (request->echo_attachment()) {
            cntl->response_attachment().append(cntl->request_attachment());
        }
    }
};

int main(int argc, char** argv) {
    gflags::ParseCommandLineFlags(&argc, &argv, true);
    if (FLAGS_mode == "server") {
        brpc::Server server;
        VerifyService service;
        if (server.AddService(&service, brpc::SERVER_DOESNT_OWN_SERVICE) != 0) {
            return 1;
        }
        brpc::ServerOptions options;
        options.socket_mode = brpc::SOCKET_MODE_URMA;
        if (server.Start(FLAGS_port, &options) != 0 ||
            !brpc::urma::IsUrmaAvailable()) {
            LOG(ERROR) << "URMA server initialization failed";
            return 1;
        }
        LOG(INFO) << "VERIFY_READY port=" << FLAGS_port;
        server.RunUntilAskedToQuit();
        return 0;
    }
    if (FLAGS_mode != "client" || FLAGS_requests <= 0 || FLAGS_threads <= 0 ||
        FLAGS_timeout_ms <= 0 || FLAGS_fragment_bytes < 0) {
        LOG(ERROR) << "Invalid verification arguments";
        return 1;
    }
    std::vector<size_t> sizes;
    std::istringstream input(FLAGS_sizes);
    std::string item;
    while (std::getline(input, item, ',')) {
        size_t used = 0;
        try {
            const unsigned long value = std::stoul(item, &used);
            if (used != item.size() || value > 1024 * 1024) { return 1; }
            sizes.push_back(value);
        } catch (...) { return 1; }
    }
    if (sizes.empty()) { return 1; }
    brpc::Channel channel;
    brpc::ChannelOptions options;
    options.socket_mode = brpc::SOCKET_MODE_URMA;
    options.connection_type = FLAGS_connection_type;
    options.timeout_ms = FLAGS_timeout_ms;
    options.connect_timeout_ms = FLAGS_timeout_ms;
    options.max_retry = 0;
    if (channel.Init(FLAGS_server.c_str(), &options) != 0 ||
        !brpc::urma::IsUrmaAvailable()) {
        LOG(ERROR) << "URMA client initialization failed";
        return 1;
    }
    if (static_cast<size_t>(FLAGS_fragment_bytes) > brpc::urma::GetUrmaRecvBlockSize()) {
        LOG(ERROR) << "Verification fragment exceeds the registered block capacity";
        return 1;
    }
    std::atomic<int> next{0};
    std::atomic<int> passed{0};
    std::atomic<bool> failed{false};
    std::atomic<uint64_t> bytes{0};
    const int64_t start = butil::monotonic_time_us();
    std::vector<std::thread> workers;
    for (int thread = 0; thread < FLAGS_threads; ++thread) {
        workers.emplace_back([&]() {
            test::PerfTestService_Stub stub(&channel);
            while (!failed.load()) {
                const int id = next.fetch_add(1);
                if (id >= FLAGS_requests) { break; }
                std::string payload(sizes[id % sizes.size()], '\0');
                uint32_t state = 0x9e3779b9u ^ static_cast<uint32_t>(id);
                for (char& byte : payload) {
                    state ^= state << 13;
                    state ^= state >> 17;
                    state ^= state << 5;
                    byte = static_cast<char>(state);
                }
                brpc::Controller cntl;
                cntl.set_log_id(id);
                if (FLAGS_fragment_bytes == 0) {
                    cntl.request_attachment().append(payload);
                } else {
                    for (size_t offset = 0; offset < payload.size(); ) {
                        butil::IOBufAsZeroCopyOutputStream stream(
                            &cntl.request_attachment(),
                            brpc::urma::GetUrmaRecvBlockSize() + sizeof(butil::IOBuf::Block));
                        void* block = nullptr;
                        int capacity = 0;
                        const size_t len = std::min<size_t>(
                            FLAGS_fragment_bytes, payload.size() - offset);
                        if (!stream.Next(&block, &capacity) ||
                            static_cast<size_t>(capacity) < len) {
                            LOG(ERROR) << "Cannot allocate verification fragment";
                            failed.store(true);
                            return;
                        }
                        memcpy(block, payload.data() + offset, len);
                        stream.BackUp(capacity - len);
                        offset += len;
                    }
                }
                test::PerfTestRequest request;
                test::PerfTestResponse response;
                request.set_echo_attachment(true);
                stub.Test(&cntl, &request, &response, nullptr);
                if (cntl.Failed() || response.cpu_usage() != "URMA_ESTABLISHED" ||
                    cntl.response_attachment().to_string() != payload) {
                    failed.store(true);
                    LOG(ERROR) << "VERIFY_FAILED id=" << id << " bytes=" << payload.size()
                               << " error=" << cntl.ErrorText()
                               << " received=" << cntl.response_attachment().size()
                               << " transport=" << response.cpu_usage();
                    break;
                }
                ++passed;
                bytes.fetch_add(payload.size());
            }
        });
    }
    for (auto& worker : workers) { worker.join(); }
    LOG(INFO) << (failed.load() ? "VERIFY_FAILED" : "VERIFY_PASS")
              << " completed=" << passed.load() << "/" << FLAGS_requests
              << " attachment_bytes=" << bytes.load()
              << " threads=" << FLAGS_threads
              << " connection=" << FLAGS_connection_type
              << " elapsed_us=" << butil::monotonic_time_us() - start;
    return failed.load() || passed.load() != FLAGS_requests ? 1 : 0;
}
