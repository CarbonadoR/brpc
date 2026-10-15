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

// Bounded workers drive independent, persistent URMA connections. Each sweep
// visits every connection and checks its identity and response byte for byte.
#include <sys/resource.h>
#include <dirent.h>
#include <unistd.h>
#include <algorithm>
#include <atomic>
#include <fstream>
#include <functional>
#include <memory>
#include <set>
#include <sstream>
#include <string>
#include <vector>
#include <gflags/gflags.h>
#include <gperftools/malloc_extension.h>
#include "bthread/bthread.h"
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
DEFINE_string(servers, "127.0.0.1:18083,127.0.0.1:18084",
              "Comma-separated control endpoints of the SAME server process");
DEFINE_string(ports, "18083,18084", "Server listening ports");
DEFINE_int32(connections, 40000, "Simultaneously live independent connections");
DEFINE_int32(concurrency, 128, "Maximum outstanding RPCs / bthread workers");
DEFINE_int32(rounds, 3, "Complete sweeps after connection warmup");
DEFINE_int32(churn_percent, 10, "Percent of connections replaced after each sweep");
DEFINE_int32(idle_seconds, 10, "Idle hold after warmup and each sweep");
DEFINE_int32(timeout_ms, 10000, "Per-RPC timeout, with retries disabled");
DEFINE_string(sizes, "64,4032,4096,8192,65536", "Attachment sizes per sweep");
DEFINE_int32(report_seconds, 5, "Server resource report interval");
DEFINE_int32(server_threads, 16, "Server bthread worker threads");

static std::vector<std::string> Split(const std::string& value) {
    std::istringstream in(value);
    std::vector<std::string> result;
    std::string part;
    while (std::getline(in, part, ',')) { result.push_back(part); }
    return result;
}

static bool ParseNumbers(const std::string& value, size_t max,
                         std::vector<size_t>* out) {
    for (const auto& item : Split(value)) {
        if (item.empty() || item.find_first_not_of("0123456789") != std::string::npos) {
            return false;
        }
        try {
            const auto n = std::stoull(item);
            if (n > max) { return false; }
            out->push_back(n);
        } catch (...) { return false; }
    }
    return !out->empty();
}

static void Report(const char* phase, size_t connections) {
    size_t rss = 0, peak = 0, threads = 0, fds = 0;
    std::ifstream status("/proc/self/status");
    std::string line;
    while (std::getline(status, line)) {
        std::istringstream row(line);
        std::string key;
        size_t value;
        if (!(row >> key >> value)) { continue; }
        if (key == "VmRSS:") { rss = value; }
        if (key == "VmHWM:") { peak = value; }
        if (key == "Threads:") { threads = value; }
    }
    DIR* directory = opendir("/proc/self/fd");
    if (directory) {
        while (dirent* entry = readdir(directory)) {
            if (entry->d_name[0] != '.') { ++fds; }
        }
        closedir(directory);
        --fds;  // The directory descriptor itself.
    }
    size_t allocated = 0, heap = 0;
    MallocExtension::instance()->GetNumericProperty(
        "generic.current_allocated_bytes", &allocated);
    MallocExtension::instance()->GetNumericProperty("generic.heap_size", &heap);
    LOG(INFO) << "STRESS_RESOURCE phase=" << phase << " connections=" << connections
              << " rss_kib=" << rss << " peak_rss_kib=" << peak
              << " allocated_bytes=" << allocated << " heap_bytes=" << heap
              << " threads=" << threads << " fds=" << fds;
}

class StressService : public test::PerfTestService {
public:
    std::function<size_t()> connection_count;
    void Test(google::protobuf::RpcController* controller,
              const test::PerfTestRequest* request,
              test::PerfTestResponse* response,
              google::protobuf::Closure* done) override {
        brpc::ClosureGuard guard(done);
        auto* cntl = static_cast<brpc::Controller*>(controller);
        brpc::Socket* socket = brpc::ControllerPrivateAccessor(cntl).get_sending_socket();
        std::ostringstream debug;
        if (socket) { brpc::Socket::DebugSocket(debug, socket->id()); }
        if (!socket || socket->socket_mode() != brpc::SOCKET_MODE_URMA ||
            debug.str().find("state=ESTABLISHED") == std::string::npos) {
            cntl->SetFailed("Established URMA required; TCP fallback rejected");
            return;
        }
        response->set_cpu_usage("URMA_ESTABLISHED");
        response->set_verification_socket_id(socket->id());
        if (request->query_connections()) {
            response->set_active_connections(connection_count());
        }
        if (request->echo_attachment()) {
            cntl->response_attachment().append(cntl->request_attachment());
        }
    }
};

static int RunServer() {
    std::vector<size_t> ports;
    if (!ParseNumbers(FLAGS_ports, 65535, &ports) ||
        std::set<size_t>(ports.begin(), ports.end()).size() != ports.size() ||
        std::find(ports.begin(), ports.end(), 0) != ports.end()) { return 1; }
    StressService service;
    std::vector<std::unique_ptr<brpc::Server>> servers;
    service.connection_count = [&servers]() {
        size_t count = 0;
        for (const auto& server : servers) {
            brpc::ServerStatistics stat;
            server->GetStat(&stat);
            count += stat.connection_count;
        }
        return count;
    };
    for (auto port : ports) {
        std::unique_ptr<brpc::Server> server(new brpc::Server);
        brpc::ServerOptions options;
        options.socket_mode = brpc::SOCKET_MODE_URMA;
        options.idle_timeout_sec = -1;
        options.num_threads = FLAGS_server_threads;
        if (server->AddService(&service, brpc::SERVER_DOESNT_OWN_SERVICE) != 0 ||
            server->Start(port, &options) != 0 || !brpc::urma::IsUrmaAvailable()) {
            return 1;
        }
        servers.emplace_back(std::move(server));
    }
    LOG(INFO) << "STRESS_READY ports=" << FLAGS_ports;
    int ticks = 0;
    while (!brpc::IsAskedToQuit()) {
        if (ticks++ % FLAGS_report_seconds == 0) {
            Report("server", service.connection_count());
        }
        sleep(1);
    }
    for (auto& server : servers) { server->Stop(0); }
    for (auto& server : servers) { server->Join(); }
    return 0;
}

struct Connection {
    std::unique_ptr<brpc::Channel> channel;
    uint64_t peer_id = brpc::INVALID_SOCKET_ID;
    uint64_t retired_id = brpc::INVALID_SOCKET_ID;
    unsigned generation = 0;
};

struct Sweep {
    std::vector<Connection>* connections;
    const std::vector<std::string>* servers;
    const std::vector<size_t>* sizes;
    int round;
    std::atomic<size_t> next{0};
    std::atomic<size_t> passed{0};
    std::atomic<uint64_t> bytes{0};
    std::atomic<bool> failed{false};

    static void* Run(void* arg) {
        auto& sweep = *static_cast<Sweep*>(arg);
        while (!sweep.failed.load()) {
            const size_t index = sweep.next.fetch_add(1);
            if (index >= sweep.connections->size()) { break; }
            Connection& connection = (*sweep.connections)[index];
            if (!connection.channel) {
                brpc::ChannelOptions options;
                options.socket_mode = brpc::SOCKET_MODE_URMA;
                options.connection_type = "single";
                options.connection_group = "urma-stress-" + std::to_string(index) +
                    "-" + std::to_string(connection.generation);
                options.timeout_ms = FLAGS_timeout_ms;
                options.connect_timeout_ms = FLAGS_timeout_ms;
                options.max_retry = 0;
                connection.channel.reset(new brpc::Channel);
                if (connection.channel->Init(
                        (*sweep.servers)[index % sweep.servers->size()].c_str(), &options) != 0) {
                    LOG(ERROR) << "STRESS_FAILED channel_init index=" << index;
                    sweep.failed = true;
                    break;
                }
            }
            const size_t size = sweep.round == 0 ? 64 :
                (*sweep.sizes)[(index + sweep.round - 1) % sweep.sizes->size()];
            std::string payload(size, '\0');
            uint32_t state = 0x9e3779b9u ^ static_cast<uint32_t>(index) ^
                (static_cast<uint32_t>(sweep.round) * 0x85ebca6bu) ^ connection.generation;
            for (char& byte : payload) {
                state ^= state << 13;
                state ^= state >> 17;
                state ^= state << 5;
                byte = static_cast<char>(state);
            }
            brpc::Controller cntl;
            cntl.set_log_id((uint64_t(sweep.round) << 32) | index);
            cntl.request_attachment().append(payload);
            test::PerfTestRequest request;
            test::PerfTestResponse response;
            request.set_echo_attachment(true);
            test::PerfTestService_Stub stub(connection.channel.get());
            stub.Test(&cntl, &request, &response, nullptr);
            const uint64_t peer = response.verification_socket_id();
            if (cntl.Failed() || response.cpu_usage() != "URMA_ESTABLISHED" ||
                !response.has_verification_socket_id() ||
                peer == brpc::INVALID_SOCKET_ID || peer == connection.retired_id ||
                (connection.peer_id != brpc::INVALID_SOCKET_ID && peer != connection.peer_id) ||
                cntl.response_attachment().to_string() != payload) {
                LOG(ERROR) << "STRESS_FAILED round=" << sweep.round << " index=" << index
                           << " bytes=" << size << " error=" << cntl.ErrorText()
                           << " peer=" << peer << " previous=" << connection.peer_id
                           << " received=" << cntl.response_attachment().size();
                sweep.failed = true;
                break;
            }
            connection.peer_id = peer;
            sweep.bytes.fetch_add(size);
            sweep.passed.fetch_add(1);
        }
        return nullptr;
    }
};

static bool CheckServerConnections(Connection& connection, size_t expected) {
    const int64_t deadline = butil::monotonic_time_us() + int64_t(FLAGS_timeout_ms) * 1000;
    size_t observed = 0;
    do {
        brpc::Controller cntl;
        test::PerfTestRequest request;
        test::PerfTestResponse response;
        request.set_echo_attachment(false);
        request.set_query_connections(true);
        test::PerfTestService_Stub stub(connection.channel.get());
        stub.Test(&cntl, &request, &response, nullptr);
        if (cntl.Failed() || response.cpu_usage() != "URMA_ESTABLISHED" ||
            !response.has_active_connections() ||
            response.verification_socket_id() != connection.peer_id) {
            LOG(ERROR) << "STRESS_FAILED connection query: " << cntl.ErrorText();
            return false;
        }
        observed = response.active_connections();
        if (observed == expected) { return true; }
        usleep(10000);
    } while (butil::monotonic_time_us() < deadline);
    LOG(ERROR) << "STRESS_FAILED server connections=" << observed << " expected=" << expected;
    return false;
}

static int RunClient() {
    auto servers = Split(FLAGS_servers);
    std::vector<size_t> sizes;
    if (servers.empty() || !ParseNumbers(FLAGS_sizes, 1024 * 1024, &sizes)) { return 1; }
    std::vector<Connection> connections(FLAGS_connections);
    Report("before_connect", 0);
    uint64_t total = 0;
    for (int round = 0; round <= FLAGS_rounds; ++round) {
        Sweep sweep;
        sweep.connections = &connections;
        sweep.servers = &servers;
        sweep.sizes = &sizes;
        sweep.round = round;
        const int64_t start = butil::monotonic_time_us();
        std::vector<bthread_t> workers;
        for (int i = 0; i < std::min(FLAGS_concurrency, FLAGS_connections); ++i) {
            bthread_t id;
            if (bthread_start_background(&id, nullptr, Sweep::Run, &sweep) != 0) {
                sweep.failed = true;
                break;
            }
            workers.push_back(id);
        }
        for (auto id : workers) { bthread_join(id, nullptr); }
        if (sweep.failed || sweep.passed != connections.size()) { return 1; }
        // All endpoints must belong to one process, so SocketIds are globally unique.
        std::set<uint64_t> peers;
        for (const auto& connection : connections) { peers.insert(connection.peer_id); }
        if (peers.size() != connections.size()) {
            LOG(ERROR) << "STRESS_FAILED connections unexpectedly share sockets unique="
                       << peers.size();
            return 1;
        }
        if (!CheckServerConnections(connections.front(), connections.size())) { return 1; }
        total += sweep.passed;
        LOG(INFO) << "STRESS_SWEEP_PASS round=" << round << " verified=" << sweep.passed
                  << " unique_peer_socket_ids=" << peers.size()
                  << " bytes=" << sweep.bytes
                  << " elapsed_us=" << butil::monotonic_time_us() - start;
        // Do not force tcmalloc to return memory: measure the real idle footprint.
        sleep(FLAGS_idle_seconds);
        Report(round == 0 ? "connected_idle" : "sweep_idle", connections.size());
        if (round > 0 && round < FLAGS_rounds && FLAGS_churn_percent > 0) {
            size_t changed = 0;
            for (size_t i = 0; i < connections.size(); ++i) {
                if ((i + round * 17) % 100 >= size_t(FLAGS_churn_percent)) { continue; }
                auto& connection = connections[i];
                connection.channel.reset();
                connection.retired_id = connection.peer_id;
                connection.peer_id = brpc::INVALID_SOCKET_ID;
                ++connection.generation;
                ++changed;
            }
            LOG(INFO) << "STRESS_CHURN closed=" << changed;
        }
    }
    for (size_t i = 1; i < connections.size(); ++i) { connections[i].channel.reset(); }
    if (!CheckServerConnections(connections.front(), 1)) { return 1; }
    LOG(INFO) << "STRESS_CLEANUP_PASS server_connections=1 monitoring_connection_only";
    connections.clear();
    sleep(FLAGS_idle_seconds);
    Report("released", 0);
    LOG(INFO) << "STRESS_PASS connections=" << FLAGS_connections
              << " verified_rpcs=" << total << " rounds=" << FLAGS_rounds
              << " concurrency=" << FLAGS_concurrency;
    return 0;
}

int main(int argc, char** argv) {
    gflags::ParseCommandLineFlags(&argc, &argv, true);
    if (FLAGS_connections <= 0 || FLAGS_concurrency <= 0 || FLAGS_rounds <= 0 ||
        FLAGS_timeout_ms <= 0 || FLAGS_report_seconds <= 0 || FLAGS_idle_seconds < 0 ||
        FLAGS_server_threads <= 0 ||
        FLAGS_churn_percent < 0 || FLAGS_churn_percent > 100) { return 1; }
    // Raise only this test process's soft fd limit, never host-wide settings.
    rlimit limit;
    if (getrlimit(RLIMIT_NOFILE, &limit) != 0) { return 1; }
    const rlim_t required = static_cast<rlim_t>(FLAGS_connections) + 4096;
    if (limit.rlim_cur < required) {
        if (limit.rlim_max < required) {
            LOG(ERROR) << "Need RLIMIT_NOFILE >= " << required;
            return 1;
        }
        limit.rlim_cur = required;
        if (setrlimit(RLIMIT_NOFILE, &limit) != 0) { return 1; }
    }
    if (FLAGS_mode == "server") { return RunServer(); }
    if (FLAGS_mode == "client") { return RunClient(); }
    return 1;
}
