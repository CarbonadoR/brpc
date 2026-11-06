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

#include <fcntl.h>
#include <pthread.h>
#include <gflags/gflags.h>
#include "butil/atomicops.h"
#include "butil/logging.h"
#include "butil/time.h"
#include "brpc/server.h"
#include "brpc/socket_mode.h"
#include "brpc/urma/urma_helper.h"
#include "bvar/variable.h"
#include "bvar/latency_recorder.h"
#include "test.pb.h"
#include "sge_mode.h"

DEFINE_int32(port, 8002, "TCP Port of this server");
DEFINE_bool(use_urma, true, "Use URMA transport (true) or TCP (false)");
DEFINE_bool(use_rdma, false, "Use RDMA or not");

DEFINE_bool(server_ignore_oc, false, "Server ignore eovercrowded, false by default");
DEFINE_int32(num_threads, 5, "The max number of threads are used");
DEFINE_int32(max_concurrency, 128, "max concurrency");
DEFINE_int32(server_bthread_concurrency, 5, "server bthread concurrency");
DEFINE_int64(rsp_size, 0, "Response name size in bytes (0 means echo the request name back)");
DEFINE_int32(stats_timeout_seconds, 3, "Timeout in seconds before collecting statistics. (default 3)");
DEFINE_int32(test_seconds, 40, "Test duration in seconds (should match client)");

pthread_mutex_t g_metrics_lock = PTHREAD_MUTEX_INITIALIZER;
std::string g_cpu_usage_str;                    // cached describe_exposed("process_cpu_usage") value
butil::atomic<bool> g_cpu_usage_ready(false);   // claim token: at most one response per 1s cycle
butil::atomic<uint64_t> g_total_cnt(0);
butil::atomic<uint64_t> g_total_bytes(0);
butil::atomic<uint64_t> g_start_time(0);
butil::atomic<uint64_t> g_last_request_time(0);
butil::atomic<uint64_t> g_end_time(0);
bvar::LatencyRecorder g_server_latency_recorder("server_latency");
bvar::LatencyRecorder g_server_cpu_recorder("server_cpu");
bvar::LatencyRecorder g_server_memory_recorder("server_memory");
std::string g_name;
volatile bool g_stop = false;
brpc::Server* g_server = nullptr;

static void* StatsPrinter(void* arg) {
    brpc::Server* server = static_cast<brpc::Server*>(arg);

    while (!g_stop) {
        bthread_usleep(100000);

        // Sample process CPU/memory every 100ms, off the RPC hot path.
        const std::string cpu_usage_str = bvar::Variable::describe_exposed("process_cpu_usage");
        const double cpu_percent = atof(cpu_usage_str.c_str()) * 100;
        const double memory_mb = atof(
            bvar::Variable::describe_exposed("process_memory_resident").c_str()) / 1024 / 1024;

        pthread_mutex_lock(&g_metrics_lock);
        g_cpu_usage_str = cpu_usage_str;
        g_cpu_usage_ready.store(true, butil::memory_order_release);
        pthread_mutex_unlock(&g_metrics_lock);

        g_server_cpu_recorder << cpu_percent;
        g_server_memory_recorder << memory_mb;

        uint64_t now = butil::monotonic_time_us();
        uint64_t last = g_last_request_time.load(butil::memory_order_relaxed);
        uint64_t end = g_end_time.load(butil::memory_order_relaxed);
        uint64_t start_time = g_start_time.load(butil::memory_order_relaxed);

        if (end == 0 && start_time > 0) {
            uint64_t test_duration_us = now - start_time;
            uint64_t trigger_time_us = ((uint64_t)FLAGS_test_seconds + FLAGS_stats_timeout_seconds) * 1000000;

            if (test_duration_us >= trigger_time_us) {
                uint64_t end_expected = 0;
                if (g_end_time.compare_exchange_strong(end_expected, last)) {
                    double duration_s = (last - start_time) / 1000000.0;
                    uint64_t total_cnt = g_total_cnt.load(butil::memory_order_relaxed);
                    uint64_t total_bytes = g_total_bytes.load(butil::memory_order_relaxed);

                    std::cout << "========== Final Report ==========" << std::endl;
                    std::cout << "Duration: " << duration_s << "s" << std::endl;
                    std::cout << "Total Requests: " << total_cnt << std::endl;
                    if (duration_s > 0) {
                        std::cout << "QPS: " << total_cnt / duration_s << std::endl;
                        std::cout << "Bandwidth: " << total_bytes / 1024.0 / 1024.0 / duration_s << "MB/s" << std::endl;
                    }
                    std::cout << "Latency(avg/p99/p999/p9999): "
                              << g_server_latency_recorder.latency(10) << "/"
                              << g_server_latency_recorder.latency_percentile(0.99) << "/"
                              << g_server_latency_recorder.latency_percentile(0.999) << "/"
                              << g_server_latency_recorder.latency_percentile(0.9999) << "us" << std::endl;
                    std::cout << "CPU(avg/max): "
                              << g_server_cpu_recorder.latency(10) << "/"
                              << g_server_cpu_recorder.max_latency() << "%" << std::endl;
                    std::cout << "Memory(avg/max): "
                              << g_server_memory_recorder.latency(10) << "/"
                              << g_server_memory_recorder.max_latency() << "MB" << std::endl;
                    std::cout << "==================================" << std::endl;
                    return nullptr;
                }
            }
        }
    }
    return nullptr;
}

namespace test {
class PerfTestServiceImpl : public PerfTestService {
public:
    PerfTestServiceImpl() {}

    ~PerfTestServiceImpl() {}

    void Test(google::protobuf::RpcController* cntl_base,
              const PerfTestRequest* request,
              PerfTestResponse* response,
              google::protobuf::Closure* done) {
        brpc::ClosureGuard done_guard(done);
        uint64_t now = butil::monotonic_time_us();

        uint64_t expected = 0;
        if (g_start_time.compare_exchange_strong(expected, now)) {
            bthread_t tid;
            bthread_start_background(&tid, &BTHREAD_ATTR_NORMAL, StatsPrinter, g_server);
        }
        g_last_request_time.store(now, butil::memory_order_relaxed);

        if (g_cpu_usage_ready.exchange(false, butil::memory_order_acq_rel)) {
            pthread_mutex_lock(&g_metrics_lock);
            response->set_cpu_usage(g_cpu_usage_str);
            pthread_mutex_unlock(&g_metrics_lock);
        } else {
            response->set_cpu_usage("");  // proto2 required field: must be set explicitly
        }

        brpc::Controller* cntl = static_cast<brpc::Controller*>(cntl_base);

        if (FLAGS_rsp_size == 0) {
            response->set_name(request->name());
        } else {
            response->set_name(g_name);
        }
        if (request->echo_attachment()) {
            cntl->response_attachment().append(cntl->request_attachment());
            g_total_bytes.fetch_add(cntl->response_attachment().size(), butil::memory_order_relaxed);
        } else {
            g_total_bytes.fetch_add(response->name().size(), butil::memory_order_relaxed);
        }

        g_total_cnt.fetch_add(1, butil::memory_order_relaxed);
        g_server_latency_recorder << cntl->latency_us();
    }
};
}

namespace bthread {
    DECLARE_int32(bthread_concurrency);
}
DEFINE_string(urma_sge_mode, "auto", "Expected loaded URMA build: auto, single, or multi");

constexpr int FD_NUM = 20000;

int main(int argc, char* argv[]) {
    // 预申请fd 规避扩容时延
    int dummy_fd = open("/dev/null", O_RDONLY);
    int new_fd = dup2(dummy_fd, FD_NUM);
    if (dummy_fd != -1) close(dummy_fd);
    if (new_fd != -1) close(new_fd);

    GFLAGS_NAMESPACE::ParseCommandLineFlags(&argc, &argv, true);

    bthread::FLAGS_bthread_concurrency = FLAGS_server_bthread_concurrency;

    // Initialize URMA before any IOBuf operations to ensure all IOBuf blocks
    // are allocated from the URMA pool, not the original malloc.
    if (FLAGS_use_urma) {
        brpc::urma::GlobalUrmaInitializeOrDie();
        if (!ValidateUrmaSgeMode(FLAGS_urma_sge_mode)) {
            return 2;
        }
    }

    brpc::Server server;
    test::PerfTestServiceImpl perf_test_service_impl;

    g_name.resize(FLAGS_rsp_size, 'r');
    if (FLAGS_rsp_size == 0) {
        std::cout << "server rsp_size=0: echoing request name in response" << std::endl;
    } else {
        std::cout << "server rsp/name len is " << g_name.size() << "B" << std::endl;
    }
    std::cout << "URMA: " << (FLAGS_use_urma ? "yes" : "no")
              << ", URMA_SGE_MODE: " << FLAGS_urma_sge_mode
              << std::endl;

    g_total_cnt.store(0, butil::memory_order_relaxed);
    g_total_bytes.store(0, butil::memory_order_relaxed);
    g_start_time.store(0, butil::memory_order_relaxed);
    g_last_request_time.store(0, butil::memory_order_relaxed);
    g_end_time.store(0, butil::memory_order_relaxed);

    if (server.AddService(&perf_test_service_impl,
                          brpc::SERVER_DOESNT_OWN_SERVICE) != 0) {
        LOG(ERROR) << "Fail to add service";
        return -1;
    }
    brpc::ServerOptions options;
    if (FLAGS_use_urma) {
        options.socket_mode = brpc::SOCKET_MODE_URMA;
    } else if (FLAGS_use_rdma) {
        options.socket_mode = brpc::SOCKET_MODE_RDMA;
    } else {
        options.socket_mode = brpc::SOCKET_MODE_TCP;
    }
    options.max_concurrency = FLAGS_max_concurrency;
    options.num_threads = FLAGS_num_threads;
    options.ignore_eovercrowded = FLAGS_server_ignore_oc;
    if (server.Start(FLAGS_port, &options) != 0) {
        LOG(ERROR) << "Fail to start EchoServer";
        return -1;
    }
    g_server = &server;

    server.RunUntilAskedToQuit();
    g_stop = true;
    return 0;
}
