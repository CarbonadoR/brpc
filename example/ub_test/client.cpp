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
#include <stdlib.h>
#include <unistd.h>
#include <vector>
#include <climits>
#include <fstream>
#include <algorithm>
#include <iomanip>
#include <gflags/gflags.h>
#include <random>
#include <thread>
#include <chrono>
#include <pthread.h>
#include "butil/atomicops.h"
#include "butil/fast_rand.h"
#include "butil/logging.h"
#ifdef DBRPC_WITH_RDMA
#include "brpc/rdma/rdma_helper.h"
#endif
#include "brpc/server.h"
#include "brpc/channel.h"
#include "brpc/socket_mode.h"
#include "brpc/retry_policy.h"
#include "brpc/urma/urma_helper.h"
#include "bthread/bthread.h"
#include "bvar/latency_recorder.h"
#include "bvar/variable.h"
#include "test.pb.h"
#include "sge_mode.h"

// ==================== 命令行参数定义 ====================
// 来自ys thread_num 语义变更为总连接数
DEFINE_int32(thread_num, 1, "Total number of connections (deprecated, use link_num)");
DEFINE_int32(link_num, -1, "Total number of connections. Takes precedence over thread_num when >= 0.");
DEFINE_int32(queue_depth, 1, "Max in-flight requests across all connections (process-wide)");
DEFINE_int32(expected_qps, 0, "The expected QPS");
DEFINE_int64(initial_tokens, 10, "The initial number of tokens (smaller value = smoother start, less initial burst)");
DEFINE_int32(max_thread_num, 16, "The max number of threads are used");

// 来自ydl
DEFINE_int32(batch_size, 0, "Batch size for parallel connection (0 means no limit)");
DEFINE_int32(batch_interval_ms, 0, "Interval between batches in milliseconds");
DEFINE_int32(thread_pool_size, 8, "Thread pool size for parallel execution");
DEFINE_bool(only_first_rpc, false, "Only test first RPC connection, skip performance test");
// 压测阶段已改为全部连接一次性激活 (中央发送泵模型), 以下两个 flag 仅保留 CLI 兼容, 不再生效。
// 建链阶段的分批由 batch_size/batch_interval_ms 控制。
DEFINE_int32(test_connections_per_round, 0, "[deprecated] Number of connections to select per round");
DEFINE_int32(round_period_ms, 0, "[deprecated] Target period per round in milliseconds");

// 共用参数
DEFINE_int32(attachment_size, 0, "Attachment size is used (in Bytes)");
DEFINE_bool(echo_attachment, false, "Select whether attachment should be echo");
DEFINE_string(connection_type, "single", "Connection type of the channel");
DEFINE_string(protocol, "baidu_std", "Protocol type.");
DEFINE_string(servers, "0.0.0.0:8002+0.0.0.0:8002", "IP Address of servers (separated by '+')");
DEFINE_string(servers_file, "", "File path containing server list (one per line)");
DEFINE_bool(use_urma, true, "Use URMA transport (true) or TCP (false)");
DEFINE_bool(use_rdma, false, "Use RDMA or not");
DEFINE_int32(rpc_timeout_ms, 2000, "RPC call timeout");
DEFINE_int32(test_seconds, 20, "Test running time in seconds (0 means no duration limit, for pure iterations mode)");
DEFINE_int32(test_iterations, 0, "Total requests to complete in performance phase, process-wide (0 means disabled)");
DEFINE_int32(sender_thread_num, 0, "Number of sender pump bthreads (0 means auto: min(queue_depth, 16))");
DEFINE_int32(dummy_port, 8001, "Dummy server port number");
DEFINE_int32(connect_timeout_ms, 2000, "connect timeout");
DEFINE_string(req_size, "", "Request name sizes in bytes, comma-separated for multiple sizes "
                             "(e.g. '128,1024,4096'). Requests pick sizes round-robin by global "
                             "send index. Empty means no name payload (bandwidth then counts attachment).");
DEFINE_bool(client_ignore_oc, false, "Client ignore eovercrowded, false by default");
DEFINE_int32(max_retry, 3, "Max retry times per RPC, mapped to ChannelOptions::max_retry. "
                             "Default: 3 (up to 3 retries after initial attempt, 4 total). ");
DEFINE_int32(connect_retry_interval, 200, "Retry backoff interval for first RPC (ms). "
                                          "Actual delay is randomized in [interval, 2*interval].");
DEFINE_int32(token_tick_us, 1000, "Token generation tick interval in microseconds. Smaller tick = smoother "
                                 "token distribution, avoiding bursty sends and tail-latency degradation.");
DEFINE_int32(token_interval_period, 10, "Token generation tick multiplier (default 10, sleep = token_tick_us * token_interval_period)");
DEFINE_bool(use_connection_group, false, "Set connection_group for each channel or not");
DEFINE_bool(test_keep_alive, false, "Keep connections alive for 10 seconds after establishment");
DEFINE_bool(debug_latency, false, "Print latency details: each successful request's latency and the full first-packet latency lists");
DEFINE_int32(error_log_interval_ms, 1000, "Min interval between error logs (ms). Errors within a window "
                                          "are counted and reported with the next log to avoid log flooding "
                                          "when the server is abnormal. 0 = log every error");
DEFINE_int32(max_random_delay_us, 5000, "Max random delay in microseconds before sending request (used when expected_qps > 0)");
DEFINE_string(export_percentile_file, "", "Export percentile samples to a binary file for cross-instance aggregation (empty to skip)");
DEFINE_int32(latency_window_seconds, 10, "Percentile window in seconds for latency recorder");
DEFINE_bool(warmup, false, "Enable warmup connections before benchmark test to eliminate cold-start effects");
DEFINE_int32(warmup_connections, 100, "Number of connections to warm up (0 means same as thread_num or link_num)");

DEFINE_string(urma_sge_mode, "auto", "Expected loaded URMA build: auto, single, or multi");

// 解析有效连接数：link_num 优先，未设置（默认 -1）时回退到 thread_num
inline int GetConnectionCount() {
    return FLAGS_link_num >= 0 ? FLAGS_link_num : FLAGS_thread_num;
}

// ==================== 全局变量 ====================
// 性能统计记录器 (bvar 内部线程安全)
// 窗口长度必须在 main() 中 ParseCommandLineFlags 之后按 FLAGS_latency_window_seconds
// 构造: 静态全局对象在 main() 之前构造, 当时 gflags 尚未解析,
// WindowBase 会回退到 FLAGS_bvar_dump_interval 默认值(10s),
// 导致 save_percentile_samples 只导出最后 10 秒的快照。
bvar::LatencyRecorder* g_latency_recorder = nullptr;
bvar::LatencyRecorder g_server_cpu_recorder("server_cpu");
bvar::LatencyRecorder g_client_cpu_recorder("client_cpu");
bvar::LatencyRecorder g_client_memory_recorder("client_memory");

// 令牌桶 (使用协程信号量替代原子变量，避免忙等)
bthread_sem_t g_token_sem;
butil::atomic<bool> g_stop(false);

// 进程级在途并发控制 (中央发送泵模型):
// queue_depth 是全局在途请求上限, K 个发送泵共享 g_inflight_sem 许可,
// 轮询所有活跃连接发送请求 (取代原"每连接 queue_depth 个在途"模型)。
class PerformanceTest;  // 前置声明, 供 g_active_tests 指针使用
bthread_sem_t g_inflight_sem;                      // 全局在途许可, 数量 = queue_depth
butil::atomic<uint64_t> g_pending_req_count(0);    // 全局在途请求数
butil::atomic<int> g_send_index(0);                // 压测阶段连接轮询索引
butil::atomic<int> g_pump_num(0);                  // 发送泵数量 (供停止唤醒使用)
std::vector<PerformanceTest*>* g_active_tests = NULL;  // 压测阶段活跃连接 (只读)
uint64_t g_perf_start_time = 0;                    // 压测阶段全局开始时间

// 首包延迟存储 (需要 mutex 保护)
std::vector<int64_t> g_connect_latencies_us;
std::vector<int64_t> g_first_rpc_latencies_us;
std::vector<double> g_first_rpc_memory_mbs;
pthread_mutex_t g_latency_mutex = PTHREAD_MUTEX_INITIALIZER;
butil::atomic<int64_t> g_channel_id(0);

// 原子计数器 (多线程安全)
butil::atomic<uint64_t> g_total_bytes;
butil::atomic<uint64_t> g_all_cnt(0);
butil::atomic<uint64_t> g_total_cnt;
butil::atomic<uint64_t> g_total_error_cnt(0);
butil::atomic<uint64_t> g_last_time(0);

// 错误日志节流：按时间窗口抽样打印（默认 1s 一条），窗口内其余错误仅计数，
// 随下一条日志输出，避免服务端异常时海量失败 RPC 打爆日志磁盘
butil::atomic<int64_t> g_last_error_log_time_us(0);
butil::atomic<uint64_t> g_suppressed_error_logs(0);

// 服务器列表 (只读，无需保护)
std::vector<std::string> g_servers;

// 轮询索引 (原子操作保证线程安全)
butil::atomic<int> rr_index(0);

// 请求 name 负载: 支持多个 size (req_size 逗号分隔), 请求按全局发送索引轮询平均分配
std::vector<int64_t> g_req_sizes;   // 解析后的 size 列表 (空 = 未设置 req_size)
std::vector<std::string> g_names;   // 与 g_req_sizes 一一对应的预生成负载

// ==================== 客户端指标采样与全局停止判定 ====================
// 每 100ms 采样一次客户端 CPU/内存 (仅在成功响应路径上触发, 避免污染热路径)
static void SampleClientMetrics() {
    uint64_t last = g_last_time.load(butil::memory_order_relaxed);
    uint64_t now = butil::gettimeofday_us();
    if (now > last && now - last > 100000) {
        if (g_last_time.exchange(now, butil::memory_order_relaxed) == last) {
            g_client_cpu_recorder <<
                atof(bvar::Variable::describe_exposed("process_cpu_usage").c_str()) * 100;
            g_client_memory_recorder <<
                atof(bvar::Variable::describe_exposed("process_memory_resident").c_str()) / 1024 / 1024;
        }
    }
}

// 全局停止条件: 完成请求数达到 test_iterations, 或压测时长达到 test_seconds。
// 触发后额外发放许可/令牌唤醒所有发送泵, 泵拿到许可后检查 g_stop 直接退出,
// 不会再发送新请求 (在途请求完成后 pending 归零, 驱动流程即可回收连接)。
static void CheckGlobalStop() {
    bool should_stop = false;
    if (FLAGS_test_iterations > 0 &&
        g_total_cnt.load(butil::memory_order_relaxed) >= (uint64_t)FLAGS_test_iterations) {
        should_stop = true;
    }
    if (FLAGS_test_seconds > 0 && g_perf_start_time > 0 &&
        butil::gettimeofday_us() - g_perf_start_time >= (uint64_t)FLAGS_test_seconds * 1000000) {
        should_stop = true;
    }
    if (!should_stop) {
        return;
    }
    bool expected = false;
    if (g_stop.compare_exchange_strong(expected, true, butil::memory_order_relaxed)) {
        int pump_num = g_pump_num.load(butil::memory_order_relaxed);
        if (pump_num > 0) {
            bthread_sem_post_n(&g_inflight_sem, pump_num);
            if (FLAGS_expected_qps > 0) {
                bthread_sem_post_n(&g_token_sem, pump_num);
            }
        }
    }
}

// ==================== req_size 解析 ====================
// 解析逗号分隔的多 size (如 "128,1024,4096"), 生成 g_req_sizes/g_names。
// 未设置时 g_names 退化为单个空字符串 (与旧 req_size=0 行为一致)。
// 返回 0 成功, -1 失败 (错误信息写入 err)。
static int ParseReqSizes(std::string* err) {
    g_req_sizes.clear();
    g_names.clear();
    if (FLAGS_req_size.empty()) {
        g_names.push_back(std::string());
        return 0;
    }
    std::string::size_type pos1 = 0;
    while (true) {
        std::string::size_type pos2 = FLAGS_req_size.find(',', pos1);
        std::string token = FLAGS_req_size.substr(
            pos1, pos2 == std::string::npos ? std::string::npos : pos2 - pos1);
        // 去掉首尾空白
        token.erase(0, token.find_first_not_of(" \t"));
        token.erase(token.find_last_not_of(" \t") + 1);
        if (token.empty()) {
            *err = "empty size in req_size: " + FLAGS_req_size;
            return -1;
        }
        char* end = NULL;
        long v = strtol(token.c_str(), &end, 10);
        if (end == token.c_str() || *end != '\0' || v < 0) {
            *err = "invalid size in req_size: " + token;
            return -1;
        }
        g_req_sizes.push_back((int64_t)v);
        g_names.push_back(std::string((size_t)v, 'r'));
        if (pos2 == std::string::npos) break;
        pos1 = pos2 + 1;
    }
    return 0;
}

// ==================== 性能测试类 ====================
// 首包延迟记录功能
class PerformanceTest {
public:
    PerformanceTest(int attachment_size, bool echo_attachment)
        : _addr(NULL)
        , _channel(NULL)
        , _connect_latency_us(0)
        , _first_rpc_latency_us(0)
        , _first_rpc_memory_mb(0)
        , _init_result(-1)
    {
        if (attachment_size > 0) {
            _addr = malloc(attachment_size);
            butil::fast_rand_bytes(_addr, attachment_size);
            _attachment.append(_addr, attachment_size);
        }
        _echo_attachment = echo_attachment;
    }

    ~PerformanceTest() {
        if (_addr) {
            free(_addr);
        }
        delete _channel;
    }

    inline int64_t connect_latency_us() const { return _connect_latency_us; }
    inline int64_t first_rpc_latency_us() const { return _first_rpc_latency_us; }
    inline double first_rpc_memory_mb() const { return _first_rpc_memory_mb; }
    inline int init_result() const { return _init_result; }

    // 初始化 RPC 通道并执行首次 RPC (真正建立 TCP 连接)
    // 首包延迟记录
    int Init() {
        // FLAGS_max_retry is mapped to ChannelOptions::max_retry for all RPCs
        // on this Channel. For the first RPC below, we explicitly set
        // cntl.set_max_retry(0) to disable brpc's internal retry and instead
        // use a manual retry loop for accurate single-RPC latency measurement.
        // Retry semantics (error filtering, retry count) are aligned with
        // brpc's DefaultRetryPolicy. Jittered backoff uses connect_retry_interval.

        brpc::ChannelOptions options;
        if (FLAGS_use_urma) {
            options.socket_mode = brpc::SOCKET_MODE_URMA;
        } else if (FLAGS_use_rdma) {
            options.socket_mode = brpc::SOCKET_MODE_RDMA;
        } else {
            options.socket_mode = brpc::SOCKET_MODE_TCP;
        }
        options.protocol = FLAGS_protocol;
        options.connection_type = FLAGS_connection_type;
        options.timeout_ms = FLAGS_rpc_timeout_ms;
        options.connect_timeout_ms = FLAGS_connect_timeout_ms;
        options.max_retry = FLAGS_max_retry;
        if (FLAGS_use_connection_group) {
            options.connection_group = "pt_" + std::to_string(g_channel_id.fetch_add(1, butil::memory_order_relaxed));
        }

        std::string server = g_servers[rr_index.fetch_add(1, butil::memory_order_relaxed) % g_servers.size()];
        _channel = new brpc::Channel();

        int64_t start_ns = butil::cpuwide_time_ns();
        int ret = _channel->Init(server.c_str(), &options);
        int64_t end_ns = butil::cpuwide_time_ns();
        _connect_latency_us = (end_ns - start_ns) / 1000;

        if (ret != 0) {
            LOG(ERROR) << "Fail to initialize channel";
            _init_result = -1;
            return -1;
        }

        test::PerfTestRequest request;
        request.set_echo_attachment(_echo_attachment);
        request.set_name(g_names[0]);  // 首包 RPC 固定使用第一个 size
        test::PerfTestService_Stub stub(_channel);

        // First RPC with manual retry: disable brpc's internal retry per-call
        // so we measure the latency of a single successful RPC, not the
        // cumulative time of all retries.
        int retry_count = 0;
        while (true) {
            brpc::Controller cntl;
            cntl.set_max_retry(0);  // disable brpc internal retry for accurate latency
            test::PerfTestResponse response;
            int64_t rpc_start_ns = butil::cpuwide_time_ns();
            stub.Test(&cntl, &request, &response, NULL);
            int64_t rpc_end_ns = butil::cpuwide_time_ns();

            if (!cntl.Failed()) {
                _first_rpc_latency_us = (rpc_end_ns - rpc_start_ns) / 1000;
                _first_rpc_memory_mb = atof(bvar::Variable::describe_exposed("process_memory_resident").c_str()) / 1024 / 1024;
                _init_result = 0;
                return 0;
            }

            // Only retry on errors that brpc's DefaultRetryPolicy considers retryable
            // (EFAULTEDSOCKET, ECONNREFUSED, ETIMEDOUT, etc. — 13 error codes total).
            if (!brpc::DefaultRetryPolicy()->DoRetry(&cntl)) {
                LOG(ERROR) << "[First] RPC call failed with non-retryable error: " << cntl.ErrorText();
                _init_result = -1;
                return -1;
            }

            if (retry_count >= FLAGS_max_retry) {
                LOG(ERROR) << "[First] RPC call failed after " << FLAGS_max_retry
                           << " retries: " << cntl.ErrorText();
                _init_result = -1;
                return -1;
            }

            std::random_device rd;
            std::mt19937 gen(rd());
            std::uniform_int_distribution<> distrib(
                FLAGS_connect_retry_interval, 2 * FLAGS_connect_retry_interval);
            int random_ms = distrib(gen);
            LOG(WARNING) << "Retry " << (retry_count + 1) << "/"
                         << FLAGS_max_retry << " for first RPC after "
                         << random_ms << "ms: " << cntl.ErrorText();
            bthread_usleep(random_ms * 1000);
            ++retry_count;
        }
    }

    // 发送请求 (纯组装 + 异步发送; 并发/限流控制由中央发送泵 SendPump 完成)
    void SendRequest() {
        g_pending_req_count.fetch_add(1, butil::memory_order_relaxed);
        RespClosure* closure = new RespClosure;
        test::PerfTestRequest request;
        closure->resp = new test::PerfTestResponse();
        closure->cntl = new brpc::Controller();
        if (FLAGS_client_ignore_oc) {
            closure->cntl->ignore_eovercrowded();
        }
        request.set_echo_attachment(_echo_attachment);
        // 按全局发送索引轮询分配各 size, 保证各 size 请求数平均
        uint64_t send_index = g_all_cnt.fetch_add(1, butil::memory_order_relaxed);
        request.set_name(g_names[send_index % g_names.size()]);
        closure->cntl->request_attachment().append(_attachment);
        google::protobuf::Closure* done = brpc::NewCallback(&HandleResponse, closure);
        test::PerfTestService_Stub stub(_channel);
        stub.Test(closure->cntl, &request, closure->resp, done);
    }

    // 异步 RPC 响应闭包
    struct RespClosure {
        brpc::Controller* cntl;
        test::PerfTestResponse* resp;
    };

    // 静态回调函数 (运行在 brpc 响应线程)
    // 注意：RespClosure 由 SendRequest 中 new 分配，这里用 unique_ptr 接管所有权，
    // 保证所有返回路径都能正确释放（修复原版闭包从未 delete 的内存泄漏）。
    static void HandleResponse(RespClosure* raw_closure) {
        std::unique_ptr<RespClosure> closure(raw_closure);
        std::unique_ptr<brpc::Controller> cntl_guard(closure->cntl);
        std::unique_ptr<test::PerfTestResponse> response_guard(closure->resp);

        g_total_cnt.fetch_add(1, butil::memory_order_relaxed);
        if (closure->cntl->Failed()) {
            // 错误日志按时间窗口抽样打印（FLAGS_error_log_interval_ms，默认 1s 一条）：
            // 服务端异常时会有海量失败 RPC，逐条打日志会占满磁盘；
            // 窗口内其余错误只累计计数，随下一条日志一并输出。
            int64_t now = butil::monotonic_time_us();
            int64_t last = g_last_error_log_time_us.load(butil::memory_order_relaxed);
            if (FLAGS_error_log_interval_ms <= 0
                || (now - last >= (int64_t)FLAGS_error_log_interval_ms * 1000
                    && g_last_error_log_time_us.exchange(now, butil::memory_order_relaxed) == last)) {
                uint64_t suppressed = g_suppressed_error_logs.exchange(0, butil::memory_order_relaxed);
                LOG(ERROR) << "[Performance] RPC call failed: " << closure->cntl->ErrorText()
                           << (suppressed > 0 ? " (suppressed " + std::to_string(suppressed)
                                                  + " errors since last log)" : "");
            } else {
                g_suppressed_error_logs.fetch_add(1, butil::memory_order_relaxed);
            }
            g_total_error_cnt.fetch_add(1, butil::memory_order_relaxed);
            bthread_usleep(1000);
        } else {
            *g_latency_recorder << closure->cntl->latency_us();
            if (FLAGS_debug_latency) {
                printf("[latency] %ldus\n", (long)closure->cntl->latency_us());
            }
            if (closure->resp->cpu_usage().size() > 0) {
                g_server_cpu_recorder << atof(closure->resp->cpu_usage().c_str()) * 100;
            }

            if (!FLAGS_req_size.empty()) {
                g_total_bytes.fetch_add(closure->resp->name().size(), butil::memory_order_relaxed);
            } else {
                g_total_bytes.fetch_add(closure->cntl->request_attachment().size(), butil::memory_order_relaxed);
            }

            SampleClientMetrics();
        }

        cntl_guard.reset(NULL);
        response_guard.reset(NULL);

        // 释放一个全局在途许可, 发送泵据此补发下一个请求
        g_pending_req_count.fetch_sub(1, butil::memory_order_relaxed);
        bthread_sem_post(&g_inflight_sem);

        // 全局停止判定 (成功/失败响应都参与)
        CheckGlobalStop();
    }

private:
    void* _addr;
    brpc::Channel* _channel;
    int64_t _connect_latency_us;
    int64_t _first_rpc_latency_us;
    double _first_rpc_memory_mb;
    int _init_result;
    butil::IOBuf _attachment;
    bool _echo_attachment;
};

// ==================== 中央发送泵 ====================
// 全局并发模型: queue_depth 为进程级在途请求上限。K 个发送泵共享 g_inflight_sem
// 许可, 按轮询 (RR) 从所有活跃连接中选取连接发送请求; 响应线程在 HandleResponse
// 中释放许可, 泵据此补发, 取代原"每连接响应驱动自续"模型。
// 获取顺序为 许可 → 令牌: 许可由响应释放、按响应节奏逐个到达, 令牌消费因此被
// 响应节奏摊平, 避免令牌批量到账时多个泵同时抢令牌造成发送突发 (尾延迟劣化)。
// 两处等待均为 stop-aware 的 100ms timedwait; 持有许可等令牌超时需归还许可,
// 防止许可泄漏 (占着并发名额不发送)。
static void* SendPump(void* /*arg*/) {
    while (!g_stop.load(butil::memory_order_relaxed)) {
        // 并发: 先等待全局在途许可 (响应驱动)
        struct timespec abstime;
        struct timeval tv;
        gettimeofday(&tv, NULL);
        abstime.tv_sec = tv.tv_sec;
        abstime.tv_nsec = tv.tv_usec * 1000 + 100000000L;
        if (abstime.tv_nsec >= 1000000000L) {
            abstime.tv_sec += 1;
            abstime.tv_nsec -= 1000000000L;
        }
        int rc = bthread_sem_timedwait(&g_inflight_sem, &abstime);
        if (rc != 0) {
            if (g_stop.load(butil::memory_order_relaxed)) break;
            continue;
        }
        // 拿到许可后才发现停止: 直接退出 (许可无需归还, 信号量在下次 Test() 时重建)
        if (g_stop.load(butil::memory_order_relaxed)) {
            break;
        }

        // 限流: 再等待令牌桶令牌 (可选)
        if (FLAGS_expected_qps > 0) {
            struct timespec abstime2;
            struct timeval tv2;
            gettimeofday(&tv2, NULL);
            abstime2.tv_sec = tv2.tv_sec;
            abstime2.tv_nsec = tv2.tv_usec * 1000 + 100000000L;
            if (abstime2.tv_nsec >= 1000000000L) {
                abstime2.tv_sec += 1;
                abstime2.tv_nsec -= 1000000000L;
            }
            rc = bthread_sem_timedwait(&g_token_sem, &abstime2);
            if (rc != 0) {
                // 超时未拿到令牌: 归还持有的许可后重试, 避免占着并发名额不发送
                bthread_sem_post(&g_inflight_sem);
                if (g_stop.load(butil::memory_order_relaxed)) break;
                continue;
            }
            bthread_usleep(butil::fast_rand_less_than(FLAGS_max_random_delay_us));
        }

        if (g_active_tests == NULL || g_active_tests->empty()) {
            bthread_sem_post(&g_inflight_sem);
            if (FLAGS_expected_qps > 0) {
                bthread_sem_post(&g_token_sem);
            }
            continue;
        }
        size_t n = g_active_tests->size();
        PerformanceTest* test =
            (*g_active_tests)[(size_t)g_send_index.fetch_add(1, butil::memory_order_relaxed) % n];
        test->SendRequest();
    }
    return NULL;
}

// ==================== 连接池工作参数与入口函数 ====================
// 用有限个 bthread（pool_size）循环处理连接建链，
// 替代每连接一个 bthread 的模型，避免大量 bthread 的栈内存开销。

struct PoolWorkArg {
    int batch_end;
    int attachment_size;
    std::vector<PerformanceTest*>* tests;
    butil::atomic<int>* work_index;
};

static void* PoolWorkerFunc(void* arg) {
    PoolWorkArg* a = static_cast<PoolWorkArg*>(arg);
    while (true) {
        int idx = a->work_index->fetch_add(1, butil::memory_order_relaxed);
        if (idx >= a->batch_end) break;

        (*a->tests)[idx] = new PerformanceTest(a->attachment_size, FLAGS_echo_attachment);
        (*a->tests)[idx]->Init();
    }
    return nullptr;
}

// ==================== 令牌桶生成器 ====================
// 使用协程信号量实现令牌桶，通过 bthread_sem_post_n 发放令牌，
// 消费端通过 bthread_sem_wait 阻塞等待，避免忙等自旋。
// 细粒度 tick 平滑发放: 每 token_tick_us 补发一次差额 (默认 1ms)。
// 若按粗粒度 (如 10ms) 批量 post, 会同时唤醒大量等待泵造成发送突发,
// 服务端排队导致 99.9th+ 尾延迟劣化 (详见 README 并发模型一节)。
static void* GenerateToken(void* arg) {
    int64_t start_time = butil::monotonic_time_ns();
    int64_t accumulative_token = FLAGS_initial_tokens;
    while (!g_stop.load(butil::memory_order_relaxed)) {
        bthread_usleep(FLAGS_token_tick_us * FLAGS_token_interval_period);
        int64_t now = butil::monotonic_time_ns();
        if (accumulative_token * 1000000000 / (now - start_time) < FLAGS_expected_qps) {
            int64_t delta = FLAGS_expected_qps * (now - start_time) / 1000000000 - accumulative_token;
            if (delta > 0) {
                bthread_sem_post_n(&g_token_sem, static_cast<size_t>(delta));
                accumulative_token += delta;
            }
        }
    }
    return NULL;
}

// ==================== 辅助函数 ====================
// 计算百分位延迟
static int64_t CalcPercentile(std::vector<int64_t>& latencies, double percentile) {
    if (latencies.empty()) return 0;
    std::sort(latencies.begin(), latencies.end());
    size_t idx = static_cast<size_t>(latencies.size() * percentile);
    if (idx >= latencies.size()) idx = latencies.size() - 1;
    return latencies[idx];
}

// 延迟统计结构
struct LatencyStats {
    int64_t avg = 0;
    int64_t min = INT64_MAX;
    int64_t max = 0;
    int64_t p99 = 0;
    int64_t p999 = 0;
    int64_t p9999 = 0;

    void Calculate(std::vector<int64_t>& latencies) {
        if (latencies.empty()) {
            min = 0;
            return;
        }
        for (auto lat : latencies) {
            avg += lat;
            if (lat > max) max = lat;
            if (lat < min) min = lat;
        }
        avg /= latencies.size();
        p99 = CalcPercentile(latencies, 0.99);
        p999 = CalcPercentile(latencies, 0.999);
        p9999 = CalcPercentile(latencies, 0.9999);
    }

    void Print(const std::string& name) const {
        std::cout << name << "(avg/min/max/p99/p999/p9999): ";
        if (min != 0 || max != 0) {
            std::cout << avg << "/" << min << "/" << max << "/" << p99 << "/" << p999 << "/" << p9999 << "us";
        } else {
            std::cout << "N/A";
        }
    }
};

// ==================== 分批建链函数（bthread 池实现） ====================
// 使用固定数量的 worker bthread（= thread_pool_size）循环处理连接建链，
// 每个 worker 通过原子计数器竞争工作项，pool_size 自然限制并发度。
// record_latency: false 时仅建链不记录延迟（用于预热阶段）
static int BatchEstablishConnections(
    int total_connections,
    int attachment_size,
    std::vector<PerformanceTest*>& tests,
    bool record_latency = true) {

    int batch = (FLAGS_batch_size > 0) ? FLAGS_batch_size : total_connections;
    int pool_size = FLAGS_thread_pool_size;
    std::cout << "Initializing connections with bthread pool (pool_size="
              << pool_size << "), batch size=" << batch << "..." << std::endl;

    uint64_t start_time = butil::gettimeofday_us();
    butil::atomic<int> work_index(0);

    for (int batch_start = 0; batch_start < total_connections; batch_start += batch) {
        int batch_end = std::min(batch_start + batch, total_connections);
        int current_batch_size = batch_end - batch_start;
        int batch_index = batch_start / batch + 1;
        int num_workers = std::min(pool_size, current_batch_size);

        std::vector<bthread_t> tids(num_workers);
        work_index.store(batch_start, butil::memory_order_relaxed);

        PoolWorkArg pool_arg = {batch_end, attachment_size, &tests, &work_index};

        for (int k = 0; k < num_workers; ++k) {
            bthread_start_background(&tids[k], &BTHREAD_ATTR_NORMAL, PoolWorkerFunc, &pool_arg);
        }

        std::cout << "Batch " << batch_index << ": " << current_batch_size
                  << " connections, " << num_workers << " workers"
                  << (FLAGS_batch_interval_ms > 0 && batch_end < total_connections
                      ? ", sleep " + std::to_string(FLAGS_batch_interval_ms) + "ms" : "")
                  << std::endl;

        for (int k = 0; k < num_workers; ++k) {
            bthread_join(tids[k], NULL);
        }

        if (FLAGS_batch_interval_ms > 0 && batch_end < total_connections) {
            std::this_thread::sleep_for(std::chrono::milliseconds(FLAGS_batch_interval_ms));
        }
    }

    // 收集结果
    int total_success = 0;
    for (int k = 0; k < total_connections; ++k) {
        if (tests[k]->init_result() == 0) {
            total_success++;
            if (record_latency) {
                pthread_mutex_lock(&g_latency_mutex);
                g_connect_latencies_us.push_back(tests[k]->connect_latency_us());
                g_first_rpc_latencies_us.push_back(tests[k]->first_rpc_latency_us());
                g_first_rpc_memory_mbs.push_back(tests[k]->first_rpc_memory_mb());
                pthread_mutex_unlock(&g_latency_mutex);
            }
        }
    }

    uint64_t end_time = butil::gettimeofday_us();
    uint64_t total_duration_us = end_time - start_time;
    std::cout << "Total: " << total_success << "/" << total_connections << " connections established in "
              << total_duration_us << "us" << std::endl;
    return total_success;
}

// ==================== 首包统计输出 ====================
static void PrintLatencyStats() {
    LatencyStats connect_stats, first_rpc_stats;
    connect_stats.Calculate(g_connect_latencies_us);
    first_rpc_stats.Calculate(g_first_rpc_latencies_us);

    connect_stats.Print("Channel-Init-Latency");
    std::cout << ", ";
    first_rpc_stats.Print("First-RPC-Latency");

    if (!g_first_rpc_memory_mbs.empty()) {
        double avg_mem = 0, min_mem = g_first_rpc_memory_mbs[0], max_mem = g_first_rpc_memory_mbs[0];
        for (auto m : g_first_rpc_memory_mbs) {
            avg_mem += m;
            if (m > max_mem) max_mem = m;
            if (m < min_mem) min_mem = m;
        }
        avg_mem /= g_first_rpc_memory_mbs.size();
        std::cout << ", First-RPC-Memory(avg/min/max): " << avg_mem << "/" << min_mem << "/" << max_mem << "MB";
    }

    std::cout << std::endl;
}

// ==================== 性能测试函数 ====================
// 中央发送泵模型: 全部连接一次性激活, queue_depth 为进程级在途并发上限,
// 停止条件为全局 test_iterations (总完成请求数) / test_seconds (压测时长)。
// 清理顺序: g_stop 置位 → 泵不再发送 → 在途请求全部完成 (pending 归零) →
// join 泵/令牌线程 → 由调用方释放连接对象 (无 use-after-free 风险)。
static void RunPerformanceTest(std::vector<PerformanceTest*>& success_tests) {
    std::cout << "Starting performance test: " << success_tests.size() << " connections, "
              << "global queue_depth=" << FLAGS_queue_depth
              << ", duration " << FLAGS_test_seconds << "s"
              << (FLAGS_test_iterations > 0
                  ? ", iterations " + std::to_string(FLAGS_test_iterations) : "")
              << std::endl;

    g_active_tests = &success_tests;
    g_perf_start_time = butil::gettimeofday_us();
    g_pending_req_count.store(0, butil::memory_order_relaxed);
    g_send_index.store(0, butil::memory_order_relaxed);
    g_stop.store(false, butil::memory_order_relaxed);

    // 启动令牌桶生成线程（如果需要限流）
    bthread_t token_tid = INVALID_BTHREAD;
    if (FLAGS_expected_qps > 0) {
        bthread_start_background(&token_tid, &BTHREAD_ATTR_NORMAL, GenerateToken, NULL);
    }

    // 启动中央发送泵 (默认个数 = min(queue_depth, 16), 可用 sender_thread_num 覆盖)
    int pump_num = FLAGS_sender_thread_num > 0
        ? std::min(FLAGS_sender_thread_num, FLAGS_queue_depth)
        : std::min(FLAGS_queue_depth, 16);
    g_pump_num.store(pump_num, butil::memory_order_relaxed);
    std::vector<bthread_t> pump_tids(pump_num);
    for (int i = 0; i < pump_num; ++i) {
        bthread_start_background(&pump_tids[i], &BTHREAD_ATTR_NORMAL, SendPump, NULL);
    }

    // 等待全局停止: 停止标志置位且所有在途请求完成
    while (!(g_stop.load(butil::memory_order_relaxed) &&
             g_pending_req_count.load(butil::memory_order_relaxed) == 0)) {
        bthread_usleep(10000);
    }
    uint64_t end_time = butil::gettimeofday_us();
    uint64_t start_time = g_perf_start_time;

    // 回收发送泵与令牌线程 (泵已退出, join 保证释放连接前不再有发送路径)
    for (int i = 0; i < pump_num; ++i) {
        bthread_join(pump_tids[i], NULL);
    }
    if (FLAGS_expected_qps > 0 && token_tid != INVALID_BTHREAD) {
        bthread_join(token_tid, NULL);
    }
    g_active_tests = NULL;

    double throughput = g_total_bytes / 1.048576 / (end_time - start_time);

    double qps = (double)g_total_cnt.load(butil::memory_order_relaxed) * 1000 * 1000
                      / (end_time - start_time);
    std::cout << "Avg-Latency: " << g_latency_recorder->latency()
        << ", 50th-Latency: " << g_latency_recorder->latency_percentile(0.5)
        << ", 90th-Latency: " << g_latency_recorder->latency_percentile(0.9)
        << ", 99th-Latency: " << g_latency_recorder->latency_percentile(0.99)
        << ", 99.9th-Latency: " << g_latency_recorder->latency_percentile(0.999)
        << ", 99.99th-Latency: " << g_latency_recorder->latency_percentile(0.9999)
        << ", Max-Latency: " << g_latency_recorder->max_latency()
        << ", Throughput: " << throughput << "MB/s"
        << ", QPS: " << std::fixed << std::setprecision(1) << qps << std::defaultfloat << std::setprecision(6)
        << ", Server CPU(avg/max): " << g_server_cpu_recorder.latency(10) << "/" << g_server_cpu_recorder.max_latency() << "\%"
        << ", Client CPU(avg/max): " << g_client_cpu_recorder.latency(10) << "/" << g_client_cpu_recorder.max_latency() << "\%"
        << ", Client Memory(avg/max): " << g_client_memory_recorder.latency(10) << "/" << g_client_memory_recorder.max_latency() << "MB"
        << ", Total Sent: " << g_all_cnt.load(butil::memory_order_relaxed)
        << ", Total Errors: " << g_total_error_cnt.load(butil::memory_order_relaxed)
        << ", Total Requests: " << g_total_cnt.load(butil::memory_order_relaxed)
        << ", Error rate: "
        << ((g_total_cnt.load(butil::memory_order_relaxed) > 0)
            ? (g_total_error_cnt.load(butil::memory_order_relaxed) * 100.0 / g_total_cnt.load(butil::memory_order_relaxed))
            : 0.0) << "%";
    std::cout << std::endl;

    // Export percentile samples for cross-instance aggregation
    if (!FLAGS_export_percentile_file.empty()) {
        LOG(WARNING) << "--export_percentile_file is not supported in this build";
    }

}

// ==================== 测试入口函数 ====================
// thread_num 直接作为总连接数使用
void Test(int thread_num, int attachment_size) {
    // 输出测试配置
    std::cout << "[Connections: " << thread_num
        << " (servers: " << g_servers.size()
        << ", per_server: " << (g_servers.empty() ? 0 : thread_num / g_servers.size()) << ")"
        << ", Attachment: " << attachment_size << "B"
        << ", string sizes: [" << (FLAGS_req_size.empty() ? "0" : FLAGS_req_size) << "]B"
        << ", URMA: " << (FLAGS_use_urma ? "yes" : "no")
        << ", URMA_SGE_MODE: " << FLAGS_urma_sge_mode
        << ", RDMA: " << (FLAGS_use_rdma ? "yes" : "no")
        << ", Echo: " << (FLAGS_echo_attachment ? "yes" : "no")
        << ", Batch: " << (FLAGS_batch_size > 0 ? std::to_string(FLAGS_batch_size) : "unlimited")
        << ", BatchInterval: " << FLAGS_batch_interval_ms << "ms"
        << ", MaxConcurrency: " << FLAGS_thread_pool_size
        << ", QueueDepth: " << FLAGS_queue_depth
        << ", Warmup: " << (FLAGS_warmup ? "yes" : "no")
        << (FLAGS_warmup ? ("(" + std::to_string(FLAGS_warmup_connections > 0 ? FLAGS_warmup_connections : thread_num) + ")") : "")
        << "]" << std::endl;

    if (FLAGS_queue_depth <= 0) {
        LOG(ERROR) << "queue_depth must be >= 1 (got " << FLAGS_queue_depth << ")";
        return;
    }

    // 重置令牌桶信号量 (支持多次 Test() 调用时重新初始化)
    if (FLAGS_expected_qps > 0) {
        bthread_sem_destroy(&g_token_sem);
        bthread_sem_init(&g_token_sem, static_cast<unsigned>(FLAGS_initial_tokens));
    }

    // 重置全局计数器
    g_total_bytes.store(0, butil::memory_order_relaxed);
    g_all_cnt.store(0, butil::memory_order_relaxed);
    g_total_cnt.store(0, butil::memory_order_relaxed);
    g_connect_latencies_us.clear();
    g_first_rpc_latencies_us.clear();
    g_first_rpc_memory_mbs.clear();

    // 阶段0: 预热建链（可选，不计入延迟统计）
    // 通过预先建立连接消除服务端冷启动效应（如 JIT、内存分配、连接池预热等）
    if (FLAGS_warmup) {
        int warmup_count = FLAGS_warmup_connections > 0 ? FLAGS_warmup_connections : thread_num;
        std::vector<PerformanceTest*> warmup_tests(warmup_count);
        int warmup_success = BatchEstablishConnections(warmup_count, attachment_size, warmup_tests, false);
        // 立即释放预热连接
        for (auto t : warmup_tests) {
            delete t;
        }
    }

    // 阶段1: 分批建立连接（收集首包延迟统计）
    // 总连接数 = thread_num，使用 bthread 实现并发建链
    std::vector<PerformanceTest*> tests(thread_num);

    int total_success = BatchEstablishConnections(thread_num, attachment_size, tests);

    if (total_success == 0) {
        LOG(ERROR) << "No connections established, exiting...";
        for (auto t : tests) delete t;
        return;
    }

    // 阶段2: 统计首包延迟
    if (FLAGS_debug_latency) {
        std::cout << "[Debug] Connect-Latencies (" << g_connect_latencies_us.size() << "): ";
        for (auto lat : g_connect_latencies_us) {
            std::cout << lat << " ";
        }
        std::cout << std::endl;
        std::cout << "[Debug] First-RPC-Latencies (" << g_first_rpc_latencies_us.size() << "): ";
        for (auto lat : g_first_rpc_latencies_us) {
            std::cout << lat << " ";
        }
        std::cout << std::endl;
    }

    PrintLatencyStats();

    // 只测首包模式
    if (FLAGS_only_first_rpc) {
         if (FLAGS_test_seconds > 0) {
            std::cout << "Keeping " << total_success << " connections alive for "
                      << FLAGS_test_seconds << " seconds..." << std::endl;
            std::this_thread::sleep_for(std::chrono::seconds(FLAGS_test_seconds));
            std::cout << "Keep duration elapsed, exiting..." << std::endl;
        }
        for (auto t : tests) delete t;
        return;
    }

    // 阶段3: 性能测试 - 复用建链阶段成功的连接, 中央发送泵模型驱动
    std::vector<PerformanceTest*> success_tests;
    for (int k = 0; k < thread_num; ++k) {
        if (tests[k] && tests[k]->init_result() == 0) {
            success_tests.push_back(tests[k]);
        }
    }

    if (success_tests.empty()) {
        LOG(ERROR) << "No connections available for performance test";
        return;
    }

    if (FLAGS_test_iterations <= 0 && FLAGS_test_seconds <= 0) {
        LOG(ERROR) << "At least one of test_iterations/test_seconds must be > 0, "
                   << "otherwise the performance test never stops";
        return;
    }

    // 重建全局在途许可信号量 (支持多次 Test() 调用;
    // 上一轮 CheckGlobalStop 的唤醒补偿许可会抬升信号量计数, 必须清零)
    bthread_sem_destroy(&g_inflight_sem);
    bthread_sem_init(&g_inflight_sem, static_cast<unsigned>(FLAGS_queue_depth));

    RunPerformanceTest(success_tests);

    if (FLAGS_test_keep_alive) {
        std::cout << "Keeping connections alive for 10 seconds..." << std::endl;
        std::this_thread::sleep_for(std::chrono::seconds(10));
    }

    for (auto t : success_tests) {
        delete t;
    }
}

constexpr int FD_NUM = 20000;

// ==================== 主函数 ====================
int main(int argc, char* argv[]) {
    // 预申请fd 规避扩容时延
    int dummy_fd = open("/dev/null", O_RDONLY);
    int new_fd = dup2(dummy_fd, FD_NUM);
    if (dummy_fd != -1) close(dummy_fd);
    if (new_fd != -1) close(new_fd);

    GFLAGS_NAMESPACE::ParseCommandLineFlags(&argc, &argv, true);

    g_latency_recorder = new bvar::LatencyRecorder("client", FLAGS_latency_window_seconds);

    std::string parse_err;
    if (ParseReqSizes(&parse_err) != 0) {
        LOG(ERROR) << "Failed to parse req_size: " << parse_err;
        return -1;
    }

    // bthread_sem_init(&g_token_sem, static_cast<unsigned>(FLAGS_initial_tokens));
    bthread_sem_init(&g_token_sem, 0);
    // 占位初始化, 实际数量在每次 Test() 阶段3前重建为 queue_depth
    bthread_sem_init(&g_inflight_sem, 0);

    // Initialize URMA before any IOBuf operations (e.g. _attachment.append
    // in PerformanceTest constructor).  GlobalUrmaInitializeOrDie replaces
    // the IOBuf block allocator with PoolAllocate.  If IOBuf blocks are
    // allocated from the original malloc before this call, they get cached
    // in thread-local storage and later fail GetPoolSegFor with ERDMAMEM.
    if (FLAGS_use_urma) {
        brpc::urma::GlobalUrmaInitializeOrDie();
        if (!ValidateUrmaSgeMode(FLAGS_urma_sge_mode)) {
            return 2;
        }
    }

#ifdef WITH_RDMA
    if (FLAGS_use_rdma) {
        brpc::rdma::GlobalRdmaInitializeOrDie();
    }
#endif

    brpc::StartDummyServerAt(FLAGS_dummy_port);

    // 解析服务器列表
    if (!FLAGS_servers_file.empty()) {
        std::ifstream file(FLAGS_servers_file);
        if (!file.is_open()) {
            LOG(ERROR) << "Failed to open servers file: " << FLAGS_servers_file;
            return -1;
        }
        std::string line;
        while (std::getline(file, line)) {
            if (!line.empty() && line[0] != '#') {
                g_servers.push_back(line);
            }
        }
        file.close();
        std::cout << "Loaded " << g_servers.size() << " servers from file: " << FLAGS_servers_file << std::endl;
    } else {
        std::string::size_type pos1 = 0;
        std::string::size_type pos2 = FLAGS_servers.find('+');
        while (pos2 != std::string::npos) {
            g_servers.push_back(FLAGS_servers.substr(pos1, pos2 - pos1));
            pos1 = pos2 + 1;
            pos2 = FLAGS_servers.find('+', pos1);
        }
        g_servers.push_back(FLAGS_servers.substr(pos1));
    }

    if (g_servers.empty()) {
        LOG(ERROR) << "No servers specified";
        return -1;
    }

    // 解析有效连接数：link_num 优先，未设置时回退到 thread_num
    int connections = GetConnectionCount();
    std::cout << "Total connections: " << connections
              << " (link_num=" << FLAGS_link_num
              << ", thread_num=" << FLAGS_thread_num
              << ", servers: " << g_servers.size() << ")" << std::endl;

    if (connections > 0 && FLAGS_attachment_size >= 0) {
        Test(connections, FLAGS_attachment_size);
    }
    else if (connections <= 0 && FLAGS_attachment_size >= 0) {
        for (int i = 1; i <= FLAGS_max_thread_num; i *= 2) {
            Test(i, FLAGS_attachment_size);
        }
    }
    else if (connections > 0 && FLAGS_attachment_size < 0) {
        for (int i = 1; i <= 1024; i *= 4) {
            Test(connections, i);
        }
    }
    else {
        for (int j = 1; j <= 1024; j *= 4) {
            for (int i = 1; i <= FLAGS_max_thread_num; i *= 2) {
                Test(i, j);
            }
        }
    }

    return 0;
}
