# UrmaTransport: URMA-based Remote Memory RPC

UrmaTransport is a transport implementation that uses openEuler's
[URMA](https://atomgit.com/openeuler/umdk) (Unified Remote Memory Access) SDK
for remote-memory RPC. It is the Route B transport proposed in
[#3217](https://github.com/apache/brpc/discussions/3217) and complements the
OBMM-based UBRing transport (Route A, [#3226](https://github.com/apache/brpc/issues/3226))
for large-packet / cross-node scenarios (Route C, "double backend").

## Technical Background

The pooled transport uses independent RM JFS/JFR objects and the CTP path.
Each process shares 48 JFS slots and 64 JFR slots by default, with a separate
JFC/JFCE per slot. SEND uses `urma_post_jfs_wr`; receives use `urma_post_jfr_wr`.
Only event mode is supported in this phase. Credit/ACK, polling, bonding and
one-sided READ are outside the current scope.

## Build Configuration

### Build with CMake

```bash
# Build brpc with URMA support
cmake -B build -DWITH_URMA=ON -DWITH_TCMALLOC=ON
make -C build -j$(nproc)

# Build the urma_performance example
cd example/urma_performance
cmake -B build
make -C build -j$(nproc)
```

`WITH_URMA=ON` compiles against upstream UMDK headers. CMake prefers an
installed SDK and, following Mooncake's mock setup, downloads UMDK at a pinned
commit when the headers are unavailable. Set `DOWNLOAD_URMA_HEADERS=OFF` to
disable downloading.
When `liburma` is found it is linked for the hardware data path. Otherwise,
brpc uses its link-time mock so URMA code and tests can still be built without
hardware. `URMA_USE_MOCK` defaults to `AUTO`; set it to `ON` to force the mock,
or to `OFF` to require `liburma` and exclude the mock.

Pooled URMA requires gperftools headers and **libtcmalloc** in all build modes.
Use `CMAKE_PREFIX_PATH` for a non-system installation. AddressSanitizer cannot
be combined with tcmalloc. Make also requires the headers and `libtcmalloc`;
Bazel requires `@gperftools//:tcmalloc`, sharing the same allocator target
with the tests.

The default send gathers a 64-byte header SGE and as many body SGEs as fit
within the JFS SGE limit (device capability, capped by `--urma_max_sge`) and
the 4032-byte body limit. Gathering spans both blocks and consecutive IOBufs.
`URMA_SINGLE_SGE=ON` in CMake (or the C++ macro `BRPC_URMA_SINGLE_SGE=1`)
enables copying header and up to 4032 body bytes into one registered send
buffer, regardless of the number of input fragments. Receives
always use two SGEs; both builds share the same wire bytes.
CTP limits each complete SEND to 4096 bytes, including the 64-byte header.
Both send modes therefore split bodies into fragments of at most 4032 bytes,
even when the registered receive block is larger.

### Build with Make

```bash
sh config_brpc.sh --headers=/usr/include --libs=/usr/lib \
    --with-urma
make -j4

# Force the mock, or require the real liburma
sh config_brpc.sh --headers=/usr/include --libs=/usr/lib \
    --with-urma --with-urma-mock
sh config_brpc.sh --headers=/usr/include --libs=/usr/lib \
    --with-urma --without-urma-mock
```

Without either override, the Make configuration selects the implementation
automatically from whether `liburma` is available. `--without-urma-mock`
requires `liburma` in the configured library paths.

### Build with Bazel

```bash
# Build brpc with URMA support
bazel build --define=BRPC_WITH_URMA=true //:brpc

# This only shows the download switch; headers must still be declared by the toolchain or a local BUILD target
bazel build --define=BRPC_WITH_URMA=true \
    --define=BRPC_DOWNLOAD_URMA_HEADERS=false //:brpc

# Build and run the URMA unit tests (using the mock, without URMA hardware)
bazel test --define=BRPC_WITH_URMA=true \
    --define=BRPC_URMA_USE_MOCK=true //test:brpc_unittests

# Use the real liburma; the linker must be able to find -lurma
bazel build --define=BRPC_WITH_URMA=true \
    --define=BRPC_URMA_USE_MOCK=false //:brpc
```

Bazel fetches the pinned UMDK commit and exposes its headers through
`@umdk//:urma_headers` by default. With
`BRPC_DOWNLOAD_URMA_HEADERS=false`, Bazel does not depend on that external
repository; the actual build must still declare `urma_api.h`, `urma_types.h`,
and `urma_ubagg.h` through the toolchain or a local BUILD target. Passing only
`--copt=-I/path` to a directory outside the workspace does not make undeclared
headers available inside Bazel's default sandbox.
`BRPC_URMA_USE_MOCK=true` forces the link-time mock, while `false` excludes it
and links `liburma`. If unspecified, URMA builds use the mock by default.

The `ub_test` targets are `//example:ub_test_client` and
`//example:ub_test_server`. They require Linux and URMA support. On a build
host with UMDK installed, use:

```bash
# Multi-SGE, real URMA, required tcmalloc and CPU-frequency timing
bazel build --config=ub_test //example:ub_test_client //example:ub_test_server

# Single-SGE copying mode with the same workload and timing configuration
bazel build --config=ub_test_single_sge //example:ub_test_client //example:ub_test_server
```

Outputs are `bazel-bin/example/ub_test_client` and `ub_test_server`. Save each
mode's binaries before building the other mode, which updates the same paths.
`--define=BRPC_URMA_SINGLE_SGE=true` configures the macro in both brpc and its
consumers; defining it only for the benchmark does not change the transport.
Both configurations exclude the mock and enable optimized builds. The matrix
runner in `example/ub_test/run_ub_test_matrix.py` still uses the two-mode shared
library archive produced by the CMake-based `example/ub_test/build_package.py`;
the Bazel targets build the benchmark programs independently.

The matrix runner accepts `--io-jfs-count=48` to set the shared JFS count on
both nodes and verify the actual count in their startup logs. Compare
`--io-jfs-count=128` with `--io-jfs-count=48` to measure the effect of the send
queue working set; omitting the option uses the library default. The option
does not change QD, target QPS, message sizes, queue depths, or statistics.

## Usage

Select the transport by setting `socket_mode` on the channel / server:

```cpp
// Client
brpc::ChannelOptions opt;
opt.socket_mode = brpc::SOCKET_MODE_URMA;
opt.protocol = "baidu_std";   // URMA supports baidu_std only
brpc::Channel channel;
channel.Init("127.0.0.1:8003", &opt);

// Server
brpc::ServerOptions sopt;
sopt.socket_mode = brpc::SOCKET_MODE_URMA;
server.Start(port, &sopt);
```

If the peer does not speak URMA (e.g. a TCP-only client connecting to a
URMA-enabled server), the transport transparently falls back to TCP after
the 4-byte magic handshake. No application code change is required.

## Architecture

`urma_resource_pool.{h,cpp}` owns independent JFS/JFR slots, their completion
queues, receive posting and reference-counted remote imports. An EventAdapter
separates brpc Socket/epoll integration from resource management. Slots have
stable addresses and remain alive until both pool and carrier Socket release
them. `SetUrmaResourcePoolFactory` can replace the pool before initialization.

`urma_memory_pool.{h,cpp}` defines the replaceable UrmaMemoryPool interface.
The default implementation registers separate mmap regions for IOBuf blocks
and raw 64-byte headers, with sharded locks and no unregistered header fallback.
Use `SetUrmaMemoryPoolFactory` before initialization to install another backend.

Each endpoint owns only connection imports, SocketIds and sequence/reorder
state. The import cache key is `(EID, UASID, JFR id)`. TxCompletionCtx owns its
header, IOBuf references and connection imports until its CQE, including after
disconnect. RxRecvCtx owns the header and receive IOBuf until completion.

### Handshake and ordering

TCP exchanges the shared JFR id and full versioned SocketId. Binary v2 retains
`URMA`, uses implementation version 2 and appends SocketId at body offset 82
(90-byte body, 94-byte packet). Protobuf v3 retains `URM3`, uses field 3 for
JFR id, and adds fields 12/13 for SocketId/implementation version. Legacy
per-connection Jetty peers are rejected; the binary length/version prefix is
checked before reading the extended body to avoid deadlock with old peers.
Segment fields and import_seg-before-import_jfr ordering are retained for
provider routing; this phase does not issue READs.

The 64-byte header contains network-order destination SocketId (8 bytes),
sequence (2 bytes), type (1 byte, MSG_SMALL=1), and 53 zero reserved bytes.
A WR gathers consecutive body fragments across the input IOBuf list until
either the total SGE count (including the header) or body byte limit is reached.
Each body SGE retains its own registered segment and IOBuf reference until CQE.
Caller data is consumed and sequence advanced only after a successful post.
The compatibility mode reduces the body limit to leave header space in the
registered send block. Receive body capacity is the configured block size
minus IOBuf metadata (normally 8160 bytes for an 8192-byte block).

Receives route through Socket::Address, then reorder before appending to the
protocol input. Reordering looks up the expected key, so 65535->0 works; it
rejects duplicate/stale sequences and forward distances of 32768 or more.
Buffered data is dispatched only after the final TCP handshake confirmation.

Reset affects only the endpoint. Shutdown detaches events, stops hardware
queues, reclaims uncompleted contexts, and deletes JFC/JFCE resources. If a
provider cannot stop DMA, its resources are retained. Stop application IO and
connection creation before global shutdown. Memory still referenced by IOBuf
TLS caches is retained during initialization rollback.

## Configuration

| Flag | Default | Purpose |
|---|---|---|
| `--urma_io_jfs_cnt` | 48 | Process-wide send slots |
| `--urma_io_jfr_cnt` | 64 | Shared receive slots |
| `--urma_io_sq_depth` | 128 | Depth per JFS |
| `--urma_io_rx_depth` | 512 | Depth per JFR |
| `--urma_header_buf_count` | 65536 | Registered 64-byte headers |
| `--urma_buffer_size` | 8192 | IOBuf block size, >=1024 and a multiple of 64 |
| `--urma_buffer_count` | 48000 | Registered data blocks |
| `--urma_memory_pool_limit_mb` | 400 | Combined registered data/header budget in MiB |
| `--urma_cqe_poll_once` | 32 | CQ batch size, clamped to 1–32 |
| `--urma_recv_zerocopy` | true | Enable zero-copy receive |
| `--urma_zerocopy_min_size` | 512 | Copy smaller receives |
| `--urma_device` | empty | Device name; empty selects the first |
| `--urma_max_sge` | 0 | Cap total send SGEs including header; 0 uses device maximum; copying mode always sends one SGE |
| `--urma_client_handshake_version` | 2 | Binary=2, protobuf=3 |

Header count must cover total RX depth plus total TX depth. Data block count
must exceed total preposted RX depth. Initialization validates these bounds.
The default pool preposts 32768 receive WRs. Data and header pools total
397410304 bytes (about 379.00 MiB). Initialization rejects configurations above
the budget; registered capacity does not grow with connection count. This
budget excludes pool metadata, tcmalloc heap, sockets, stacks, provider queues,
and application data. Measure process RSS separately. The old `urma_sq_size`,
`urma_rq_size`, and `urma_prepared_jetty_cnt` flags are deprecated and no
longer size resources. `urma_disable_bthread` is also deprecated: shared CQs
dispatch every RPC callback to bthreads so a blocked callback cannot prevent
receive replenishment on other connections. Polling and bonding configurations
are rejected.

## 40,000-connection stress test

Build `urma_connection_stress` with the examples and deploy only its binary and
shared libraries to the peer. tcmalloc is required. Wait for `STRESS_READY`
before starting the client. Multiple listeners below belong to one process and
share the same JFS/JFR and memory pools.

```bash
./urma_connection_stress --mode=server --ports=18083,18084,18085,18086 \
  --urma_device=udmac0d1e2 --server_threads=16 --bthread_concurrency=16

./urma_connection_stress \
  --servers=SERVER_IP:18083,SERVER_IP:18084,SERVER_IP:18085,SERVER_IP:18086 \
  --urma_device=udmac0d1e2 --connections=40000 --concurrency=1024 \
  --rounds=20 --churn_percent=10 --idle_seconds=5 --bthread_concurrency=16
```

Each channel uses a unique `connection_group`. Every sweep checks distinct,
stable server SocketIds, the actual server connection count, and byte-for-byte
echoes of 64, 4032, 4096, 8192, and 65536-byte attachments. Churn replaces 10%
of connections between sweeps and requires new SocketIds. TCP fallback and RPC
retries are disallowed; errors return a nonzero exit code. `connections` counts
live sockets; `concurrency` bounds in-flight RPCs driven by bthreads.

Cleanup retains one monitoring connection until the server reports that all
other connections were reclaimed, then emits `STRESS_CLEANUP_PASS` and
`STRESS_PASS`. Resource reports include RSS, peak RSS, tcmalloc allocated/heap
bytes, threads, and file descriptors. Idle measurements do not force allocator
trimming. The test raises only its own soft fd limit to `connections + 4096`,
or fails if the hard limit is insufficient. Use at least two destination ports
for 40,000 connections and preferably four during churn to allow for TIME_WAIT.
All endpoints must belong to the same server process.

The registered pool budget is separate from total RSS. Validate message sizes
and in-flight concurrency against finite queues and memory: 40,000 persistent
connections do not imply 40,000 simultaneous large RPCs. ACK/credit flow control
is not implemented; passing a bounded test does not guarantee error-free
operation under arbitrary overload. Failures must be reported, not silently
corrupt the byte stream.

## Coexistence with UBRing

UrmaTransport is the recommended transport for **large packets and
cross-node** high-throughput paths, while UBRing (`SOCKET_MODE_UBRING`) is
optimal for **small packets and same-node IPC** (sub-microsecond, zero
syscalls). For a mixed workload, select the transport per service:

| Scenario | Recommended `socket_mode` |
|----------|---------------------------|
| Same-host IPC | `SOCKET_MODE_UBRING` |
| Cross-node small packets (< 64 KB) | `SOCKET_MODE_UBRING` (UBS-Mem) or `SOCKET_MODE_URMA` |
| Cross-node large packets (>= 64 KB) | `SOCKET_MODE_URMA` |
| Traditional RoCE / IB datacenter | `SOCKET_MODE_URMA` or `SOCKET_MODE_RDMA` |

A single-connection hybrid that auto-routes by packet size (Route C, scheme B)
is tracked as a future enhancement.

## Cross-node correctness verification

The `example/urma_performance` CMake project provides `urma_verify`. Link it
against a real-provider brpc build (`URMA_USE_MOCK=OFF`) and tcmalloc. Only
the executable and its shared libraries are needed on the peer.

```bash
./urma_verify --mode=server --port=18083 --urma_device=udmac0d1e2
./urma_verify --server=SERVER_IP:18083 --urma_device=udmac0d1e2 \
  --requests=150 --threads=4 --urma_client_handshake_version=2
./urma_verify --server=SERVER_IP:18083 --urma_device=udmac0d1e2 \
  --requests=70000 --sizes=64
./urma_verify --server=SERVER_IP:18083 --urma_device=udmac0d1e2 \
  --requests=1000 --threads=8 --fragment_bytes=127 \
  --sizes=4031,4032,4033,4096,65536
```

Every response must echo the request's distinct attachment byte for byte.
The server verifies the receiving Socket's established URMA state and rejects
TCP fallback. A failed check returns a nonzero exit code. Use `--sizes` for
attachment sizes, `--connection_type=pooled|short|single` for connection reuse,
and `--urma_client_handshake_version=3` for protobuf handshakes.
`--fragment_bytes` splits attachments into separate registered blocks of that
size; the default of 0 disables forced fragmentation.

After the send-gathering change, another 10,400 cross-node RPCs passed with
127-byte fragments, 0–64KiB attachments, mixed send modes and all three
connection types. Provider instrumentation observed up to 13 send SGEs in the
default mode and exactly one in the copying mode. Both reached the 4096-byte
wire limit without exceeding it. Explicit limits of two SGEs (default mode)
and one SGE (copying mode) also passed. Both builds passed 38 URMA unit tests
and SGE configuration checks; all 23 Socket regression tests passed.

On 2026-10-15, 17 cases totaling 142,820 RPCs passed between two aarch64 nodes
using UMDK 25.12, `udmac0d1e2`, CTP and tcmalloc. Coverage included both
directions, v2/v3 handshakes, 0–64KB attachments, the 4032/4033-byte boundary,
both send-SGE modes and mixed-mode peers, concurrent single/pooled/short
connections, and the then-default 128-JFS/64-JFR as well as 4-JFS/2-JFR pools. Each
send mode also completed 70,000 consecutive RPCs across sequence wraparound.

These checks exposed and fixed the CTP total-length limit and shared
`UrmaConnect` callback state in concurrently connecting derived sockets.
Both builds passed all 31 URMA unit tests; all 23 Socket regression tests
also passed. Throughput benchmarks and link fault injection remain pending.

## Limitations

- No peer-RQ credit/ACK protocol yet: overload can cause RNR/send failures.
- The 16-bit sequence requires reorder spans below 32768; there is no credit
  window to disambiguate packets separated by an entire sequence cycle.
- Mock tests verify software behavior and the CTP 4KB wire limit. Hardware
  correctness coverage is described above; sustained-load and link-fault
  testing remain pending.

- `baidu_std` protocol only (same as RDMA). SSL, RTMP, NSHEAD, MONGO are
  rejected at `ContextInitOrDie`.
- A hardware data path requires a supported UMDK provider and `liburma`.
- The current implementation targets Linux.
