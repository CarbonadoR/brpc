# UrmaTransport：基于 URMA 的远程内存 RPC

UrmaTransport 是使用 openEuler
[URMA](https://atomgit.com/openeuler/umdk)（Unified Remote Memory Access）SDK
实现的传输层。它是
[#3217](https://github.com/apache/brpc/discussions/3217) 中提出的路线 B，
与基于 OBMM 的 UBRing 传输（路线 A，
[#3226](https://github.com/apache/brpc/issues/3226)）互补，承担大包/跨节点
高吞吐场景，共同构成路线 C（双后端）。

## 技术背景

当前池化实现使用独立的可靠消息（`URMA_TM_RM`）JFS/JFR 和 CTP 路径。
每个进程默认共享 128 个 JFS、64 个 JFR，每个 slot 拥有自己的 JFC/JFCE；
使用 `urma_post_jfs_wr` / `urma_post_jfr_wr` 收发，通过 JFCE fd 事件驱动。
本阶段实现池化设计 13 的适配点 1–15；不包含 credit/ACK、忙轮询、bonding
或大包单边 READ。超过一个 block 的消息分成多个 SEND。

## 编译配置

### CMake 编译

```bash
# 带 URMA 支持编译 brpc
cmake -B build -DWITH_URMA=ON -DWITH_TCMALLOC=ON
make -C build -j$(nproc)

# 编译 urma_performance 示例
cd example/urma_performance
cmake -B build
make -C build -j$(nproc)
```

`WITH_URMA=ON` 使用上游 UMDK 头文件进行编译。CMake 优先使用系统安装的
SDK；找不到头文件时，会参照 Mooncake 的 mock 构建方式下载固定版本的
UMDK commit，可通过 `DOWNLOAD_URMA_HEADERS=OFF` 禁止下载。找到 `liburma` 时使用
真实硬件数据通路，否则链接 brpc 的 mock，使 URMA 代码和测试仍可在无硬件
环境编译。`URMA_USE_MOCK` 默认为 `AUTO`；设为 `ON` 可强制使用 mock，设为
`OFF` 则要求找到 `liburma` 并排除 mock。

池化 URMA **必须链接 tcmalloc**。CMake 要求 gperftools 的头文件和
`libtcmalloc`；非系统安装可用 `CMAKE_PREFIX_PATH` 指定安装前缀。
不能与 AddressSanitizer 同时启用。Make 的 URMA 配置也要求头文件及
`libtcmalloc`；Bazel 使用必需的 `@gperftools//:tcmalloc` 依赖，
与测试复用同一个分配器目标。

默认发送两个 SGE：64B header + 一个连续 body。编译时设置
`-DURMA_SINGLE_SGE=ON`（CMake）或 `-DBRPC_URMA_SINGLE_SGE=1`
（C++ 编译宏），可启用拷贝合并发送；接收仍固定为两个 SGE。
两种构建使用相同的线格式，不需要逐连接协商 SGE 模式。

### Make 编译

```bash
sh config_brpc.sh --headers=/usr/include --libs=/usr/lib \
    --with-urma
make -j4

# 强制使用 mock，或强制使用真实 liburma
sh config_brpc.sh --headers=/usr/include --libs=/usr/lib \
    --with-urma --with-urma-mock
sh config_brpc.sh --headers=/usr/include --libs=/usr/lib \
    --with-urma --without-urma-mock
```

默认根据 `liburma` 是否存在自动选择；`--without-urma-mock` 要求配置路径中
存在 `liburma`。

### Bazel 编译

```bash
# 编译带 URMA 支持的 brpc
bazel build --define=BRPC_WITH_URMA=true //:brpc

# 仅展示关闭下载的开关；头文件仍须由工具链或本地 BUILD 目标声明
bazel build --define=BRPC_WITH_URMA=true \
    --define=BRPC_DOWNLOAD_URMA_HEADERS=false //:brpc

# 编译并运行 URMA 单元测试（使用 mock，不需要 URMA 硬件）
bazel test --define=BRPC_WITH_URMA=true \
    --define=BRPC_URMA_USE_MOCK=true //test:brpc_unittests

# 使用真实 liburma；要求链接器可找到 -lurma
bazel build --define=BRPC_WITH_URMA=true \
    --define=BRPC_URMA_USE_MOCK=false //:brpc
```

Bazel 会自动获取固定 commit 的 UMDK 头文件，并通过 `@umdk//:urma_headers`
提供给 URMA 目标。设置 `BRPC_DOWNLOAD_URMA_HEADERS=false` 后，Bazel 不再依赖
该外部仓库；实际构建仍需通过工具链或本地 BUILD 目标声明
`urma_api.h`、`urma_types.h` 和 `urma_ubagg.h`。仅通过
`--copt=-I/path` 指向工作区外目录，在默认 Bazel 沙箱下不会把未声明的头文件
带入编译输入。设置
`BRPC_URMA_USE_MOCK=true` 强制加入 mock；设置为 `false` 则排除 mock 并链接
`liburma`。未指定时，URMA 构建默认使用 mock。

## 使用

通过在 channel / server 上设置 `socket_mode` 选择传输层：

```cpp
// 客户端
brpc::ChannelOptions opt;
opt.socket_mode = brpc::SOCKET_MODE_URMA;
opt.protocol = "baidu_std";   // URMA 仅支持 baidu_std
brpc::Channel channel;
channel.Init("127.0.0.1:8003", &opt);

// 服务端
brpc::ServerOptions sopt;
sopt.socket_mode = brpc::SOCKET_MODE_URMA;
server.Start(port, &sopt);
```

若对端不支持 URMA（例如 TCP 客户端连接 URMA 服务端），在 4 字节 magic
握手后透明回退到 TCP，应用代码无需改动。

## 跨节点正确性验证

`example/urma_performance` 的 CMake 构建新增 `urma_verify` 目标。
用 `URMA_USE_MOCK=OFF` 构建 brpc，再为示例指定对应的 `BRPC_LIB`、
`BRPC_INCLUDE_PATH`、真实 `URMA_LIB` 和 `TCMALLOC_LIB`。
可只向对端部署可执行文件及动态库，并用 `LD_LIBRARY_PATH` 指向部署目录。

```bash
# 服务端；两端都显式指定物理设备
./urma_verify --mode=server --port=18083 --urma_device=udmac0d1e2

# 客户端；替换 SERVER_IP
./urma_verify --server=SERVER_IP:18083 --urma_device=udmac0d1e2 \
  --requests=150 --threads=4 --urma_client_handshake_version=2

# 并发建链和回收
./urma_verify --server=SERVER_IP:18083 --urma_device=udmac0d1e2 \
  --requests=60 --threads=2 --connection_type=short

# 16 位序号回绕
./urma_verify --server=SERVER_IP:18083 --urma_device=udmac0d1e2 \
  --requests=70000 --sizes=64
```

校验程序对每条请求生成不同的附件内容，逐字节核对回显；服务端检查实际
Socket 的 URMA ESTABLISHED 状态，拒绝 TCP 回退。失败返回非零退出码。
`--sizes` 可配置尺寸列表，`--connection_type` 支持 single/pooled/short，
v3 握手使用 `--urma_client_handshake_version=3`。

2026-10-15 在两台 aarch64 节点、UMDK 25.12、`udmac0d1e2` CTP 模式及
tcmalloc 下完成 17 组校验，共 142,820 次 RPC，全部通过：

- 正反向收发，v2/v3 握手，附件覆盖 0–64KB 及 4032B/4033B 分片边界。
- 双 SGE、单 SGE，以及两种发送模式混用；接收均为双 SGE。
- 4 线程共用连接、并发 pooled 连接、并发 short 连接。
- 双/单 SGE 各 70,000 次连续 RPC，覆盖 16 位序号回绕。
- 默认 128 JFS / 64 JFR，以及缩小的 4 JFS / 2 JFR 配置。

验证中修正了 CTP 总长度限制和并发派生 Socket 共享 `UrmaConnect` 回调状态的问题。
两种构建各 31 项 URMA 单测及 23 项 Socket 回归测试通过。吞吐、延迟压测和
故障注入仍待后续验证。

## 架构与生命周期

`urma_resource_pool.{h,cpp}` 管理 `UrmaJfsSlot` / `UrmaJfrSlot`、独立
JFC/JFCE、SQ 原子容量预留、接收预投、import 缓存及清理。
`UrmaResourcePool::EventAdapter` 将 brpc Socket/epoll 接入与资源管理分开。
非可移动 slot 以稳定地址保存，公共 `UrmaIoSlot` 类型安全地区分 JFS/JFR；
pool 和事件 Socket 各持有引用，延迟到达的回调不会访问已释放的 slot。

`urma_memory_pool.{h,cpp}` 提供独立 `UrmaMemoryPool` 接口和默认的分片加锁
mmap 注册池。8KB 档供 IOBuf block 使用，64B 档单独供 header 使用。
可在初始化前通过 `SetUrmaMemoryPoolFactory` 替换分配器，或通过
`SetUrmaResourcePoolFactory` 替换资源池/选择策略。替代实现必须遵守
注册段与 DMA 生命周期契约，并使用相同的 slot 和完成上下文协议。

每个连接只持有 `UrmaConnResource`、双方 SocketId、发送序号和接收重排队列。
import 缓存按 `(EID, UASID, JFR id)` 去重；加入 UASID 防止不同地址空间冲突。
断链释放连接引用；在途 `TxCompletionCtx` 继续持有连接及其 import，直到 CQE。
pooled/short 派生 Socket 各自创建 `UrmaConnect`，不共享主 Socket 的握手回调状态。

### 建链和线格式

1. 初始化全局资源池并预投全部共享 JFR 接收 WR，随后建立 TCP 控制连接。
2. 从共享 JFR 池轮转选择本连接的接收 slot。
3. TCP hello 交换 EID、UASID、JFR id、完整 64 位 SocketId、接收容量和 segment。
   v2 仍用 `URMA`，body 从 82B 扩展到 90B，`impl_ver=2`；SocketId 位于末尾。
   v3 仍用 `URM3`，protobuf 字段 3 表示 JFR id，新增字段 12/13 为 SocketId/实现版本。
   旧 Jetty 格式不能互通；先读长度/版本前缀再决定读取 body，避免等待旧格式不存在的字节。
4. 保留 `import_seg` 先于 `import_jfr` 的 provider 路由准备顺序。segment 本阶段不用于 READ。
5. 客户端发送 4B 建链确认；服务端确认前可缓存已收到的有序数据，确认后才调用消息解析。

每条数据消息是 `UrmaMsgHeader(64B) + body`，头部包含网络字节序的
`dst_socket_id:uint64`、`seq:uint16`、`type:uint8=MSG_SMALL` 和 53 个零字节。
默认每个 WR 只发送一个 IOBuf block 的一段，所有 body SGEs 均由注册段支撑。
CTP 每条 SEND 的总长度上限为 4096B，包含 64B header，因此两种发送模式
均将 body 限制在 4032B 以内；较大的 IOBuf 会拆成多个带序号的 WR。
单 SGE 模式在注册块内拼接头和 body，并相应缩小该 WR 的 body 上限。
接收 SGE[0] 为独立 64B header，SGE[1] 为 IOBuf block data 区
（默认 8192 减去 IOBuf Block 元数据，通常为 8160B）。

### 收发和回收

- TX 从共享池 CAS 预留 SQ 容量，逐 WR 轮转选择 JFS。完整构造
  `TxCompletionCtx` 后 post；只有 post 成功才消费上层 IOBuf 和递增序号。
  默认只复制 IOBuf 引用；单 SGE 兼容模式复制字节。
- RX 的 `RxRecvCtx::data_iobuf` 持有 DMA 目标。按 header 的目标 SocketId
  查找 URMA transport/endpoint；错误或过期目标也回收 context 并补投。
- 重排按 `_deliver_base` 精确查找连续序号，正确处理 `65535 -> 0`。
  数据在重排完成后才进入 read_buf；重复/过期或超过半个 16 位序号空间的消息使连接失败。
- TX CQE 无论 SocketId 是否仍有效，都释放 header、IOBuf 引用和 SQ 预留。
  Reset 清理连接及重排状态，不重置共享 slot。
- GlobalRelease 先停止事件入口，删除硬件队列使 DMA 停止，再回收尚未完成的
  WR context，最后释放 JFC/JFCE。provider 删除失败时保留仍可能被 DMA 使用的资源。
  运行期间调用全局释放前应先停止业务收发和连接创建。
- 64B 池耗尽返回错误，不返回未注册的 malloc 缓冲区。普通 IOBuf 可以回退
  原分配器，但未注册的块不能直接作为 URMA SGE；失败不会消费调用方数据。
  IOBuf TLS 仍引用的 mmap 内存在初始化回滚时保留，避免晚析构访问已解除映射的内存。

### 设计 13 对应项

| 适配点 | 实现位置/行为 |
|---|---|
| 1 | `UrmaResourcePool` 与 `UrmaConnResource` 分离 |
| 2 | 初始化时 `AttachSlotSocket` 注册固定数量的事件入口 |
| 3 | `PostRecvToSlot` 固定 header/body 双 SGE |
| 4 | `ImportedJfrCache` 引用计数复用 import |
| 5 | `PollIoJfc` 按 slot 排空 CQ |
| 6 | 事件 Socket 的 user 为 `UrmaIoSlot` |
| 7 | JFS/JFR 各自独立 JFC/JFCE |
| 8 | 保留 segment 字段及导入顺序 |
| 9 | v2/v3 hello 交换完整 SocketId |
| 10 | 独立 64B 注册池与可替换内存接口 |
| 11 | 16 位 seq、重排 map、回绕和重复检查 |
| 12 | 断链后由 per-WR context 继续保活和回收 |
| 13 | Reset 只重置连接状态 |
| 14 | TX/RX context 内嵌 IOBuf，替代 per-connection 环形数组 |
| 15 | 停止事件、停止 DMA、回收 WR、释放全局池 |

## 配置

| Flag | 默认 | 用途 |
|---|---|---|
| `--urma_io_jfs_cnt` | 128 | 全局发送 slot 数 |
| `--urma_io_jfr_cnt` | 64 | 全局接收 slot 数 |
| `--urma_io_sq_depth` | 128 | 每个 JFS 深度 |
| `--urma_io_rx_depth` | 512 | 每个 JFR 深度 |
| `--urma_header_buf_count` | 65536 | 64B header 数量 |
| `--urma_buffer_size` | 8192 | IOBuf 注册块大小，至少 1024 且为 64 的倍数 |
| `--urma_buffer_count` | 48000 | 数据块数量 |
| `--urma_memory_pool_limit_mb` | 400 | data + header 注册池容量上限，单位 MiB |
| `--urma_cqe_poll_once` | 32 | 每次 poll 上限，实际限制在 1–32 |
| `--urma_recv_zerocopy` | true | 启用接收零拷贝 |
| `--urma_zerocopy_min_size` | 512 | 小于阈值的消息拷贝到读缓冲区 |
| `--urma_device` | 空 | 指定设备，否则选第一个 |
| `--urma_max_sge` | 0 | 设备能力上限覆盖；默认发送至少需要 2 |
| `--urma_client_handshake_version` | 2 | 二进制=2，protobuf=3 |

header 数量至少覆盖所有 RX 和 TX slot 深度之和，数据块数量须大于
预投 RX 总数，初始化时检查这些约束。默认预投 32768 个接收 WR。默认数据池为 393216000 字节，header 池为
4194304 字节，合计 397410304 字节（约 379.00 MiB）；超过配置上限时
初始化拒绝，容量不会随连接数增加。池元数据、tcmalloc 堆、Socket、线程栈、
驱动队列和应用数据不属于这项注册池预算，应另外记录进程 RSS。
旧的 `urma_sq_size`、`urma_rq_size`、`urma_prepared_jetty_cnt` 不再控制资源；
改用上表中的池化参数。`urma_disable_bthread` 已弃用，共享 CQ 总是将
RPC 回调交给 bthread，避免阻塞同一 JFR 上其他连接的接收补投。`urma_use_polling=true` 和 bonding 设备当前明确拒绝。

## 4 万连接压力验证

`urma_connection_stress` 与 `urma_verify` 一起构建，仍然只需向对端复制
可执行文件和动态库。tcmalloc 是必选依赖。先等待服务端输出 `STRESS_READY`，
再启动客户端；两端均显式指定物理设备。

```bash
# 多个监听端口属于同一个进程，共享同一组 JFS/JFR 和内存池。
./urma_connection_stress --mode=server --ports=18083,18084,18085,18086 \
  --urma_device=udmac0d1e2 --server_threads=16 --bthread_concurrency=16

./urma_connection_stress \
  --servers=SERVER_IP:18083,SERVER_IP:18084,SERVER_IP:18085,SERVER_IP:18086 \
  --urma_device=udmac0d1e2 --connections=40000 --concurrency=1024 \
  --rounds=20 --churn_percent=10 --idle_seconds=5 --bthread_concurrency=16
```

每个 Channel 使用独立 `connection_group`，所有轮次核对 40000 个互不重复的
服务端 SocketId，同时检查服务端实际连接计数。相同连接必须保持 SocketId，
被重建的连接必须获得新的 SocketId。预热之后每轮遍历全部连接，逐字节比较
64、4032、4096、8192、65536 字节附件，并拒绝 TCP 回退；每轮后关闭并重建
10% 的连接。RPC 重试禁用，首次错误返回非零退出码。

`connections` 是同时保留的连接数，`concurrency` 是有界的在途 RPC 数，
不为每条连接创建 OS 线程。结束时保留一条监控连接，确认其余连接在服务端
全部回收后再关闭，输出 `STRESS_CLEANUP_PASS` 和 `STRESS_PASS`。
`STRESS_RESOURCE` 分别记录 RSS、RSS 高水位、tcmalloc 已分配/保留堆、线程数
和 fd 数；空闲阶段不主动调用 tcmalloc 释放函数，以保留真实观测值。

测试只提高自身的 soft `RLIMIT_NOFILE` 至 `connections + 4096`，hard limit
不足则明确失败。单个目标端口受临时源端口范围限制；4 万连接至少使用两个
监听端口，反复重建时建议四个端口，避免 TIME_WAIT 占满端口。`servers` 中的
所有地址必须指向同一个服务端进程，否则无法进行 SocketId 和连接数校验。

注册池上限不是总进程 RSS 上限。内存及队列均有界，压力参数应结合包长和
在途请求数验证；4 万常驻连接不等同于 4 万个大 RPC 同时发送。当前没有
ACK/credit 流控，不能把有限压力测试通过解读为任意过载下无错误的保证。
接收错误、内存不足和连接故障会使请求明确失败，不允许静默数据损坏。

## 与 UBRing 协同

UrmaTransport 推荐用于**大包和跨节点**高吞吐路径，而 UBRing
（`SOCKET_MODE_UBRING`）对**小包和同机 IPC**最优（亚微秒、零系统调用）。
混合负载可按服务选择传输层：

| 场景 | 建议 `socket_mode` |
|------|---------------------|
| 同机 IPC | `SOCKET_MODE_UBRING` |
| 跨节点小包（< 64KB） | `SOCKET_MODE_UBRING`（UBS-Mem）或 `SOCKET_MODE_URMA` |
| 跨节点大包（>= 64KB） | `SOCKET_MODE_URMA` |
| 传统 RoCE/IB 数据中心 | `SOCKET_MODE_URMA` 或 `SOCKET_MODE_RDMA` |

单连接内按包大小自动分流的方案（路线 C 方案 B）作为后续演进方向。

## 限制

- 当前为 SEND/RECV 池化阶段，尚无对端 RQ credit/ACK。负载超过补投能力时可能
  出现 RNR/发送失败，不应视为已具备生产级拥塞和流控保障。
- 16 位序号要求乱序跨度小于 32768；没有跨完整序号周期消除歧义的信用窗口。
- mock 验证软件逻辑、拼接字节流及 CTP 4KB 总长度限制；实机正确性覆盖见上文。
  尚未执行跨节点吞吐/延迟压测和链路故障注入。

- 仅支持 `baidu_std` 协议（与 RDMA 一致）。SSL、RTMP、NSHEAD、MONGO 在
  `ContextInitOrDie` 阶段拒绝。
- 硬件数据通路需要受支持的 UMDK provider 和 `liburma`。
- 当前实现面向 Linux。
