# URMA 共享队列传输

UrmaTransport 使用 openEuler [UMDK](https://atomgit.com/openeuler/umdk) 的
URMA 接口承载 brpc 消息。当前数据通路采用可靠消息模式 `URMA_TM_RM`、
CTP 和独立的 JFS/JFR，通过共享硬件队列服务多个逻辑连接。

## 资源组织

`UrmaMemoryPool` 管理已注册内存，`UrmaResourcePool` 管理队列及其生命周期，
`UrmaEndpoint` 保存连接状态并处理分片、重排和消息交付。
三者通过内存池工厂、资源池工厂和事件适配接口连接。

JFS 是发送队列，JFR 是接收队列。每个 slot 独立持有 JFC、JFCE 和用于
事件分发的 Socket。进程默认创建 48 个 JFS、64 个 JFR，队列数量不随
逻辑连接数量线性增长。每个连接保存分配的本地 JFR、对端资源引用、
SocketId、收发序号以及重排状态，不单独创建一套硬件队列。

对端 JFR 导入缓存使用 EID、UASID 和 JFR ID 共同作为键，以引用计数
共享导入结果。连接销毁后，未完成发送仍持有相关资源引用，直到完成处理
释放它们。

## 注册内存池

默认内存池使用 mmap 分配连续内存并注册为 URMA segment，以固定大小的
块进行分配和回收。数据池的默认块大小为 8KiB，供 IOBuf 使用；可用数据
空间需扣除 IOBuf 的块头。独立 header 池的块大小为 64B，不经过 IOBuf
块分配器。

空闲块分为 64 个分片，每个分片分别加锁。分配从当前线程对应的分片开始，
必要时扫描其他分片。内存池初始化后不自动扩容；耗尽时返回失败。
注销注册内存前必须先结束 DMA。仍被应用 IOBuf 引用的数据块需要保留
其映射及回收器；即使 segment 已注销，也不能提前释放这些块。

`UrmaMemoryPool` 提供分配、回收、地址归属、segment 查询和注销接口。
可在全局初始化前通过 `SetUrmaMemoryPoolFactory` 更换实现，由全局初始化
创建并安装数据池与 header 池。替代实现必须保持内存注册及引用生命周期
约束。用户注册数据通过 `RegisterMemoryForUrma` 和
`DeregisterMemoryForUrma` 管理；发送期间不得注销仍被使用的内存。

## 建链与协商

连接先通过 TCP 完成握手，再切换到 URMA 数据通路。握手交换接收块大小、
接收队列信息、带版本的 SocketId、EID/UASID、传输路径类型和 segment
描述。客户端支持二进制 v2 和 Protobuf v3 两种握手编码，默认使用 v2。
共享队列协议要求实现版本 2，并校验远端资源及地址范围。

协商与资源导入完成后才发布 `URMA_ON` 状态。旧的、不含共享队列字段的
握手不能作为池化连接使用；不支持 URMA 的对端走现有协商失败及 TCP
回退流程。并发连接各自持有握手对象，避免复用仍在进行中的回调状态。

## 发送路径

每个 SEND 分片带有 64B header，包含目标 SocketId、16位序号和消息类型，
多字节字段使用网络字节序。CTP 的单次消息总长度不超过 4096B，因此
header 之后最多承载 4032B body；实际长度还受双方接收能力约束。

默认多 SGE 模式使用一个 header SGE，加上一个或多个 body SGE。
发送函数跨输入 IOBuf 及其数据块收集 body，直到达到总 SGE 上限或 body
长度上限。已注册 body 直接引用原内存，不为拼接报文复制整个 payload。
未注册的片段不能直接作为 DMA 源。

编译时启用 `URMA_SINGLE_SGE` 后，发送在一个注册块内原地构建 header，
并将 body 复制到其后，以一个 SGE 提交；输入片段数不再限制本次收集量。
两种模式使用相同线格式，接收均采用 header/body 两个 SGE。

JFS affinity 默认开启：连接首次发送时通过轮询选择首选 JFS，后续尽量
复用该队列；无空位时尝试其他共享 JFS。每个 WR 预留 SQ 槽位，提交失败
归还槽位，成功后由 TX CQE 回收。当前实现逐 WR 调用 post，并不构造
WR 链批量提交。

发送上下文保留 header、数据和连接资源引用。调用方数据只在提交成功后
消费。共享 SQ 的等待者在池级登记，任一队列释放空位时均可通知等待者，
并处理登记、通知和退出之间的竞争。

## 接收、重排与交付

每个 JFR 预投递多个接收上下文。接收完成后检查 header 和数据长度，
使用目标 SocketId 查找逻辑连接，再按分片序号恢复该连接的有序字节流。
顺序到达的分片走快速路径，不插入重排 map；乱序分片暂存，缺口补齐后
连续交付。序号处理支持16位回绕，重复或非法分片会使连接失败。

接收上下文及 header 可在补收时复用，已交给应用的 body 由 IOBuf 引用
保活。IOBuf 交接使用 movable 转移引用，避免按值浅拷贝带来的额外引用
维护；这不等同于复制 payload。

默认按一次 CQ poll 的结果收集需要解析的连接，完成该批次的接收处理后，
每个连接调用一次消息解析入口。此批次按连接合并解析调用，不改变消息
协议边界或同一连接内的字节顺序。

完成事件处理支持延后执行最后一条消息：先完成 CQ 事件交接并释放相关锁，
再允许整个交付范围内至多一条消息在当前 bthread 执行。其余消息通过
消息调度机制处理，避免业务回调持有共享队列锁。

## 并发与退出

slot 锁协调同一队列的 post、poll 和销毁，不在不同 slot 之间串行化。
发送与接收上下文的 pending 集合记录尚未回收的资源；Socket 和资源池
共同持有 slot 引用，防止排队的事件回调访问已释放对象。

关闭时先停止事件与队列活动，唤醒发送等待者，并按资源依赖释放本地队列、
完成上下文和导入资源。内存注销以 DMA 结束为前提，存储释放还需保证应用引用不再使用它；不能
为了回收对象而提前释放设备或应用仍可能访问的内存。

## 构建与启用

启用该模块需要 UMDK 头文件和 `liburma`。CMake 使用 `WITH_URMA=ON`，
优先查找安装的 SDK；找不到头文件时可下载固定版本的 UMDK 头文件，
`DOWNLOAD_URMA_HEADERS=OFF` 可禁止下载。可通过 `URMA_ROOT`、
`URMA_INCLUDE_PATH`、`URMA_BOND_INCLUDE_PATH` 和 `URMA_LIB` 指定 SDK。

Make 使用 `config_brpc.sh` 的 `--with-urma`，并通过 `--headers`、`--libs`
提供依赖路径。Bazel 使用 `BRPC_WITH_URMA=true`，链接真实的 `liburma`；
关闭 `BRPC_DOWNLOAD_URMA_HEADERS` 后，需要由工具链或本地依赖声明头文件。

CMake 的 `URMA_SINGLE_SGE` 与 Bazel 的 `BRPC_URMA_SINGLE_SGE` 控制发送
SGE 模式，默认关闭。该设置必须作用于 brpc 库，不能仅修改调用方。

客户端和服务端分别通过 `ChannelOptions::use_urma` 与
`ServerOptions::use_urma` 启用传输。当前支持 `baidu_std` 协议，使用
非 bonding 设备及事件驱动模式；不支持忙轮询、端到端 credit/ACK 流控，
也不提供大包单边 READ/WRITE。大于单次 CTP 上限的数据继续分片 SEND。

## 主要运行参数

| 参数 | 默认值 | 含义 |
| --- | --- | --- |
| `urma_device` | 空 | 指定设备；空值选择枚举到的首个设备 |
| `urma_io_jfs_cnt` | 48 | 进程共享发送队列数量 |
| `urma_io_jfr_cnt` | 64 | 进程共享接收队列数量 |
| `urma_io_sq_depth` | 128 | 每个发送队列的在途槽位数 |
| `urma_io_rx_depth` | 512 | 每个接收队列的预投递深度 |
| `urma_buffer_size` | 8192 | 数据池块大小，包含 IOBuf 块头 |
| `urma_max_sge` | 0 | 0采用设备能力；正值限制发送 SGE 数 |
| `urma_cqe_poll_once` | 32 | 每次 poll 最多取出的 CQE 数 |
| `urma_jfs_affinity` | true | 优先使用连接的首选 JFS |
| `urma_rx_batch_parse` | true | 同一 poll 中按连接合并解析调用 |
| `urma_inline_last` | true | 事件交接后执行延后的最后一条消息 |
| `urma_client_handshake_version` | 2 | 客户端握手编码，支持2或3 |

队列数量、深度与内存池参数应在全局初始化前确定。接收预投递需要充足
数据块，header 数量需要覆盖接收和发送在途槽位；初始化会校验组合是否
可用。硬件能力同样约束队列深度、SGE 数和消息大小。
