# JFS/JFR 优化阶段 1：Bazel 基线

日期：2026-11-04。基线源代码 HEAD：`5277d87517ebb5e697abd61a3b492a859f86372f`。
工作区忽略行尾后的 diff 为空；远端构建副本文本由 CRLF 转为 LF，避免 shell genrule 的 here-document 失败。运行时源代码未修改。

## 构建

145（aarch64）使用 Bazel 7.4.1：

```bash
/home/h00665128/bazel-7.4.1-arm64 build --config=ub_test \
  --define BRPC_DOWNLOAD_URMA_HEADERS=false \
  --copt=-I/usr/include/ub/umdk/urma \
  --copt=-I/usr/local/include/ub/umdk/urma --jobs=16 \
  //example:ub_test_server //example:ub_test_client \
  //tools/jfs_validation:brpc_shared
```

`ub_test` 配置使用 opt、真实 liburma、multi SGE、CPU frequency 计时。客户端/服务端保留仓库默认静态链接 brpc 的方式，另构建配套 `libbrpc.so`。不能通过替换该 so 改变已静态链接的 ub_test 行为。

新增的 `tools/jfs_validation` 仅提供共享库构建目标和打包工具，不修改数据路径。打包工具记录依赖、Bazel module lock、构建参数、源码校验和运行脚本。

## 产物

- 项目外层：`jfs_发送优化/测试/ub_test_jfs_baseline_bazel_20261104.tar.gz`。
- 压缩包大小：16,737,394 字节。
- SHA256：`6f1116edb6f0137b3e7d58db5ba3849992e97e2f74e9b43d5303cc27e5201ad2`。
- 原始源码归档：`测试/基线_20261104_bazel/source_snapshot.tar.gz`，SHA256 为 `99e7ecf427e439fc2dea823cfd66059f5a502406f3cd5a33b0e2fcedb77d6c57`。
- 构建和验证日志位于 `测试/基线_20261104_bazel/`。二进制和大日志不纳入源码提交。

## 已完成验证

- 真实 URMA 构建：客户端、服务端及共享库均通过。
- 145/151 均确认 NUMA3 包含 CPU 288–303，设备 `udmac1d1e2` 可用。
- 服务端日志确认 multi SGE、CTP、priority 6、48 JFS/64 JFR。
- 151 解包后 14 项校验全部通过；系统依赖可解析。
- 145 服务端、151 客户端，NUMA3/288–303，1K 请求响应，QD1，目标 500 QPS，50 iterations smoke：退出成功，错误为 0。程序汇总报告 51 次请求，含测试框架额外计数；运行不足采样周期，时延显示 0，不能作为性能数据。
- Bazel `//test:brpc_urma_unittest`：57 项测试全部通过，真实链接时切换为 mock 配置，日志已归档。
- 32 条独立 connection_group 链路全部建立，进程 QD10、50 iterations smoke，程序最终报告 60 次请求、错误为 0；并发请求收尾可能超过目标次数。
- 初次 1K 对称请求响应、1 链路、QD1、500 QPS、10 秒：实际 499.5 QPS，平均 36 µs、P50 33 µs、P99 119 µs，错误为 0。这是单轮记录，不是三轮对比结论；程序 CPU 汇总为 0，不用作有效 CPU 测量。

mock 命令：

```bash
/home/h00665128/bazel-7.4.1-arm64 test -c opt \
  --define BRPC_WITH_URMA=true --define BRPC_URMA_USE_MOCK=true \
  --define BRPC_DOWNLOAD_URMA_HEADERS=false \
  --copt=-I/usr/include/ub/umdk/urma \
  --copt=-I/usr/local/include/ub/umdk/urma --jobs=16 \
  //test:brpc_urma_unittest
```

## SDK 合约与后续验证点

仓库 `umdk/src/urma/hw/udma/udma_u_jfs.c` 的 `udma_u_post_sq_wr()` 逐个遍历 WR 链：首个失败 WR 写入 `bad_wr`，之前的成功前缀仍执行 doorbell；`lock_free=false` 时使用 provider 内部 spinlock。`udma_set_sqe()` 在 SQ overflow 时返回 `URMA_ENOMEM`。

不能只按 ENOMEM 名称认定所有失败都是可重试 SQ 满；实现阶段必须结合已接受前缀、真实设备计数和 provider 状态判断。该 provider 的 WQEBB 核算随 SGE/inline 形式变化，后续需在小 SQ/高 SGE 用例验证项目的 WR 配额限制与 provider 的真实接纳能力。

当前代码使用第一个可用 EID 条目创建 context，无显式 eid_idx 配置；基线保留该行为。多链路压力需设置 `--use_connection_group=true`，否则多个 Channel 可能共享底层连接。

本阶段没有修改 WR/context/owner/选路逻辑；后续按方案逐阶段实现，每步独立验证后提交。
