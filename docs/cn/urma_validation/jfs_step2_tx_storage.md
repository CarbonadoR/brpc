# JFS/JFR 优化阶段 2：稳定描述符与 TX 生命周期

## 修改

- 每个 TxCompletionCtx 内持有稳定 WR 和可内嵌 16 项的 SGE 存储，不再把异步排队需要的描述符留在调用栈上。
- 提交者持有一个引用；post 前增加预期 CQE 引用。成功提交后提交者释放自己的引用，完成处理释放 CQE 引用；post 失败归还预期 CQE 引用后交给调用方处理。
- 即使 CQE 在 PostSend 包装调用返回前已被处理，提交者仍保护 context、SGE、源数据及连接资源；本阶段保留原 slot mutex 和单 WR post，MPSC/32 WR 批次在阶段 3 接入。
- 增加 sq_device_inflight；post 前加、CQE 减、post 未接受时回退。sq_inflight 继续统计接纳量。
- 新增真实双机验证工具 tools/jfs_validation/run_pair.py，保存命令和两端日志；每次使用独立二进制副本和运行目录，只停止本工具启动的服务端。

## 验证

- Bazel mock：59 项全部通过。新增稳定 WR/SGE 地址、计数归零、提前 CQE 后提交者仍可访问数据两项测试。
- Bazel 真实 SDK multi 版本编译通过。
- 145 服务端、151 客户端，设备 udmac1d1e2，NUMA3/CPU 288–303，multi/event。
- 对称 1K、8K、100K，请求和响应大小相同；每个大小分别 1/32 条独立链路，进程 QD10、目标 500 QPS、50 iterations：6 组均正常退出，RPC 错误为 0。
- 结果位于外层 `测试/阶段2_真机验证重试_20261104/`，包含 exact commands/results.json 和每组两端日志。短时 smoke 的 0 时延不作为性能结论。

首次真机新进程在内核 close() 的 synchronize_srcu 等待点暂时进入 D 状态；未修改基线也出现同样现象。未重启或更改系统配置，随后等待点恢复，重试完成全部 6 组。初版工具用 TCP 连接探测 readiness 导致服务端出现探测连接 EOF 日志，已改为 ss 监听检查，不将该探测日志计作 RPC 错误。

复测命令（在 WSL 仓库目录执行；路径必须是新的运行/输出目录）：

```bash
python3 tools/jfs_validation/run_pair.py \
  --binary-dir /home/h00665128/jfs_send_opt_20261104/baseline/src/bazel-bin/example \
  --remote-root /home/h00665128/jfs_send_opt_20261104/step2_replay \
  --output /tmp/jfs_step2_replay \
  --sizes 1024 8192 102400 --links 1 32 --qd 10 --iterations 50 --seconds 0
```

远端 `baseline/src` 为增量构建工作副本；未优化版本以已冻结的源码归档和二进制包为准，不再从这个工作副本重建基线。
