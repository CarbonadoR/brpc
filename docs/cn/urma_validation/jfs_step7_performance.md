# JFS/JFR 阶段7：Bazel 构建、性能对照与复测

本目录代码仅包含双边SEND；8K/100K通过CTP分片传输，不使用另一个代码仓的READ/WRITE大包路径。

## 构建

源码在Windows目录 `jfs_发送优化/brpc_bk` 修改后同步到145账号目录；实际构建使用ARM64 Bazel 7.4.1。`baseline/src` 是复用的增量构建目录；原始基线以冻结快照和baseline/packages为准。仓库 `.bazelversion` 原值7.2.1未改，复测本轮构建请显式调用7.4.1，确保与冻结基线相同。

```bash
cd /home/h00665128/jfs_send_opt_20261104/baseline/src
/home/h00665128/bazel-7.4.1-arm64 build --config=ub_test \
  --define BRPC_DOWNLOAD_URMA_HEADERS=false \
  --copt=-I/usr/include/ub/umdk/urma \
  --copt=-I/usr/local/include/ub/umdk/urma --jobs=16 \
  //example:ub_test_server //example:ub_test_client \
  //tools/jfs_validation:brpc_shared
```

`--config=ub_test` 明确opt、真实SDK、multi SGE及CPU frequency计时。本轮两版本均未开启额外RPC逐阶段打点或链长日志。SDK使用系统已安装的版本；不修改系统urma二进制、头文件或库。

首次下载外部依赖可在本地开启：

```bash
ssh -i /mnt/c/Users/hujia/.ssh/id_ed25519 -o IdentitiesOnly=yes \
  -N -R 127.0.0.1:17890:127.0.0.1:7890 h00665128@192.168.25.145
```

另一个终端在145构建shell设置 `HTTPS_PROXY=http://127.0.0.1:17890 HTTP_PROXY=http://127.0.0.1:17890`。7890必须是当前本地代理端口；已缓存依赖时不需要重新下载。

## 构建标识与二进制

- 冻结基线C++代码：`5277d87517ebb5e697abd61a3b492a859f86372f`。
- 新版C++代码：阶段6提交 `f5dca74d`；阶段7只补充复测工具、打包及本记录。
- 基线包 `ub_test_jfs_baseline_bazel_20261104.tar.gz`，新版包 `ub_test_jfs_lockfree_bazel_20261104.tar.gz`，均放在项目外层 `jfs_发送优化/测试/`。
- 两包包含客户端、服务端、libbrpc.so、依赖清单及SHA256SUMS。ub_test沿用默认静态链接BRPC，切换版本必须同时换两个可执行文件，不能仅替换libbrpc.so。
- 新包附带本阶段文档、阶段1–6验证记录及复测脚本。构建参数、Bazel版本、SDK依赖和逐文件源代码hash见provenance与manifest。

解压后先运行 `sha256sum -c SHA256SUMS`，再按README使用run_server.sh/run_client.sh。默认NUMA3、CPU288–303、udmac1d1e2；黄区需先核对拓扑并使用CPUS/NUMA/UB_DEVICE覆盖。

## 对照方法

145服务端、151客户端，两端NUMA3 CPU288–303、udmac1d1e2，event、multi；统一默认48JFS/SQ128、64JFR/RX512。进程总QD为上限，目标500QPS不代表始终保持QD个请求在途。所有多链路启用 `use_connection_group=true`。

三轮交替顺序：第一/三轮基线先运行，第二轮新版先运行。每个profile两版本相邻运行，固定场景、消息大小、核和设备。

| profile | 独立链路 | 总QD | QPS | 对称消息 | 每组时长 |
|---|---:|---:|---:|---|---:|
| low | 1 | 1 | 500 | 1K/8K/100K | 10s |
| common | 32 | 10 | 500 | 1K/8K/100K | 20s |
| contention | 32 | 128 | 0（不限） | 1K/8K | 20s |
| sparse | 1024 | 64 | 500 | 1K/8K | 20s |

WSL运行（工具也在新版包tools下，可用参数更换SSH账号/节点/key/CPU/设备）：

```bash
python3 tools/jfs_validation/compare_bazel.py \
  --baseline-bin /home/h00665128/jfs_send_opt_20261104/baseline/packages/ub_test_jfs_baseline_bazel_20261104/bin \
  --new-bin /home/h00665128/jfs_send_opt_20261104/final/packages/ub_test_jfs_lockfree_bazel_20261104/bin \
  --remote-root /home/h00665128/jfs_send_opt_20261104/compare_retest \
  --output /tmp/jfs_compare_retest --rounds 3
```

remote-root/output必须使用新目录，避免覆盖基线和上次数据。baseline-bin/new-bin指向服务端节点中的目录，工具自动将匹配客户端同步到另一端。只测部分profile可添加 `--profiles low common`；单次自定义场景使用 `run_pair.py --help`。

原始数据在项目外层 `performance_results/20261104_JFS_JFR_Bazel对照/`：settings.json记录参数，r1/r2/r3保留命令与两端日志，measurements.json含每轮均值/P50/P99/QPS/错误及CPU，对比汇总.md给出三轮算术平均。P99列是三轮P99均值，不是合并请求样本的P99。

CPU采集窗口包含预热/连接建立和部分退出处理，100%为一个逻辑CPU；尤其1024连接场景不能将此CPU值直接解释为正式压测阶段稳态CPU。短smoke用例时延输出0不参与性能统计。链长及提交次数诊断见阶段3日志，正式性能运行关闭逐次日志。

时延统计口径：客户端默认 `latency_window_seconds=10`，结束时的Avg-Latency、P50、P99取最近10秒的bvar窗口，受约1秒采样边界影响；20秒测试也不代表20秒全部请求的总体均值。QPS、完成数及错误数使用整个正式压测阶段。各轮时延为整数µs，三轮汇总小数仅为算术平均精度。两版本使用相同口径。

## 环境异常与继续补跑

本轮在冻结基线也遇到进程退出时 `synchronize_srcu` 等待：151客户端PID1201661已完成正式RPC后无法及时退出，随后自行恢复；145服务端PID1544173的一个worker等待SRCU且未释放监听端口，后续基线启动PID1546874也进入同类等待。没有修改系统配置、驱动或重启机器；仅凭wchan不能确认具体fd/驱动函数。

失败/部分完成组保留于 `*_incomplete*` 及runner日志，排除出对照汇总。补跑使用新远端目录；更换端口不能解决设备初始化等待。环境恢复后，可对原output添加 `--resume`，并使用新的remote-root；可用 `--port` 指定独立端口。脚本跳过全部场景已成功完成的组，保留每次补跑设置，不覆盖旧数据。

采集脚本把客户端stdout直接保存在远端，超时时不无限等待D状态进程，也会记录客户端PID和已有日志。这不能解除内核等待；退出异常的测试组仍不视为成功。

<!-- RESULTS_BEGIN -->
## 三轮结果

三轮均已完成，共60条完整场景结果，正式RPC合计100,354,793次，RPC错误均为0。第三轮在环境内核等待解除后补齐；异常/不完整组已排除，日志仍保留。

纳入轮次：r1, r2, r3；完整场景结果60条。异常/未完成组不纳入。

单位µs；每格为多轮算术平均。P99列是各轮P99的平均，不是合并样本P99。
时延为ub_test结束时最近10秒窗口；QPS/错误为正式压测全阶段。
CPU按单核100%计，窗口含初始化/预热；原始日志保留每轮结果。

| 场景 | 基线均值 | 新版均值 | 差值(新版-基线) | 基线P99 | 新版P99 | 基线/新版QPS |
|---|---:|---:|---:|---:|---:|---:|
| common / s102400_links32_qd10 | 347.000 | 304.333 | -42.667 | 409.000 | 371.000 | 500.2 / 500.1 |
| common / s1024_links32_qd10 | 53.333 | 50.000 | -3.333 | 88.000 | 77.000 | 500.2 / 500.2 |
| common / s8192_links32_qd10 | 60.000 | 64.000 | +4.000 | 86.667 | 91.333 | 500.2 / 500.1 |
| contention / s1024_links32_qd128 | 249.667 | 233.667 | -16.000 | 433.000 | 417.667 | 499703.0 / 531096.3 |
| contention / s8192_links32_qd128 | 387.333 | 371.333 | -16.000 | 676.667 | 658.333 | 310924.8 / 323794.9 |
| low / s102400_links1_qd1 | 174.667 | 148.333 | -26.333 | 209.333 | 172.333 | 499.6 / 499.3 |
| low / s1024_links1_qd1 | 37.000 | 34.667 | -2.333 | 97.667 | 59.667 | 499.6 / 499.6 |
| low / s8192_links1_qd1 | 42.000 | 43.000 | +1.000 | 85.000 | 66.000 | 499.6 / 499.6 |
| sparse / s1024_links1024_qd64 | 50.000 | 49.000 | -1.000 | 73.000 | 70.333 | 500.7 / 500.7 |
| sparse / s8192_links1024_qd64 | 61.333 | 64.667 | +3.333 | 85.000 | 95.333 | 500.7 / 500.7 |

| 场景 | 基线客户端CPU% | 新版客户端CPU% | 基线服务端CPU% | 新版服务端CPU% |
|---|---:|---:|---:|---:|
| common / s102400_links32_qd10 | 14.70 | 10.89 | 12.46 | 7.44 |
| common / s1024_links32_qd10 | 3.32 | 3.30 | 1.12 | 1.07 |
| common / s8192_links32_qd10 | 3.89 | 3.77 | 1.56 | 1.40 |
| contention / s1024_links32_qd128 | 710.72 | 746.22 | 781.34 | 769.42 |
| contention / s8192_links32_qd128 | 784.27 | 794.37 | 959.82 | 886.87 |
| low / s102400_links1_qd1 | 8.55 | 7.14 | 5.42 | 3.65 |
| low / s1024_links1_qd1 | 3.74 | 3.79 | 0.88 | 0.91 |
| low / s8192_links1_qd1 | 4.06 | 4.07 | 1.19 | 1.10 |
| sparse / s1024_links1024_qd64 | 3.59 | 3.61 | 90.50 | 89.88 |
| sparse / s8192_links1024_qd64 | 4.19 | 4.13 | 90.74 | 90.59 |

100K及共享提交吞吐改善；8K时延没有改善，部分多链路场景略有回退。本轮组合测试没有将原因进一步拆到准备/分配/原子操作，不能把小幅差异直接归因某一项。
<!-- RESULTS_END -->
