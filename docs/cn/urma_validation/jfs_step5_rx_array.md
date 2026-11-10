# JFR 阶段 5：固定 RX 数组与 slot 去锁

每个 JFR 初始化一次 `rx_depth` 大小的 `RxRecvCtx[]`，数组中保存header、data IOBuf和posted状态。user_ctx仍指向固定数组元素；范围及对齐检查取代set查找。CQ消费者处理一个完成时归还posted计数，补投成功后再增加；`rx_posted` 与配置深度之差就是实际缺额。

移除JFR的pending set及共享基类slot mutex。JFS/JFR均使用关闭位+活动计数屏障保护销毁；正常RX解析和补投由该JFC的唯一消费者执行。内存池、import cache、Socket重排锁及每JFS等待登记锁仍各自保护不同共享对象，不在本轮去除。

补投ENOMEM时记录JFR编号、实际posted/配置深度和errno，保留数组及未投递元素，继续交付已收到的数据，也允许其他接收槽继续工作。不自动补齐缺额；后台按容量补投是后续扩展。其他提供者post错误仍停止JFR并报告错误。初始化无法完整预投时初始化失败。

删除JFR成功之后才释放整个RX数组；删除失败保留数组，防止DMA访问已释放存储。

Bazel mock 68项通过。验证固定元素在多轮复制/零拷贝接收中地址不变、历史IOBuf不会被覆盖；ENOMEM后数据仍交付且深度准确减1；其余元素继续收发；关闭等待正在补投的CQ操作退出才释放数组。原有event回调、inline-last、重排及真实RPC握手测试继续通过。

真机验证：145服务端、151客户端；NUMA3 CPU288–303、udmac1d1e2、event/multi，2JFS/SQ64、2JFR/RX512；1K/8K/100K、1/32链路、QD32、50iterations。原始命令与结果在项目外层 `测试/阶段5_真机验证_20261104/`，短测试不作为性能结论。
