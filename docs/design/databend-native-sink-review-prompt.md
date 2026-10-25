# 定稿后的实现评审入口：按 bend-cdc 既有语义迁移

用户已确认设计评审通过，[定稿v1.2](databend-native-sink-design-review.md)按v0.4.1范围进入 `feat/databend-native-sink` 分支实现。后续优先评审实际代码与该定稿的偏差，不重新扩展通用协议；当前实现状态见设计§8.6及官方迁移报告；[旧基线验收报告](./databend-native-sink-e2e-report.md)只记录当时的9组PASS、32列及排空重启，不自动计为官方基线通过。单测与受控链路通过不等于在途故障/自动HA验收。

最新补充：[bend-cdc能力对照与真实故障验收](./databend-native-sink-parity-fault-report.md)。重点看已验证的迁移差异，不把旧版“warning跳过”算已支持；非空唯一键fallback已验证，参数化字符widening局部修复见源码。

最新用户裁决已实现：URI `conflict-key`、指定RENAME/DROP DATABASE warning跳过；真实3组通过，见补充报告§8。此前对应“仍拒绝/未迁移”的结论是历史状态；分区保护未开放。

当前基线已纠正为官方仓库；旧2bf6dacd分支及旧验收为历史。请先读[官方基线迁移报告](./databend-native-sink-upstream-port.md)，不要把tiflow备份分支当当前实现。

## 基线与范围

- 新架构TiCDC：`/Users/jiangjianyuan/work/git/pingcap/ticdc-official`，官方 `pingcap/ticdc/master`，固定commit `b9179182f3c1c5db5a018029f41eb07eab804051`，当前目录 `ticdc-official`。
- bend-cdc：`/Users/jiangjianyuan/work/git/pingcap/webapi` 下 `bend-cdc`，固定commit `b6481621021e82aee659ad88e8e6c00f6dd11906`。
- 设计基线按固定Git对象核验；实现位于上述新分支，以实际分支/工作区代码为准，不切分支或覆盖开发中的改动。
- 用户要求：只对齐bend-cdc当前TiDB增量路径已正确支持的语义，核心改动集中sink，不为迁移重做TiCDC框架。
- [v0.3](databend-native-sink-design-review-v0.3.md)、[v0.2](databend-native-sink-design-review-v0.2.md)以及旧review comment都是历史讨论；manifest/K-H-S/AppliedCycle/新raw_generation不是当前要求。

## 唯一主线

```text
原生事件adapter（不经Canal编解码）
 → 有界内存/限额spool与按表worker
 → 迁移现有Databend raw写入、Flush/MERGE、compaction
 → 目标完成后回调；SEALED分片计数独立回收
```

复用现有raw字段、复合PK、TSO排序、墓碑、stream/raw source、schema epoch和TRUNCATE顺序，不默认增加per-row manifest读回、event_id/payload_hash、DML ledger、分布式锁/预算、collector信用/progress迁移。

普通DML无需逐成员回读，但多条raw SQL必须全部成功且Flush覆盖对应工作后才确认。**首版显式固定raw source，不自动创建/消费stream**；stream代码可保留，启用前验证batch limit/no-op与代数记账覆盖，不能循环pending已清零的旧Flush冒充排空。full-raw扫描和大量保留键重复更新成本仍要测。

## 本轮六项明确决策

1. 首版沿用固定CloudStorage默认拆分模式`raw-change=false`，撤销强制true的接入；保留scanner的DELETE/延后INSERT顺序。非聚簇PK变键要真实验证；若意外收到oldPK!=newPK的原始UPDATE则拒绝，不直接交After-only核心。默认源码不等于实际旧任务配置，后者须另核验。
2. Canal原始值入口与NativeNormalizer规范行入口分离，汇合后共享raw写入/MERGE；规范化恰一次，不伪造Source绕过解码/丢失TSO。数字label ENUM('2','x')必须测试。
3. 默认raw source解决首版部分stream消费前提；writeGen/mergedGen仍不是独立覆盖证明，不因“沿用”就跳过T3。
4. 在客户端/核心**内层重试之前**传播COMMITTED/NOT_COMMITTED/UNKNOWN；UNKNOWN不得被executed分支吞成成功或transient分支重发，外层也不能再重试。
5. spool成功回调列表为空，源callback只在SEALED tracker；取消/Discard/Release不能间接完成源事件。enqueue前拆分/拒绝oversized，不接受后再声称有界。
6. add_time先按真实存储精度量化再保证单调；检验SQL参数/落库，而非只测Go内存递增；不把它提升为任意源冲突的业务版本。

schema映射固定：raw event_pos=CommitTs，实际DDL epoch=FinishedTs，TableInfoVersion只解释schema。迁移外层writeBatch的旧事件过滤与pending阻新数据；bootstrap不得凭空保存applied epoch。

不做Dumpling/CSV/COPY INTO、全量编排或历史空表预建。单writer/无外部写的运行前提必须成立；不因来源相同就假设旧SQL、原生session/callback与S3文件恢复完全相同。

## 当前实现与证据边界

运行时代码集中在 `/Users/jiangjianyuan/work/git/pingcap/ticdc/downstreamadapter/sink/databend/`。当前已经有native mapper、HTTP终态/参数绑定、按表lane、共享spool与回调、schema epoch恢复，以及factory接入；不是只有接口草图。

先读定稿§8.2–8.3/§10：受控PoC明确拒绝分区dispatcher/复杂DDL，未验证自动HA；新journal附带必要的post-DDL定义与步骤，不自动接管旧bend-cdc目标。local/race/lint/build通过不代表真实Databend SQL和完整控制面恢复通过。请特别核对源码已有的scope限制、边界输入与SQL实际语义，避免把HTTP fixture当成数据库实现。

## 请按“是否退化”评审，不默认增加理想化要求

每个发现请标注：

1. **迁移退化**：bend-cdc已正确处理，而当前native/spool/callback适配会破坏；必须修。
2. **接入差异**：去掉S3或使用TiCDC原生生命周期后新增的必要责任；给最小sink/接入补丁。
3. **既有缺口**：旧代码也存在假设或潜在bug；明确证据及是否在支持场景下会造成错误，不能盲抄，也不能默认为迁移要重建整个系统。
4. **增强需求**：比基线更强的HA/跨表原子性/全局硬预算等，默认后移，不作为新增sink首版合入要求。

优先检查：

- native与Canal转换后的内部行差分：复合键、二进制、ENUM/SET、decimal/JSON/时间、同TSO合法变化、UK更新。
- 现有WriteToRawTable/Flush/mergeTable/writeGen/mergedGen是否足以绑定完成的callback；stream部分消费时是否可能错误确认，最小修正是什么。
- raw子SQL部分成功、MERGE提交未知、callback前重启后重放；不新增ledger能否在受控排他前提下收敛。
- 现有schema epoch的数值含义与原生FinishedTs/表版本是否正确映射，pending/applied与TRUNCATE重放是否会退化；不能用新raw_generation替代必要源码审查。
- spool正常压力/封口/回收、transport reset不丢已接管数据、fatal结束旧生命周期；既有框架接入若不够，给确切最小修改，不恢复整套delivery协议。
- 现有warning跳过的DDL不算完整支持；已知问题不能因“等价迁移”被宣称正确，支持范围之外不无限扩展。
- 对相同SLO/负载比较raw source与stream、旧链与新sink的扫描和总成本；不预设任一方案更便宜。

## 输出

总体：接受 / 修改后接受 / 拒绝。

每项：证据commit/定位、基线与新方案差异、最小触发时序、影响、最小修复、验证断言；标注上述四种类型以及“sink内 / 小接入 / 超出范围”。按正文T1–T7提出具体验收，不把旧本地测试通过当新sink通过。

本轮只读评审；不要修改文件/代码、切分支、创建任务或执行生产操作。
