# Review comment：请先评审“sink 内聚、最小框架改动”的收敛方案

> 状态更新：设计已接受，当前规范为[定稿v1.0](/Users/jiangjianyuan/work/git/pingcap/ticdc/docs/design/databend-native-sink-design-review.md)，以bend-cdc既有正确性为基线。以下是历史review comment，其中manifest/K-H-S候选及raw-change=true已由当前基线迁移决策替代，不是第二套实施要求；继续评审请使用[当前任务说明](/Users/jiangjianyuan/work/git/pingcap/ticdc/docs/design/databend-native-sink-review-prompt.md)。方向裁决不代表实现/实验通过。

## 结论与评审约束

v0.2 对数据正确性的讨论有价值，但新增 collector delivery/信用协议、progress 迁移和跨 capture 账本，会把“新增一个 Databend sink”扩大成 TiCDC 框架改造，不利于合入。本轮请优先评审下面的收敛方案，**不要默认通过增加框架协议来修复每个反例**。

这是针对 v0.2 的架构调整提议，不是已实现或已验证的 v0.3。原报告尚未整体替换为本文；有冲突处作为本轮待裁决事项，不能把两套协议同时当实施要求。

固定源码基线：

- TiCDC：`2bf6dacd0aec9ac2fad464600e47341f9aa951f2`，仓库 `/Users/jiangjianyuan/work/git/pingcap/ticdc`。
- bend-cdc：`b6481621021e82aee659ad88e8e6c00f6dd11906`，仓库 `/Users/jiangjianyuan/work/git/pingcap/webapi` 下的 `bend-cdc`。
- 当前 checkout 不代表上述新架构版本，请用固定 Git 对象复核，不切分支。

## 1. 建议保留的工作流程

```text
现有 dispatcher 交付原生 DML/DDL
  → sink 内取 TableInfo、主键、commit_ts、before/after
  → 统一类型转换与内部行记录
  → 有界内存缓冲 + 限额本地 spool
  → 同表/同 schema 攒批，不同表有限并行
  → 批量 INSERT 持久化 raw staging（带 batch_id / 事件身份）
  → MERGE INTO 目标表
  → 分片应用成功后独立释放本地资源
  → 事件 SEALED 且全部分片完成后，现有 flush callback 恰执行一次
  → TiCDC 原有机制推进 checkpoint
```

- 保留内存+磁盘 spool，不改回纯内存；raw 写成功不能确认源事件。
- 同表 raw INSERT、MERGE、DDL、compaction 首版串行，避免先引入高并发覆盖协议。
- 保留 `OPEN → SEALED → COMPLETED` 和已应用分片独立回收，不为等事件全集确认而钉住全部 spool。
- Databend 内部固定 `raw-change=true`，只增加必要的初始化/注册接入；普通二级 UK 更新必须正确。真实 PK/handle 变更仍需源事件实验，不把这个开关当万能解决方案。

## 2. 首版考虑不以 stream 作为应用/确认依据

建议评审一个更小的替代：**按明确 batch_id 从 raw 读取本批数据进行 MERGE，不依赖 stream 的消费 cursor 判断完成。** Stream 不作为首版必要依赖，后续若成本实测证明有必要，再单独引入。

这样可以移除本轮“stream hint 只消费了 F1，却给 F1/F2 写了 witness”的故障路径；也不再需要为 stream 构造跨层消费证明系统。但必须注意：

1. batch_id 是下游写入批次身份，不是源业务版本；本次逻辑批次的重试复用身份，重启重新分批仍需按事件身份/commit_ts 幂等收敛。
2. 每个批次写 raw 可拆多个 SQL；全部待应用记录明确写入成功后才执行 MERGE。不能用 INSERT/MERGE 返回的操作行数充当事件成员覆盖证明。
3. 不能简单把本批旧值直接覆盖目标。候选思路是：取本批涉及主键，结合同代际、合法 schema 范围的 raw 历史，重算各键最高有效版本并实际应用，包含 DELETE 墓碑。
4. 不能只因 raw 有更高版本就跳过并确认旧事件；更高版本也可能尚未应用。若采用上述重算，应真的将最高版本纳入当前 MERGE，而非仅用它过滤旧行。
5. 相同 key/TSO 的冲突、跨 schema 历史、同键乱序/重放、未完成批次、compaction 都必须有明确行为；SQL 与快照一致性仍待验证。
6. 若某批只部分可应用，未覆盖分片不能确认。`batch_id + MERGE 成功`不是自动正确性证明，必须说明 SQL 来源集合完整、无意外过滤/限制，以及对应 callback 的成员集合。

请比较这个直接批次方案与 stream 方案的正确性、SQL复杂度和扫描成本。**去掉 stream 可能增加 raw 扫描，不能预先认定一定更便宜；本提议也不视为关闭所有覆盖风险。**

## 3. 不改 collector/dynstream/TableProgress 的入口处理

删除 v0.2 新增的 delivery 重投、transport epoch 重绑、progress blocker 迁移等定制协议，优先沿用现有 AddDMLEvent / PostEnqueue / PostFlush 生命周期。

- 已被 sink 接管的数据由 sink 生命周期管理，不因 collector 的 transport session reset 直接丢弃。源重放由现有框架处理，sink 负责幂等；不能伪造 PostFlush 清 blocker。
- sink 的入口队列、编码窗口、spool 和元数据必须有界。PostEnqueue 按真实接管/现有表路径背压语义触发，不提前虚报。
- 不允许在 AddDMLEvent 中无限等待磁盘并占住共享处理线程。无法在有界入口内接管时，首版可通过既有 sink 错误路径停止任务，不无限排队、静默丢弃或自行搬迁 progress；需要复核现有错误路径确实会完成必要的生命周期清理。
- 上述取舍不承诺“只靠 sink 就能硬限制全 TiCDC RSS”，也不承诺资源耗尽时其他表完全不受影响。原生大 chunk 已在 sink 前分配的问题要明确限制/测量，不靠文档声称消失。

请给出“不新增 delivery 协议”的最小调用时序，核查 session reset、接管失败、任务停止/重启时是否仍会遗留 blocker 或漏事件。如果原框架存在无法规避的问题，先给具体反例和最小补丁位置，不能直接恢复整套信用协议。

## 4. DDL、恢复及容量控制的简化边界

- 建表、类型映射、raw/MERGE、白名单 DDL、DDL journal 与 TRUNCATE 恢复保留在 sink。使用已有 DDL 屏障，不重做全局调度；历史空表可由独立初始化预建，不为此新增框架 schema 服务。
- 保留 TRUNCATE 的持久化身份/代际和“APPLIED 前不开新代际”的恢复顺序；不能为了减少代码去掉必要的破坏性操作幂等保护。
- 提交未知时不确认，不继续同表下一批；先核验旧 SQL 终态。驱动取消返回 nil、进程退出、固定等待时间均不是终态证明。
- 正常迁移/异常接管仍需排除旧在途 SQL；across-nodes=false 不等于禁用自动 HA。若只能依赖大规模框架变更才可生产安全，首版应明确缩为隔离、受控、需人工恢复的 PoC，不虚报自动 HA。
- 远端 raw/tombstone 增长仍需监控、阈值停写和扩容/重初始化流程。但首版不为此默认新增跨 capture 强一致预算服务；严格 feed 总预算应另评估静态配额切分/已有外部配额。无法保证时，明确是监控型停止阈值而非全局原子硬额度，不能对外承诺严格封顶。

## 5. 允许与不允许的修改范围

| 范围 | 本轮要求 |
|---|---|
| 新 Databend sink 包 | 事件适配、类型/SQL、buffer/spool、batch worker、tracker、DDL、恢复、错误与指标 |
| scheme/type/config/factory/metrics | 必要注册与参数校验 |
| raw-change模式、schema store、recovery接入 | 允许有限分支/小接口，需列出确切触点及原因，不能伪装成MySQL sink |
| collector/dynstream/TableProgress/调度核心 | 不做通用协议改造；如发现确实阻塞，先提交最小必要变更论证 |

“主要改在 sink”不等于强求外部零改动；但新增 sink 不应变成 TiCDC 全链路流控/调度重构。

## 6. 请 reviewer 给出明确裁决

请重点回答：

1. 不新增 delivery/信用协议，复用原有接口能否做到不丢数据、正常完成、资源失败可诊断？列出最小必要的外部改动。
2. 直接 batch_id + raw MERGE 能否比 stream/witness 方案明显简单？请给出候选 SQL/成员映射及会击穿它的最小反例，不只说“需验证”。
3. 旧事件重放、不同 batch 重复、最新墓碑、同TSO合法更新、跨schema恢复是否仍正确？
4. raw扫描/历史保留成本是否会抵消去掉中间链路的收益，最小实验怎么设计？
5. 哪些风险必须在sink中解决，哪些需要小接入补丁，哪些只能通过明确缩减首版范围处理？

输出：**接受 / 修改后接受 / 拒绝**；每个发现写证据、触发时序、影响、最小修正、验证方法，并标注“sink内修改 / 小接入修改 / 超出本轮范围”。不要为了给通过而补隐含前提，也不要只为追求理想化全局保障无限扩大框架范围。

本轮仅评审，未实现该收敛方案，未运行真实 Databend/新 sink 故障实验。不要修改代码、切分支、创建任务或执行生产操作。
