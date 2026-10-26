# RFC：TiCDC 原生 Databend Lake Warehouse Sink 详细设计与评审报告

| 项目 | 内容 |
|---|---|
| 文档版本 | v0.2，2026-10-24，吸收两份独立评审后的协议修订版 |
| 决策状态 | Proposed；存在生产阻塞项，不是 Ready for Production |
| 实现状态 | 仅设计文档；未新增 sink、未提交、未部署 |
| 用户已确定要求 | 原生 TiCDC sink；迁移 bend-cdc 核心能力；批量写 staging/MERGE；第一版必须有内存 + 限额本地磁盘 spool + 背压 |
| 分析对象 | Databend SQL warehouse；不泛指 Iceberg/Delta/Hudi 文件格式 |
| 主动评审目标 | 找出丢数据、状态回退、错误确认、DDL 重放、双写、内存/磁盘失控及不可恢复场景 |

> 阅读提醒：本文将“源码事实”“拟定设计”“待验证假设”分开描述。章节完整不代表所有协议已证明。第 21 节列出阻塞项；任何 agent 都不应把它们自动视为已解决。

## 0. 评审导航与版本固定

### 0.1 两份源码基线

| 对象 | 本地仓库 | 固定 commit |
|---|---|---|
| bend-cdc | `/Users/jiangjianyuan/work/git/pingcap/webapi`，子目录 bend-cdc | `b6481621021e82aee659ad88e8e6c00f6dd11906` |
| 新架构 TiCDC | `/Users/jiangjianyuan/work/git/pingcap/ticdc` | `2bf6dacd0aec9ac2fad464600e47341f9aa951f2` |

TiCDC remote 是 `git@github.com:sdojjy/tiflow.git`；被分析的新架构分支名为 `feat/create-paused-changefeed`。这是 fork 分支，不是上游最新主干，也没有证据证明它就是线上部署版本。

**当前 checkout 已切换**：本报告编写时，本地 TiCDC 是旧架构 `master` / `03ed2432bd7782198742d3aa20e07edcc47097e6`。因此新架构代码一律通过 `git show` 读取固定 commit；没有切分支，没有覆盖其他工作。不能在当前 HEAD 下搜索不到新接口，就认定报告引用错误；也不能拿旧架构接口直接实现本文。

复核方法（只读，无需 checkout）：

```bash
git -C /Users/jiangjianyuan/work/git/pingcap/ticdc show \
  2bf6dacd0aec9ac2fad464600e47341f9aa951f2:downstreamadapter/sink/sink.go
git -C /Users/jiangjianyuan/work/git/pingcap/ticdc show \
  2bf6dacd0aec9ac2fad464600e47341f9aa951f2:pkg/sink/spool/spool.go
git -C /Users/jiangjianyuan/work/git/pingcap/webapi show \
  b6481621021e82aee659ad88e8e6c00f6dd11906:bend-cdc/pkg/sink/databend/databend_cdc.go
```

### 0.2 推荐阅读顺序

1. 目标与非目标（§1–2）。
2. 安全不变量、事件与表状态机（§4–5）。
3. 内存/磁盘预算和攒批（§6–7）。
4. MERGE/恢复/DDL/所有权（§8–12）。
5. 类型、配置、运行保障（§13–17）。
6. 测试、上线、阻塞项、评审问题（§18–23）。

评审结论应给出具体执行时序和反例，不只判断“总体可行”。文末 TiCDC 源码索引用 T 编号，bend-cdc 用 C 编号，阻塞项用 B 编号，三者不混用。TiCDC 链接固定 commit；bend-cdc 本机链接须配合本节固定 commit 复核。

### 0.3 v0.2 评审处置与变更摘要

本轮只修订设计，不把新增协议描述当作实现/实验成功。两份评审中有价值的反例均进入验收；对无证据的事实断言和不充分修复，作如下限定：

| 评审主题 | 处置 | 本版对应内容 |
|---|---|---|
| PK不变、二级UK更新也会拆成同TSO DELETE/INSERT | 接受；是普通UPDATE的支持契约问题，不只是PK变键 | §11.4，固定raw-change模式；B05 |
| DDL不能套用DML事务fence；KillQuery返回nil不是终态 | 接受 | §9.4、§12.2分mutation矩阵；B01/B03 |
| 增量分片提前确认/整事件回收死锁 | 接受 | §5.2、§7.4；新增B17 |
| 入口共享线程与共享chunk绕过预算 | 接受，需要跨层接入，不是只改sink | §6.7；新增B16 |
| raw/tombstone在维护成功时仍随历史键增长 | 接受 | §9.6；新增B18 |
| no-op MERGE必然不消费stream，是规范要求 | 不接受其普遍性断言；存在版本相关已知修复，必须测实际SQL/版本 | §9.7；B02 |
| enable-table-across-nodes=false等于禁用HA | 不接受；它限制拆表分布，不阻止整表迁移/接管 | §12.5；T15 |
| 固定sleep、进程panic或取消成功能保证旧SQL终止 | 不接受；只能在可信终态/真正fence后放行 | §12.2、§12.5 |
| Segment pinning、stream失效、bootstrap竞态完全遗漏 | 原版已识别但不可验收，细化并分别跟踪 | §6.8、§9.8、§10.6；B13–B15 |
| 必须用事务DAG归约/唯一约束journal/标记行推进stream | 不预设实现成立；先验证源净事件、目标能力与原子性 | §10–12；不盲选SQL tie-break/无界事务缓存 |

新阻塞项编号保留评审建议的B13–B15，另外增加B16入口信用、B17完成封口、B18远端容量。既有B01–B12不重编号；“设计已明确”与“实现/实验已关闭”分别记录。

## 1. 背景、目标与成功标准

### 1.1 当前链路及代价

```text
TiDB → TiCDC 原生事件 → Canal JSON 编码 → S3/OSS 文件
     → bend-cdc LIST/GET/SQS → Canal JSON 解码
     → 元数据/类型处理 → Databend raw → stream → MERGE → target
```

中间链路有编码/解码、对象请求、文件扫描、网络流量及独立消费进程成本。它也提供较长的中间缓冲、审计与回放窗口；去掉对象存储不等于只删成本而不损失能力。

### 1.2 目标链路

```text
TiDB → TiCDC 原生 DMLEvent / DDLEvent
     → 事件适配/元数据/有界 admission
     → 内存热缓冲 → 必要时溢写限额本地 spool
     → 同表同 schema epoch 攒批 → Databend raw INSERT
     → append-only stream → MERGE → 目标表可见
     → 释放覆盖的 spool + 完成 flush callback → checkpoint 可推进
```

目标是：保留核心同步正确性，降低增量中间链路成本；不是完全消除 JSON、磁盘或 Databend MERGE。第一版 raw_data 仍可用 JSON 作为受控行载体，但不再使用 Canal JSON envelope 传输。

### 1.3 成功标准

- 最终状态与源在同一已完成逻辑水位上一致；包括 DELETE、不发生旧数据复活。
- source-to-target 按 at-least-once 传输，重复与重启后幂等收敛。
- 新增可确认进度必须有目标应用成功的证据；正常路径要求MERGE成功，合法幂等no-op要求同等级的已支配/消费证据（§9.7），绝不以仅raw提交代替。
- 支持权威建表、复合主键、批量 staging、核心类型映射和明确白名单 DDL。
- 进程内存、spool 磁盘、节点总预算有界；满载后背压而不是无界积压。
- 达到双方约定的吞吐、target-applied lag 和恢复时间；数值待压测/产品确认，不预设达标。
- 固定相同数据量、下游和延迟 SLO 后，总成本实测低于旧链；不预先承诺节省百分比。

## 2. 范围、非目标与第一版支持矩阵

### 2.1 本设计包含

原生增量 sink、连接与鉴权、schema bootstrap、类型转换、内存/磁盘 spool、批处理、raw/stream/MERGE、checkpoint 回调、DDL 屏障与恢复、所有权/接管、配额、监控、灰度和回退。

### 2.2 非目标

- 不把 TiCDC 改成 MySQL/Postgres/MongoDB 通用捕获器。
- 不把新 sink 当成自动全量导出器；全量初始化继续独立执行。
- 不保证跨表或跨物理片段的源事务原子可见，也不保证读取者获得源数据库级一致快照。
- 不承诺复现所有 TiDB/MySQL 约束、索引、collation、默认表达式和生成列计算行为。
- 不支持外部应用同时改目标表/内部 raw/stream/journal。
- 不以本地 spool 提供节点永久丢失后的恢复能力；它不是 WAL。
- 不支持无限期下游停机、无限 raw 历史或无限磁盘预算。

### 2.3 第一版边界

| 能力 | 第一版目标 | 约束 |
|---|---|---|
| 非分区表，非空单列/复合 PK | 支持 | PK 列保留于完整 after image，明确 before image 契约 |
| 无 PK/nullable UK/force-replicate | 拒绝 | 不自动换用隐式 handle，也不静默 append-only |
| 分区表/table-across-nodes | 拒绝 | 未解决 logical table owner 汇聚前不开放 |
| 同逻辑目标表一个 owner | 必须 | 正常调度与异常接管都需成立 |
| PK不变、仅二级UK值更新 | 必须支持 | 固定raw-change事件模式，禁止继承默认拆分模式；见§11.4 |
| 主键值更新 | 验证门槛，未验收前不承诺支持 | 必须证明入口净变化、旧键删除与重放；不能假设所有变键都以UPDATE暴露而承诺可检测拒绝 |
| 主键定义改变 | 拒绝 | 改 PK 列集合/类型可能重定义 row identity |
| 历史空表自动建表 | 必须提供路径 | start-ts schema bootstrap 或全量初始化预建 |
| 列 DDL | 白名单 | ADD/DROP/RENAME 非键列，有限 MODIFY |
| TRUNCATE | 核心对齐项 | table incarnation 与中断恢复验收前不能上线 |
| DROP/RENAME TABLE/SCHEMA、复杂分区 DDL | 初期阻塞 | 不以 warning 代替正确处理 |
| 二级索引 DDL | 有条件 no-op | 已证明不改变行身份/投影后才确认 |
| 不支持的同步选项 | 创建时拒绝 | 如未经设计的 active-active、BDR、syncpoint，不能悄悄忽略 |

“第一版目标”不等于实现完成；若某核心项仍未验证，只能发布带明确限制的 PoC，不能对外声称 bend-cdc 核心能力全量对齐。

## 3. 代码现状与迁移策略

### 3.1 bend-cdc 实际入口

事实：TiDB 路径是 `TiDBRunner → TiDBCDCManager → DatabendMergeSink → DatabendSink/CDCManager`（C01–C03）。不是简单 `DatabendSink.WriteBatch` 的固定 id INSERT/DELETE 实现。

| 已有职责 | 迁移策略 |
|---|---|
| 文件 discovery、SQS、Canal 解码、S3 key checkpoint | 删除这层职责，使用 TiCDC 原生事件与上游进度 |
| 每表 PK 推导、目标路由、epoch 管理 | 保留语义，从 TableInfo/DDL 获取权威数据 |
| raw 参数化 INSERT、stream、MERGE | 迁移核心算法，不照搬 goroutine/循环生命周期 |
| 乱序抑制、最新水位、DELETE tombstone | 必须保留 |
| 类型转换、binary/ENUM/SET/JSON 测试 | 保留测试语义，重写原生 datum 表示 |
| schema 白名单和 TRUNCATE | 保留并补充 TiCDC 原生恢复协议 |
| 每表连接池/后台循环、全局 Background context | 不照搬；改为受限共享资源与可取消生命周期 |
| Dumpling/CSV/COPY INTO | 独立初始化路径，接口共享 mapper，任务生命周期不并入 sink |

事实：raw 是普通持久化表，附带 append-only stream；不是临时内存表。严格顺序模式查询 retained raw history；compaction 保留每个键最新版本，删除也保留墓碑（C03）。因此 raw 容量可能随**历史键集合**增长，而不只是当前目标行数。

### 3.2 TiCDC 现有接入点

T01 的 Sink 接口支持 AddDMLEvent、FlushDMLBeforeBlock、WriteBlockEvent、SetWriteGate、Run、Close。T02 提供 CommitTs、TableInfo、before/after 和 enqueue/flush 分离回调。

不能仅改 factory：需要 scheme/config、SinkType、TableSchemaStore、dispatcher、恢复、metrics 和测试等配套。冒充 MysqlSinkType 会触发 `*mysql.Sink` 强转（T07），不可采用。

### 3.3 复用现有 S3 spool 的边界

固定新架构基线已有 spool（T03–T05），具备内存热数据、segment 溢写、磁盘配额等待、释放和回调拆分。拟复用/提取通用能力，不直接复用 Canal 编码器或 S3 payload builder。

已发现必须审查的差异：

1. `TryEnqueue` 对单 entry 大于磁盘配额有“内存接受后立即刷”的例外，不能当严格内存上限。
2. `prepareWorkDir` 启动清空所属目录，证明这不是重启恢复 WAL。
3. `acceptEntryLocked` 在溢写时先序列化 blob，存在额外瞬时内存；必须计入预算。
4. S3 `buildPayload` 使用 bytes.Buffer 拼完整上传批次；新 sink 不能照搬后声称磁盘回读完全流式。
5. 基线 S3 路径有 unlimited channel 等结构；不应复制到要求硬预算的新 sink。
6. 现有 spool 预算不是现成的节点所有 changefeed 总预算，更不是全部磁盘消费者的全局预算。

## 4. 设计原则与安全不变量

| 编号 | 必须始终成立的约束 |
|---|---|
| I1 | 未有目标应用成功或合法幂等no-op完成证据的事件不得触发flush callback；仅raw提交、结果行/计数不构成证据 |
| I2 | 同事件重放不能使目标版本回退、重复插入业务行或复活已删除键 |
| I3 | 同逻辑目标表只有一个有效 merge/DDL owner；锁不能只存在单进程 |
| I4 | DDL fence 前的 DML 已应用；fence 后 DML 不使用旧投影；旧 DML 重放不能污染新 schema |
| I5 | collector交付/共享chunk、spool、队列、编码、回读、元数据预算有界；磁盘有单任务和节点总预算；远端raw有停写阈值 |
| I6 | 正在等待配额的生产者不能阻止负责 MERGE/回收的消费者前进 |
| I7 | 未物理释放的 segment 空间不能从磁盘预算中提前扣掉 |
| I8 | 失租/停止后的异步工作不能继续执行新的下游变更；在途旧请求必须被接管协议覆盖 |
| I9 | 最新键水位与 DELETE tombstone 不能被普通 TTL 清除 |
| I10 | schema bootstrap/version 与“某 DDL 前数据已应用”的 durable fence 不能混为一谈 |
| I11 | metrics/result parsing 失败不能被简单当成业务未提交；提交不确定也不能被简单当成功 |
| I12 | 所有“不支持”功能可诊断地拒绝/阻塞，不用静默降级代替正确性 |

## 5. 组件划分与状态机

### 5.1 组件

```text
TiCDC dispatcher
  │ DMLEvent/DDL
  ▼
DatabendSink adapter
  ├─ capability/config validation
  ├─ schema/route registry
  ├─ budgeted admission + typed row normalizer
  ├─ hot memory + disk spool + transaction completion tracker
  ├─ per-table lane scheduler
  │    └─ raw INSERT → MERGE → covered-batch completion
  ├─ DDL barrier/recovery coordinator
  └─ target client + write gate + owner fencing adapter

Capture-wide BudgetManager / free-space guard
  └─ 所有 Databend changefeed 共享，若与 S3/redo 共盘还需联合规划
```

候选代码目录：

- `/Users/jiangjianyuan/work/git/pingcap/ticdc/downstreamadapter/sink/databend/`：原生事件、callback、lane、barrier、生命周期。
- `/Users/jiangjianyuan/work/git/pingcap/ticdc/pkg/sink/databend/`：配置、客户端、mapper、SQL、journal、恢复。
- `/Users/jiangjianyuan/work/git/pingcap/ticdc/pkg/sink/spool/`：仅必要的通用预算与存储扩展，避免破坏 S3 行为。
- `/Users/jiangjianyuan/work/git/pingcap/ticdc/tests/integration_tests/databend_sink/`：真实链路验证。

以上是新架构上的拟定目录，不表示当前旧架构 HEAD 已有这些代码。

### 5.2 事件/分片状态机

事件完成与分片生命周期必须分开，不再把“事件ACK后才释放全部entry”当顺序要求：

```text
事件tracker：OPEN → SEALED → COMPLETED
            └────────────→ FAILED / CANCELLED（未完成不确认）
分片：REGISTERED → STAGED → RAW_COMMITTED → APPLYING → APPLIED → RECLAIMED
                                            └→ COMMIT_UNKNOWN → RECONCILING
APPLIED分片可独立回收；其源事件仍可处于OPEN或SEALED
```

APPLIED表示目标已包含该分片，或按§9.7证明它被已应用版本正确支配；不是只看到raw中有更高版本。回收失败不撤销APPLIED、不重复MERGE，但空间继续计费。

tracker协议（拟实现，非现有DMLEvent保证）：

1. 单一编码producer持有OPEN状态；每个分片必须先原子登记、增加registered/pending，再发布给消费者。发布失败也不能把它伪装成已应用，需重试该分片或置FAILED。
2. 只有编码遍历完整个源事件、没有尚未登记的行，才可Seal；Seal与失败/完成检查在同一状态同步域内。若先前分片均已完成但producer还OPEN，仍不得确认。
3. 确认条件为 `SEALED && pending == 0 && !failed && CAS(completed,false,true)`；Seal与最后一个Apply竞争时由CAS保证只触发一次。零分片也必须Seal，过滤语义通过后才完成。
4. 每个分片APPLIED转换也必须一次性；仅递减pending一次，并独立释放payload。只保留未完成分片的小描述符和有界tracker，不永久保存所有已回收分片的位图；每事件/每lane限制未完成窗口。
5. DetachPostCallbacks不捕获DMLEvent，但其flush闭包自身没有once保护，只有enqueue有CAS（T02）；由tracker提供flush的一次性。失败/取消后迟到完成只能清理资源，不能把未完成事件改成成功。
6. 一个大事件可边编码边应用/回收，累计编码量可以大于spool额度；并发驻留量、源chunk与元数据仍受§6约束。不能靠一次性复制整事件换取封口。

反例验收：暂停F2登记，让F1完成，此时flush次数必须为0；随后登记F2、Seal、应用F2后恰为1。再测试大于可用spool的事件、重复完成、Seal/Apply并发、发布失败与取消。这里的“事件”是TiCDC完成单位，不承诺跨表事务原子性。

### 5.3 表状态机

```text
DISCOVERED → BOOTSTRAPPING → ACTIVE
ACTIVE → DRAINING_BEFORE_DDL → DDL_PENDING → DDL_APPLIED → ACTIVE
任意状态 → FENCED / STOPPING / FAILED
重启：RECOVERING → 验证 owner/schema/journal/未完成 raw → ACTIVE
```

只有 ACTIVE 接受正常新 epoch DML；旧版本重放走受控验证/跳过流程，不以普通写入路径解释。

v0.2补充暂停/恢复转移，避免把所有阻塞都混成retry：

| 状态 | 可继续的工作 | 恢复ACTIVE的必要条件 |
|---|---|---|
| LOCAL_RESOURCE_BLOCKED | 已接管分片的编码窗口/应用/回收与控制，受保留池约束 | 物理配额/信用回到低水位，未丢唤醒且无fatal IO |
| REMOTE_CAPACITY_BLOCKED | 已raw提交coverage的应用、安全维护与故障协调 | 扩额或安全清理后的新鲜测量+增长预留通过；旧chunk不能无界排队 |
| COMMIT_UNKNOWN | 同lane仅执行权威终态查询/协调 | 服务端提交/回滚证据及coverage核验；不能仅等到timeout |
| HANDOFF_BLOCKED / FENCED | 终态核验、释放自有资源；无新业务mutation | 新有效owner取得mutation排他，完整恢复后走RECOVERING |
| RECOVERY_REQUIRED | 按证据执行授权恢复，正常admission关闭 | journal/ledger/raw/stream/binding一致且unknown均已处置 |
| REINITIALIZATION_REQUIRED | 隔离旧目标、显式新快照初始化，不在旧lane继续写 | 新基线/新内部对象校验与显式切换；不是直接改回ACTIVE |

多个阻塞原因并存时保存原因集合；释放一个配额不能清掉COMMIT_UNKNOWN或DDL fence。RESOURCE_BLOCKED是本地资源阻塞的通称，对外状态使用LOCAL_RESOURCE_BLOCKED区分远端容量。进程失败/取消仍遵守§15，不因解除某一局部阻塞自动恢复写入。

## 6. 内存、磁盘 spool 与预算

### 6.1 为什么第一版必须有 spool

这是已确定需求。热数据留内存，下游抖动或待 MERGE 数据增加时溢写磁盘；磁盘不是无限缓存。防止写爆依靠**硬预算 + 可用空间保护 + 背压**，不是“用了磁盘就安全”。

### 6.2 预算对象

定义：

- `M_hot`：spool 内存 payload。
- `M_encode`：规范化、序列化和参数构造临时内存。
- `M_read`：磁盘回读和 SQL 批次缓冲。
- `M_meta`：entry、callback、表状态和事务 tracker。
- `M_in`：尚未转交/释放的源 chunk 和入口队列。
- `D_live`：segment 实际占用，包括已逻辑释放但尚未回收部分。
- `D_reserved`：在途写预留，包括头部、对齐、允许的分配开销。

要求 admission 预留预算后才接受自有数据；不能只统计 M_hot 然后无限持有 M_in/M_meta。进程 RSS 还包括 TiCDC 其他组件与 Go/runtime 开销，应单独观测，不能保证等于 sink 配额。

单任务 `D_live + D_reserved ≤ D_feed`，所有任务共享 `ΣD ≤ D_capture`。每个批次先占有预算，再读回/编码；并发 worker 数乘以最大批次不能超过 M_read 总池。

### 6.3 磁盘 admission 算法

```text
1. 估计/增量计算 entry 的编码及落盘字节，申请内存/磁盘预算。
2. 在 capture 共享锁下预留预算；不能持锁做网络 IO。
3. 留内存或 append segment；失败回滚尚未使用的预留。
4. 对实际文件分配/长度进行结算，超出预留视为实现错误或停止 admission。
5. 额度不足：通知压力调度；等待仅发生在有信用约束的异步任务，不阻塞共享dynstream处理线程（§6.7）。
6. 删除/截断成功后才释放实际磁盘额度并唤醒等待者。
```

文件系统 guard 要考虑：statfs 可用空间、配置的保留量、未落盘预留、segment 分配开销及 inode。不同实例共享预算时必须原子预留；其他进程写同一文件系统无法由本组件完全控制，ENOSPC 仍需容错，不能承诺绝不发生。

建议 dedicated spool volume 或明确与 eventstore/redo/log 的共盘预算。仅限制 Databend spool 不会限制 TiCDC 其他组件的磁盘增长。

### 6.4 队列、callback 与超大事务

- 所有数据队列按**字节与元素数**同时限额；每个小事件也占 M_meta，避免零小 payload 绕过限制。
- admission 前不能无界 detach/复制所有 chunk；增量编码和所有权转移需有明确时机。
- callback 不得捕获整块原始 DMLEvent/chunk；使用独立 completion tracker（参考 T02），但要证明所有数据引用已解除。
- 大事务按行/字节拆分；原子性不扩大为整个源事务，多片 MERGE 期间目标可能看到部分事务，这是公开限制。
- 单行大于支持上限时终止该表/changefeed并保留错误，不采用任意内存直通。
- 最大并行事务 tracker 与等待确认 entry 数另有限制，防止“数据落磁盘了，callback 仍把内存占满”。

### 6.5 高低水位和死锁规避

达到高水位时暂停新 admission，低水位后恢复，避免频繁抖动。具体水位按有效内存+磁盘预算定义，不盲用 S3 的比例。

MERGE、清理、lease/DDL 控制必须有独立可运行资源。配额等待者不得占住唯一 worker、表锁或整个 executor。额度不足可强制提前 MERGE，忽略通常的 merge-trigger-rows/interval；否则“额度满但没达到 MERGE 阈值”会永久等待。

### 6.6 文件生命周期与重启

- 目录按 sink-kind、capture/进程实例、changefeed 实例隔离；启动只能清理已确认非活跃的自身旧目录。
- orphan 目录即使不用于恢复，也必须计入磁盘扫描与回收流程，不能因进程换 ID 每次留下 10 GiB。
- MERGE 后 entry 逻辑释放不保证整个 segment 可删除，必须处理长寿命 entry 造成空间钉住；优先缩短 segment、密度控制或安全复制存活 entry。
- 本地文件不要求承担 crash durability；重启仍从 checkpoint 重放。是否 fsync、何时 fsync要按临时 spool 语义确定，不能给出 WAL 级承诺。
- checksum/长度校验发现运行中 spool 损坏：停止、不确认，从 checkpoint 恢复；不跳过坏记录继续。

### 6.7 collector→dispatcher→sink入口信用协议

**已确认接入事实**：BasicDispatcher同步调用AddDMLEvent，dynstream线程同步等待Handle返回；collector在调用HandleEvents之前已执行BatchDMLEvent.AssembleRows，多个DMLEvent可共享chunk（T12–T14）。因此原接口本身不能证明本设计的端到端有界性。

拟新增的能力协议如下，名称是概念接口，不表示当前代码已有：

```text
ReceiveEnvelope(deliveryID, path, sessionEpoch, wireBytes, rowCount, decodedBound)
  → collector TryReserveDelivery(bytes, elements, decodeExpansion)
      无信用：block该path并保留最多一个已计费delivery；返回共享线程
      有信用：受限executor解码/AssembleRows，附SharedChunkLease
  → dispatcher一次登记该delivery的全部tableProgress blocker
  → sink TryTransfer(deliveryID, lease, events)
      接管：转移有界入口队列的所有权，快速返回；异步增量编码
      暂停：delivery仍归上层，block该path；异步信用通知后重投同delivery
  → 行数据复制到有预算的内部格式后逐步解除chunk引用
  → 最后一个使用者释放SharedChunkLease；归还decoded预算
```

必须实现的细则：

- 在线路收包/排队阶段就限制最大消息字节、元素数和同时在途delivery，不能等AssembleRows后才算账。发送端按事件边界/行数/字节切batch，协议协商硬上限；解码膨胀按保守上界预留并增量校验，超限拒绝/重建会话，不继续分配。
- 传输编码大小不等于chunk内存。SharedChunkLease按实际分配容量保守计费**一次**，覆盖所有DMLEvent视图；每个事件视图和callback再计M_meta。不通过“这个事件只用了一行”只计那一行。
- 原始chunk生命周期由lease所有者管理，sink不能擅自把共享Rows清空而破坏同batch其他视图。编码worker复制受限行窗口后归还引用；最后一个视图释放前全chunk仍计费。
- 信用是额度凭证，不是“承诺之后归还”的无界债务。collector/dynstream已有内存控制若可复用，必须证明同一buffer单一拥有/跨层转移，无遗漏、无双计；接入前做分配链路审计。
- TryTransfer未接管时不Detach回调、不发PostEnqueue，也不重复注册tableProgress。用稳定deliveryID和sessionEpoch记状态：RECEIVED/PROGRESS_REGISTERED/TRANSFERRED；重投仅重试转移。所有事件先加blocker再允许任何一个完成，沿用基线防checkpoint跳跃约束。
- AddDMLEvent旧签名不能表达拒收/稍后重试。新增可选AdmissionSink capability和dispatcher/collector分支（或等价显式接口）；旧sink走原路径。不能仅改Databend.Add内部实现后宣称协议已接通。
- 每path最多一个已交付等待batch，跨path有字节/数量总上限；阻塞的是该path，不是共享handler线程。只有在sink入口lease和元数据已接管后才可PostEnqueue；它仍不表示应用成功。
- data信用与控制事件/DDL/lease通知分池，并为B表保留最小公平信用。只有A达到每path上限、全局仍有保留额度时，保证B可推进；全局预算耗尽时允许整个feed背压，但MERGE/回收/控制必须可运行。不承诺在全节点资源耗尽时无限保障B的新DML。
- 释放信用时先完成可重入的状态更新，再通知可重试path，避免丢唤醒；session失效的旧delivery只释放，不能重投新owner。

`max-delivery-bytes`包括已解码chunk上界而非仅wire长度，`max-delivery-events`限制小事件元数据。若现有eventservice不能拆分单个过大原生事件，必须增加有序分片/完成标记协议或明确在接收前拒绝该输入；不能因为源事务大而无界AssembleRows。`max-row-bytes`不能代替这些上限。

本节扩大了原方案范围：B16未实现并通过共享线程公平性与真实内存实验前，不能宣称128/256MiB示例是端到端内存硬界。大源事务可支持为多个有界delivery，不要求所有源事务进一个chunk。

### 6.8 Segment pinning与压力排空协议

保留物理字节计费，不以逻辑Release数估算空间。拟给segment维护有界的存活引用→lane反向索引；压力时：

1. BudgetManager以pressureEpoch合并通知，唤醒所有持有受压segment引用的lane，而不只唤醒最新申请者。
2. lane跳过通常行数/时间阈值，刷出当前可应用分片；OPEN大事件的已应用片也可回收。DDL/COMMIT_UNKNOWN阻塞的lane记录原因，不能为释放空间越过barrier。
3. 独立M_read/控制池执行排空；数据admission不能占用这部分保留量。按最老entry和公平轮转调度，避免热点饿死冷表。
4. 首版优先验收“强制排空+物理回收”，不把segment搬迁当现成解法。若仍有长寿命pin且有必要搬迁，需预留maintenance disk/memory，先复制并校验、原子切换引用、等待读者退出，再删旧segment；复制期间两份都计费。不能在配额已满时临时多写一份。
5. 不能排空/搬迁时进入RESOURCE_BLOCKED并告警，冻结新输入；通过恢复故障或人工扩额解除，不删除存活数据。可诊断的下游阻塞不等于无条件保证活性。

验收需覆盖热表A与冷表B、同segment混排、额度满但未达MERGE阈值、坏segment/删除失败、搬迁额度不足，以及大事件OPEN期间空间循环。原版已有时间触发，故不能把“冷表永远等5万行”作为既定事实；测试的是压力传播和保留资源是否真正有效。

## 7. 攒批、调度和确认语义

### 7.1 两层批处理

第一层按**目标表 + incarnation + schema version**攒 raw INSERT；任一条件触发：行数、编码字节、最长等待、DDL barrier、配额压力、优雅停止。

第二层合并若干 raw 批次再 MERGE；行数/时间/压力/barrier 均可触发。计时从最老未完成记录/批次入队起算，不能每次新事件到来都重置导致小表永不刷出。

上次示例 5000 行、8 MiB、1s raw flush，50000 行或 5s MERGE，只是压测起点，不是默认 SLO。真实延迟还包括排队、网络、SQL、重试、调度和 checkpoint 广播。

### 7.2 第一版采用保守的表级串行 lane

拟定决策：同一逻辑目标表在一个 merge cycle 内，先提交选中的 raw 批次，再停止该表新的 raw INSERT，完成 MERGE及必要状态确认后，开启下一 cycle。新的源事件仍可进入受配额控制的 spool。

这样避免“MERGE 固定 stream 快照后，另一 raw writer 插入更高版本，被全历史过滤看到但并未一起应用”的复杂竞态。不同表有限并行。该决策以吞吐换证明简单，后续若并发 raw 与 MERGE，必须另写 snapshot/coverage 协议，不能直接去掉互斥。

多个 DMLEvent 可以共用一个 cycle；一个大事件也可跨 cycle，但回调等所有片段 APPLIED。数据 replay 事件可以被更新版本覆盖，不等于必须逐个中间状态呈现在目标。

### 7.3 覆盖集合与回调

每个cycle固定本地 `coverage = {entry/fragment IDs}`，其raw INSERT已明确成功，且不混入尚未应用DDL的schema。MERGE终态与消费覆盖证明满足§9后才将分片APPLIED；事件是否可回调还须§5.2的SEALED条件。

本地 generation/entry ID 是覆盖记账工具，不是持久化恢复身份，也不能当作 TiDB row version。进程重启后重新从 source checkpoint 构造完成关系。

`PostEnqueue` 只表示 sink 接管数据且满足背压策略，不能用于推进 durable checkpoint。`PostFlush` 才表示 target applied。沿用 detach 回调时要审查每个 hook 对动态流唤醒、流控与内存释放的影响。

### 7.4 释放与回收顺序

下游应用/消费已确认 → 分片一次性APPLIED → 归还其payload/entry并触发物理回收 → 更新tracker pending；满足SEALED完成条件后独立执行事件callback。上述状态更新需有同步与可重入保证；不要求磁盘unlink成功才回调，但未释放物理空间仍计费。

关键区别：F1已应用时即使事件仍OPEN、F2未登记或未应用，也可回收F1。tracker只保留已完成计数/有界未完成集合，不捕获F1数据；从而事件总大小可超过spool可用额度而不循环等待整事件ACK。

callback 不能重复执行；回收失败不撤销 APPLIED、不重复业务 SQL，继续保留磁盘计费。callback 后进程马上崩溃而上游 checkpoint 未持久化时，重放仍应安全。

### 7.5 对 TiCDC 接口的具体映射

| 接口 | 拟定职责 | 禁止事项 |
|---|---|---|
| AdmissionSink能力 / AddDMLEvent接入 | 先通过§6.7信用协议；原Add只接收已拥有入口lease的交付，快速转入异步编码 | 在共享处理线程等磁盘；未接管却发PostEnqueue；重复登记progress |
| FlushDMLBeforeBlock | 向受影响 lane 插入有序 flush marker，强制 marker 前 coverage 完成 | 拿着 admission 锁等待消费者；把本地排空当全局 DDL 完成 |
| WriteBlockEvent | 由协调后的指定执行者执行 DDL/recovery，成功才 PostFlush | 所有 capture 重复执行同一个 TRUNCATE |
| AddCheckpointTs | 接收已计算进度用于观测或另行设计的维护，不承担必要的首次 MERGE 触发 | 等 checkpoint 回调才 MERGE，导致进度闭环死锁 |
| SetWriteGate | 将许可检查接入每个实际 mutation 边界 | 只在 sink 构造时检查一次 |
| SetTableSchemaStore | 注册 schema 名称/范围依赖；补齐专属 SinkType 分支 | 假设名称清单就是可用的历史完整 schema |
| Run / Close | 生命周期、受限 worker、取消、正常 drain/失租区分 | detached Background worker 或无限等待 |

必要关系是 `独立计时/行数/压力触发 MERGE → PostFlush → TiCDC 计算 checkpoint → AddCheckpointTs`，不能反过来依赖 AddCheckpointTs 推动第一步。全局 checkpoint 是已确认覆盖的聚合结果，不是单表 `MAX(commit_ts)`。

cycle 的控制伪代码（不表示现有接口已有该实现）：

```text
entries = lane.selectBoundedEntries(schema, memoryReadBudget)
coverage = freeze(entries)
for batch in splitByRowsBytesParams(entries):
    assertCurrentOwnerAndWriteGate()
    rawInsertAndClassifyCommit(batch)  # UNKNOWN: 停留协调，不越过
freezeFurtherRawWritesForThisTable()
assertCurrentOwnerAndWriteGate()
result = mergeAndClassifyCommit(schema, incarnation, coverage)
if result in {APPLIED_CONFIRMED, NOOP_CONFIRMED} and coverageAndConsumptionProved(result):
    markApplied(coverage)
    releaseEntriesAndSchedulePhysicalReclaim()
    tryCompleteSealedEventsOnce()
else:
    keepCoverageUnackedAndReconcileOrFail()
```

原始输入、batch 和 coverage 的引用必须在各预算内；伪代码的 entries 不能实现为一次把磁盘积压全部读回内存。

## 8. 持久化数据模型与事件身份

### 8.1 隔离域

`source identity + changefeed instance + logical table ID + table incarnation + target mapping` 共同定义同步对象。复用 changefeed 名称或 DROP 后同名 CREATE 不能复用旧状态。

表名只是显示与路由名，不是唯一历史身份。内部对象名使用稳定 ID/hash 加可读后缀，避免用户表名长度/转义、同名表和 `_cdc_raw` 撞名。初始化必须验证内部对象 metadata 指向同一源与任务，不能见到 IF NOT EXISTS 就接管。

### 8.2 行身份与版本

- `row_key`：按 PK 列 ID、类型、长度前缀和值构造确定性编码；保留完整 canonical key。hash 仅用于索引/命名，不能以碰撞概率为零代替相等判断。
- `commit_ts`：事务 TSO，使用无损整数表示，不能转 float64；选择 Databend UINT64 或经范围验证的 DECIMAL(20,0)。
- `event_id`：上述隔离域、schema version、canonical key、commit_ts、净动作的确定性摘要/编码。
- `payload_hash`：同 event_id 对应规范化 payload 相同；冲突时 fail-closed，不能通过到达时间选一条。
- `add_time`：观测/清理辅助字段，绝不定义业务先后。

**commit_ts 不是每行唯一 ID**。同一事务不同键共享 TSO。相同键同一 TSO 的多条输入，必须通过已验证的源净变化契约归约；无法区分时不能用 dispatcher Seq、slice index、进程时钟创造伪顺序。

### 8.3 概念 raw schema（不是可直接执行的完整 DDL）

```text
RawRecord {
  source_id, changefeed_instance, logical_table_id, incarnation,
  source_schema_version,
  row_key, event_id, payload_hash,
  commit_ts,
  action: UPSERT | DELETE,
  payload: exact JSON / typed normalized row,
  ingest_time
}
```

第一版可以每目标表一套 raw/stream，但必须全任务共享 pool/budget；不要给每张表单独起一套无限后台 goroutine。

### 8.4 元数据/journal

需要区分至少三类状态：

1. `SchemaDescriptor`：列 ID、名称、类型、精度、PK、源 schema version、目标 fingerprint。
2. `DDLJournal`：DDL ID/commitTs、before/after fingerprint、操作内容、pending/applied、incarnation。
3. `OwnerRecord/Binding`：任务实例、表身份、target 映射、所有权代际/能力状态。

schema version 表示“如何解释行”，applied DDL fence 表示“这一边界前已处理到什么程度”；bootstrap 加载已有 schema 不自动证明旧 DML 已应用（I10）。本地 writeGen/mergedGen 和数据库每键水位同样不能互相替代。

### 8.5 applied witness与“更高raw版本已应用”的证明

retained raw本身是ingested watermark，不天然是applied watermark。若SQL过滤旧candidate依赖更高版本，必须能证明该更高版本已应用；否则I1不成立。

拟定首选证据为`AppliedCycle(operation_id, incarnation, schema_fence, coverage_digest, consumed_boundary)`，与target MERGE及stream消费**同一已验证事务提交**。若P0证实该组合可原子提交，恢复时用该证据关联operation ledger，而不是把本机mergedGen写到数据库后声称原子。APPLIED标记不存所有payload，长期保留/汇总与journal同受远端预算控制。

如果无法提供上述原子证据，不允许单独“MERGE成功后补写marker”冒充无窗口协议。可另行验证在单owner冻结的同schema/incarnation范围，对受影响key从retained raw重算最高有效净动作并在本cycle重新应用，避免把未证实higher raw仅用作过滤条件；跨schema历史不自动这样重投。该替代需要明确SQL快照、coverage和no-op消费证明，未验收就保持RECOVERY_REQUIRED，不静默降级。

因此持久化applied witness/等价重应用算法是B02/B03的一部分；新raw schema的event_id唯一性、目标一行内容或MAX(TSO)都不能替代它。

## 9. raw INSERT、stream、MERGE 与 compaction

### 9.1 raw INSERT

使用参数绑定，不拼接业务值；显式列清单和全限定表名，避免 USE/session 对连接池的隐式依赖。控制 SQL/请求字节和参数数；大 batch 在字节预算内拆分。

重试必须使用同一逻辑 event_id 与 payload。INSERT 成功响应丢失可能带来物理重复 raw 行；必须先设计 MERGE去重，不能假设数据库替我们提供唯一约束。

### 9.2 stream 的用途和能力前提

raw 的每次业务变化都 INSERT 一条带 action 的记录，因此 stream 使用 append-only；业务 DELETE 是 raw 的 INSERT tombstone，不依赖 stream 捕获 raw DELETE。raw 历史清理不应成为新业务删除。

公开文档确认有 append-only stream，且 CREATE STREAM 页面标注 Enterprise 能力；部署许可/可用性需预检，不能假定所有 Databend 版本/发行版都有。[CREATE STREAM](https://docs.databend.com/sql/sql-commands/ddl/stream/create-stream)

公开指南描述 stream 消费与提交绑定、并发消费者有冲突限制；**这不等于已经证明本文 MERGE/事务/no-op 的全部语义**。[Stream workflow](https://docs.databend.com/guides/load-data/continuous-data-pipelines/stream)

第一版默认要求 stream 能力。现有 bend-cdc 建 stream 失败时回退全 raw 查询（C03），新 sink 不应悄悄照搬：性能和覆盖语义改变，必须显式配置、单独验收，默认 fail-closed。

### 9.3 MERGE 算法草图

在单 owner、表 lane 已冻结 raw INSERT、schema 一致的前提下：

1. 确定本 cycle coverage，验证每个 fragment 已 raw committed。
2. 从该 stream 待消费 delta 选 candidate，并参考同 incarnation、允许 schema fence 下的 retained raw history。
3. 按canonical key选最高commit_ts的有效净动作；必须先满足§11.4源事件模式/净变化契约。不能把默认UK拆分产生的合法同版本DELETE/UPSERT误判为源数据冲突。契约满足后才对剩余同版本异payload fail-closed，完全相同replay合并。
4. 过滤低于 retained key watermark 的旧 replay；不能因 newer 仅存在于尚未应用的另一个未协调 writer 的 raw 就确认旧事件。
5. 使用显式列投影转换目标值，按 typed PK 相等匹配：匹配 UPSERT 更新，匹配 DELETE 删除，不匹配 UPSERT 插入，不匹配 DELETE不插入。
6. 正常写入和零物理修改分别按§9.4/§9.7取得终态、coverage与消费证据后标记APPLIED；不是要求目标行数必须大于0。缺证据进入COMMIT_UNKNOWN/停写，不发callback。

MERGE 的增删改语法由官方文档支持，但这里的去重/覆盖/fence 算法是拟定设计，需真实 SQL 验证。[MERGE](https://docs.databend.com/sql/sql-commands/dml/dml-merge)

不能直接把本文草图当生产 SQL：特别要验证 raw self-reference 与 stream 读取是否共享正确快照、无匹配行或只含被抑制旧事件时 stream 是否前进、查询投影失败后是否完全回滚。C03 的既有 SQL可作起点，不是证明。

### 9.4 提交结果判定

#### 9.4.1 adapter返回的证据结构

拟定TargetClient返回下面的逻辑结构，而不是`executed bool`：

```text
ExecutionEvidence {
  operation_id, sql_digest, owner_generation, table_incarnation,
  schema_fence, coverage_digest, mode: AUTOCOMMIT | EXPLICIT_TXN,
  attempt_id, request_id, server_query_id?, server_transaction_id?,
  terminal: SUCCEEDED | FAILED | ROLLED_BACK | UNKNOWN,
  commit: COMMITTED | NOT_COMMITTED | UNKNOWN,
  stream_before?, stream_after?, consumption_proof?,
  error_class, server_error_code?, evidence_source, observation_counts?,
  observed_at, server_version, driver_version
}
```

operation_id属于一次固定SQL/coverage；attempt_id区分物理发送。query_id不等于幂等键，重试标签也不自动覆盖DDL。request/attempt必须在发请求前登记；收到server ID立即补记，不能仅保存在即将关闭的Rows里。SQL digest不含业务原文；错误与证据日志脱敏。

结果分为`APPLIED_CONFIRMED`、`NOOP_CONFIRMED`、`NOT_APPLIED_CONFIRMED`、`COMMIT_UNKNOWN`。`OBSERVATION_FAILED`是附加观测状态，不覆盖前三种提交判定。显式事务中单条MERGE的SUCCEEDED仍非COMMITTED，必须另取COMMIT终态，且同连接/会话正确绑定。

#### 9.4.2 首选取得路径与错误分类

首版拟使用受控HTTP/driver adapter：保留原始响应状态/错误与query ID，持续读取/轮询到服务端成功终态；自动提交需证明该终态与提交绑定，显式事务还需COMMIT成功。只经database/sql拿到Rows不能提供全部证据；若v0.9.4不暴露必需信息，应增加窄接口或单独客户端，不能从私有字段猜测。具体endpoint、认证、终态字段、节点路由、保留期需在P0按实际server版本落实，未落实就禁止生产/自动接管。

| 观察结果 | 分类与动作 |
|---|---|
| 权威服务端确认语法/权限失败且无提交 | NOT_APPLIED_CONFIRMED，停止或按错误策略重试；不callback |
| 已收到结果页，但最终轮询报执行错误 | 不是统计失败；依据服务端提交/回滚语义判断，不能证明则UNKNOWN |
| 网络断开/超时/首响应丢失 | UNKNOWN，停止同lane后续mutation，查询已有query/request关联状态 |
| Rows值解码/类型转换错误 | 与提交证据分离；有独立COMMITTED证据才保留成功，否则UNKNOWN |
| 服务端成功终态已确认，仅操作计数解析失败 | APPLIED或NOOP保持，统计unavailable，禁止为补统计重做SQL |
| Kill/Close返回nil、查询列表中暂时不存在 | 不是提交/终止证明，保持UNKNOWN |
| 显式事务中语句成功、COMMIT响应丢失 | UNKNOWN，按transaction/query关联恢复；不能按语句成功确认 |

已核实D01：v0.9.4 `waitForData`在有缓冲行时可退出，不强制ReadFinished；KillQuery丢弃请求重试错误并返回nil。C07则在QueryContext返回Rows后立即设executed。由此只能得出“现有标记不足以作为证明”，**不能声称已复现MERGE先返回数据再提交失败**。

#### 9.4.3 不确定结果、重启与找不到query

1. 进入UNKNOWN即冻结该lane及可能冲突的DDL/compaction，不确认coverage。保留本地payload直至明确结果或受控停止；不得无限重发。
2. 在旧owner仍有效时，用有界协调worker查询服务端权威终态；证明已提交则按原coverage继续，证明未提交/已回滚且旧请求不再运行才可重试。
3. 发请求前把在途操作描述写入目标durable operation ledger并确认（或由P0证明等价外部持久化查询目录）。ledger状态写入本身也有提交不确定性：未明确登记成功不发送业务SQL；更新丢响应按相同operation_id读回核验。它只存小元数据，不存全部源payload，也不构成分布式锁。
4. 重启先取得§12排他性，再扫描未终结ledger、关联query/transaction终态。若数据库不支持客户端指定/关联request ID，首响应丢失时仍可能无法定位SQL，此时自动恢复不可用，进入RECOVERY_REQUIRED；不能假装ledger消除了这个窗口。
5. query记录过期、查不到、权限不足或只查到“当前不在运行”均不等于NOT_COMMITTED。可通过已证明原子提交的应用标记/消费元数据等替代证据协调；**目标中查到一行业务值或MAX(TSO)不足以证明整个coverage**。
6. 无可信证据则fail-closed，运维确认所有旧查询终结并完成目标核验后恢复或重新初始化。超时只转人工状态，不自动转成功。

operation ledger的持久化/并发控制与ID查询路径尚未实测，B03必须以接口字段、实际请求响应、故障实验和重启证据关闭，不能以“已审driver”关闭。

### 9.5 compaction

第一版同表 compaction 与 raw INSERT/MERGE 串行。只在已明确成功的 cycle 之后删除被支配历史；不能删除唯一最新水位/墓碑，也不能删除恢复 pending DDL 所需状态。

最新事件的物理重复行要有明确去重策略：仅删除严格更旧版本不能清除相同 event_id 的重复，长期重试会膨胀；用 ROW_NUMBER 删除相同行也不能误删全部。是否支持稳定物理 row ID、重写去重表或安全重建 stream，列为待验证项 B06。

compaction 失败独立记账和告警；APPLIED 不回退，但持续失败超过 backlog/空间阈值应停止 admission。raw 保留全 payload 的最新行成本高，后续可分离小 watermark/tombstone 表与短期原始 payload；第一版不借此增加跨表原子性依赖。

### 9.6 远端raw/watermark容量与成功运行下的停写边界

持续新PK INSERT→DELETE可让target接近空而raw每键保留一个墓碑；MERGE和compaction全部成功仍增长。因此新增独立于本地spool的远端预算，覆盖raw、历史watermark/tombstone、stream依赖保留快照、内部journal/ledger及物理重复。

拟定控制：

- 每feed配置remote-max-bytes、remote-max-history-keys、检查周期和软/硬水位；部署必须给出有限值，不以0代表默认无限。
- capacity observer使用真实版本支持的表统计/存储计量；明确active逻辑字节、retained snapshot与物理计费字节差异，不把COUNT(*)当物理占用。历史键数使用保守上界，不能低估删除键。
- observer结果过旧或查询失败超过限期后停止新raw admission。新cycle先预留“最大可能增长”额度，再raw INSERT；并发表共享feed预算，禁止每表分别看一遍旧统计后各自超额写入。
- 测量延迟、SQL失败残留、压缩/快照放大意味着应用阈值不是数据库物理硬quota。设置安全余量，预算覆盖已预留在途写；若要求绝对计费上限，还需warehouse/provider硬配额能力，未证实时不承诺。
- 软水位告警并提高安全compaction优先级；到admission阈值进入REMOTE_CAPACITY_BLOCKED，停止新raw INSERT，允许已提交raw的MERGE、必要DDL恢复和安全清理。DDL自身要保留维护余量，不能借“维护”无限扩张。
- 恢复仅有三种：显式扩额并重新测量；有证明地消除物理重复/被支配历史；从新一致快照S2重建独立目标与内部状态，校验后切换。重建需要额外预算与旧feed冻结/隔离，禁止原地删最新墓碑。

验收：固定target有效行数、持续新键churn，记录历史键、物理存储/计费增长、扫描量与成本；compaction均成功时达到预算仍必须可诊断停写，停止后新raw写入次数为0（已预留请求除外且计入余量）。B18跟踪停写控制，B06跟踪去重和保留算法。

### 9.7 no-op、已支配重放与stream消费

零目标行修改不等于失败，也不等于已经完成。确认分为：

| 情况 | 完成所需证据 |
|---|---|
| 正常MERGE改写业务行 | 服务端COMMITTED终态 + 本cycle覆盖/stream消费契约成立 |
| 原coverage全部因更高版本而被抑制 | 更高版本已应用证明（不能仅raw存在）+本次终态/消费证明，返回NOOP_CONFIRMED |
| DELETE匹配不到目标或完全重复写 | 相同版本净动作/墓碑一致 +终态/消费证明，不要求操作计数>0 |
| 空stream但本地coverage非空 | 默认恢复异常；需证明coverage已由历史消费覆盖，否则不能确认 |
| 无待确认coverage的空cycle | 直接结束调度，不制造源事件callback |

本版选择保守规则：所有已写入raw的coverage，在当前或可证明历史消费覆盖后才完成；不因为“raw中有更大TSO”提前确认。单owner恢复先解决遗留raw与未终结查询，再接入新cycle；不能用未应用的新raw支配旧事件。

**版本事实**：Databend已合并[#20671](https://github.com/databendlabs/databend/pull/20671)，修复某些no-op DML跳过目标提交时丢失stream offset更新的问题，并说明planner-level no_effect短路不在修复范围。这不是“所有版本空MERGE都不消费”的规范，也不证明部署已含补丁。

P0必须取得server build与补丁覆盖证据，分别测实际SQL的数据依赖过滤为空、常量空集/优化短路、仅旧重放、DELETE不命中、重复事件、autocommit与显式COMMIT/ROLLBACK。cursor证据采用真实版本可核验的metadata或等价前后delta实验；不能只看counts=0或假定一定有可查offset API。

若实际组合不能可靠消费，首版拒绝该server/SQL组合或修复服务端再验收。显式WITH CONSUME/标记表只能另行证明与MERGE、owner、coverage同一提交边界后采用；禁止在MERGE前独立消费，禁止向业务target塞伪业务行。没有消费证明时不callback，但必须告警/进入恢复态，不能静默无限循环重刷。

### 9.8 stream过期/丢失后的恢复顺序

检测stale/缺失/依赖snapshot丢失立即冻结lane，不静默新建空stream。先排除旧owner/未知SQL；核验目标incarnation、DDL journal、最新墓碑与尚未应用raw的完整性。若raw仍完整，需通过显式full-raw回放覆盖算法重建stream起点并验收，且不混旧schema；“CREATE新stream”本身不会补已存在raw。

缺任何恢复证据就进入REINITIALIZATION_REQUIRED，从新一致快照重建；从上游checkpoint重放不是充分条件，因为上次已确认但被目标外部破坏的数据可能不会重发。重建路径未实现前，首版只支持诊断停写+人工重新初始化，不承诺自动修复。B14专门验收，不能因已有fail-closed就标成自动恢复完成。

## 10. DDL、建表与 schema 演进

### 10.1 初始 schema 与历史空表

已有表不会因为新建 changefeed 而重新执行历史 CREATE TABLE。首条 DML 带 TableInfo 可懒建非空表，但永不变更的历史空表必须由 start-ts schema snapshot bootstrap 或独立初始化任务预建。

bootstrap 要与同一时点 schema 匹配：不能查询“当前最新 schema”再解释较早 DML。TableSchemaStore 中名称清单不等于完整列定义。具体元数据读取 API 接口与调度顺序需实现前落定（B04）。

建目标表使用权威列顺序、精度、NULL/default 规则；建后 DESCRIBE/fingerprint 校验。目标已存在时不盲目接管：结构、主键唯一性与初始化水位均需验证。未经初始化的既有目标可能本身有重复 PK，MERGE 不能自动修复这一前置问题。

### 10.2 正常 DDL 协议

```text
收到 DDL(D)
  → 禁止受影响表的新 epoch 进入 raw 写路径
  → 本地 flush marker，强制所有旧 epoch entry MERGE完成
  → maintainer 完成受影响 dispatcher 的全局协调
  → 验证当前 owner 与 before fingerprint
  → durable journal(PENDING, D, operation, before/after schema)
  → 执行翻译后的 Databend DDL / 重置
  → 校验 after fingerprint
  → journal(APPLIED)
  → DDL PostFlush
  → 放行新 schema DML
```

FlushDMLBeforeBlock 只证明它负责的本地队列完成，不能冒充全局屏障。DB/All 范围 DDL 必须枚举所有影响表；TableSchemaStore 需求分支需新增（T06）。NotSync/过滤 DDL 的语义必须显式接入。

### 10.3 DDL 白名单

| 操作 | 处理 |
|---|---|
| CREATE DATABASE/TABLE | 翻译并校验；不原样下推 TiDB DDL |
| ADD 非键列 | 字面量默认值/NULL 语义明确才支持；对已有行回填需与源一致 |
| DROP/RENAME 非键列 | 旧 epoch排空，更新 fingerprint 和投影；防止 drop 后同名 re-add 混列 |
| MODIFY | 沿用有限安全白名单；不因目标已碰巧同类型就跳过未验证变更 |
| 默认值/NULL 属性 | 区分未声明、DEFAULT NULL、空字符串和表达式；不支持表达式则拒绝 |
| 二级索引 | 证明无行投影/身份变化后 no-op，仍更新必要元数据 |
| PK 定义更改/复杂多操作 | 拒绝/阻塞 |
| RENAME/DROP TABLE/SCHEMA | 未实现前明确阻塞，不能仅 warning继续 |

复用 C04/C05 白名单时要保留适用限制：已有 VARCHAR→TEXT、TINYINT→INT、INT→BIGINT、部分 INT→文本，并不等价于支持任意类型改写。

### 10.4 journal结构、恢复优先级与普通DDL转移

DDL identity使用`source/任务实例/逻辑表血缘/源DDL ID或可证明唯一的等价身份`，包含commit_ts、old/new physical table ID与DDL内容摘要。只用同名表或全局MAX(ddlTs)不可区分重建/多表DDL。journal记录before/after列ID fingerprint、drain fence证据、phase、owner generation、operation/query关联和新代际是否已开放。

不假设Databend支持journal上强制唯一约束；单有效owner下同DDL身份单调推进，读回重复内容必须一致，多owner排他由§12提供。journal写入也经§9.4分类，不明确持久化就不得越过该步骤；冲突/倒退/未知身份fail-closed。

恢复优先级固定为：

```text
所有新mutation保持关闭
 → 取得owner排他并处置旧在途SQL
 → 核验source/target绑定、journal/ledger完整性与schema snapshot
 → 恢复PENDING DDL/TRUNCATE（先于任何旧DML普通解码/写入）
 → 安装已证实的applied-DDL fence与incarnation映射
 → 对旧事件执行身份校验后的skip/recovery规则
 → 协调遗留raw/stream与未知cycle
 → 完成当前schema bootstrap核验，开放正常增量
```

| durable phase / 当前证据 | 允许动作 | 禁止动作 |
|---|---|---|
| 无journal且收到新DDL | drain并持久化PREPARED，成功后发送DDL | 先DDL后补journal |
| PREPARED +无未知SQL +可信before fingerprint | 按同DDL身份执行白名单DDL，记录执行attempt | 在旧DDL可能仍运行时重发 |
| EXECUTION_UNKNOWN | 查询服务端终态；只有after结构可唯一证明本DDL完成时才收敛 | 只见列不存在/目标同类型就猜成功 |
| 可信after fingerprint +执行终态/等价完成证据 | 写VERIFIED，再写APPLIED与drain fence，均确认持久化 | journal未确认即放行新schema DML |
| APPLIED +相同DDL重放 | 验证目标仍属于该代际，跳过业务DDL，只完成重放事件 | 重做ALTER/TRUNCATE |
| 旧schema DML重放 | 只有身份匹配且drain fence证明该前缀已应用/由TRUNCATE合法清除时跳过 | 仅凭当前schema更新时间丢DML |
| 元数据/目标不一致或无法判定中间状态 | RECOVERY_REQUIRED，维持写关闭 | 用IF EXISTS掩盖冲突继续 |

fingerprint不是万能提交证明：TRUNCATE前后schema相同；有外部DDL时相同结构也可能来自别的操作。目标禁止外部写是前置条件，仍须记录mutation身份。多操作DDL首版不支持，不能拆成多句后只验最终schema而忽略中途失败。

TiCDC基线MySQL专属恢复查询/skip标记需抽窄RecoveryCapability或Databend专属分支，覆盖普通dispatcher和merge task入口（T07）。恢复返回的是明确skip范围、代际与证据，不是笼统`lastDDLTS`。该API/持久化实现仍由B04/B09阻塞，表格只固定允许的状态转移。

### 10.5 TRUNCATE逐步恢复算法

TRUNCATE改变源物理表ID/代际；目标名可能不变。新代际不得在APPLIED记录持久化前接收任何DML。优先新建带代际后缀的raw/stream、隔离旧对象，旧对象物理清理后移，不先删唯一恢复证据。

| phase | 正常动作 | 崩溃/响应丢失后的恢复 |
|---|---|---|
| R0 DRAINED | 全局旧DML排空；登记DDL身份、old/new incarnation、reset目标与drain证明 | 无可信drain证明则重新协调，不直接清空 |
| R1 PREPARED | durable记录新代际CLOSED；旧SQL已终结后发送目标TRUNCATE，登记operation | 先协调unknown旧TRUNCATE。只有证明新代际从未开放且无新数据写入、目标独占时，才可在旧SQL终结后重复reset；否则停写 |
| R2 TARGET_RESET | 终态确认后记目标reset完成；创建新代际raw/stream并验证binding | 不从“表为空”推导旧query已终结；新对象存在须核验identity，不能见名接管 |
| R3 NEW_BOUND | 持久化新binding/schema/fence，旧raw标retired而非可消费 | 新代际仍CLOSED；补完缺失journal后读回一致性 |
| R4 APPLIED | 所有前置步骤完成且持久化APPLIED，再允许DDL回调/开放新代际 | 相同DDL只skip；即使checkpoint还在TRUNCATE之前也不得再清target |
| R5 ACTIVE | 新代际可写；旧raw/stream仅由受fence维护清理 | APPLIED/绑定冲突或丢失时停写核验，绝不通过再TRUNCATE“修复” |

安全依据是“新代际写入发生在可信APPLIED之后”，不是TRUNCATE天然幂等。R1的可重做条件必须由journal、排他所有权与未开放协议共同证明，不能只依赖本机布尔值。若目标/journal被外部删除或查询无法确认，算法选择阻塞而不是猜测。

验证每个R0–R5之前/之后崩溃、步骤提交后响应丢失、journal更新不确定、旧owner晚到TRUNCATE。重点：R5写入@510后从@490重放@500 TRUNCATE，@510不得被再次清空。即使@510理论上可能重放，也不把这当允许重复破坏的理由。

### 10.6 历史空表bootstrap与DDL交叉

初始化取一致schema snapshot S并建立bootstrap epoch，表清单与列定义必须属于同一个S；枚举和分页期间收到的DDL按源顺序缓存于有界队列/上游保留，不能最新schema补齐旧清单。snapshot完成并持久化binding前，相关表不开放DML写入。

若DDL(D>S)先到而某空表尚未建好：该表先以S建表/核验，再走正常D的drain/journal；若采用直接建D后schema的优化，必须证明跳过区间无未应用数据，首版不默认采用。schema读取失败/历史版本不可用就停止bootstrap，不退回查询最新DESCRIBE。

测试无历史CREATE重放的空表、分页时ADD/DROP列、同名列drop/re-add、DDL后首批DML、bootstrap中断重启。B15验证时序，B04验证获取API与dispatcher恢复接入，两者不能互相代替。

## 11. 主键、净变化与乱序处理

### 11.1 普通更新和删除

INSERT/UPDATE 归一成完整 after-image UPSERT；DELETE 使用旧 PK 与 tombstone。完整 image 要由源契约保证，不得将缺失字段自动填 NULL。

同键较大 commit_ts 胜出；DELETE 也参加比较。收到 `UPSERT@100, DELETE@200, delayed UPSERT@150` 后，目标必须仍不存在，raw 保留至少 DELETE@200 的版本水位。

### 11.2 主键值更新

必须处理旧键消失和新键存在。源入口可能将变键 UPDATE 拆成 DELETE/INSERT，也可能暴露 before/after；两种都要真实测试。

同事务多行交换键/复用键需要按最终 KV净状态归约，不能简单规定“最后到达赢”，也不能未证明就让 DELETE 总比 INSERT 新。若源不能提供确定净结果，第一版必须拒绝该能力而不是静默留脏行（B05）。

### 11.3 collation 与身份

canonical key 的字节相等、源 PK 约束相等与 Databend SQL JOIN 相等可能不同。测试大小写、Unicode、尾空格、binary/text 键、DECIMAL 规范化和 NULL。

不能只在 raw 用区分大小写的字符串 key、target 用另一种 collation，然后宣称相等语义一致。第一版可限制到已验证 PK 类型/collation 集合；拒绝规则必须在创建/发现新表时可执行。

### 11.4 原生事件模式：二级UK、真实变键与净变化

源码事实（T11）：outputRawChangeEvent=false时，IsUKChanged对索引列/unique标志列比较，更新二级唯一键也可能SplitUpdate；INSERT可被缓存到DELETE之后甚至跨分片输出。T12的初始化只为CloudStorage/Kafka读取该开关，新SinkType若不加分支就保持false。

**本版决策：Databend固定要求outputRawChangeEvent=true，作为内部事件契约，不开放随意切回false的调优项。** 必须贯通manager→SharedInfo→dispatcher注册/重注册→eventservice→scanner；bootstrap、迁移、重试/重建session、兼容版本都校验实际生效。无法协商/确认则拒绝启动，不以sink端猜DELETE优先级补救。排除会改写/过滤DML两半的未经设计选项。

| 输入/场景 | 规范化规则与限制 |
|---|---|
| PK相同，二级UK从a→b | 保留完整before/after UPDATE，输出同PK一个UPSERT@C；不制造DELETE@C |
| 同PK普通列UPDATE | 完整after UPSERT；不完整image拒绝，不用NULL补齐 |
| UPDATE明确携带oldPK!=newPK | 只有已验证该源表示和事务净契约后输出旧键DELETE+新键UPSERT；不以到达顺序解决同TSO冲突 |
| clustered PK/record handle变更以独立KV DELETE/INSERT出现 | raw=true未必合并成UPDATE；必须验证提交时按record key净结果及跨分片契约，不能假定旧/new可配对 |
| 同事务反复修改/复用同键 | 先证明源提交事件已是确定净结果；若还需归约，必须有稳定事务结束边界/有界或可溢写归约状态，不能把DMLEvent结束等同完整事务结束 |
| 同key/TSO的相反净动作或不同payload在契约后仍存在 | 停止并保留诊断，不用ROW_NUMBER任意选一条，也不规定DELETE/UPSERT一律胜出 |

因此，raw=true解决的是**避免人为UK拆分**，不是对真实PK变化、同事务多键替换和所有冲突归约的万能证明。第一版在真实TiDB事件与重启测试通过前不宣称支持变键；如果输入无法区分“变键”与普通DELETE/INSERT，就不能承诺可检测地禁用PK UPDATE，必须收紧可保证的源使用范围或补源端契约/能力后再发布。

测试用例先验证源SQL合法性。不能直接把可能违反即时唯一约束的单句PK互换当作可复现负载；选择TiDB实际允许的中间键/多语句事务与最终KV事件，保存事件dump。必须覆盖PK不变仅UK更新、raw开关true/false对照、两半强制跨事件/cycle/重启、同事务多行相同TSO、真实handle变更。不能为了证明确定性而在sink构造虚假的行序号。

此决策应纳入scheme创建时校验及DDL新增唯一索引后的持续校验，B05的关闭条件同时覆盖普通UPDATE与可选变键，不再缩写成“PK更新测试通过”。

## 12. 所有权、失租和故障接管

### 12.1 为什么进程 mutex 不够

每个 TiCDC capture 都可能建立 sink，同表/同 stream 不能有两个独立 merge owner。即使表跨节点调度关闭，正常迁移、旧节点网络分区及未结束 SQL 仍可能重叠。

T08 的 write gate 检查新请求是否可以发出，但请求通过检查后失租，服务端可能仍在运行。`ctx.Cancel()` 或客户端断连不等于服务端已回滚。

### 12.2 所有权协议要求

正常 handoff：旧 owner停 admission → drain/停止提交 → 确认其在途 mutation 的终态 → 释放 owner → 新 owner取得新 generation → 恢复 journal/raw → 放行。

异常接管：必须有目标侧原子 fencing/冲突机制，或可证实旧 SQL 已终止的机制，再开放新 owner。旧 DDL/compaction/stream 消费尤其危险；仅依靠幂等 row event_id 不能保护 schema 和 truncate。

候选A仅适用于可证明同事务原子冲突的mutation，不能泛化到DDL。官方说明DDL会提交当前多语句事务，所以先检查owner再DDL不提供该保护。[BEGIN/DDL限制](https://docs.databend.com/sql/sql-commands/ddl/transaction/begin)

| mutation类别 | 首版排他要求 | A候选是否可直接覆盖 |
|---|---|---|
| raw INSERT /目标MERGE | 单owner lane；接管前旧query终态确认；候选事务fence需验证隔离/写冲突及提交原子性 | 仅待P0证明，普通SELECT owner再写不够 |
| journal/operation ledger/binding DML | 与owner代际关联，单调、读回一致；旧owner写元数据也须被排除 | 同上，元数据表不自动是锁 |
| CREATE/ALTER/TRUNCATE/DROP | 控制面串行授权 +所有相关旧SQL已终结 +本DDL终态与journal恢复 | 否，不把DDL塞BEGIN当fence |
| stream创建/替换/重建 | 与DDL同级排他；先冻结消费和raw写，验证未丢coverage | 否 |
| raw compaction/退役对象清理 | 独立登记mutation；只处理已证实retired/被支配对象，排除旧owner维护SQL | 若为事务DML需另测，不能默认 |

候选B要求服务端终态确认，而非“调用取消成功”。D01的KillQuery丢弃错误，返回nil可能根本未取消；即使取消请求被接收，也要确认旧query不再能提交。未知request ID、服务端查询记录过期、节点不可达、取消接口受阻都保持HANDOFF_BLOCKED。

正常handoff与异常接管都使用同一mutation registry，涵盖业务与维护SQL。需要证明查询枚举/登记完整，不能只查当前客户端连接或某一个query ID。旧owner已退出只防止未来发送，不能证明已发送SQL终止。

**不同mutation的保护均未经过实际server验证，B01仍为生产阻塞项。** 外部锁可序列化新授权，不能撤销已发送SQL；先读owner表再MERGE也不原子。首版可先做受控非HA PoC，但必须满足§12.5，不把候选描述标成已落地。

### 12.3 mutation 边界

raw INSERT、MERGE、CREATE/ALTER/TRUNCATE、journal写入、stream 创建/重建、compaction和内部对象清理都必须检查 write gate/owner。验证连接不应随便创建数据库等副作用；需要区分只读预检与用户授权初始化。

### 12.4 多任务和映射冲突

同库同表的绑定注册必须跨 changefeed 生效，不是每 capture 一个 map。规则变更、大小写折叠、前后缀、同名源库映射都可能撞目标。未能实现可靠排他时要求运维静态独占并在重启核验 metadata；这仍不是完整生产自动化方案。

### 12.5 未闭合fencing时的PoC运行边界

`scheduler.enable-table-across-nodes=false`只禁止一表拆span跨节点，并不禁止整表调度、故障接管或维护迁移（T15）。因此它是必要规模限制，不是“禁用HA”的实现。

受控PoC必须增加Databend恢复准入：任何新capture/session owner在取得授权后仍保持写gate关闭，完成registry扫描/旧query终态核验/DDL恢复后才开放。未实现跨节点准入时只能在隔离单capture实验环境运行，禁用自动替换/重启写入并要求人工恢复；不能只配置一个false就接入生产调度。

失租立即阻止本机新mutation并取消worker；进程fail-stop可作纵深措施，不构成服务端fence。禁止用固定sleep>query timeout或驱动Kill nil作为安全接管证据。无法证明query超时上界、取消终态与枚举完整性时保持fail-closed。

真实故障验收：阻断取消/终态接口并延迟旧TRUNCATE，新owner必须始终拒绝写；恢复可达并取得可信终态后，才完成恢复并放行。控制面锁、客户端write gate、目标query终态三者角色分别记录，不混为一个“lease有效”布尔值。

## 13. 类型映射与 schema/value 统一

### 13.1 统一 mapper

一套 mapper 同时服务 schema bootstrap、DDL、raw 编码和 MERGE projection；输入是 FieldType/flags/Flen/Decimal/Elems/列 ID，不是样本值。沿用 C03/C06 的测试案例，避免各路径字符串映射漂移。

| 源类型 | 目标原则 | 必测边界 |
|---|---|---|
| signed integer | 保留精确宽度或无损提升 | 最小/最大值，负值 |
| unsigned integer | 使用对应 UINT，BIGINT→UINT64 | 大于 2^63−1，最大 2^64−1 |
| DECIMAL(p,s) | 精确保留 p/s，不经 float64 | DECIMAL(65,0)、最大 scale、负数/前导零 |
| BIT(n) | UINT64 | BIT(64) 全 1，NULL |
| FLOAT/DOUBLE | 明确非有限值策略 | NaN/Inf 支持差异、精度 |
| BINARY/BLOB | 原始 bytes→HEX或已验证二进制参数 | 0x00–0xFF、空 bytes、NULL |
| ENUM/SET | typed value/Elems→约定字符串表示 | 空值、非法 ordinal、声明顺序、逗号/转义 |
| JSON | 精确 JSON 文本或 typed value，不经通用 float64 map | 大整数、SQL NULL vs JSON null、数组/嵌套 |
| DATE/DATETIME/TIMESTAMP | 明确时区/精度/范围；DATETIME不隐式平移 | 零日期、DST、微秒、超范围 |
| TIME/YEAR | TIME 字符串，YEAR 小整数 | 负 TIME、超过 24h、零 YEAR |
| TEXT/CHAR/VARCHAR | 字符集解码与目标字符语义明确 | 引号、反斜杠、控制字符、Unicode |
| VIRTUAL/STORED generated | VIRTUAL 不写；STORED写物化值 | 初建表、DDL后、仅生成列 |
| vector/geometry/其他 | 未定义前拒绝或显式安全映射 | 不静默 infer VARCHAR |

原生 binary 不能套 Canal Latin-1 还原；原生 ENUM label 不能再当 Canal ordinal 解码。JSON 验证不应 unmarshal 成 float64 导致精度损失。

### 13.2 raw payload 与 projection

第一版可用规范化 JSON：decimal/大整数字符串或精确数字文本必须约定一种表示；binary采用明确 HEX marker/列类型；SQL NULL 使用单独 null语义。不要让同一字段有时 raw bytes、有时 base64、有时 Latin-1 字符串。

所有值转换失败必须停止对应应用，不以 TRY_CAST 静默填 NULL；可选容错模式不属于第一版。缺少字段与字段值 NULL必须可区分，必要时带列 presence，避免不完整行覆盖。

### 13.3 默认值与目标侧行为

源值优先于目标 default；CDC INSERT已有完整数据时不重新计算 CURRENT_TIMESTAMP。ADD COLUMN 的历史回填只在明确兼容时执行；普通建表 default 的翻译不能成为源时间语义漂移来源。

## 14. 配置接口草案

**全部 Databend 新字段均未实现，以下不是当前可运行配置。** 数字是测试起点，不是已承诺默认值。

连接标识：`databend://HOST:PORT/DB_NAME?sslmode=require`。DB_USER/TOKEN应由受保护配置或运行时 credential provider注入；具体接口待安全设计，避免把真实 token 写进日志、提交记录或 CLI 参数。Bend-cdc 已有 runtime AccessTokenLoader，可借鉴但要接入 TiCDC生命周期。

```toml
[filter]
rules = ["DB_NAME.*"]

[scheduler]
enable-table-across-nodes = false

[sink.databend-config]
auto-create-table = true
# 镜像模式固定处理DELETE；不开放allow-delete=false
# outputRawChangeEvent内部强制true，不提供切换为false的用户项

batch-max-rows = 5000
batch-max-bytes = 8388608          # 8 MiB，编码后数据预算；请求开销另计
flush-interval = "1s"
merge-trigger-rows = 50000
merge-interval = "5s"
merge-concurrency = 4             # 跨表，同表串行

spool-memory-quota = 67108864      # 64 MiB 热数据，不等于整个 sink RSS
spool-disk-quota = 10737418240     # 10 GiB，含维护预留，单 changefeed
spool-maintenance-reserve = 67108864 # 64 MiB，包含在磁盘总额内，不可用于普通admission
max-inflight-bytes = 268435456     # 256 MiB，含入口/热数据/编码/读回/元数据及控制预留
max-row-bytes = 8388608            # 规范化后的单行含key/元数据；大事件应拆分
max-delivery-bytes = 33554432      # 32 MiB，解码后chunk容量预算；wire另有协议上限
max-delivery-events = 1024         # 元数据数量限制，不代表允许一次交付1024个最大事件

remote-max-bytes = 1099511627776   # 1 TiB示例admission预算，不是已证明的物理计费硬quota
remote-max-history-keys = 100000000 # 1亿历史键示例；按实际行宽/成本重新设定
remote-check-interval = "30s"
remote-observation-max-age = "2m"  # 过期后停新raw，不能永久盲写
```

节点级拟定（server配置；字段名尚未实现/定稿）：

```toml
[sink-spool]
base-dir = "/PATH_TO_DEDICATED_SPOOL"
total-disk-quota = 107374182400    # 100 GiB，capture 所有相关任务共享
min-free-disk-bytes = 21474836480  # 至少保留 20 GiB，容量按节点实际调整
```

配置校验：非负/上下界、单行与batch预算、内存池及并发可行性、磁盘总额/余量、未知字段拒绝、不支持功能拒绝。max-inflight必须包含编码和读回池，不能让四个 worker各额外分配无限 batch。

256MiB只是便于预算拆分的示例，不是已验证RSS保证。例如热64、入口32、编码32、读回64、元数据16、控制48MiB，总和256；真实分池根据最大row/delivery/request放大、并发数校验。任何一个8MiB行若连同参数/协议开销不能装进预留，必须提前拒绝或调小有效行上限，不能临时超额分配。sync.Pool只是复用机制，不是预算；超大buffer不能无限留在pool。

远端软水位与admission安全阈值由上述预算扣除观测误差、增长预留和维护余量推导，生产配置需给出推导结果；示例没有承诺1TiB成本封顶。每个pool和阈值的计费定义/保守放大系数应在启动诊断中展示，但不泄漏业务数据。

v0.2决定镜像模式固定处理DELETE，不再开放`allow-delete=false`；旧计划/客户端传此不支持项时显式拒绝，不静默忽略。删除版本和墓碑按统一规则保留。非镜像append模式不属于第一版。

更改配额下降到当前用量以下：停止新 admission、继续消费者排空，不直接删文件。改变目标映射、PK策略、spool根目录、类型映射等不默认支持在线生效，需停止和显式迁移。

## 15. 错误、生命周期与安全

### 15.1 错误分类

| 错误 | 行为 |
|---|---|
| 临时网络/限流 | 有限指数退避 + jitter，保持原 event_id，不确认 |
| 类型/权限/语法/不支持 DDL | 永久或人工介入状态，保留首因，停止对应 feed |
| COMMIT_UNKNOWN | 等待/查询终态/协调，不能盲目与新 owner 并发重试 |
| token过期 | 受控刷新并重试；刷新失败停止，不打印 token |
| spool配额满 | 背压并主动调度 MERGE，不是丢数据错误 |
| 入口信用耗尽 | block所属path，快速返回共享线程；等待信用重投，不重复progress登记 |
| 远端预算达到/观测过期 | REMOTE_CAPACITY_BLOCKED，停新raw；保留排空与安全维护路径 |
| stream失效/应用证据缺失 | RECOVERY_REQUIRED或REINITIALIZATION_REQUIRED，不静默重建空stream |
| ENOSPC/文件损坏/IO失败 | 停 admission，明确告警；不能假装写入成功 |
| compaction/回收失败 | 业务成功不回退，空间继续计费；达到运维阈值停止新输入 |
| metrics解析失败 | 业务终态独立；观测标不可用，不重复执行已提交业务 |

只保留一层主要 retry预算，避免内外两层循环把错误隐藏几十分钟。所有重试可取消，受单操作 timeout和feed级最大恢复策略约束。

### 15.2 正常关闭 vs 失租

正常关闭：停止 admission，按设定期限尝试 drain/MERGE，成功的确认，超时未完成不确认，再取消并关闭；不能无限 Background flush。

失租/强制终止：不执行“为了收尾再写一次”的新 mutation。停止工作，收集在途状态并交给接管协议。取消、等待和pool关闭需幂等；不得边清 spool边还有 worker读写。

### 15.3 安全与权限

TLS默认要求验证证书；最小权限覆盖目标 DML、必要建表/ALTER、内部元数据/stream，不要求仓库管理员。只读 Verify和有副作用初始化分离。标识符全量转义，业务值参数绑定；不同任务内部对象权限隔离。

本地 spool含业务明文可能敏感：受限目录权限、独占卷/宿主盘加密、保留与清理策略；不能因为“临时文件”就忽略数据保护。清理仅限实例拥有目录，不跟随不可信符号链接。

## 16. 可观测性、容量与成本模型

### 16.1 指标分层

- input accepted/bytes、spool memory/physical disk/reserved/metadata、排队最老年龄。
- raw committed rows/bytes、raw待应用批次、MERGE周期耗时和终态。
- target applied progress vs source resolved/checkpoint，分别展示；MAX(commitTs)不等于覆盖水位。
- quota/free-space waiting、ENOSPC、segment reclaim failure/orphan bytes、每feed公平性。
- DDL pending duration、owner generation/fenced、commit unknown query数、token刷新失败。
- target mutation counts和source event counts分开，失败读取计数时 unavailable，不编造 0。
- delivery credit/共享chunk已分配字节、blocked path、重投/重复转移、progress登记次数。
- OPEN/SEALED tracker数、pending分片、Seal等待年龄、duplicate completion、已应用未回收bytes。
- remote active/retained/reserved bytes、历史键上界、观测年龄、远端预算停写原因。
- no-op confirmed/消费证据缺失、ledger未终结operation与服务端终态查询失败。

标签避免无限高基数；事件原始值、PK、token不写常规日志。异常样本需脱敏、本机保存、访问受控。

### 16.2 容量估算

若平均编码输入速率为 R bytes/s、有效本地可用配额为 D，粗略缓冲时间 `T ≈ D / R`；编码放大、元数据、未回收segment与已有积压会缩短窗口。多feed还受共享预算和公平分配约束。

spool只是有限吸收抖动。长时间停机后必须在上游 GC/日志保留范围内恢复；容量不够要背压，不能承诺不停机/零延迟。内部 eventstore等其他磁盘也要计入运维容量。

### 16.3 成本对照

固定输入变更率/行宽/更新热点/表数/目标可见延迟后比较：

```text
旧成本 = TiCDC编码 + S3请求/容量/网络 + bend-cdc消费计算
       + Databend raw/MERGE/compaction + 运维资源
新成本 = TiCDC原生适配/spool IO/资源增量
       + Databend raw/MERGE/compaction + 网络 + 运维资源
```

必须记录 TiCDC CPU、RSS、磁盘吞吐/写放大、spool空间、Databend credit/query cost、MERGE次数、raw扫读量、小表对象数。不能仅用少一个 deployment证明总成本下降。

## 17. 全量初始化与 start-ts 接续

sink只做增量。独立初始化协议：

1. 选择可重现一致快照 S，固定 schema S。
2. 在保留窗口允许范围内启动/准备从 S接续的changefeed，确保增量不丢；可停留在受控bootstrap状态，不能让内存无限积压。
3. Dumpling导出、COPY INTO目标，使用同一mapper；全量失败可按任务批次恢复。
4. 校验全量数据/目标结构/元数据绑定，登记 baseline S。
5. 放行约定边界之后的增量；确认 TiCDC实际 start-ts 是排除还是包含边界，不能靠口头假设。

初始化期间既要容忍 DDL，也要明确是否禁止 DDL或按快照版本隔离；不能查询最新 schema处理旧快照。全量导入不能在增量写完后覆盖回旧状态。必要的全量S3文件与增量链路去S3不矛盾。

## 18. 恢复矩阵与反例

| 中断点 | 允许的残留 | 预期恢复 | 不允许做什么 |
|---|---|---|---|
| 内存/磁盘 spool 后，raw前 | 本地临时文件 | 从checkpoint重放 | 把spool落盘当确认 |
| raw成功、响应丢失 | 相同event_id重复raw | 协调后幂等重试 | 新增本机时间当业务版本 |
| raw成功、MERGE前 | raw积压/stream增量 | 回放源并重新应用 | 只依赖丢失的pendingEvents内存map |
| MERGE明确失败 | target未推进 | 重试/人工修复 | 触发PostFlush |
| MERGE/COMMIT结果不明 | SQL可能仍执行 | COMMIT_UNKNOWN协调 | 新owner立即并发DDL/清理 |
| MERGE成功、callback前 | target已更新 | 重放不回退 | 清除最新墓碑 |
| callback后checkpoint保存前 | 重复source事件 | 幂等收敛 | 把重复当新写入时间更晚版本 |
| MERGE成功、compaction失败 | raw大、spool可能已释放 | 保留APPLIED，重试维护/限制新输入 | 重复业务操作修复统计 |
| entry释放、segment删除失败 | 真实磁盘仍占用 | 继续计费、重试回收/告警 | 配额假释放继续写满 |
| DDL pending后 | journal存在、目标旧schema | fingerprint确定后执行 | 跳过检查直接applied |
| ALTER成功、journal未applied | 目标新schema | reconcile并隔离旧DML | 旧行按新列名写入 |
| applied TRUNCATE后重启 | 新代际数据可能已存在 | 跳过同DDL，恢复正确代际 | 再次清空目标 |
| capture永久丢失 | 本地spool丢失 | 新owner在fence后从checkpoint重放 | 依赖本地磁盘保证数据可恢复 |
| stream过期/内部对象被删 | 恢复证据缺失 | 停止并进入显式重建协议 | 静默新建空stream丢旧raw |
| F1已应用、F2未登记 | OPEN tracker、F1可已回收 | 不回调；重启从checkpoint重放整个源事件 | 只看registered==applied就确认 |
| delivery已登记progress但未转入sink | 上层仍有lease/blocker | 同session按deliveryID重投转移，或取消恢复 | 再加一遍progress、丢chunk计费 |
| no-op成功但消费未知 | target未改/stream可能未推进 | 核验版本和消费证据，未证明就阻塞 | counts=0当失败循环重刷或当成功直接确认 |
| 远端持续新键churn | target小、raw墓碑持续增大 | 到远端预算停新raw，显式扩额/重建 | 以本地spool已排空宣称容量安全 |

关键反例：

- DELETE后延迟INSERT复活：检验I2/I9。
- 高于MERGE快照的新raw抑制旧candidate却未应用：检验§7串行lane和coverage。
- 生产者拿着表锁等磁盘，MERGE也要同锁：检验I6。
- 同一segment一个长事务钉住空间，其余全释放：检验I7和物理回收。
- 失租旧TRUNCATE晚于新owner写入提交：检验I3/I8，幂等DML不能解决。
- 初始schema updateTS被误当已应用DDL fence导致漏首批数据：检验I10。

## 19. 测试与验收计划

### 19.1 分层验证

| 层级 | 验证内容 | 证据要求 |
|---|---|---|
| 单测/property | mapper、key规范化、状态机、预算、回调exactly-once-in-process | 固定seed与随机序列，包含错误路径 |
| mock客户端 | commit unknown、cancel、权限、retry、回收失败 | 精确检查未完成事件不确认 |
| 真实Databend | stream/MERGE、no-op、事务、session、原子恢复 | 固定server/driver版本、SQL记录与最终表状态 |
| TiDB/TiCDC E2E | 原生before/after、DDL、快照边界、回放、故障调度 | 固定版本与事件时序，checksum/count/schema |
| 并发/资源 | race、内存、磁盘、并发feed、满盘恢复 | 实际RSS/allocated bytes、死锁与泄漏检查 |
| 灰度/成本 | 影子目标、同SLO、费用与可见延迟 | 不用本地单测替代线上或计费证据 |

### 19.2 必测清单

1. 空批、低流量、热点更新、大表、小表海量、混合表大小、整库空表。
2. PK/复合PK、变键、多行同事务、换键、同TSO净变化；不支持表在发现时拒绝。
3. 所有§13边界值、字段缺失vsNULL、case/collation、生成列与DDL后mapper刷新。
4. insert/update/delete/reinsert、乱序、重复、同event_id异payload冲突。
5. 每个§18中断点；网络断开、超时、服务端SQL继续运行、节点失租。
6. PENDING/APPLIED DDL、无后续DML、drop/re-add同名列、TRUNCATE多步中断。
7. 多feed共享配额、热/冷表公平性、segment pin、孤儿清理、ENOSPC/inode耗尽。
8. 配额达到但merge-trigger未达到、等待者被取消、长事务超过可用spool、单行超限。
9. stream没新数据、只有重复或被抑制旧数据、no-op MERGE、SELECT是否消费、回滚后cursor。
10. 正常Close、失租Close、快速重复start/stop、token刷新、所有权切换、目标被外部改写。
11. 同名不同库映射、目标schema已存在但不兼容、内部对象属于别的任务。
12. 快照S前后写入与DDL、源GC保留不足、stream失效和恢复失败。

### 19.2.1 v0.2新增的可执行验收矩阵

下面每项均未执行；它们是实现/P0必须交付的测试，不是本文已通过的证据。

| ID | 注入点与负载 | 必须断言 | 关闭项 |
|---|---|---|---|
| V01 | 固定PK仅改二级UK，分别raw=true/false，切分cycle并在两半间重启 | 正式模式只输出正确净变化；最终仅新值，无版本冲突/迟到DELETE破坏；迁移重注册仍raw=true | B05 |
| V02 | F1先APPLIED，暂停F2登记；并发Seal/Apply/重复完成 | OPEN时callback=0，全集完成后=1；已APPLIED分片可提前回收 | B17 |
| V03 | 累计事件大于spool，可用空间小于事件总量，多个有界delivery | 编码/应用/回收循环前进；所有分片未完成前不确认；入口chunk/元数据不越预算 | B08/B16/B17 |
| V04 | A/B共用dynstream线程；A用尽path信用，预留B信用且下游健康 | A handler快速返回；B DML/DDL在测试设定的有界期限推进；重投progress/lease各一次 | B16 |
| V05 | 多DMLEvent共享大chunk，wire小但解码膨胀；断session/取消 | chunk容量只计一次且释放恰一次，无丢计/双计；超限在分配前拒绝；RSS剖析含临时内存 | B08/B16 |
| V06 | 热/冷表共享segment；配额满，冷表未达普通阈值；unlink/搬迁失败 | 受压引用lane均被唤醒；保留池可排空；不假释放物理quota，无无限busy loop | B07/B13 |
| V07 | 首结果页后注入服务端失败/传输断开；提交后响应丢失；计数解析失败 | 前两种无独立证据不ACK；已提交统计失败不重做；UNKNOWN可跨重启协调或明确停写 | B03 |
| V08 | 阻断Kill接口，延迟旧TRUNCATE/compaction，新owner请求接管 | Kill nil不放行，固定sleep不放行；取得可信终态后才能恢复写 | B01 |
| V09 | 实际no-op SQL各形态，旧/含修复build对照；事务提交/回滚 | 目标可0修改但消费证据与callback正确；无planner短路漏消费；重试标签不偷消费下一批 | B02 |
| V10 | R0–R5每步前后崩溃；R5写新数据后回放旧TRUNCATE | 新数据不被再清空；unknown query先协调；journal缺失/冲突不猜测 | B04/B09 |
| V11 | stream依赖snapshot过期/内部对象删除 | 可诊断停写；恢复不跳过遗留raw；不足证据进入显式重初始化 | B10/B14 |
| V12 | schema S分页时并发空表ADD/DROP/re-add，bootstrap中断 | 每表按正确历史schema接DDL；无首批漏写/错列 | B04/B15 |
| V13 | target恒定小、持续新键churn，compaction始终成功 | 远端预算触发停新raw；统计过期也停写；不删最新墓碑；多表在途增长预留一致 | B06/B18 |

V07的“服务端先出数据后失败”可先用mock验证客户端分类，但真实MERGE是否可出现该时序须另记录，不把mock当服务端复现。V04对B的期限应由固定压测配置定义；不在全局资源耗尽时作无法兑现的公平性承诺。

### 19.3 属性/模型测试建议

构建内存reference model：按源净事件顺序维护 PK→值/墓碑，随机插入重复、乱序、cancel、重启和DDL边界，比较“完成全部已确认覆盖后”的目标最终状态。

断言：确认集合不超出已证明APPLIED；每个回调最多一次；预算无负数/超额预留；无未计费文件；取消后无新mutation；旧DDL/旧epoch不污染新代际。

### 19.4 发布门槛

必须关闭所有会造成数据丢失、错误确认、双owner、不可恢复DDL、资源无界的阻塞项。性能不足可以限制规模后发布PoC；正确性缺口不能只通过缩短flush interval掩盖。

## 20. 上线、回退与交付拆分

### 20.1 阶段

| 阶段 | 交付物 | 退出标准 |
|---|---|---|
| P0语义验证 | 原生事件契约、tracker、入口信用、终态/fence四份协议；server/driver/SQL能力实验 | V01/V02/V04/V07/V08/V09设计/原型证据；无法证明fence只能限制隔离单capture实验，不能开放生产HA |
| P1纵切PoC | 原生scheme/config、跨层信用接入、SEALED tracker、spool、单表PK批写/MERGE、远端预算 | 受限单owner故障重放与资源有界，包含UK UPDATE与大事件；不能跳过端到端入口预算 |
| P2核心对齐 | 复合PK/类型/建表/空表、DDLjournal/恢复/TRUNCATE | 支持矩阵所有已承诺项完成E2E |
| P3灰度 | 影子目标、切换/回退、容量与成本结果 | 正确性、可见延迟、恢复和费用均验收 |
| P4可选扩展 | 分区、跨节点汇聚、typed staging、高并行raw | 单独RFC与不变量证明 |

此前“PoC约1–2周”的估算未包含完整共享配额/owner/DDL/真实故障覆盖，不能当本详细范围的排期承诺。先做P0再按已关闭风险重估。

### 20.2 灰度切换

旧链继续写旧目标，新sink写独立影子目标，不能共用raw/stream/journal。固定初始化S和对照水位，验证目标checksum/count/schema与业务查询，连续观察后切读，再停旧链。

影子对照必须明确水位：任意时刻比较两个异步目标的行数可能自然不同，不直接当错误。新sink不保证全changefeed在同一时刻的snapshot read，需要在受控drain/已对齐边界比较。

### 20.3 回退

保留旧链数据与checkpoint覆盖窗口；先让旧链追平到验证边界再切读。不把新sink内部raw/journal当旧bend-cdc文件游标，不直接复用内部表。

升级涉及raw schema、key编码、schema journal或spool格式需版本化。临时spool可丢弃重放不等于目标journal可丢；回退二进制必须理解已有持久化元数据，否则阻止启动。

## 21. 已知不足、待验证假设与生产阻塞项

| ID | 风险/未闭合问题 | 当前拟定措施 | 关闭所需证据 |
|---|---|---|---|
| B01 | 各mutation的fencing/旧在途SQL与新owner重叠未证明 | §12分DML/DDL/stream/维护排他；取消返回值不是证据 | V08及完整registry、正常迁移/异常接管；非HA准入不能只配across-nodes=false |
| B02 | stream+MERGE提交/快照/no-op/cursor/applied witness未实测 | 固定build及补丁、§8.5/§9.7覆盖和消费证明 | V09、实际SQL与版本矩阵、atomic witness或等价算法，含planner短路 |
| B03 | QueryContext/Rows/COMMIT不确定结果判定与重启定位 | §9.4证据结构、错误分类、durable operation ledger；UNKNOWN停写 | V07、真实endpoint/字段/保留期、首响应丢失/查不到query及ledger自身不确定结果 |
| B04 | start-ts完整schema bootstrap/空表与恢复API接入未定稿 | 权威snapshot与窄recovery接口 | 既有空表、schema变更并发、重启E2E |
| B05 | 普通二级UK更新拆分与PK/换键净事件契约 | §11.4固定raw=true贯通注册；真实handle变更另验 | V01、迁移重注册、源合法事务dump与checksum；不能只测试PK UPDATE |
| B06 | raw相同event_id物理重复与墓碑长期容量 | 逻辑去重+安全compaction，禁止TTL删最新 | 等值重复清理SQL、stream不丢数据、增长基准 |
| B07 | node级预算与其他磁盘消费者共盘 | 全feed共享budget+free guard，建议独占卷 | 多feed/多组件满载测试、物理字节计费 |
| B08 | 源chunk/序列化/blob/readback/callback瞬时内存 | 分阶段预算，禁止无界channel和oversized旁路 | 内存剖析、极端行/事务压力与race测试 |
| B09 | DDL journal不是现成原生恢复协议，TRUNCATE多步 | before/after fingerprint、generation、skip/recovery接入 | 所有DDL中断点/旧DML重放测试 |
| B10 | STREAM企业能力、许可、保留窗口/失效 | 明确capability校验，默认不静默fallback | 实际warehouse版本/权限/许可与失效演练 |
| B11 | 跨表原子性不提供，目标读取可能见部分事务 | 产品明确语义，影子比对在受控边界 | 使用方接受与文档/测试一致 |
| B12 | 性能与成本尚无数据；保守串行lane可能限制热点吞吐 | P3同SLO benchmark，先保正确性 | 固定负载和费用证据 |
| B13 | 跨lane segment pin与压力/回收活性 | §6.8反向引用、压力广播、保留排空池；搬迁需双份预算 | V06，热/冷表、删除失败、资源不足不假释放；不以原版有定时器就认为已解决 |
| B14 | stream失效后的安全恢复/重新初始化 | §9.8冻结、证据核验；不够证据只允许显式重初始化 | V11，full-raw恢复若未实现不得宣称自动修复 |
| B15 | 历史空表bootstrap与DDL/schema竞态 | §10.6同一S快照、bootstrap epoch、顺序DDL | V12，分页/同名列重建/中断，不能以最新schema兜底 |
| B16 | collector→dynstream→sink入口信用与共享chunk责任 | §6.7新增capability、deliveryID、progress只登记一次、跨层lease | V03/V04/V05、协议batch上限、兼容旧sink、真实分配/RSS与控制事件推进 |
| B17 | 动态分片全集封口与独立回收 | §5.2 OPEN/SEALED/COMPLETED、先登记后发布、once完成 | V02/V03及race/失败/取消；flush闭包自身不是once |
| B18 | 成功维护下远端历史键增长/观测过期 | §9.6有限远端预算、增长预留、停写/扩额/重建流程 | V13，active/retained计量与费用，不能借删除最新墓碑降量 |

本轮B01–B18均未因“写了设计”关闭。B01–B10、B13–B18若影响受支持场景的正确性/资源边界，必须在该场景发布前用实现和实验关闭；不能以缩短flush interval或新增风险编号代替。B11需明确语义接受，B12需按固定SLO验收。首版范围缩减必须可执行，不能写一个无法在源事件上识别的“禁用PK更新”开关冒充保护。

## 22. 替代方案与取舍

| 方案 | 优点 | 代价/拒绝原因 |
|---|---|---|
| 保留S3+bend-cdc | 变动小，有对象存储缓冲/回放 | 中间编码/消费成本仍在，可作为fallback |
| TiCDC纯内存直写 | 实现较简单 | 已被用户否决；无法满足第一版磁盘spool要求 |
| 原生sink + 本地spool + raw/stream/MERGE | 复用核心语义、去中间链、有限吸收抖动 | owner/DDL/提交/预算仍需正式实现；本文选择 |
| raw落盘立即确认，后台异步MERGE | 易释放本地资源、吞吐可能更高 | 需要独立durable任务恢复、applied水位和接管；第一版不选 |
| 每条直接UPSERT/DELETE目标 | 链路短 | 小SQL多，成本高；旧事件防回退和删除墓碑仍需解决 |
| 每batch临时表然后MERGE并删除全部staging | 局部逻辑简单 | 丢失跨批次watermark/tombstone，迟到重放可能回退 |
| 多capture直接共享同一stream | 表面并发高 | ownership/覆盖与DDL竞态未解决，不选 |
| 无stream，全raw扫描MERGE | 不依赖stream许可 | 扫描成本和coverage策略不同，需独立验收，非静默fallback |
| typed staging/独立watermark表 | 可能减少JSON与raw历史成本 | schema/事务复杂度高，放后续独立优化 |

## 23. 发给 reviewer 的具体问题和输出约定

请优先寻找以下反例，每条提供触发条件、事件序列、违反的不变量、影响和最小修正：

1. MERGE后才确认的coverage定义，在重放、空stream和旧事件被抑制时是否充分？
2. 单表raw/MERGE串行是否仍存在进程间/接管竞态？什么条件才能真正排除？
3. 本地磁盘配额计费能否被segment pin、回收失败、孤儿文件、其他capture绕过？
4. 超大事务、callback tracker和chunk引用会不会让spool有界而RSS仍无界？
5. 配额不足时是否有消费者等待生产者、DDL等待callback、callback等待DDL的循环？
6. retained raw历史与目标schema变更是否会混用epoch？初始bootstrap是否可能错误跳过DML？
7. PK变键/同事务换键是否存在无法靠commitTs消歧的合法输入？
8. 有无在不提供目标侧fence时仍安全自动HA的证明？如果没有，应缩减什么承诺？
9. DDL结果不确定、TRUNCATE部分完成和旧owner晚到SQL如何收敛？
10. stream无业务行更新时是否一定消费？许可/失效/保留期会不会让恢复停摆？
11. 同event_id重复物理记录的compaction是否可能删光水位，或无限累积？
12. 固定镜像DELETE语义后，已有目标数据、collation、full-image缺失等前置条件是否仍不可执行或存在漏洞？
13. 此设计是否真的完整覆盖bend-cdc“建表、批写、MERGE、类型改写、DDL”核心能力？遗漏请逐项列出。
14. 配置数量/共享模块改动是否过度设计？能否在不破坏不变量下缩小首版？

输出建议：`结论(接受/修改后接受/拒绝) → P0/P1/P2发现 → 每项证据/时序/修复 → 发布阻塞 → 可后移优化`。不要以测试没跑作为全部结论；需要指出该测试要证明哪个语义。也不要替本文补全缺失假设后默默给通过。

## 24. 证据、实际验证与可复核性

### 24.1 已执行与未执行

- 本会话前序已执行 bend-cdc：`go test ./pkg/sink/databend ./pkg/source/tidb -count=1`，两包通过；这是已有代码的本地测试结果，不是新sink验证。
- 未配置真实Databend测试连接，相关集成测试按条件跳过。未跑真实stream/事务、TiDB/TiCDC新sink E2E、故障注入、benchmark或部署。
- 本次新增/修改Markdown并检查格式、代码证据引用；未实现新配置或SQL，未构建TiCDC，未创建分支/提交/PR。
- 编写时读取的是固定Git对象与当前bend-cdc源码；公开文档仅用于能力与限制背景，不能代替部署版本/真实运行证据。

### 24.2 新架构TiCDC固定源码索引

以下链接基于已用git对象读取的commit；远程仓库可见性受reviewer权限影响，无法打开时使用§0的git show。引用的固定路径是Git对象路径，不要求当前旧架构checkout存在对应文件。

- **T01**：[Sink接口与factory](https://github.com/sdojjy/tiflow/blob/2bf6dacd0aec9ac2fad464600e47341f9aa951f2/downstreamadapter/sink/sink.go)。
- **T02**：[DMLEvent、原生行与回调](https://github.com/sdojjy/tiflow/blob/2bf6dacd0aec9ac2fad464600e47341f9aa951f2/pkg/common/event/dml_event.go)。
- **T03**：[Spool、TryEnqueue、oversized与目录生命周期](https://github.com/sdojjy/tiflow/blob/2bf6dacd0aec9ac2fad464600e47341f9aa951f2/pkg/sink/spool/spool.go)。
- **T04**：[S3 buffer manager与完整payload构造](https://github.com/sdojjy/tiflow/blob/2bf6dacd0aec9ac2fad464600e47341f9aa951f2/downstreamadapter/sink/cloudstorage/buffer_manager.go)。
- **T05**：[S3 writer、写入后回调与release](https://github.com/sdojjy/tiflow/blob/2bf6dacd0aec9ac2fad464600e47341f9aa951f2/downstreamadapter/sink/cloudstorage/writer.go)。
- **T06**：[TableSchemaStore的sink-type需求](https://github.com/sdojjy/tiflow/blob/2bf6dacd0aec9ac2fad464600e47341f9aa951f2/pkg/common/event/table_schema_store.go)。
- **T07**：[dispatcher manager的MySQL恢复分支](https://github.com/sdojjy/tiflow/blob/2bf6dacd0aec9ac2fad464600e47341f9aa951f2/downstreamadapter/dispatchermanager/dispatcher_manager.go)；[merge task恢复分支](https://github.com/sdojjy/tiflow/blob/2bf6dacd0aec9ac2fad464600e47341f9aa951f2/downstreamadapter/dispatchermanager/task.go)。
- **T08**：[capture写许可gate](https://github.com/sdojjy/tiflow/blob/2bf6dacd0aec9ac2fad464600e47341f9aa951f2/pkg/writelease/write_gate.go)。
- **T09**：[DDL原生事件](https://github.com/sdojjy/tiflow/blob/2bf6dacd0aec9ac2fad464600e47341f9aa951f2/pkg/common/event/ddl_event.go)；[dispatcher屏障](https://github.com/sdojjy/tiflow/blob/2bf6dacd0aec9ac2fad464600e47341f9aa951f2/downstreamadapter/dispatcher/basic_dispatcher.go)。
- **T10**：[S3 writers创建spool及上游队列](https://github.com/sdojjy/tiflow/blob/2bf6dacd0aec9ac2fad464600e47341f9aa951f2/downstreamadapter/sink/cloudstorage/dml_writers.go)。
- **T11**：[UPDATE的UK拆分，L970–1014](https://github.com/sdojjy/tiflow/blob/2bf6dacd0aec9ac2fad464600e47341f9aa951f2/pkg/eventservice/event_scanner.go#L970-L1014)；[IsUKChanged，L219–259](https://github.com/sdojjy/tiflow/blob/2bf6dacd0aec9ac2fad464600e47341f9aa951f2/pkg/common/event/codec.go#L219-L259)。
- **T12**：[raw-change初始化，L320–350](https://github.com/sdojjy/tiflow/blob/2bf6dacd0aec9ac2fad464600e47341f9aa951f2/downstreamadapter/dispatchermanager/dispatcher_manager.go#L320-L350)；[dispatcher同步交付与progress先登记，L295–335](https://github.com/sdojjy/tiflow/blob/2bf6dacd0aec9ac2fad464600e47341f9aa951f2/downstreamadapter/dispatcher/basic_dispatcher.go#L295-L335)；[session注册传播，L590–655](https://github.com/sdojjy/tiflow/blob/2bf6dacd0aec9ac2fad464600e47341f9aa951f2/downstreamadapter/eventcollector/dispatcher_session.go#L590-L655)。
- **T13**：[dynstream同步Handle与path blocking，L340–356](https://github.com/sdojjy/tiflow/blob/2bf6dacd0aec9ac2fad464600e47341f9aa951f2/utils/dynstream/stream.go#L340-L356)。
- **T14**：[collector提前AssembleRows并交付，L360–395](https://github.com/sdojjy/tiflow/blob/2bf6dacd0aec9ac2fad464600e47341f9aa951f2/downstreamadapter/eventcollector/dispatcher_stat.go#L360-L395)。
- **T15**：[across-nodes配置定义，L34–38](https://github.com/sdojjy/tiflow/blob/2bf6dacd0aec9ac2fad464600e47341f9aa951f2/pkg/config/scheduler_config.go#L34-L38)；[控制初始span拆分，L201–208](https://github.com/sdojjy/tiflow/blob/2bf6dacd0aec9ac2fad464600e47341f9aa951f2/maintainer/span/span_controller.go#L201-L208)。

### 24.3 bend-cdc源码索引

下列本机链接在本报告编写时的bend-cdc commit上有效；异地reviewer应按§0的bend-cdc基线读取同路径的Git对象，不能以未来HEAD替代。C编号只表示源码，不是§21的B编号阻塞项。

- **C01**：[TiDBRunner](/Users/jiangjianyuan/work/git/pingcap/webapi/bend-cdc/pkg/service/tidb_runner.go)。
- **C02**：[DatabendMergeSink、PK/schema/转换](/Users/jiangjianyuan/work/git/pingcap/webapi/bend-cdc/pkg/source/tidb/databend_merge_sink.go)。
- **C03**：[CDCManager、raw/stream/MERGE/compaction](/Users/jiangjianyuan/work/git/pingcap/webapi/bend-cdc/pkg/sink/databend/databend_cdc.go)。
- **C04**：[DDL翻译](/Users/jiangjianyuan/work/git/pingcap/webapi/bend-cdc/pkg/source/tidb/schema_ddl.go)；[schema元数据与建表](/Users/jiangjianyuan/work/git/pingcap/webapi/bend-cdc/pkg/source/tidb/schema_meta.go)。
- **C05**：[列变更白名单](/Users/jiangjianyuan/work/git/pingcap/webapi/bend-cdc/pkg/sink/databend/schema_change.go)；[epoch](/Users/jiangjianyuan/work/git/pingcap/webapi/bend-cdc/pkg/sink/databend/schema_epoch.go)；[TRUNCATE](/Users/jiangjianyuan/work/git/pingcap/webapi/bend-cdc/pkg/sink/databend/truncate.go)。
- **C06**：[类型与连接](/Users/jiangjianyuan/work/git/pingcap/webapi/bend-cdc/pkg/sink/databend/databend.go)；[TiDB值转换](/Users/jiangjianyuan/work/git/pingcap/webapi/bend-cdc/pkg/sink/databend/tidb_values.go)。
- **C07**：[MERGE结果判定](/Users/jiangjianyuan/work/git/pingcap/webapi/bend-cdc/pkg/sink/databend/merge_result.go)。
- **C08**：[applied watermark](/Users/jiangjianyuan/work/git/pingcap/webapi/bend-cdc/pkg/source/tidb/applied_watermark.go)；[Dumpling初始化](/Users/jiangjianyuan/work/git/pingcap/webapi/bend-cdc/pkg/source/tidb/dumpling_executor.go)。

### 24.4 官方文档证据边界

1. [MERGE语法](https://docs.databend.com/sql/sql-commands/dml/dml-merge)：支持source/subquery驱动的增删改；不证明源端幂等算法。
2. [CREATE STREAM](https://docs.databend.com/sql/sql-commands/ddl/stream/create-stream)：append-only选项、Enterprise标记；需实际部署许可验证。
3. [Stream工作流](https://docs.databend.com/guides/load-data/continuous-data-pipelines/stream)：提交消费与并发限制的公开说明；MERGE/no-op组合仍需实验。
4. [Transaction](https://docs.databend.com/sql/sql-commands/ddl/transaction)：事务控制入口；不据此假设所有DDL/owner校验支持所需隔离与原子性。
5. [BEGIN中DDL限制](https://docs.databend.com/sql/sql-commands/ddl/transaction/begin)：DDL提交当前多语句事务；不能泛化DML事务fence到DDL。
6. [no-op stream offset修复#20671](https://github.com/databendlabs/databend/pull/20671)：已合并版本相关修复，明确存在未覆盖的planner短路；不证明目标部署已包含，也不是所有空MERGE的规范定义。

### 24.5 驱动证据与本轮验证边界

- **D01**：bend-cdc go.mod引用databend-go v0.9.4；本机读取[waitForData，rows.go L27–50](/Users/jiangjianyuan/go/pkg/mod/github.com/datafuselabs/databend-go@v0.9.4/rows.go#L27-L50)、[KillQuery，client.go L746–767](/Users/jiangjianyuan/go/pkg/mod/github.com/datafuselabs/databend-go@v0.9.4/client.go#L746-L767)。异地reviewer需获取相同版本，不把本机路径当远程可用。
- 本次读取文件SHA256：rows.go `065c09c10a3a7b334da795949de1b717a5fd0014000d55be80e5fbd4fb5da6b9`；client.go `b3d9b42925ae4d584e84afbc244bfa97096eb5591eda257ecd2747894a35aecf`。这是文件内容指纹，不代表已运行驱动集成实验。
- v0.2只增加协议/状态转移/验收条件并更新评审入口；V01–V13均未实际执行。Markdown结构、TOML语法、文件/Git对象引用检查与新sink正确性验证严格分开。
- 不把独立评审的P0/P1标签当复现证据；例如no-op规范、PK互换SQL合法性、Kill接口能力、永久丢数据时序，都需按本文限制重新核验。没有创建新review agent、没有切分支、没有修改业务Go代码。

---

**最终建议**：方向可行，进入独立评审和P0验证；不要直接宣布可生产实施。优先要求reviewer击穿owner fencing、提交结果判定、DDL恢复和资源硬预算这四条关键路径，而不是只讨论字段命名或批大小。
