# TiCDC 原生 Databend / Lake Warehouse Sink：代码分析与迁移计划

日期：2026-10-24。状态：历史分析摘要；当前设计已评审定稿，首版实现及受控真实链路验收通过，未提交、未部署共享CDC。

当前规范见 [定稿设计 v1.2](databend-native-sink-design-review.md)，配套 [实现评审入口](databend-native-sink-review-prompt.md)。本文件保留为初始分析历史摘要，不是当前实施规范；唯一主路径与范围以v1.0为准。实现分支为 `feat/databend-native-sink`；v0.2/v0.3已另存历史版本，不同时实施。

v0.4.1局部修正：首版raw-change=false保持默认拆分和源顺序，首版固定raw source；Native只规范化一次；UNKNOWN在核心内层截断；spool清理不挂成功回调且不进oversized旁路；add_time按落库精度单调；DDL epoch明确取FinishedTs并迁移外层过滤。上述为已接受的范围；首批核心、运行时接入与待验收状态见定稿§8.1–8.2/§10，没有扩大框架范围。

最新裁决：对齐bend-cdc既有正确性，迁移raw/stream或raw source/MERGE、mapper和schema epoch核心，只新增native adapter、spool与TiCDC callback适配。删除manifest/K-H-S新算法、DML ledger与新raw_generation，不因review无限增设框架协议。保留MERGE后确认、SEALED/分片独立回收和必要失败关闭；stream全批覆盖、终态、原生事件与旧路径差异要局部验证。当前有T1–T6的部分本地覆盖及[9组真实TiUP/Lake功能验收](./databend-native-sink-e2e-report.md)，在途故障/HA与成本仍未验收，T1–T7不整体关闭；无全量工具工作包。

需求修订（2026-10-24）：第一版必须采用新架构 S3 sink 类似的 **内存缓冲 + 有配额本地磁盘 spool + 背压**，不再采用仅内存攒批的初版建议。磁盘 spool 是临时缓冲，不是恢复日志；确认点仍是 MERGE 成功。

当前基线已迁至官方 `https://github.com/pingcap/ticdc` 的 `b9179182f`，本文件其余内容保留历史分析；实际差异/验收以[官方迁移报告](./databend-native-sink-upstream-port.md)为准。

## 1. 结论与边界

可行，建议新增独立的 Databend sink，直接消费 TiCDC 的原生 DML/DDL 事件，迁移 bend-cdc 的 **schema/type mapping + raw staging + stream + MERGE + ordering/tombstone + schema epoch**，不搬运它的对象存储消费者。

本文的 lake 指 bend-cdc 当前实际连接的 Databend SQL warehouse，不是泛化的 Iceberg/Delta/Hudi 文件 sink。如果目标接口不同，需要另做 writer/事务语义设计。

现状：

```text
TiDB → TiCDC → Canal JSON 编码 → S3/OSS
     → bend-cdc LIST/GET/SQS → Canal JSON 解码
     → schema/类型转换 → Databend raw table → stream → MERGE → target
```

建议：

```text
TiDB → TiCDC DMLEvent / DDLEvent
     → Databend sink [原生类型适配 + schema barrier + 有界批处理]
     → 有界内存 / 限额本地磁盘 spool（超限背压）
     → Databend 持久化 raw table → append-only stream → MERGE → target
                                                         ↓
                                              成功后触发 PostFlush
```

去掉的是增量链路中的 Canal JSON envelope、文件生产/扫描/下载、独立 bend-cdc consumer，以及相应 CPU、内存和网络开销。**不去掉** Databend 自身存储、raw 写入、MERGE、必要的重放水位，也不承诺完全没有 JSON：第一版仍可用 raw_data JSON 承载行内容。

代码分析基线：webapi `b6481621021e82aee659ad88e8e6c00f6dd11906`，ticdc `2bf6dacd0aec9ac2fad464600e47341f9aa951f2`，调查开始时两边 working tree 干净。均为本地源码证据，不代表生产版本。

## 2. 现有实现真正要迁移的内容

### 2.1 路径与职责

| 模块 | 已确认行为 | 迁移处理 |
|---|---|---|
| `service/tidb_runner.go` | TiDB 专用 runner，明确绕过通用 channel pipeline，保存对象游标 | 不迁 runner；让 TiCDC 管生命周期/checkpoint |
| `source/tidb/canal_json.go` | 读取 pkNames/mysqlType/_tidb.commitTs，文件分类与 schema version | 不迁解码；从 DMLEvent/TableInfo 取信息 |
| `source/tidb/manager.go`、`schema_queue.go` | 表级队列、存储进度、schema 对象排在新 epoch DML 前 | 改为 TiCDC dispatcher + sink barrier |
| `source/tidb/databend_merge_sink.go` | 每目标表一个 DatabendSink；PK 推导、库表映射、schema epoch、事件转换 | 迁移语义，重写入口及资源管理 |
| `sink/databend/databend.go` | 连接、建表、DESCRIBE cache、类型推导、加列 | 迁移；用权威 TableInfo 替代首行猜类型 |
| `sink/databend/databend_cdc.go` | raw 批量参数化 INSERT、append-only stream、MERGE、去重、重试、清理 | 核心迁移；重写调度/确认/所有权 |
| `sink/databend/schema_change.go`、`schema_epoch.go`、`truncate.go` | 列变更白名单，pending/applied epoch，清空 target/raw/stream | 迁移并接入 TiCDC DDL 恢复流程 |
| `sink/databend/tidb_values.go`、`enum_set.go` | Canal 字符串格式的 binary/ENUM/SET/JSON 还原 | 迁移语义和测试，不照搬 Canal 专用表示 |
| `source/tidb/applied_watermark.go` | 区分对象消费进度与 MERGE 可见进度 | 保留“可见才确认”的契约，不迁对象轮询算法 |
| `source/tidb/dumpling*.go` | 全量 schema、CSV、COPY INTO、快照元数据 | 独立保留，不能用增量 sink 自动替代 |

**不要选错入口**：`source/tidb/databend_sink.go` 是简单 INSERT/DELETE 路径（DELETE 固定取 id），实际 TiDB runner 创建的是 `DatabendMergeSink`。只迁前者会丢失现有核心能力。

### 2.2 “临时表”实际上是持久化状态

当前建的是普通 `<table>_cdc_raw`，不是 TEMP TABLE。包含 `id/table_name/raw_data/add_time/event_pos/action`；另外创建 `<table>_cdc_stream`，使用 `APPEND_ONLY = true`。

`buildMergeSQL` 使用 ROW_NUMBER 按冲突键选最新事件；严格顺序模式还查 retained raw history，拒绝被更大 event_pos 覆盖的旧事件。DELETE 同样保留最新 raw 记录作为墓碑。`buildRawCompactionSQL` 删除旧版本而保留每键最新水位，避免旧事件重放后复活已删除数据。

所以迁移目标应是 **持久化 staging + 安全 compaction**，不能在 MERGE 后直接清空全部 raw，不能只按 24h TTL 删除最新墓碑。保留每键水位意味着空间通常随历史键集合增长，删除过的键也会占空间；它不是固定大小缓存。

## 3. TiCDC 已有接入能力，以及不能漏掉的改动

`downstreamadapter/sink/sink.go` 已有统一接口：

- `AddDMLEvent` 接受含 TableInfo、CommitTs、Row/PreRow 的原生事件。
- `FlushDMLBeforeBlock` 是 DDL/syncpoint 前的 flush/barrier 钩子。
- `WriteBlockEvent` 成功后需要调用事件 `PostFlush`。
- `Run/Close/IsNormal` 管理生命周期及错误。
- `SetWriteGate` 要求在真实下游写边界再次检查写许可。
- `AddCheckpointTs` 接收已经计算出的 changefeed checkpoint，不是写入完成确认接口，不能反过来用它触发必须先完成的 MERGE，形成进度依赖环。

建议新目录：

```text
downstreamadapter/sink/databend/       # 事件适配、队列、barrier、callback、生命周期
pkg/sink/databend/                    # 客户端、配置、schema、类型、raw、merge、恢复
tests/integration_tests/databend_sink/ # 真实 TiDB/TiCDC/Databend 故障与正确性测试
```

配置建议 `databend://...` + 独立 `DatabendConfig`；不是 MySQL scheme，也不是 Canal JSON protocol。下面是设计项，当前均未实现：

1. sink `New/Verify` 分支、scheme 常量、独立 `DatabendSinkType`。
2. `pkg/config/sink.go`、changefeed config 的默认值、校验、克隆/序列化/更新、`RmUnusedFields`，避免新增字段被清掉。
3. `pkg/common/event/table_schema_store.go` 新 sink 的需求分支；现有默认分支会 panic。DB/All 级 DDL 所需表集合要明确维护。
4. dispatcher/dispatchermanager 中 sink-type 分支、恢复、routing、调度兼容性；不能只改 factory。
5. 不能返回 `MysqlSinkType` 偷用流程：manager 和 task 中会将 sink 强转成 `*mysql.Sink` 调 `GetTableRecoveryInfo`。
6. 将必要恢复能力抽成窄接口，或新增 Databend 专属恢复分支；不借此重构所有 sink。
7. URI/DSN/token 脱敏、TLS、token 刷新、权限预检、metrics、示例配置、mock/集成测试。

生产实现不建议直接 import 整个 bend-cdc module：会耦合多源捕获、应用配置和日志体系，还会影响 TiDB parser/驱动/Arrow 依赖选择。先做有来源记录的最小代码迁移，保留适用版权与 NOTICE；只有确有两边持续共享需求时再抽公共库。

## 4. 第一版必须确定的正确性契约

### 4.1 完成确认：MERGE 成功后再推进 checkpoint

当前 bend-cdc `Write()` 返回只代表 raw INSERT 成功；MERGE 是异步的。TiCDC 不能把它直接当作“目标已写完”。

第一版建议：

1. 原生事件转换成自有、不可变记录，按目标表/epoch 进入内存/磁盘 spool，保存事务 flush callback；内存与磁盘预算均显式受限，不能无限接收。
2. 数据转交后的 enqueue 与最终 flush 分开；可参考 storage sink `DetachPostCallbacks` 的做法，不能提前释放仍被 worker 引用的 chunk。
3. 达到行数、字节数或 flush interval 任一上限就批写 raw；低流量表也必须被定时 flush。
4. 同一目标表只运行一个 MERGE；明确它覆盖了哪些已提交 raw 批次。
5. MERGE 确认提交后，释放其覆盖的 spool entry，并只完成已覆盖事件的 callback，每个事务 callback 恰好一次；拆批事务必须等全部片段完成。不能因本地落盘或 raw INSERT 成功提前调用 PostFlush。
6. MERGE 已提交但 callback/上游 checkpoint 未保存：允许重放，依靠稳定身份和版本去重收敛。
7. MERGE 失败：不确认；临时错误有限重试，永久错误由 `Run` 返回并使 changefeed 可见地失败。

目标保证是 **at-least-once 传输 + 幂等最终状态**，不是未经证明的 exactly-once，也不默认保证跨表事务原子可见。

raw 落盘即确认也可以设计，但那需要独立持久化的待 MERGE 任务、重启扫描、接管协议及另一套 applied watermark；不建议第一版为此引入额外复杂度。bend-cdc 的 pendingEvents/writeGen/mergedGen 是内存状态，不能直接充当接管后的恢复日志。

### 4.2 事件身份与顺序

- 直接使用 `DMLEvent.CommitTs`，不用毫秒时间或本机 add_time 定义业务顺序。
- **commit TSO 是事务版本，不是每行唯一 ID**。同一事务可以更新多行，现有 Canal 代码“每行唯一”的注释不能成为新实现假设。
- 用源集群/命名空间、changefeed、逻辑表身份/建表代际、schema epoch、规范化复合键定义隔离域；事件版本另保存 commitTs，必要的同事务子操作顺序必须来自稳定的归约规则，不能使用重启后改变的 dispatcher seq/本机时钟。
- 同一键、同一版本重放必须内容一致；不一致要检测并报错，不能通过新的 add_time 随机覆盖。
- PK 值变更要处理 before/after 身份：旧键 tombstone + 新键 upsert。现有 CDCManager 的 UPDATE 只使用 After，不能直接宣称覆盖主键值更新。还要验证 TiCDC 入口是否已拆成 DELETE/INSERT，覆盖同事务换键/交换键的净结果。
- 第一版仅接受已验证的非空单列/复合主键，不把 TiCDC handle key 一概当作用户主键；无 PK、nullable unique key、force-replicate 表拒绝或单独设计 append-only 模式。
- 字符串键需测试大小写、collation、尾空格与 Unicode；复合键编码避免分隔符歧义。目标库表映射必须检测多源碰撞。

### 4.3 多节点与接管

bend-cdc 是单任务管理目标表，map/mutex 只保护进程内；TiCDC 每节点都有 sink，不能原样复制后同时消费同一个 stream、MERGE 同一目标表。

第一版限制为：**每个逻辑目标表一个明确 owner**，禁用 table-across-nodes，拒绝多 changefeed 写同一目标表，并验证调度/迁移时确实满足限制。分区表可能存在多个 physical-table dispatcher 汇入同一个逻辑表，禁用 table-across-nodes 不自动解决这个问题；未实现 owner 汇聚前第一版拒绝分区表。

接管必须包括停旧 writer、取消/等待其在途 SQL、fence、恢复 pending DDL、识别未完成 raw、继续 MERGE；验证 SQL 结果不确定时的重放。`SetWriteGate` 要覆盖 raw INSERT、MERGE、建表/ALTER、epoch 写、cleanup，不只是 AddDMLEvent 入口。capture lease 只是必要条件，还要证明调度转移期间旧请求不会晚于新 owner 落库；不能声称进程 mutex 或入口 gate 就提供分布式 fencing。

更大规模阶段再选择：逻辑表单 owner 汇聚多个 dispatcher，或分 writer staging + 单 merge owner。不能让每个节点自行操作共享 stream cursor。

### 4.4 DDL 与恢复

继续采用：旧 epoch DML 全部 MERGE → durable pending DDL journal → ALTER/TRUNCATE → applied journal → DDL PostFlush → 新 epoch DML。

需要复用 TiCDC 的 block 协调：本地 `FlushDMLBeforeBlock` 完成本节点相关队列屏障，配合 maintainer 对受影响 dispatcher 的协调，不把一个节点的 flush 当成整个 changefeed 的全局屏障。

仅保存 applied epoch 不够：还应保存可恢复的 DDL 标识/类型、目标身份、schema fingerprint/操作内容与 pending 状态。重启从 TiCDC checkpoint 较早位置重放时，必须识别已执行 DDL、跳过已覆盖的旧 schema DML，并禁止旧 DML 用新表结构解释。当前只有 MySQL-class 有显式 `GetTableRecoveryInfo`，这是新 sink 的正式开发项，不是自动继承能力。

支持策略：

| DDL | 第一版建议 |
|---|---|
| 建库/建表 | 支持，依据权威 TableInfo，保留列顺序/精度/NULL 属性 |
| ADD/DROP/RENAME COLUMN | 非键列、明确默认值语义下支持，保留 epoch 恢复 |
| MODIFY COLUMN | 沿用明确的安全转换白名单，不做任意 CAST |
| 二级索引 DDL | 明确 no-op，并推进必要 schema 状态 |
| TRUNCATE | 支持前需完成 table ID 代际、raw/stream 重置和中断恢复测试 |
| PK 定义变更、复杂多操作 ALTER | 第一版拒绝 |
| RENAME/DROP TABLE/SCHEMA、分区 DDL | 明确阻塞/拒绝，第二阶段扩展，不静默忽略 |

现有 bend-cdc 对某些不支持 DDL 会告警并推进 epoch，新 sink 不应无条件照搬；只有已证明不影响数据投影/身份的操作才可跳过。DDLEvent 的 `NotSync` 与过滤语义也要遵守。

**既有空表建表是独立问题**：新 changefeed 不保证为所有历史表重放 CREATE TABLE。首个 DML 的 TableInfo 可用于懒建表，但空表永远没有首行；需从 start-ts 对应 schema snapshot 获取元数据做 bootstrap，或由全量初始化任务预建表。不能把 schema store 里的名称清单当成完整列定义。

## 5. 类型改写：语义保留，表示重写

以 TableInfo/ColumnInfo 的 FieldType、flags、Flen/Decimal、Elems 和源时区为权威；同一套 mapper 服务建表、DML 编码及 DDL，消除 bend-cdc 中多个字符串映射函数的漂移。

| 源类型 | 目标与编码原则 |
|---|---|
| signed/unsigned integer | 精确宽度映射；BIGINT UNSIGNED → UINT64，禁止 float64 中转 |
| DECIMAL(p,s) | 精确保留 p/s；不沿用缺 metadata 时 DECIMAL(76,30) 的妥协，更不能转 DOUBLE |
| BIT(n) | UINT64，验证 BIT(64) 最大值 |
| BINARY/VARBINARY/BLOB | 目标 binary；直接从 datum 原始 bytes 转 HEX/二进制参数，禁止套 Canal 的 Latin-1 字符串还原 |
| ENUM/SET | 目标字符串，依据 typed datum/Elems 还原；不能把原生 label 再当 Canal 数字解析 |
| JSON | VARIANT/JSON；保留数值文本精度，区分 SQL NULL 与 JSON null；只解析一次 |
| DATE/DATETIME/TIMESTAMP | 明确时间范围、时区、精度和零日期策略；DATETIME 不应意外做时区平移 |
| TIME/YEAR | 延续 TIME 字符串、YEAR 小整数语义，验证负 TIME 和超 24h 值 |
| CHAR/VARCHAR/TEXT | 转 string；测试字符集、大小写、引号/反斜杠/控制字符 |
| generated column | 沿用 TiCDC 可见列策略：VIRTUAL 不写，STORED 作为物化值写入，不复制 TiDB 生成表达式到 Databend |
| 其他/不支持类型 | 明确报错或显式配置映射，不静默推成字符串 |

首版可继续使用 parameterized INSERT 写 raw_data，再用受控 projection 转目标类型。后续评估 typed staging 或驱动批量上传路径；不为追求“完全零 JSON”阻塞正确性迁移。

官方 [MERGE 文档](https://docs.databend.com/sql/sql-commands/dml/dml-merge) 确认支持基于 source/subquery 的 INSERT/UPDATE/DELETE。目标 warehouse 的 stream、事务、DDL、session sticky state、结果不确定恢复与性能，仍须用实际版本验证；公开语法支持不能替代真实兼容性测试。

## 6. 性能与资源设计

1. 不沿用“每张表一个连接池 + 一组后台循环”：建议每 changefeed/目标 endpoint 共享有界连接池和 worker pool，表状态按需维护，设置空闲回收。
2. 同时配置 max batch rows、max bytes、flush interval、max in-flight bytes 和 merge concurrency。按目标表串行、多表有限并行，避免大量小表各自频繁小 MERGE。
3. backpressure 要贯通到 TiCDC dispatcher；不能无限保存未确认事件，也不能用无限 goroutine 批触发 MERGE。
4. raw INSERT、MERGE、compaction 分别计时/计费；cleanup 失败不能把已成功业务写入误报为未提交并导致无界重试。compaction 不能删除尚未安全应用的更新或最新墓碑。
5. 所有后台任务使用 sink 生命周期和超时；不把现有 context.Background() merge 循环直接带入 TiCDC，避免 Close/失租后继续写。
6. 监控区分 accepted、raw durable、target applied、checkpoint，以及 raw backlog/bytes、MERGE SQL 时间、重试、schema 阻塞、写许可阻塞。吞吐不能只统计进入队列的行数。

### 6.1 第一版必需：有界内存 + 限额磁盘 spool

正常在内存中攒批；达到内存预算后，新增记录溢写本地 segment 文件。按批大小、等待时间或 DDL barrier 刷到 Databend raw，再执行 MERGE。第一版将 spool entry 保留至对应 MERGE 确认提交后释放，使未完成工作受同一套预算约束。

采用基线 `2bf6dacd0` 的 `spool.TryEnqueue`、segment 管理和 flush marker 思路，但不能直接照搬全部默认策略：该实现有“单 entry 超过磁盘配额时在内存接受并立即刷出”的特殊路径。新 sink 要对超大记录/事务单独定义限制，不能声称复用后内存天然存在严格硬上限。

必须具备的空间与背压约束：

- **单 changefeed 配额 + capture 共享总预算**：多张表、多 writer 共享所属配额；所有 changefeed 还受节点总预算约束，不能每表/每 worker 各自分配一份上限后将磁盘累加写满。
- **磁盘实际占用计费**：包括 segment 文件头、未回收空间和在途写入预留；删除/截断失败时继续计费，不能只扣逻辑消息大小就宣称空间已释放。
- **最低剩余空间保护**：写入前同时检查 spool 配额及文件系统可用空间，为日志、其他 TiCDC 数据及其他进程留余量；多个 spool 实例原子预留共享预算。其他进程仍可能抢占空间，因此 ENOSPC/IO 错误必须可诊断、停止接收且不提前确认。
- **达到限制立即背压**：先调度已有 raw flush/MERGE，释放空间后再接收；不删除未完成数据腾空间，不无限重试落盘，也不通过无界内存队列绕过配额。
- **防止调度死锁**：MERGE、回收、DDL barrier 等控制路径不与等待配额的生产者互相占用唯一 worker/锁；下游不可用时允许进度停住，恢复后应能自动排空积压。
- **读回同样有界**：spool 回读、编码缓冲和 SQL batch 都有预算，不能一次将所有磁盘数据加载回内存。源事件/chunk 的生命周期及队列元数据也必须计入内存设计，不能把 spool payload 配额当成进程 RSS 上限。
- **超大事务/单行**：事务可以按预算拆分持久化/写入，回调等所有片段 MERGE 后完成；单行超过支持的最大大小时明确报错，不无限加内存或磁盘预算。
- **重启恢复**：spool 不作为 WAL。重启/节点迁移仍由已保存 checkpoint 重放，依赖 raw/MERGE 幂等；清理 spool 只能操作当前实例拥有的目录，不能删除其他活跃实例数据。
- **监控与验收**：增加 spool 内存/实际磁盘字节、共享预算使用率、可用空间、配额等待、segment 回收失败；覆盖多 changefeed、磁盘满、回收失败、超大行、慢 MERGE、失租、重启和背压解除测试。

配置接口建议（待实现，不是当前可运行配置；容量仅为示例，需按节点磁盘和 changefeed 数量定额）：

```toml
[sink.databend-config]
spool-memory-quota = 67108864       # 64 MiB，单 changefeed 热数据预算
spool-disk-quota = 10737418240      # 10 GiB，单 changefeed 磁盘上限
```

节点级还必须有跨 changefeed 的 spool 总预算和最低剩余空间阈值；这两项属于 capture/server 级配置，不应复制成每表配置。具体字段名及与 TiCDC 现有 quota 的整合方式在实现前确定。上述预算不限制 Databend 端 raw 的历史水位/墓碑容量，后者仍按安全 compaction 单独管理。

成本比较固定同样输入/目标延迟/SLO：原链 TiCDC 编码成本 + 存储请求与容量 + consumer LIST/GET/解码/调度成本，对比新链新增的 TiCDC writer 成本；另单独计算 Databend raw/MERGE/compaction 与网络费用。直接 sink 会失去对象存储的长时间缓冲，warehouse 故障将更快反馈到 TiCDC lag 和上游保留窗口，需约定最长可恢复停机时间。

不提前承诺节省百分比；如果 Databend MERGE 计算是主成本，链路缩短不一定等比例降低总费用。

## 7. 全量初始化、上线与回退

增量 sink 不负责导出现存 TiDB 全量。保留 Dumpling + COPY INTO 作为独立初始化任务：选一致快照 S → 从 S 建 changefeed 并确保增量可保留 → 按 S 的 schema/数据导入 → 校验 → 放行大于 S 的增量。实施前验证 start-ts 边界和 GC/日志保留；全量期间不能直接让增量覆盖后又被旧快照写回。

切换建议使用 **独立影子目标库表**：旧链继续写原目标，新 sink 写影子目标；对齐快照/TSO 后核对 count/hash/关键业务查询及 schema，再切读并停旧链。禁止两个链路同时消费同一 stream、共用 raw 或写同一目标表。

回退需预留旧链原始数据和消费进度，在连续覆盖窗口内追平到已验证水位再切读，不能只替换二进制。新 sink 目标的 target/raw/stream/epoch/owner 状态按一组管理；不自动复用旧 bend-cdc 的内部表，也不自动转换其文件游标。

## 8. 分阶段工作包与验收

| 阶段 | 工作与产物 | 退出标准 |
|---|---|---|
| P0：语义验证 | 锁定 warehouse/驱动版本；验证 raw/stream/MERGE、重复批次、提交结果不确定、sticky session、fencing/接管、DDL 中断恢复 | 核心恢复模型明确；无法证明时先不扩展功能 |
| P1：增量纵切 PoC | 独立 scheme/type/config；原生事件适配；PK/复合 PK；内存/磁盘 spool；节点共享配额与背压；raw/MERGE；MERGE 后 ack；单 owner | insert/update/delete、重放、低流量 flush、超大事务、磁盘满/回收失败/多 changefeed 配额、cancel/Close、失租不丢数据 |
| P2：核心能力对齐 | 权威建表/空表 bootstrap；类型矩阵；列 DDL/epoch 恢复；TRUNCATE；库表映射；错误与监控 | 达到现有已支持核心行为，补齐 PK 值更新及故障案例；不支持项可诊断地拒绝 |
| P3：灰度与优化 | 快照接续、影子流量、全链故障注入、成本基准、多表并发；再评估分区/跨节点 owner 汇聚及 typed staging | 正确性与目标 SLO 达标，成本实测下降，可演练回退 |

粗估：一名熟悉两端代码的工程师，受限 PoC 约 1–2 周；DDL/恢复/完整类型和灰度再预留数周。该估算不是交付承诺，跨节点/fencing 和真实 warehouse 兼容性可能成为关键路径，应在 P0 后重新排期。

必须覆盖的验收用例：

- 复合键、NULL/极值、大整数与 DECIMAL、二进制全字节、ENUM/SET、JSON 大数、时区和生成列。
- 同毫秒不同事务；同事务多行；同键旧版本延迟；DELETE 后旧 INSERT 重放；PK 值更新和换键。
- raw INSERT 前后、MERGE 提交前后、callback 前后强杀；响应丢失；重启/调度接管；重复数据不复活/不回退。
- pending/applied DDL 各中断点，rename/drop/re-add column、TRUNCATE 代际、不带后续 DML 的 DDL、历史空表。
- 多节点失租/恢复、旧节点在途请求、目标撞名、分区/跨节点不支持时的创建拒绝。
- raw cleanup 与 MERGE 并发、墓碑保留、warehouse 长时间不可用、超限背压、token 刷新、权限变化。
- 内存溢写磁盘、节点总预算、最低剩余空间、ENOSPC、segment 回收失败、超大 entry 限制、配额等待时 MERGE 仍能推进并释放空间。
- 快照 S 接续与影子切换，不把 source checkpoint lag 当作 target applied lag。

## 9. 本次实际验证与证据索引

已执行：`go test ./pkg/sink/databend ./pkg/source/tidb -count=1`（在 bend-cdc module），两包通过。未设置 BENDCDC_TEST_DSN/TIDB_DDL_INTEGRATION_DSN，真实 Databend 集成测试按现有条件跳过，因此这些结果只验证现有本地单元/模拟测试，不证明原生 sink 已可运行。

未改生产代码，未跑新 sink E2E、故障注入、成本 benchmark 或部署。TiCDC 尚未新增 sink，且本机默认 Go 1.25.5 与该 repo go.mod 要求 1.26.8 不同；本次不构建 TiCDC。

主要源码证据（绝对路径便于本机复核）：

- [实际 TiDB runner，L66–138](/Users/jiangjianyuan/work/git/pingcap/webapi/bend-cdc/pkg/service/tidb_runner.go#L66-L138)
- [schema epoch 与写入适配，L93–260](/Users/jiangjianyuan/work/git/pingcap/webapi/bend-cdc/pkg/source/tidb/databend_merge_sink.go#L93-L260)
- [复合主键与有序模式，L305–474](/Users/jiangjianyuan/work/git/pingcap/webapi/bend-cdc/pkg/source/tidb/databend_merge_sink.go#L305-L474)
- [raw 写入与异步调度，L642–855](/Users/jiangjianyuan/work/git/pingcap/webapi/bend-cdc/pkg/sink/databend/databend_cdc.go#L642-L855)
- [持久化 raw/stream 与参数化写入，L888–1016](/Users/jiangjianyuan/work/git/pingcap/webapi/bend-cdc/pkg/sink/databend/databend_cdc.go#L888-L1016)
- [墓碑 compaction，L1188–1203](/Users/jiangjianyuan/work/git/pingcap/webapi/bend-cdc/pkg/sink/databend/databend_cdc.go#L1188-L1203)
- [严格顺序 MERGE，L1625–1704](/Users/jiangjianyuan/work/git/pingcap/webapi/bend-cdc/pkg/sink/databend/databend_cdc.go#L1625-L1704)
- [现有列 DDL 白名单，L80–188](/Users/jiangjianyuan/work/git/pingcap/webapi/bend-cdc/pkg/source/tidb/schema_ddl.go#L80-L188)
- [TiCDC sink 接口和工厂，L36–105](https://github.com/sdojjy/tiflow/blob/2bf6dacd0aec9ac2fad464600e47341f9aa951f2/downstreamadapter/sink/sink.go#L36-L105)
- [原生 DML 元数据，L375–430](https://github.com/sdojjy/tiflow/blob/2bf6dacd0aec9ac2fad464600e47341f9aa951f2/pkg/common/event/dml_event.go#L375-L430)
- [DML flush/enqueue callback，L660–705](https://github.com/sdojjy/tiflow/blob/2bf6dacd0aec9ac2fad464600e47341f9aa951f2/pkg/common/event/dml_event.go#L660-L705)
- [TableSchemaStore sink-type 分支，L65–109](https://github.com/sdojjy/tiflow/blob/2bf6dacd0aec9ac2fad464600e47341f9aa951f2/pkg/common/event/table_schema_store.go#L65-L109)
- [MySQL 专属恢复分支，L551–570](https://github.com/sdojjy/tiflow/blob/2bf6dacd0aec9ac2fad464600e47341f9aa951f2/downstreamadapter/dispatchermanager/dispatcher_manager.go#L551-L570)
- [storage sink callback 解耦参考，L51–68](https://github.com/sdojjy/tiflow/blob/2bf6dacd0aec9ac2fad464600e47341f9aa951f2/downstreamadapter/sink/cloudstorage/task.go#L51-L68)
