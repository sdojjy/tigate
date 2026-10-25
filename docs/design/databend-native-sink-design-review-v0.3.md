# RFC v0.3：TiCDC 原生 Databend Sink——直接 raw 批次与最小接入

| 项目 | 结论 |
|---|---|
| 版本 / 日期 | v0.3 / 2026-10-24 |
| 状态 | Proposed；唯一候选主路径，待实现及实验，不是生产就绪 |
| 目标 | 将 bend-cdc 的建表、类型转换、批写 staging、MERGE、必要 DDL/恢复迁入原生 sink，去掉增量 Canal JSON→S3→消费者链 |
| 改动约束 | 核心位于 Databend sink；外部仅必要注册、raw-change、schema/recovery、失败生命周期接入 |
| 首版运行范围 | 隔离单 capture、静态独占目标、人工核验旧 SQL 后恢复的受控 PoC；不承诺自动 HA |
| 全量工具范围 | 不迁入 Dumpling/CSV/COPY INTO，不实现或编排全量初始化；只处理 TiCDC 交付的增量事件及必要表元数据 |
| 缓冲 | 有界入口/内存 + 限额本地 spool + 现有 PostEnqueue 背压 |
| 应用主路径 | manifest → raw → 本批主键 K → retained history H → 每键净动作 S → MERGE → flush callback |
| 不采用 | stream cursor、消费 hint/witness、普通 DML durable applied ledger、新 delivery/信用协议、progress 迁移、跨 capture 强一致预算服务 |
| 实现状态 | 未实现新 sink，未执行真实 Databend/新 sink 故障实验 |

**本文替代 v0.2 作为本轮唯一实施候选。** [v0.2 历史设计](/Users/jiangjianyuan/work/git/pingcap/ticdc/docs/design/databend-native-sink-design-review-v0.2.md)只供风险分析，不再构成并行实施要求；其中 B01–B18/V01–V13 保留为历史编号。当前仅跟踪本文 Q1–Q7、A01–A12，不把删掉旧协议当作其风险已通过实验。

## 0. 固定基线与本轮裁决

| 对象 | 仓库 | 固定 commit |
|---|---|---|
| 新架构 TiCDC | `/Users/jiangjianyuan/work/git/pingcap/ticdc` | `2bf6dacd0aec9ac2fad464600e47341f9aa951f2` |
| bend-cdc | `/Users/jiangjianyuan/work/git/pingcap/webapi` 下 `bend-cdc` | `b6481621021e82aee659ad88e8e6c00f6dd11906` |

TiCDC origin 为 `git@github.com:sdojjy/tiflow.git`，上述是 fork 新架构基线，不代表上游/生产最新版本。编写时 checkout 是旧架构 master / `03ed2432bd7782198742d3aa20e07edcc47097e6`；只通过 Git 对象读取新架构，没有切分支。

```bash
git -C /Users/jiangjianyuan/work/git/pingcap/ticdc show \
  2bf6dacd0aec9ac2fad464600e47341f9aa951f2:downstreamadapter/sink/sink.go
```

本轮采纳三项裁决：

1. 采用“manifest 明确成员 + K/H/S + 直接 MERGE”，不再把 stream 消费范围当确认边界；普通 DML 不默认要求 durable applied ledger。
2. 复用现有 dispatcher/progress/callback 生命周期；transport reset 不丢 sink 已接管数据。接管失败必须让旧生命周期停止，不能只 reset session 接着跑。
3. 自动 HA 和严格 feed 全局远端额度移出受控 PoC 承诺；保留旧 SQL 排他检查、容量监控和可诊断停止，不以缩小范围掩盖数据错误。

## 1. 流程、职责与核心约束

```text
TiCDC dispatcher（现有 progress 登记）
  → AddDMLEvent 非阻塞接管到有界入口
  → 异步按权威 schema/主键转换原生行
  → 内存热缓冲，必要时溢写限额 spool
  → 同表/同 incarnation/同 raw_generation 攒批并封口 manifest
  → 批量 INSERT raw，核验所有 manifest 成员到齐
  → 独占该表 lane：冻结其他 raw/DDL/compaction
  → K = manifest 主键；H = K 对应 retained raw；S = 每键最高版本净动作
  → MERGE target USING S，取得提交成功终态
  → 本批分片 APPLIED，独立释放本地数据
  → 源事件 SEALED 且全部分片完成，flush callback 一次
  → TiCDC 原有机制推进 checkpoint
```

明确约束：

- raw 是普通持久化 staging 表，不是 TEMP，也不是本地 spool。raw INSERT 成功不等于目标应用完成。
- commit_ts 是事务 TSO，不是每行唯一 ID。版本不使用到达时间、批次序号或进程时钟。
- 同逻辑目标表首版一条串行 lane；跨表有限 worker；没有外部应用改 target/raw/journal。
- 不承诺跨表/跨分片事务原子可见。不实现、调用或编排全量导出/导入，不自动扫描源表历史行；目标基线由使用方负责，不构成sink工作包。
- 不确认未应用的数据、不伪造 flush 清进度；所有“不支持”操作显式失败。
- 限制 sink 自持内存与磁盘，不宣称硬限制全 TiCDC RSS；源 chunk 在进入 sink 前的分配不是本组件能消除的。

### 1.1 核心功能迁移

| bend-cdc 职责 | 原生 sink 处理 |
|---|---|
| Canal JSON 解码、文件 discovery、SQS、S3 游标 | 不迁移，改吃原生事件与现有 checkpoint |
| 主键、库表路由、建表、类型转换 | 按 TableInfo/列 ID/FieldType 适配，复用既有语义与测试案例 |
| raw、stream、MERGE | 保留 raw/MERGE；stream 不作为首版必要依赖 |
| 乱序、重试、最新版本和删除墓碑 | 必须保留，使用 K/H/S 实际应用最高有效版本 |
| DDL 白名单、pending/applied、TRUNCATE | 保留必要持久化恢复，接入现有 DDL 屏障 |
| Dumpling/CSV/COPY INTO | 不迁移、不引入依赖，也不开发独立全量初始化模块或共享全量mapper接口 |

## 2. 接入边界与首版支持范围

| 修改位置 | 允许范围 |
|---|---|
| Databend sink 新包 | adapter、buffer/spool、manifest、tracker、raw/MERGE SQL、mapper、DDL journal、错误与指标 |
| scheme/type/config/factory/metrics | 新 sink 正常注册与校验，不伪装 MySQL SinkType |
| dispatcher manager/session 注册 | Databend 强制 raw-change=true 的小分支，贯通初建/重注册 |
| TableSchemaStore / recovery | 必要类型分支及专属恢复结果；沿用既有 dispatcher 创建/DDL 协调 |
| manager fatal 错误生命周期 | 先验证既有关闭路径；确有缺口才加有限失败分支，不引入通用交付协议 |
| collector/dynstream/TableProgress/调度核心 | 不改 delivery/信用/重投/progress 迁移协议；如确实不能接入，先给最小阻塞证据 |

首版支持：非分区表、非空单列/复合 PK、完整行镜像、普通增删改与二级 UK 值更新、权威建表、有限非键列 DDL、通过恢复测试的 TRUNCATE。

首版不承诺：无 PK/nullable UK/force-replicate、分区或一表跨节点、多任务撞同一目标、复杂 DDL/PK 定义变更、active-active/BDR/syncpoint、外部写入、全自动 HA。也不要求历史空表自动发现/预建；没有增量事件的历史表可以暂不出现在目标，不能默认为会重放历史CREATE。

PK/handle 值变化及同事务复用键必须按真实源事件验证；不能假设全部变键都能识别为 UPDATE。未通过测试时不宣称支持，也不拿一个实际上无法识别所有变键的开关冒充拒绝保护。

## 3. 原生事件、类型转换与内部数据

### 3.1 原生事件模式

固定基线在 raw-change=false 时会因 UK 变化拆 UPDATE，PK 不变也可能产生同 key/TSO 的 DELETE/INSERT。Databend 内部固定 true，不开放用户切回 false；校验 manager→SharedInfo→注册/重注册→eventservice 实际生效。未知旧协议拒绝，不靠 SQL 任意选动作补救。

- INSERT / PK 不变 UPDATE：完整 after-image UPSERT。
- DELETE：旧 PK + tombstone。
- 同 key/TSO 完全相同净事件：重放，逻辑去重。
- 同 key/TSO 异动作/异 payload：先检查源净变化契约，未解释的冲突在 MERGE 前失败；不能 ROW_NUMBER 任意决胜，也不能规定 DELETE 永远胜出。
- raw=true 不等于真实 record-handle 变化已归约。测试先确认源 SQL 合法，再保存实际事件与最终键集合。

### 3.2 类型与 schema

一个 mapper 同时服务建表、raw 编码、MERGE projection、DDL。输入是列 ID/FieldType/flags/精度/Elems，不按样本值猜类型。

| 类别 | 要求 |
|---|---|
| signed/unsigned、BIT | 无损整数，验证 UINT64/BIT64 边界，commit_ts 不转浮点 |
| DECIMAL | 明确目标精度能力，精确文本/参数，不经 float64；不支持范围拒绝 |
| binary/blob | 原生 bytes 用固定 HEX/已验证参数；不用 Canal Latin-1 还原 |
| ENUM/SET | 按原生 datum/Elems 转换，不重复做 Canal ordinal 解码 |
| JSON | 保留大整数，区分 SQL NULL/JSON null/字段缺失 |
| DATE/DATETIME/TIMESTAMP/TIME/YEAR | 时区/微秒/零值/负 TIME/超范围策略明确，不隐式修正 |
| 字符串/PK collation | 源键相等、canonical key 与 target JOIN 语义须一致，限制到已验证集合 |
| generated | VIRTUAL 不写，STORED 使用物化值；不把目标生成逻辑当源语义替代 |

转换失败停止，不 TRY_CAST 静默写 NULL。ADD COLUMN 历史回填/default 必须与源相容；复杂表达式未定义前拒绝。

### 3.3 最小内部记录

```text
RawRow {
  source_binding, logical_table, incarnation, raw_generation,
  source_schema_version, row_key, commit_ts,
  action: UPSERT | DELETE,
  event_id, payload_hash, payload,
  batch_id
}
```

row_key 使用确定类型/长度编码，保留完整键；hash 不代替相等性。event_id 基于源身份、代际、规范键、TSO、净动作等稳定信息，不依赖重启后重分批。batch_id 只标识一次逻辑 raw 批次，同批重试复用；重启可重新分批，业务幂等依赖事件身份与版本，不要求重建原 batch_id。

## 4. 有界接管、正常背压与失败生命周期

### 4.1 正常接管

```text
dispatcher 按现状登记该组事件的 progress
  → AddDMLEvent 尝试非阻塞接管
  → 成功：sink 保留事件与回调的责任，有界入口中的引用计费
  → worker 增量编码/spool，不把整个事务一次复制
  → 根据水位/驻留引用与剩余额度，延迟或触发原有 PostEnqueue
  → raw/MERGE 后按 tracker 触发 PostFlush
```

PostEnqueue 释放的是现有表路径的背压，不是 durable 确认。正常磁盘压力优先通过 spool、高低水位、强制小批 MERGE、延迟 PostEnqueue 吸收；不能只因普通水位升高就报 fatal。

入口按保守字节与元素数限制，并预留一个现有交付组可能带来的有限突发空间。共享 chunk 按其被 sink 引用的实际容量保守计费；可多计不能漏计。Detach 回调后逐步解除源数据引用；必须证明后台任务没通过闭包继续捕获整个 chunk。

现有交付组超过可接纳范围、单行超支持上限、磁盘/IO 严重错误属于明确异常策略：失败停机而非无限分配或无限等待。现有源 batch 没有足够硬上限时，不承诺永不触发该策略；用压测记录支持规模，不为此默认改 collector。

### 4.2 transport reset 与 owner/lifetime 分离

- 同 manager/dispatcher 的 transport session reset 不取消或丢弃 sink 已接管的数据，不因 collector epoch 改变清空入口/spool。
- 重放可能与已接管事件重复，按源身份收敛；每个现有 progress 登记仍由自己的真实完成回调完成，不能因为逻辑事件重复就丢其 callback。
- 未接管发生致命失败后，不在同 dispatcher 中尝试自定义 delivery 重绑/清 blocker；结束旧生命周期，重建后从安全 checkpoint 重放。
- 真正 owner/lifetime 结束时，停止该旧 sink 新写入；这不证明已经发出的远端 SQL 已终止，见§9。

### 4.3 接管失败不是只设 IsNormal=false

必须以首次 fatal 原因驱动单调状态：

```text
RUNNING → FAILED（不可在本生命周期内复活）
  → 立即关闭本地 admission 和 mutation gate
  → Run 返回非context.Canceled的首因；上报控制面
  → 旧 manager/dispatcher 最终关闭，未完成事件不flush
  → 核验旧远端 SQL；新生命周期从安全 checkpoint 重建
```

固定基线中 Run 错误经 handleError/collectErrors 上报并重发，**并不立即销毁 dispatcher**。因此：

1. sink 自己立即拒绝后续 Add/DDL/新 SQL；所有 worker 检查相同 failed gate。后续调用不能继续排队；失败回调只记首因，不忙循环。
2. 不为清理旧 blocker 伪造 PostFlush。旧生命周期保留未完成 blocker 是安全停机状态；不能把它当健康任务继续 session reset。
3. 验证控制面收到错误后关闭旧 manager/dispatcher，恢复时创建新对象。若既有路径不保证关闭，增加 Databend fatal 专属小接入，复用已有关闭/LocalFence能力；不能在运行 goroutine 里等待自身退出，不能取消上报上下文后把首因吞成 Canceled。
4. 关闭/上报链未实测前，受控 PoC 需要人工停掉旧进程并确认 manager 不再运行，不承诺自动恢复。不得只调用一个 Close/LocalFence 就宣称整个控制面路径已验证。

## 5. spool、批处理与事件完成

### 5.1 缓冲策略

内存热数据→限额 spool segment；spool 是临时缓冲，不是恢复 WAL。预算包含入口引用、编码临时值、payload、回读批次、tracker/callback 元数据。超大行无内存直通例外，sync.Pool 不等于配额。

磁盘按实际占用/预留计费，未删成功的 segment 继续占额；启动只清理确认不活跃的自身目录。共享本地 BudgetManager 若可复用则复用，不能因多个 feed 各有10GiB就忘记节点空间；不把预算扩成分布式服务。

压力触发唤醒所有钉住受压 segment 的表，不只热点申请者；独立读回/应用资源保证消费者能排空。首版先验证强制排空和物理回收，不默认新增复杂搬迁系统；不能排空则告警/停止新接管。ENOSPC 仍可能来自共盘其他组件，要明确处理，不承诺绝不发生。

### 5.2 表内串行与攒批

每表/代际/raw_generation 一条 lane，按行数、编码字节、最长等待、DDL marker、压力触发。计时从最老待处理数据起算。不同表有限并发，单表 raw/核验/MERGE/DDL/compaction 不交错。

raw batch 可包含多个事件分片；大事件可跨多批。通过有界窗口边编码边应用，不能把累计事件总量作为必须同时驻留的大小。

### 5.3 SEALED tracker 和独立回收

```text
事件：OPEN → SEALED → COMPLETED（或 FAILED）
分片：REGISTERED → STAGED → RAW_VERIFIED → MERGING → APPLIED → RELEASED
```

- 分片先登记、增加 pending，再发布给消费者；生产者遍历完整事件后才 Seal。
- 只有 `SEALED && pending==0 && !failed && CAS(completed)` 才调用 flush 一次。
- APPLIED 分片立即可释放 payload/entry、回收空间，不等整事件完成；只保留有界未完成描述符和小 tracker。
- Detached flush 闭包自身没有 once 保护，由 tracker 保证；PostEnqueue 与 flush 不混用。
- 停止后未完成分片不确认；Close 可回收临时文件，恢复靠源 checkpoint 重放，不能靠文件删除当作已完成。

## 6. manifest、K/H/S 与直接 MERGE

### 6.1 本批 manifest

每批在 sink 内有只读 manifest，含：batch_id、binding/incarnation/raw_generation/schema、分片到成员映射，以及每个成员的稳定 event_id、完整 row_key、commit_ts、净动作、payload_hash。

manifest 是有界 batch 元数据，可随 spool 暂存；不要求普通 DML durable applied ledger。不能只保存预期行数或摘要：需要能精确判断成员存在、内容一致以及属于哪个事件分片。源恢复可重建新批次/manifest。

### 6.2 raw 完整性核验

1. 参数化批量写 raw，可拆多个有限 SQL；失败重试同批成员，不改变业务版本。
2. 读取/核验该 batch 的 raw 逻辑成员集合，核对完整事件身份和内容；重复物理行按相同身份去重，相同身份异内容为错误。
3. 缺任何 manifest 成员则继续补写/核验，不 MERGE、不确认；额外未知成员说明 batch 绑定错误，停止。
4. 核验结果若分页需读取完整，不能 LIMIT一页、COUNT相等或比较一个无成员能力的digest就当全集证明。核验本身也在有界批次/内存预算内。

### 6.3 集合定义与应用算法

```text
M = 已验证的本批 manifest 成员
K = DISTINCT row_key FROM M
H = 当前 binding/incarnation/raw_generation 合法范围内、row_key∈K 的 retained raw
S = H 中每键最高 commit_ts 的确定净动作（含 DELETE tombstone）
```

顺序：冻结该表其他 raw/DDL/compaction → 验证 M 全到齐 → 验证 H 的同 key/TSO 冲突 → 形成 S → MERGE target USING S → 成功终态 → 仅完成 M 对应分片。

**不是直接 MERGE 本批旧行，也不是看到 higher raw 就跳过旧行。** 例如 M 有 K@150 而 H 有 K@200：本次实际将 K@200 的净动作写入目标，即使@200之前只是raw写入尚未应用，也能支配@150。不能仅过滤掉@150后声称它已完成。

概念 SQL 结构（不是已验证可执行 SQL）：

```sql
-- 在实际实现中使用转义标识符和绑定参数；先核验 manifest/同版本冲突。
WITH batch_keys AS (
  SELECT DISTINCT row_key FROM RAW_TABLE
  WHERE batch_id = BATCH_ID AND raw_generation = RAW_GENERATION
), latest AS (
  SELECT r.* FROM RAW_TABLE r JOIN batch_keys k ON r.row_key = k.row_key
  WHERE r.raw_generation = RAW_GENERATION
  QUALIFY ROW_NUMBER() OVER (
    PARTITION BY r.row_key ORDER BY r.commit_ts DESC
  ) = 1
)
MERGE INTO TARGET_TABLE t USING latest s ON TYPED_PK_MATCH
WHEN MATCHED AND s.action = 'DELETE' THEN DELETE
WHEN MATCHED AND s.action = 'UPSERT' THEN UPDATE SET EXPLICIT_COLUMNS
WHEN NOT MATCHED AND s.action = 'UPSERT' THEN INSERT EXPLICIT_COLUMNS;
```

ROW_NUMBER只在同版本净动作/内容已验证一致后用于消除物理重复，不能拿它解决冲突。真实SQL必须包含完整binding/代际约束、精确payload投影/NULL规则，不能跨schema误读。

### 6.4 覆盖条件

本批完成需同时证明：

- M完整进入raw；K准确对应M，没有未解释缺失/额外成员。
- 对每个M成员，S包含同key、版本不低于它的合法净动作；其更高版本被**实际纳入本次MERGE**。
- SQL无额外LIMIT/漏JOIN/过滤，目标PK匹配/投影覆盖完整S；同表冻结期间核验到MERGE使用一致有效数据，快照/查询计划行为需真实验证。
- MERGE服务端提交终态明确成功；零业务行修改也可以完成，如删除不存在的键，但不能仅凭返回计数判断成功。

S可能顺便应用H中其他批次的更高事件，但只完成M对应callback；其他批次仍走自己的核验/幂等应用，不猜其进度。若只覆盖部分分片，未覆盖部分保持pending，不一次确认整事件。

没有stream，stream hint/cursor/no-op消费witness不参与正确性；但MERGE源集合与终态证明仍不可省略。若无法证明上述条件，停止，不加一张写有“计划coverage”的表假装解决。

## 7. 提交、重试与恢复：不引入普通 DML applied ledger

| 情况 | 行为 |
|---|---|
| 部分 raw INSERT 成功 | 不确认，补写缺少成员；同事件物理重复允许但逻辑去重 |
| 明确未提交/回滚 | 在旧请求不再运行且所有权有效后重试 |
| MERGE成功、callback之前崩溃 | 从checkpoint重放，重建manifest并按K/H/S重应用，不要求恢复旧batch_id |
| MERGE成功、响应丢失 | COMMIT_UNKNOWN，不确认、不发下一批；先核验旧请求终态 |
| 业务提交已确认，仅操作统计解析失败 | 保留成功，统计unavailable，不为补统计重做SQL |
| 查询记录缺失、断连、取消返回nil | 不能推导未提交/已终止，进入人工恢复 |

最小 TargetClient 需能保留当前操作/query ID、读取完整结果和服务端执行终态，并区分执行错误、传输错误、值解码错误与统计解析失败。QueryContext返回Rows不是通用提交证明，驱动Kill/Close成功也不是服务端终止证明。

不要求把每次普通DML attempt写入durable applied ledger。代价是：重启丢query关联、旧查询查不到时，**不承诺自动恢复**；人工确认所有旧mutation已终结且目标独占，再从安全checkpoint幂等重应用。进程退出/固定sleep不能替代确认。业务幂等不意味着可以允许旧SQL晚于新批次提交。

DDL仍有必要durable journal，因为重复TRUNCATE会破坏新代际；不能将“普通DML无需ledger”扩展为“所有持久化恢复元数据都不要”。

## 8. 建表、DDL、schema fence 与 TRUNCATE

### 8.1 建表与历史数据范围

首次DML按权威TableInfo建表/核验，接收到CREATE TABLE等受支持DDL时也按事件建表；不查询最新schema解释旧事件。目标已存在须验证结构、任务绑定与PK唯一性，不盲目IF NOT EXISTS接管。没有DML/DDL的历史空表不要求出现在目标，不做全库发现/预建，也不借此引入全量工具。

同一raw_generation只接受可由同一projection解释的数据。已完成会改变行投影的DDL后，优先切换新的raw_generation；旧raw隔离，不把旧payload直接CAST成新schema。此前目标行的回填/默认值由已验证DDL处理，后续完整after-image重新建立新generation的raw记录。

旧schema事件的跳过依据是durable applied-DDL fence，必须证明该边界前数据已排空或被TRUNCATE合法清除；不是当前schema的更新时间。只有对源顺序前缀/fence契约成立时才可安全退役旧raw墓碑，否则停止而非清理。

### 8.2 普通 DDL

```text
现有 FlushDMLBeforeBlock / 全局 DDL 协调
 → 强制旧分片全部MERGE完成
 → sink核验DDL身份/owner/当前schema
 → durable journal PREPARED（含before/after fingerprint及drain fence）
 → 执行白名单DDL，取得终态并校验
 → 切换/验证projection和必要raw_generation
 → journal APPLIED持久化
 → DDL PostFlush，放行新schema
```

支持CREATE、有限非键ADD/DROP/RENAME/MODIFY；默认值/NULL/生成列须明确。复杂多操作、PK定义更改、DROP/RENAME表等未定义项停止。二级索引DDL只有不改变行身份/投影时才可no-op，raw-change契约持续有效。

### 8.3 DDL/TRUNCATE最小恢复顺序

先关闭新写→人工/受控确认旧SQL终结和目标独占→核验binding/journal→恢复pending DDL/reset→安装已应用fence→协调旧DML重放→开放当前代际。

| durable状态 | 恢复行为 |
|---|---|
| PREPARED，尚未发送或明确未提交 | 核验before条件，按同DDL身份执行；未知旧SQL先协调 |
| 执行结果不明 | 查终态/校验可唯一证明的after状态；不能只见列不存在就猜成功 |
| APPLIED但上游checkpoint尚未保存 | 相同DDL只跳过，不重复破坏性操作；旧DML按已证fence处理 |
| metadata冲突/丢失 | 停止、人工核验或重新初始化，不用重新TRUNCATE“修复” |

TRUNCATE记录源DDL身份、旧/新表血缘和代际，不只用表名/全局MAX(TSO)：

```text
DRAINED → PREPARED(new incarnation CLOSED)
 → TARGET_RESET已确认
 → 新raw_generation/binding校验完成
 → APPLIED持久化
 → 才可开放新代际DML
```

PREPARED中重做reset仅在旧SQL明确终结、目标独占且新代际从未开放/无新写入证据充分时允许；APPLIED之后绝不重做。journal写入本身不确定就停在该步骤，不越过。不存在假定的强制唯一约束/跨DDL事务万能锁。

重点验收：TRUNCATE@500完成，新代际@510写入后从@490重放，@510不得被再次清空。此顺序属于sink必要恢复，不因此重写调度框架。

## 9. 受控 PoC 的排他与生命周期边界

首版只在隔离单capture、静态独占目标环境验证；禁止正常运行时跨owner漂移，自动替换/重启后也不能未经核验直接开放写入。`enable-table-across-nodes=false`仅限制拆表分布，不等于禁止整表HA，不能将其当唯一保护。

失租/fatal立即关本地gate，禁止新raw/MERGE/DDL/compaction；已发送的远端SQL仍需查终态。旧MERGE晚到也可能覆盖新值，旧TRUNCATE晚到可清空新代际，去掉stream并不解决这些问题。

若目标版本/接口不能可靠定位并终结旧SQL，保持停机，人工核验或重初始化；不发布自动HA。自动HA若需要目标侧fence或更多控制面工作，单独RFC，不夹进新增sink的首版合入。

## 10. 清理、容量与成本

- raw保留每键最新版本及删除墓碑，删除严格被支配历史；最新物理重复需安全去重策略，不因为SQL成功就认为空间不再增长。
- raw compaction与同表写入/MERGE串行；不能删manifest仍需要的成员/恢复证据。未处理完的批次受本地预算约束，不能清raw后才发现还需要核验成员。
- 持续新键INSERT→DELETE会让target很小但raw增长，必须监控raw/墓碑/retained快照、历史键及扫描成本，达到阈值停新raw；旧SQL/清理在途也计入告警判断。
- 首版是**监控型停写阈值**，有测量延迟、物理放大和多capture超调风险；不承诺全局原子feed硬额度。受控PoC不引入跨capture预算服务；将来需要严格额度时单独评估静态切分或已有外部配额。
- 不删最新墓碑解决超额。恢复方式是显式扩额、安全去重或新一致快照重初始化；物理文件/快照释放可能延迟，不把逻辑删除量当已回收字节。
- 无stream不等于便宜：K关联H可能扫很多raw。固定事件率、行宽、历史键数、热点、小表数量、可见延迟SLO，实测Databend扫描/credit、TiCDC CPU/RSS/spool IO，再与旧链比较。

## 11. 配置草案与增量接入边界

所有Databend新字段均未实现；以下是可读的压测起点，不是可运行配置。认证通过受保护配置/运行时凭据注入，不把真实TOKEN写入URI/日志。

连接标识：`databend://HOST:PORT/DB_NAME?sslmode=require`。

```toml
[filter]
rules = ["DB_NAME.*"]

[scheduler]
enable-table-across-nodes = false # 不是禁用HA；PoC还需隔离部署和恢复准入

[sink.databend-config]
auto-create-table = true
batch-max-rows = 5000
batch-max-bytes = 8388608
flush-interval = "1s"
worker-count = 4                # 跨表，同表串行
entry-queue-max-events = 1024
sink-memory-quota = 268435456   # 含入口引用/编码/回读/metadata，不等于进程RSS
spool-memory-quota = 67108864   # 包含在sink-memory-quota内
spool-disk-quota = 10737418240
raw-observe-interval = "30s"
raw-stop-threshold-bytes = 1099511627776 # 示例告警/停写阈值，非硬计费quota
```

镜像模式固定处理DELETE，内部固定raw-change=true；不开放allow-delete=false、stream消费hint、DML ledger模式、自动HA、分布式预算开关。单行/交付规模、SQL请求开销和内存分池必须校验兼容，不能让queue count乘以巨大chunk绕过字节预算。配置减少到用量以下时停新输入继续安全排空，不直接删文件。

sink从TiCDC交付的增量事件开始处理，遵守已配置start-ts与现有checkpoint，不负责搬运该边界之前的存量行。用户若要求目标成为全量镜像，需要在本功能之外准备可衔接的目标基线；本设计不指定工具、不实现或编排该流程，也不强制所有增量用例先做全量导入。没有基线且历史行一直不变时，这些行不会自动出现在目标，这是增量范围边界而非自动补数能力。源保留窗口不足仍应失败，不承诺无限期背压恢复。

## 12. 具体验收：先实现最小路径，不继续扩通用协议

以下均为待执行测试，未因为本文写出而通过。

| ID | 场景 | 必须证明 |
|---|---|---|
| A01 | PK固定仅UK更新；raw=true初建/重注册；合法真实PK变化对照 | 普通UPDATE无人工DELETE/INSERT冲突；未验证变键不虚报支持 |
| A02 | 一批多个raw子写，部分成功、重复物理行、缺成员/同身份异payload | manifest逐成员核验，缺失不MERGE、不提前回调；跨batch重复收敛 |
| A03 | UPSERT@100→DELETE@200→迟到UPSERT@150；higher raw已入但未应用 | K/H/S实际应用最高净动作，删除不复活，不因仅raw存在就确认 |
| A04 | 实际MERGE SQL、不同schema/代际、查询投影失败、零目标修改 | 完整S应用与终态成立；无stream hint依赖；不跨schema解释旧payload |
| A05 | F1完成时F2未登记；事件总量>spool；Seal/Apply/取消竞争 | OPEN不确认、分片可独立回收、全集完成恰回调一次、驻留有界 |
| A06 | 正常磁盘压力、冷表pin、删除失败、入口无法接管 | 常规压力由背压/排空吸收；异常非阻塞fatal，无假释放/无限队列 |
| A07 | 已接管后transport reset；Add接管失败；Run错误上报/控制面关闭 | reset不丢数据；fatal后无新接管/SQL；旧blocker不伪flush；新manager从安全checkpoint恢复而非旧session复活 |
| A08 | MERGE提交后响应丢失、callback前崩溃、统计解析失败、旧query查不到 | 无未知结果确认；终态排他后幂等重应用；统计失败不重做；查不到时人工停机 |
| A09 | DDL各journal中断、schema generation切换、旧DML/DDL重放 | 正确fence/投影，APPLIED前不开新schema；不靠最新版schema猜跳过 |
| A10 | TRUNCATE每步骤崩溃、新数据写入后回放旧TRUNCATE、晚到旧SQL | 新代际不再被清空；旧SQL终态无法证明则不放行 |
| A11 | target小但历史键churn；同SLO旧链/无stream SQL成本对照 | 成功清理下仍能告警停写；量化延迟/超调，不声称严格quota；扫描/费用实测 |
| A12 | start-ts/checkpoint接入、首条DML/CREATE建表、类型边界/复合键/collation | 只验增量交付边界和数据转换，不要求全量工具或历史空表预建；既有bend-cdc增量核心案例逐项对照 |

### 12.1 当前仅保留七个落地门槛

| ID | 待关闭事项 | 处理边界 |
|---|---|---|
| Q1 | manifest成员核验、K/H/S SQL、同版本净事件与重放 | sink内实现+A01–A04，不能用新ledger替代集合证明 |
| Q2 | 非阻塞接管、正常背压与fatal生命周期闭环 | sink为主+A06/A07；仅证明确需时加manager小接入 |
| Q3 | SEALED tracker、spool字节预算和回收活性 | sink内实现+A05/A06，不能承诺框架全RSS硬界 |
| Q4 | 服务端终态与旧SQL排他 | sink客户端+A08/A10；未证明则人工恢复PoC，不扩自动HA承诺 |
| Q5 | DDL journal、schema fence、TRUNCATE与恢复分支 | sink为主、schema/recovery小接入+A09/A10 |
| Q6 | raw保留/去重、监控停写、扫描与机器总成本 | sink内+A11，严格跨capture额度不在首版 |
| Q7 | 事件元数据建表、类型矩阵、真实源契约和兼容接入 | 增量mapper/建表及小注册分支+A01/A12；不包含全量工作包 |

Q1–Q7均未以实现/实验关闭。功能或规模缩减必须可执行；若最小路径在既有框架不能正确停止/恢复，先提出精确小补丁，不继续扩写通用信用/progress/调度系统。

## 13. 交付顺序、灰度和回退

1. P0：固定server/driver，验证K/H/S SQL、raw完整核验、提交终态、原生事件及现有fatal关闭调用链；不创建生产资源。
2. P1：受控单capture、PK表DML纵切，完成buffer/spool、manifest、tracker、raw/MERGE、错误停机；覆盖A01–A08。
3. P2：事件驱动建表/类型、有限DDL与TRUNCATE恢复，对齐bend-cdc已支持的增量核心行为，完成A09–A12；不引入全量导出/导入工具。
4. P3：独立影子目标、对齐快照/水位比对，测容量与成本后决定是否继续生产化；自动HA另议，不因PoC通过默认开放。

旧链写旧目标，新sink写独立影子目标，不共享raw/journal；受控drain/对齐边界比较，不能任意时刻比两个异步目标行数。切读前保留旧链回放窗口，回退先追平旧链再切读。内部raw/journal格式版本化，不直接复用bend-cdc文件游标或不兼容旧schema。

## 14. 源码证据与验证状态

以下是固定源码证据，不代表新sink已经运行。路径基于Git对象，无需当前checkout存在同文件。

- [Sink接口/factory](https://github.com/sdojjy/tiflow/blob/2bf6dacd0aec9ac2fad464600e47341f9aa951f2/downstreamadapter/sink/sink.go)。
- [raw-change初始化与Run错误传递](https://github.com/sdojjy/tiflow/blob/2bf6dacd0aec9ac2fad464600e47341f9aa951f2/downstreamadapter/dispatchermanager/dispatcher_manager.go)；[session注册/重注册传播](https://github.com/sdojjy/tiflow/blob/2bf6dacd0aec9ac2fad464600e47341f9aa951f2/downstreamadapter/eventcollector/dispatcher_session.go)。
- [UK拆分](https://github.com/sdojjy/tiflow/blob/2bf6dacd0aec9ac2fad464600e47341f9aa951f2/pkg/eventservice/event_scanner.go#L970-L1014)；[IsUKChanged](https://github.com/sdojjy/tiflow/blob/2bf6dacd0aec9ac2fad464600e47341f9aa951f2/pkg/common/event/codec.go#L219-L259)。
- [同步Add与progress先登记](https://github.com/sdojjy/tiflow/blob/2bf6dacd0aec9ac2fad464600e47341f9aa951f2/downstreamadapter/dispatcher/basic_dispatcher.go#L293-L335)；[progress按flush移除](https://github.com/sdojjy/tiflow/blob/2bf6dacd0aec9ac2fad464600e47341f9aa951f2/downstreamadapter/dispatcher/table_progress.go#L97-L125)。
- [Run错误进入handleError，L393–404](https://github.com/sdojjy/tiflow/blob/2bf6dacd0aec9ac2fad464600e47341f9aa951f2/downstreamadapter/dispatchermanager/dispatcher_manager.go#L393-L404)；[错误队列/持续上报，L699–749](https://github.com/sdojjy/tiflow/blob/2bf6dacd0aec9ac2fad464600e47341f9aa951f2/downstreamadapter/dispatchermanager/dispatcher_manager.go#L699-L749)；[TryClose/LocalFence/stopWritePath，L1057–1173](https://github.com/sdojjy/tiflow/blob/2bf6dacd0aec9ac2fad464600e47341f9aa951f2/downstreamadapter/dispatchermanager/dispatcher_manager.go#L1057-L1173)。
- [flush/enqueue及Detach闭包](https://github.com/sdojjy/tiflow/blob/2bf6dacd0aec9ac2fad464600e47341f9aa951f2/pkg/common/event/dml_event.go#L660-L711)；[spool实现](https://github.com/sdojjy/tiflow/blob/2bf6dacd0aec9ac2fad464600e47341f9aa951f2/pkg/sink/spool/spool.go)。
- [TableSchemaStore类型分支](https://github.com/sdojjy/tiflow/blob/2bf6dacd0aec9ac2fad464600e47341f9aa951f2/pkg/common/event/table_schema_store.go)；[dispatcher恢复专属分支](https://github.com/sdojjy/tiflow/blob/2bf6dacd0aec9ac2fad464600e47341f9aa951f2/downstreamadapter/dispatchermanager/task.go)。
- bend-cdc：[实际MERGE适配入口](/Users/jiangjianyuan/work/git/pingcap/webapi/bend-cdc/pkg/source/tidb/databend_merge_sink.go)、[raw/MERGE/compaction](/Users/jiangjianyuan/work/git/pingcap/webapi/bend-cdc/pkg/sink/databend/databend_cdc.go)、[DDL epoch](/Users/jiangjianyuan/work/git/pingcap/webapi/bend-cdc/pkg/sink/databend/schema_epoch.go)、[TRUNCATE](/Users/jiangjianyuan/work/git/pingcap/webapi/bend-cdc/pkg/sink/databend/truncate.go)、[现有MERGE结果标记](/Users/jiangjianyuan/work/git/pingcap/webapi/bend-cdc/pkg/sink/databend/merge_result.go)。

验证状态：

- 前序已运行bend-cdc既有两包本地测试通过，真实Databend连接未配置、相关集成用例跳过；不能当作新sink证据。
- 本轮只统一文档和评审入口，保留v0.2历史快照；未改业务Go代码、未切分支、未构建/部署、未创建PR。
- A01–A12、K/H/S真实SQL、错误控制面关闭链、终态/恢复、扫描和费用实验均未执行。
- 文档结构/TOML/Git对象链接检查与上述运行验证分开报告。去掉旧协议是范围裁决，不是正确性已被证明。
