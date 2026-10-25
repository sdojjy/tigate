# 定稿 v1.1：TiCDC 原生 Databend Sink——按 bend-cdc 既有语义迁移

| 项目 | 当前裁决 |
|---|---|
| 版本 / 日期 | v1.1 / 2026-10-25；v1.0定稿后按用户要求增加conflict-key及指定DDL跳过 |
| 状态 | 设计已接受，首版实现及受控真实链路验收通过；不表示生产HA/成本已验收 |
| 实现分支 | `feat/databend-native-sink`，从固定新架构commit `2bf6dacd0aec9ac2fad464600e47341f9aa951f2` 创建 |
| 正确性基线 | 对齐固定版本 bend-cdc 的 TiDB 增量路径，不为迁移另造更强的通用同步系统 |
| 实施主线 | 迁移现有 Databend raw/MERGE、mapper、schema epoch 核心；新增原生事件、spool、TiCDC回调适配 |
| 输入与来源默认值 | 与固定版本CloudStorage默认配置保持拆分模式 raw-change=false；首版固定raw source，stream仅保留后续验收入口 |
| 不新增 | batch manifest持久化/逐成员读回、K/H/S新SQL算法、event_id/payload_hash体系、AppliedCycle/DML ledger、raw_generation协议、collector信用/progress迁移、分布式锁/预算服务 |
| 缓冲 | 保留有界内存 + 限额本地spool + 现有PostEnqueue背压 |
| 全量范围 | 不支持/编排Dumpling、CSV、COPY INTO，不实现历史空表发现/预建 |
| 首版边界 | 沿用单逻辑目标表单writer、目标静态独占前提；故障恢复先验证，不承诺新建自动HA体系 |
| 已验证 | 本地/race/lint/build及真实TiUP→CDC→staging Lake：9组PASS、32列、磁盘spool/排空重启；见§8.2–8.3，未验收生产HA/成本 |

**本文是唯一当前定稿设计。** [v0.3](/Users/jiangjianyuan/work/git/pingcap/ticdc/docs/design/databend-native-sink-design-review-v0.3.md)和[v0.2](/Users/jiangjianyuan/work/git/pingcap/ticdc/docs/design/databend-native-sink-design-review-v0.2.md)仅保留历史分析；其中manifest、K/H/S、raw generation等不是并行实施要求。设计评审不再以新增通用协议扩大范围；实现中若发现不可规避的范围变更，单独说明并复审。

## 0. 固定源码与原则

- TiCDC：`/Users/jiangjianyuan/work/git/pingcap/ticdc`，commit `2bf6dacd0aec9ac2fad464600e47341f9aa951f2`，origin `git@github.com:sdojjy/tiflow.git`。
- bend-cdc：`/Users/jiangjianyuan/work/git/pingcap/webapi` 下 `bend-cdc`，commit `b6481621021e82aee659ad88e8e6c00f6dd11906`。
- 设计期间曾在旧架构master / `03ed2432bd7782198742d3aa20e07edcc47097e6` 下按Git对象核对；用户批准实现后已创建并切到上述新架构基线的实现分支，没有在旧master开发。

原则：**替换输入和运行时外壳，不先重设计已经存在的目标应用算法。** 新增约束必须归入三类之一：既有行为迁移、移除S3后必须补的接入、可延期的增强；不能只因理论上可加强就进入首版。

“同样消费TiCDC”不意味着故障语义完全相同：原链有S3持久缓冲、文件/epoch顺序与独立消费进程；直接sink必须把必要的顺序/完成语义接到现有dispatcher/callback。也不能把旧代码注释/假设当成全部合法输入都已正确的证明。

本轮只补六个局部缺口：拆分输入兼容、规范化一次、raw source默认值、内层UNKNOWN截断、spool无成功回调清理、落库精度排序。没有重新引入manifest/ledger/witness或collector/progress协议。默认值来自固定源码，不代表已核实任何线上changefeed配置；旧链若显式启用raw=true，其PK更新行为需另列既有缺口。

## 1. 实际代码基线：已有的保证、假设与限制

| 主题 | bend-cdc实际代码 | 迁移处理 |
|---|---|---|
| 核心输入 | CanalJSONMessage转common.Event，再经normalizeCDCEvent交CDCManager | 原生DMLEvent经NativeNormalizer进入规范行入口，不二次做Canal解码 |
| 版本 | 优先 `_tidb.commitTs` → TiDBCommitTS → event_pos；无扩展时兼容退回毫秒TS | 直接用原生CommitTs，复用排序规则；不继承“TSO每行唯一”的错误注释 |
| 主键 | 完整PK集合canonical化、复合conflictIdentity与MERGE JOIN | 直接从TableInfo获取相同键语义，保留复合键测试 |
| raw记录 | `id, table_name, raw_data, add_time, event_pos, action` | 优先保持形状，不为迁移增加manifest/event_id表或新一套版本身份 |
| UPDATE/DELETE | UPDATE使用After，DELETE使用Before；默认拆分模式提供PK/UK变更的DELETE/INSERT | 保持默认拆分和输出顺序；不把raw=true变键UPDATE直接交After-only核心 |
| 顺序与重放 | 严格模式按(event_pos,add_time)选候选，并查询retained raw抑制旧值 | 先迁移既有SQL和清理语义；同TSO合法净动作须做源事件对照，不用到达时间掩盖新增差异 |
| 清理 | 删除被更高版本支配历史，保留每键最新watermark/tombstone | 保留，不引入TTL删最新墓碑；不承诺历史键集合有界 |
| stream | 创建append-only stream；失败时用raw作为同一个MERGE builder的source | 首版显式固定raw；stream代码可保留，但未验收不创建/消费，不用静默fallback当完成证明 |
| 完成进度 | raw成功后writeGen递增；mergeTable捕获startGen，成功后mergedGen推进；Flush排空 | 复用批次/代数级完成，映射TiCDC callback，不默认读回每行 |
| DDL顺序 | 文件schema版本边界先Flush，ApplySchema保存pending/applied epoch | 使用现有TiCDC屏障代替文件队列，迁移epoch/reconcile |
| TRUNCATE | Flush→pending epoch→drop stream/raw→truncate target→applied | 迁移现有reset顺序，测试重启重放，不增加多套代际状态机 |
| 不支持DDL | 某些DDL只warning并推进epoch，并没有执行目标DDL | 不把warning跳过算“已支持”；只对齐已正确投影的能力，不承诺任意DDL |

代码事实不等于运行证明。例如writeGen/mergedGen假定MERGE覆盖了已记账raw批次；QueryContext返回Rows被现有代码视作executed；这些前提接入callback前需要局部验证，不能靠新增一套架构掩盖，也不能不经验证就宣布安全。

## 2. 最小实现：三个主体，而非新框架

```text
现有TiCDC dispatcher / TableProgress / DDL协调
  → NativeAdapter
      TableInfo、CommitTs、before/after → 等价内部事件/原生类型转换
  → Buffer + Spool + TableWorker
      有界接管、按表攒批、内存/本地磁盘、有限并发、callback记账
  → 迁移的DatabendCore
      建表、参数化raw INSERT、已有MERGE/compaction、schema epoch/TRUNCATE
```

对每张表，第一版由一个worker串行控制：

```text
取一个有界批次
 → 全部raw子INSERT成功
 → 调用等价的FlushTable（复用现有Flush/mergeTable逻辑）
 → 确认本轮排空成功
 → 完成覆盖的分片、释放spool
 → 源事件全部完成时flush callback一次
```

这样无需在单表内并发raw写入与MERGE，也无需manifest逐成员核验。普通INSERT成功按完整SQL执行结果记账；如果分多条SQL，全部成功才使该批具备完成资格。失败/结果未知不确认，局部重试或停止后从checkpoint重放；不把“写了一部分raw”当作一批成功。

实现可保留writeGen/mergedGen或等价本地批次序号，属于进程内callback记账，不是持久化应用日志。移植核心算法，不照搬每表连接池、Background context、无界goroutine/重复后台循环，也不让TiCDC依赖整个webapi服务模块。

## 3. raw/stream/MERGE如何复用，哪些不能盲抄

### 3.1 不再重设计MERGE来源：首版固定raw

保留现有 `buildMergeSQL(target, source, raw, schema)`、复合键JOIN、列投影和compaction；首版调用显式传 `source=raw`，初始化不创建stream，也不在后台自动切换为stream。复用现有raw分支，不是新K/H/S算法。

- 首版使用现有full-raw路径：单writer停止新写，完整MERGE当前epoch的raw，再确认该批。无stream cursor、消费hint或witness依赖。
- stream作为后续增量读取优化保留，不以“删除stream代码”作为简化目标。启用时必须证明当前server/每个连接的有效设置下Flush覆盖所记批次，且验证no-op消费；如果只处理一部分，不能按startGen确认全部，也不能循环现有Flush冒充排空，因为pending可能已经被清零。
- 不强制新增batch_id/manifest、K/H/S或读回所有raw成员；也不因为不用stream就保证更省钱。full-raw可能扫描更多，性能决定是否需要启用已有stream优化。

验收门槛很具体：两次raw INSERT、两个不同键，Flush必须将两键应用后才完成两个callback；加入no-op、旧事件重放。stream消费限额是后续启用测试，不能“与旧代码一样”就提前确认；不新增消费witness或逐成员读回。

### 3.2 同TSO和类型：先比较等价输入

**本版撤销强制raw-change=true。** 固定CloudStorageConfig未设置该开关时返回false，manager对新增SinkType也默认false；首版沿用拆分模式，不需要新增“强制true”分支。启动/重注册测试确认传播值为false，用户不能把它当调优项切成true。该模式下PK/UK更新由既有scanner拆分；adapter/worker必须保留其DELETE/延后INSERT输出顺序，不能重新逐行交错或仅按TSO排序。

例如非聚簇PK从1变2，兼容路径应收到并应用DELETE(1)、INSERT(2)；不能将带oldPK!=newPK的raw UPDATE直接送After-only核心。若意外收到这种UPDATE，首版显式拒绝/停写，不静默仅写After。将来需要raw=true时，须补oldPK删除和同事务顺序适配后再开放，不因此新建事务DAG。

source事务TSO不是每行唯一；同key/TSO合法相反动作可能依赖上述既有源顺序。分片/重启后同样要通过最终键集合测试，不用本机时钟创造源业务版本。

**规范化恰一次**：迁移核心提供两个边界清晰的入口，汇合后共享raw构造/INSERT/MERGE：

```text
Canal原始事件 → normalizeCDCEvent（原ordinal/Latin-1等规则） ┐
Native datum → NativeNormalizer（按FieldType取label/bytes） ┴→ NormalizedRow → raw构造/INSERT
```

NormalizedRow携带规范PK/列值/动作和CommitTs；Native路径不再进入normalizeCDCEvent。Source仍是TiDB，不通过伪装其他Source绕过解码；旧getTiDBSequence仍依赖它，或在规范行接口中显式传递同一CommitTs。

比较的是规范化后的键、payload、动作、TSO，而非native datum与Canal原始字符串。数字label `ENUM('2','x')`：Canal ordinal `"1"`和native label `"2"`均只能最终得到`"2"`，不能二次解码成`"x"`。还需SET高位、全字节二进制、精确decimal、大整数JSON、SQL NULL/JSON null、时间精度；raw JSON序列化不再次做类型解码。

### 3.2.1 add_time按落库精度单调，而非仅Go时间单调

既有核心按1ns递增，但raw参数只输出6位小数，可能落成相同时间。依赖同key/TSO顺序的兼容路径需在raw构造中局部修正：先将candidate量化到实际存储/参数精度（当前格式为微秒），若不大于该表lastStored，则取lastStored+1个存储精度单位，再格式化。不要先做纳秒比较再截断。

单writer按源顺序调用该分配器；测试必须检查最终SQL参数及真实raw时间/目标状态，而不只检查内存Time.After。add_time仍是既有同TSO决胜辅助，不成为新的业务版本体系，也不解决任意源冲突/失租并发；同payload纯重复不作为该错误反例。

### 3.3 完成结果与旧缺口

**必须在迁移的客户端/核心最内层分类，不能只在外层worker判断UNKNOWN。** 旧executeMergeInto在QueryContext返回Rows后设executed，后续Rows.Err也携带true；旧mergeTable会把executed错误改成成功，其他临时错误则可能已在内部重发。原分支不能透明照搬。

迁移后使用最小提交状态 `COMMITTED / NOT_COMMITTED / UNKNOWN`，统计解析错误单独携带；raw写、MERGE和显式COMMIT同样适用。每次结果进入核心后，**先判断UNKNOWN，再判断观测失败或可重试错误**：

- COMMITTED且仅统计失败：业务成功、统计unavailable，不为补统计重做。
- 明确NOT_COMMITTED且可重试：才进入既有有界重试；不能把所有网络错误都归入该类。
- UNKNOWN：立即退出内层循环，上层不得按transient再重试；不推进mergedGen/pending，不callback，不进下一批/DDL。先核验旧SQL终态，取得排他后用retained raw幂等重应用；无法证明就停写、人工恢复。

mock检查错误分支与物理请求次数；真实实验检查提交响应丢失后的恢复。mock构造的“首页后失败”不是声称真实MERGE已复现。只改Databend客户端/核心，不引入DML ledger、AppliedCycle、查询注册中心或分布式fence。

## 4. 哪些仍是直接sink必须新增的适配

### 4.1 MERGE后才确认TiCDC

bend-cdc已有MergeProgressReporter/applied watermark区分“文件处理/raw写入”与“目标已MERGE”。原链的文件游标、S3持久数据和lake applied进度不是同一件事。

新sink用现有PostFlush表达目标应用完成，不能在raw INSERT成功时就推进TiCDC checkpoint。否则去掉S3后，可能没有一个可靠消费者负责把已确认raw继续应用。这不是追求比bend-cdc更强的exactly-once，而是把已有目标完成语义接到不同的上游接口。

同一事件的回调由本地completion对象持有；分片先登记后发布，编码完成Seal，`SEALED && pending==0`时once回调。已应用分片可独立回收，避免大事件等整事件确认才释放空间。只保留有界计数/未完成描述符，不增持久化tracker。

### 4.2 保留spool和现有流控，不改collector

- 有界入口、内存热数据、限额磁盘、字节/元素数记账；单行超上限明确失败，不无限内存直通。
- 正常压力延迟PostEnqueue、强制小批Flush；磁盘物理未回收继续计费，压力唤醒钉住segment的表。
- 已接管事件不因transport session reset丢弃。失败不伪造PostFlush，不清迁旧TableProgress。
- AddDMLEvent不无限等待共享线程；真正无法接管/IO致命错误关闭sink本地admission和写gate，Run报首因；旧manager/dispatcher需最终结束，不只reset旧session继续跑。
- 先验证现有关闭链，确有缺口仅补Databend fatal小分支。上游chunk已在sink前分配，不承诺全进程RSS硬上限；不为此改collector/dynstream信用协议。

**spool包装有两个不可透明继承的行为**：固定spool的Discard会先跑entry postFlush再Release；TryEnqueue对超过disk quota的entry有oversized内存接受分支。native包装必须隔离：

1. 源事件成功callback只由SEALED tracker持有；spool message/entry的postFlush列表保持为空，也不能间接挂上会递减tracker成功计数的回调。取消/fatal/Close仅走无成功回调的资源Release；不调用语义不匹配的Discard来确认未应用数据。PostEnqueue只用于背压，不转成成功确认。
2. enqueue前按包含metadata/编码开销的保守字节上界拆分entry；单行仍超限就拒绝，不进入oversized旁路。若缺少一致大小估算接口，仅补spool小型计量/包装，不改collector；“接受后发现oversized再失败”不能算预防超额。
3. 正常MERGE成功时由worker显式完成tracker并释放分片；重入/迟到Release不得再次完成源事件。分别在raw前、raw后MERGE前注入fatal/Close，源flush次数必须为0；普通压力仍可提前Flush恢复，而非频繁fatal。

## 5. DDL：迁移现有epoch，而不是新建DDL协调系统

沿用 `ApplySchema`、`SchemaChanges`、`SchemaEpoch/SaveSchemaEpoch`、reconcile和TruncateCDCTable已有语义：

```text
TiCDC现有屏障排空旧DML
 → 读取目标epoch；已应用的同DDL跳过
 → Flush该表
 → 保存pending epoch
 → 翻译/执行已支持DDL或TRUNCATE reset
 → 保存applied epoch
 → DDL PostFlush，再放新DML
```

固定TiCDC的SchemaFile.Build明确使用event.FinishedTs填TableVersion，因而迁移字段映射直接固定：

| 字段 | native取值/用途 |
|---|---|
| raw event_pos | DMLEvent.CommitTs |
| 实际DDL epoch | DDLEvent.FinishedTs |
| TableInfoVersion/更新时间 | 解释列元数据，不自动成为applied fence |

还必须迁移旧DatabendMergeSink.writeBatch的外层保护，不只复制CDCManager：写前读epoch；CommitTs<=epoch的旧事件仅在其排空/DDL恢复前提已成立时按旧规则跳过；pending时新数据禁止写入，先reconcile到applied。启动加载schema只读/核验，不能凭空SaveSchemaEpoch标成已处理历史。epoch==pending也不等于DDL已经执行，实际DDL仍按原恢复分支处理。

TRUNCATE继续清理旧raw/stream并清目标，applied前不得写新代际。测试执行后checkpoint未保存时重放，不重复清空新数据。若旧epoch记录不足以识别特定中断，只补必要字段/检查，不默认增加before/after全量快照、独立raw_generation和多份journal。

普通增量自动建表仍使用权威事件元数据；类型映射/有限列DDL按bend-cdc现有支持范围。warning跳过但未执行的DDL保留明确限制，不能当镜像一致性保证。复杂/无证明操作可停止，不为追平“任意DDL”扩大首版。

## 6. 外部改动边界与非目标

| 范围 | 当前处理 |
|---|---|
| 新Databend包/测试 | 主要工作量：提取迁移核心、native adapter、buffer/spool、worker/callback |
| scheme/type/config/factory/metrics | 常规sink注册小改动 |
| raw-change注册/重注册 | 首版沿用默认false；复用并测试现有传播，不新增强制true分支 |
| TableSchemaStore/recovery | 增加必要类型/恢复分支；不伪装MySQL，不改进度算法 |
| manager fatal关闭 | 仅验证不足时的有限小补丁 |
| collector/dynstream/TableProgress/调度核心 | 不做通用协议重构 |

不实现Dumpling/CSV/COPY INTO、全量编排、历史空表发现/预建；不实现新的全库schema snapshot服务；不要求跨表事务原子性、分布式预算、目标分布式锁或通用自动HA系统。

原有单目标writer、无外部写入的前提必须在TiCDC部署中成立，不能因为来源都是TiCDC就自动假定。across-nodes=false只禁止拆表分布；旧owner在途SQL不是客户端cancel就结束。首版在受控单capture/目标独占下做迁移与恢复实验，不为新增sink顺带重做HA。无法确认旧SQL结束时停写，这不是已交付生产HA。

远端raw增长按现有compaction和监控告警处理，可加本地阈值停新raw；不叫严格跨capture quota，不新增容量账本。历史PK新增后删除仍会保留墓碑，这项成本按旧链同样负载比较，不因它存在就要求另建watermark存储系统。

## 7. 迁移验收：证明不退化，另测接入差异

以相同输入和相同能力前提运行旧Canal→bend-cdc核心与新native→迁移核心，比较最终目标状态、错误与DDL处理。不是只看“两个都返回nil”，也不把旧实现的已知错误当金标准。

| ID | 迁移测试 | 核查重点 |
|---|---|---|
| T1 | 两路径规范化结果，数字label ENUM、SET高位、全字节binary、decimal/JSON/NULL/时间 | 规范化恰一次；Source/CommitTs不改丢；键、payload、动作、TSO等价 |
| T2 | 默认拆分、非聚簇PK单行变键、UK更新、合法同TSO键复用、同微秒时间、跨分片/重启 | 旧键消失新键唯一；保留源DELETE/INSERT顺序；检查SQL时间参数及raw落库精度，墓碑阻止旧值复活 |
| T3 | 首版raw source、低流量/no-op；后续stream强制部分消费 | 目标真正覆盖记账工作才确认；stream不得只凭pending/mergedGen自证排空，未消费B不得回调B |
| T4 | raw子SQL失败/UNKNOWN、MERGE首响应/提交响应丢失、callback前重启 | 最内层UNKNOWN前置截断，检查请求次数不重发；不提前确认，终态排他后重放收敛 |
| T5 | FinishedTs映射、外层epoch过滤、pending阻新DML、TRUNCATE中断/重放 | bootstrap不写applied fence；旧schema不污染新结构，旧TRUNCATE不清新数据 |
| T6 | OPEN/分片回收、spool超限、raw前/raw后MERGE前fatal/Close、reset、压力 | 源flush不得由Discard/清理触发，未应用为0；无oversized旁路，正常压力不变异常，旧生命周期结束 |
| T7 | 同吞吐/行宽/历史键/热点/延迟SLO比较旧链和新sink | 记录扫描及full-raw重复更新保留键造成的目标重写、CPU/RSS/IO/credit与总费用；不预先承诺比例 |

T1–T7是完整迁移验收门槛：当前已有其中的本地单测/HTTP模拟覆盖，真实TiDB→Databend、原生/Canal差分及成本实验仍待执行。完成它们所需的最小Databend局部修正是范围内工作；把它们替换成新调度/交付/事务协议不是默认答案。

## 8. 本轮已执行与未执行

源码静态核对覆盖：DatabendMergeSink输入/PK/配置、CDCManager raw/Flush/mergeTable/SQL/compaction、MergeProgressReporter/applied watermark、ApplySchema/schema epoch/TRUNCATE、现有相关单测与TiCDC回调接入。

设计阶段v0.4在bend-cdc执行（v0.4.1评审时未重复运行，也尚未实现新sink）：

```text
go test ./pkg/sink/databend ./pkg/source/tidb -count=1
ok  databendcloud/bend-cdc/pkg/sink/databend  20.922s
ok  databendcloud/bend-cdc/pkg/source/tidb    8.784s
```

未配置BENDCDC_TEST_DSN/TIDB_DDL_INTEGRATION_DSN，相应真实warehouse测试跳过。这证明既有本地单测/模拟测试通过，不证明stream全量消费、driver终态或新sink恢复已实测。虽然source包包含全量工具单测，运行它们不意味着迁移那些工具。

v0.4.1新增静态核对：CloudStorage raw-change默认false/manager默认值、normalizeCDCEvent的Canal专属解码/getTiDBSequence、spool Discard/oversized分支、raw时间格式与单调分配、SchemaFile.Build的FinishedTs映射、writeBatch外层epoch过滤。在v0.4.1评审时，六项修正仅在设计中明确，未标记为代码已修复；定稿后的首批实现状态见下文。

上述记录属于设计阶段。用户批准后已创建实现分支，现已完成sink运行时接入与主程序本地构建，并在2026-10-25通过受控真实TiDB/Lake链路（§8.3）；未部署共享CDC、未做在途真实故障或成本实验，未提交或推送。历史版本不作当前规范。

### 8.1 首批实现记录（2026-10-24，历史进度）

首批代码集中在 `/Users/jiangjianyuan/work/git/pingcap/ticdc/downstreamadapter/sink/databend/`：规范行入口、复合键身份、参数化raw/既有MERGE/compaction SQL、微秒时间分配、提交状态前置截断、SEALED completion及对应单测。错误码位于 `/Users/jiangjianyuan/work/git/pingcap/ticdc/pkg/errors/databend.go`。

当时仅为核心实现切片，不是已可创建changefeed的完整sink（后续接入见§8.2）：真实HTTP/driver终态客户端、DMLEvent datum适配、配置/factory注册、spool worker、自动建表/DDL epoch恢复尚待后续接入。当前writer通过执行函数边界要求明确提交结果，不能将测试注入的结果当真实服务端终态协议已完成。完整T1–T7仍待后续集成验收。

本批5个生产文件、4个测试文件（15个测试函数），没有修改collector、TableProgress、dynstream或scheduler；未注册不可用的scheme/factory。验证记录：

```text
GOMAXPROCS=4 go test -p 2 ./downstreamadapter/sink/databend ./pkg/errors -count=1
PASS（两个包）
GOMAXPROCS=4 go test -p 2 -race ./downstreamadapter/sink/databend ./pkg/errors -count=1
PASS（两个包）
GOMAXPROCS=4 go vet -p 2 ./downstreamadapter/sink/databend
PASS
```

gci/gofumports、当前文档本地链接/代码围栏/空白检查通过。单测覆盖规范值保持、实际raw参数的微秒排序、SQL构造、子INSERT/MERGE未知结果不重发、已提交后统计失败不重试，以及OPEN/SEALED/失败/并发重入回调。SQL仅检查构造和调用顺序，未在真实Databend执行；尚无完整sink运行入口，因此未运行sink集成、真实故障与性能测试，也未执行全仓测试/主程序构建。

### 8.2 运行时接入记录（2026-10-25）

实现分支仍为 `feat/databend-native-sink`，基线未变；没有提交、推送或更改webapi。

- 原生接入：`native.go`直接读取TableInfo/typed chunk；ENUM/SET取label、binary保留字节、decimal/大整数/JSON保持精确文本，SQL NULL与JSON null分开。TIMESTAMP按TiCDC配置的源时区转UTC；DATETIME保留wall clock，HTTP会话固定UTC。PK变键的未拆分UPDATE显式拒绝。
- 运行时：`sink.go`/`lane.go`固定数量lane按逻辑目标表hash，保持单表串行；源事件池按字节/元素有界，Add不阻塞共享handler。正常spool压力提前排空/延迟PostEnqueue，容量异常停止整个旧生命周期；不操作collector/progress条目。
- 攒批：支持行数/编码字节/定时/DDL屏障/配额压力触发；大事件分片边应用边回收，封口前不flush源事件。复用共享spool；新增的通用接口仅有`AccountingBytes`，用于接纳前阻止oversized旁路。源成功回调始终不放入spool消息。
- 客户端：直接使用Databend HTTP Query API的`params`与终态，绕开驱动内层POST重试。仅匹配query ID的Succeeded确认mutation；Running首页含结果不算成功。UNKNOWN带请求ID报错、停止lane/整个sink，不自动重发、不根据取消调用返回值宣布旧SQL结束。统计不可用不撤销已确认提交。URI继续请求限制同origin，禁止重定向和错误中泄漏SQL/参数/凭据。
- SQL：参数化raw INSERT→首版固定full-raw MERGE→已有strict compaction；排序时间按微秒递增，并在重启时读取retained raw的MAX(add_time)，避免本机时钟回退导致同TSO决胜顺序倒退。源列名`action`不再与内部动作列冲突；真实验收后投影改为`get(raw_data, 字面键)`，避免引号列名被错误解释为JSONPath。
- schema：按权威类型创建目标/raw；复用每表schema epoch，补充一份有界的post-DDL定义/翻译步骤供pending恢复。它不是DML ledger、manifest或新generation协议。pending恢复先于新DML，applied epoch按FinishedTs过滤旧输入；bootstrap不保存applied。TRUNCATE先清raw/target再applied，applied重放不重复清新数据。
- 接入点：scheme/type/factory、TableSchemaStore类型分支、错误分类和metrics；没有改collector、dynstream、TableProgress、调度或manager算法。Run/DDL错误使用既有unretryable路径结束旧生命周期；真实UNKNOWN故障下的控制面关闭/人工恢复仍待验收。
- 本地验证包含：原生精确类型、原始UPDATE拒绝、实际raw时间参数、OPEN/SEALED、并发/重入回调、部分raw结果未知、MERGE未知、低流量/transport epoch变化、配额压力、多表排空、Close/接纳竞争、pending列DDL与TRUNCATE恢复、applied重放、远端监控阈值、factory注册。

HTTP fixture只模拟协议与journal持久化，不执行SQL，不能证明真实Databend提交原子性。真实入口为`TestDatabendNativeLiveIntegration`，仅在显式配置`DATABEND_NATIVE_TEST_URI`时运行，创建/删除独立随机测试database；运行时接入阶段曾跳过，随后获授权连接staging Lake通过（§8.3）。没有启动或修改生产资源。

本轮验证命令与最终结果见§10.4；全量`pkg/common/event`测试曾在TiDB测试依赖中出现`duplicate metrics collector registration attempted` panic。未改无关依赖；缩小到本次改动相关的TableSchemaStore测试后通过。全仓测试未运行。

### 8.3 真实 TiUP→CDC→staging Lake 验收（2026-10-25）

获用户授权，使用其staging Lake和本地TiUP v8.5.1，当前分支构建新架构CDC；只操作随机独立库。修复后从新库完整重跑，9组全部PASS：DML/PK/UK、复合键、特殊标识符、32列类型和值、有限列DDL、TRUNCATE、批处理/spool、受控进程重启、DROP TABLE。

- 实测发现并修复DESCRIBE unsigned类型别名不兼容、含双引号列名的JSONPath投影；补回归测试并重跑完整链路。增加`lake://` HTTPS别名以支持用户实际URI。
- 1200行事务使用64 KiB磁盘spool、极小内存热集，实际41次磁盘读回；证明本负载能边应用边回收，不等于性能/RSS硬上限证明。
- 排空/暂停并终止旧CDC，停机期间积压DML/DDL，同data-dir重启后从原checkpoint恢复并逐行比对；不是在途UNKNOWN或自动HA实验。
- 真实核心测试PASS（13.36s）：旧UPSERT不复活删除键、applied过滤和重复TRUNCATE。6包race、sink lint、主程序构建通过。
- 源/目标测试库、feed按具体名字确认已清理，本次本地CDC/TiUP已停止；未提交、推送或部署共享集群。

详见[真实验收报告](./databend-native-sink-e2e-report.md)及[可重复测试说明](../../tests/integration_tests/databend_native/README.md)。真实UNKNOWN故障、跨capture/HA、长期墓碑增长及成本/SLO未验收，T1–T7不据此整体关闭。

### 8.4 bend-cdc 对照、DDL矩阵与真实响应丢失（2026-10-25）

用户要求补齐的测试已执行，详见[补充验收报告](./databend-native-sink-parity-fault-report.md)。这里纠正之前过宽的范围表述：TiCDC的`GetPrimaryKeyColumnNames`包含非空唯一键fallback；无显式PK但有NOT NULL UNIQUE的原生DML/变键已实测通过，不需要新增framework协议。

固定bend-cdc真实core对照确认：RENAME TABLE和DROP DATABASE不执行目标操作，只跳过/推进epoch，不能算已支持；真正无可用PK/非空UK时，旧版可用显式conflictKey，native尚未迁移；旧core能扁平投影分区元数据及普通DML，native仍拒绝分区dispatcher。多列组合DDL旧版拒绝，native支持的ADD/DROP/RENAME组合已实测。

本轮还复现并局部修复INT→VARCHAR(200)被过度拒绝：只开放完整容纳有符号INT32的VARCHAR(n>=11)，不开放同字符类型未知宽度/长度缩小。原failed feed更换本地二进制后从原checkpoint恢复成功；生产改动仅在sink/schema.go。

真实Lake故障注入覆盖部分raw已提交/第二条未发、MERGE成功但成功响应被丢弃，以及CREATE/ALTER/TRUNCATE/DROP共19个关键提交边界。客户端未取得证据时无成功callback、旧sink停止；在测试方已独立取得匹配终态的前提下重建manager，恢复journal及重放验证通过。该测试不伪造SQL执行，但不是每个OS指令点SIGKILL、服务端执行中崩溃或自动HA证明。具体最终矩阵与清理证据以补充报告为准。

### 8.5 显式conflict-key与跳过DDL（2026-10-25）

按用户最新要求：增加URI `conflict-key`，对应bend-cdc持久化配置`conflict_key`/运行时`ConflictKey`，不是新建SQL主键。与旧版相同，单列override可以替代无键/单列源键，但不允许覆盖复合键；按可见列元数据正名，NULL身份或未拆分的变键UPDATE拒绝。操作者保证业务键唯一、非空、稳定；真正无索引键的源表还需启用既有force-replicate。DDL journal保存实际选取的键，DML和DDL共用这一配置，不改通用框架。

RENAME TABLE/RENAME TABLES与DROP DATABASE在旧DML排空后warning跳过、正常回调，不更改目标DDL或伪造applied epoch。历史目标保留；重命名后的DML按新表名进入新投影，不会搬迁历史数据。旧DML结果未知仍必须停止，不能靠跳过后续DDL绕过。

真实TiUP→CDC→staging Lake补测：无键CRUD+ADD COLUMN及单PK override、重命名后进度/后续DML、DROP DATABASE后进度/保留目标/新库表DML，3组均通过。详细证据见[补充报告§8](./databend-native-sink-parity-fault-report.md)。分区保护未放开：lane只约束单进程，跨capture同一逻辑表排他未证明；不是Lake无法存储分区数据。

## 9. 可复核源码

bend-cdc链接按固定commit的本地源码核对；远端reviewer按相同Git对象路径读取。

- [Canal事件转换与现有目标配置，L376–559](/Users/jiangjianyuan/work/git/pingcap/webapi/bend-cdc/pkg/source/tidb/databend_merge_sink.go#L376-L559)。
- [PK/schema与ApplySchema，L93–163](/Users/jiangjianyuan/work/git/pingcap/webapi/bend-cdc/pkg/source/tidb/databend_merge_sink.go#L93-L163)。
- [raw写入与代数记账，L640–840](/Users/jiangjianyuan/work/git/pingcap/webapi/bend-cdc/pkg/sink/databend/databend_cdc.go#L640-L840)；[MergeProgress，L399–416](/Users/jiangjianyuan/work/git/pingcap/webapi/bend-cdc/pkg/sink/databend/databend_cdc.go#L399-L416)。
- [Flush排空，L503–578](/Users/jiangjianyuan/work/git/pingcap/webapi/bend-cdc/pkg/sink/databend/databend_cdc.go#L503-L578)；[MERGE/代数推进/清理，L1230–1416](/Users/jiangjianyuan/work/git/pingcap/webapi/bend-cdc/pkg/sink/databend/databend_cdc.go#L1230-L1416)。
- [现有MERGE SQL，L1628–1705](/Users/jiangjianyuan/work/git/pingcap/webapi/bend-cdc/pkg/sink/databend/databend_cdc.go#L1628-L1705)；[stream创建及fallback，L889–942](/Users/jiangjianyuan/work/git/pingcap/webapi/bend-cdc/pkg/sink/databend/databend_cdc.go#L889-L942)。
- [applied watermark与MergeProgress接口](/Users/jiangjianyuan/work/git/pingcap/webapi/bend-cdc/pkg/source/tidb/applied_watermark.go)；[schema epoch](/Users/jiangjianyuan/work/git/pingcap/webapi/bend-cdc/pkg/sink/databend/schema_epoch.go)；[TRUNCATE](/Users/jiangjianyuan/work/git/pingcap/webapi/bend-cdc/pkg/sink/databend/truncate.go)。
- [schema队列的flush/警告范围](/Users/jiangjianyuan/work/git/pingcap/webapi/bend-cdc/pkg/source/tidb/schema_queue.go)；[Flush错误传播测试](/Users/jiangjianyuan/work/git/pingcap/webapi/bend-cdc/pkg/sink/databend/flush_permanent_test.go)。
- [TiCDC Sink接口](https://github.com/sdojjy/tiflow/blob/2bf6dacd0aec9ac2fad464600e47341f9aa951f2/downstreamadapter/sink/sink.go)；[原生DML回调](https://github.com/sdojjy/tiflow/blob/2bf6dacd0aec9ac2fad464600e47341f9aa951f2/pkg/common/event/dml_event.go#L660-L711)。
- [raw-change与sink Run/error/close接入](https://github.com/sdojjy/tiflow/blob/2bf6dacd0aec9ac2fad464600e47341f9aa951f2/downstreamadapter/dispatchermanager/dispatcher_manager.go)；[原生DDL字段](https://github.com/sdojjy/tiflow/blob/2bf6dacd0aec9ac2fad464600e47341f9aa951f2/pkg/common/event/ddl_event.go)。
- [CloudStorage raw-change默认值，L774–784](https://github.com/sdojjy/tiflow/blob/2bf6dacd0aec9ac2fad464600e47341f9aa951f2/pkg/config/sink.go#L774-L784)；[scanner拆分与延后INSERT，L970–1014](https://github.com/sdojjy/tiflow/blob/2bf6dacd0aec9ac2fad464600e47341f9aa951f2/pkg/eventservice/event_scanner.go#L970-L1014)。
- [规范化入口，L14–39](/Users/jiangjianyuan/work/git/pingcap/webapi/bend-cdc/pkg/sink/databend/tidb_values.go#L14-L39)；[外层epoch保护，L215–235](/Users/jiangjianyuan/work/git/pingcap/webapi/bend-cdc/pkg/source/tidb/databend_merge_sink.go#L215-L235)。
- [spool oversized接纳，L503–510](https://github.com/sdojjy/tiflow/blob/2bf6dacd0aec9ac2fad464600e47341f9aa951f2/pkg/sink/spool/spool.go#L503-L510)；[Discard执行postFlush，L734–746](https://github.com/sdojjy/tiflow/blob/2bf6dacd0aec9ac2fad464600e47341f9aa951f2/pkg/sink/spool/spool.go#L734-L746)。
- [raw微秒格式化，L995–1003](/Users/jiangjianyuan/work/git/pingcap/webapi/bend-cdc/pkg/sink/databend/databend_cdc.go#L995-L1003)；[纳秒单调时间，L1755–1763](/Users/jiangjianyuan/work/git/pingcap/webapi/bend-cdc/pkg/sink/databend/databend_cdc.go#L1755-L1763)；[SchemaFile使用FinishedTs，L208–215](https://github.com/sdojjy/tiflow/blob/2bf6dacd0aec9ac2fad464600e47341f9aa951f2/pkg/cloudstorage/schema_file.go#L208-L215)。


## 10. 首版使用方式与实际支持边界

### 10.1 配置入口

所有Databend专属参数放在sink URI，不引入一套通用框架配置。`lake://`与`databend://`均为同一原生HTTPS sink（已通过真实Lake验收），不是MySQL协议。HTTPS：

```text
databend://DB_USER:TOKEN@HOST/DB_NAME?exclusive=true&warehouse=WAREHOUSE&workers=4&batch-rows=2048&batch-bytes=1048576&flush-interval=1s&spool-bytes=10737418240&memory-ratio=0.02&source-bytes=268435456&source-events=128&max-entries=4096&query-timeout=2m
```

`DB_USER`/`TOKEN`必须URL编码；不应把实际密码粘贴到review或日志。`databend+http://DB_USER:TOKEN@HOST:PORT?...`仅用于明确允许明文的隔离环境。留空database路径时使用源schema名；指定database时同名源表不能汇入同一目标表，不实现任意表路由。

| 参数 | 默认 | 意义 |
|---|---|---|
| exclusive | 无，必须为true | 操作者确认目标静态独占；不是锁或自动HA证明 |
| conflict-key | 未指定 | 可选feed级单列override，替代无/单列源键；不能覆盖复合键，必须唯一、非NULL且稳定，不能在原目标上随意更换 |
| workers | 4，最大64 | 固定lane数；表hash到lane，单表串行 |
| batch-rows / batch-bytes | 2048 / 1 MiB | 攒批/分片阈值；单行编码超限立即停止，不无界直通 |
| flush-interval | 1s | 低流量定时排空 |
| spool-bytes | 10 GiB | 共享本地磁盘预算；已释放但物理未回收仍由spool计费 |
| memory-ratio | 0.02 | spool内存热数据额度占磁盘预算比例；独立于source池 |
| source-bytes / source-events | 256 MiB / 128 | 整个sink已接纳原生事件池；共享chunk保守按每个event完整计费 |
| max-entries | 4096 | 每lane待应用分片/缓存表元数据上限，达到分片上限主动排空 |
| spool-dir | TiCDC data-dir下 | spool根目录；按capture/feed隔离，不是恢复日志 |
| query-timeout | 2m | 单SQL及分页总超时；到期不代表服务端已停止 |
| warehouse | 未指定 | HTTP warehouse头 |
| remote-bytes-limit | 未开启 | 基于system.tables中目标database raw压缩字节测量的停写阈值；不是跨capture硬额度 |

源chunk在进入sink之前已经分配，source池、编码、读回、HTTP和元数据分别有界不等于全进程RSS严格上限。batch/spool参数必须联合满足最小额度，未知/重复参数拒绝。普通压力用背压和提前排空吸收；事件池无法接管属于明确fatal容量异常，不伪造成功回调。

### 10.2 已实现的首版能力

- 具备TiCDC可选冲突键（显式PRIMARY KEY或全列NOT NULL的UNIQUE键），或显式配置conflict-key的普通非分区表；INSERT/UPDATE/DELETE，PK/UK变化沿用raw-change=false的scanner拆分顺序。
- 增量自动建表，复合键与保留墓碑，参数化批量raw INSERT、full-raw MERGE、strict历史清理。
- 主要TiDB标量/文本/binary/ENUM/SET/bit/decimal/JSON/date/time/datetime/timestamp映射，生成列只投影CDC可见的STORED值，不生成目标表达式。
- CREATE DATABASE、CREATE TABLE/RECOVER、DROP TABLE、TRUNCATE；有限ADD/DROP/RENAME/CHANGE/MODIFY列，保守类型widening、字面默认值与nullability；二级索引DDL不创建Databend索引但推进已应用schema fence。
- 实际列DDL遇到缺失目标时，可依据该事件定义创建空投影；不扫描或预建没有事件的历史空表。
- pending schema步骤重入恢复、applied DDL重放跳过、旧DML fence过滤；无成功回调的fatal/Close清理。

### 10.3 必须保留的限制

- 受控单capture、目标静态独占；旧SQL终态无法确认时人工排除后才能resume。`exclusive=true`是运行前提声明，不自动检测/获得下游租约。跨capture自动故障接管没有验收，不承诺生产HA。
- 首版拒绝分区dispatcher、跨节点拆表、syncpoint、active-active、redo、low-latency模式、列选择、任意路由和table级事务原子性。分区调度可能使同逻辑目标出现多writer，因此在证明隔离前不静默接纳。
- 拒绝删除/重命名冲突键、修改复合冲突键、不安全type conversion、除INT32→VARCHAR(n>=11)之外的参数化字符MODIFY/CHANGE及表达式默认值DDL；复杂分区等不做隐式有损翻译。按用户最新裁决，表重命名（含多表）及DROP DATABASE只warning后跳过，目标对象不变、进度继续；不宣称镜像执行了这些DDL。其他unsupported操作仍fail-closed。
- 不支持Dumpling/CSV/COPY INTO或全量编排；首次创建空目标不补历史存量。用户必须已独立准备一致的起始状态与start-ts。
- 不创建stream、DML ledger、manifest/witness或新owner协议。retained raw最新墓碑不TTL删除；不同历史PK持续churn仍会增长。
- HTTP端点必须实际支持Query API参数绑定与终态。Verify以绑定参数SELECT做预检；不会自动降级到字符串插值。本轮staging端点的参数绑定、终态、支持DDL/类型与排空重启已实测；不同端点/版本仍需预检，不推广到所有版本/许可或在途故障恢复。
- 当前journal是新sink专属的`epoch/applied/plan`形状，不自动接管正在运行的bend-cdc目标或其旧两列journal。迁移必须停旧链并独立验证起点；不绕过DDL恢复证据。

### 10.4 验证与发布状态

代码已接入factory并可构建；用户随后提供staging Lake，真实核心及TiUP全链路通过（§8.3），不再仅有HTTP fixture。用户随后要求提交并推送当前实现分支，提交/远端状态以该分支Git记录为准；下文实验记录描述测试当时的dirty工作区。未建PR、未部署共享CDC。成本/SLO与在途故障/自动HA未验收，T1–T7不能标为全部关闭。


2026-10-25 本地最终验证记录（最后一批代码）：

```text
GOMAXPROCS=4 go test -p 2 -race ./downstreamadapter/sink/databend ./downstreamadapter/sink ./pkg/sink/spool ./pkg/errors ./pkg/config ./downstreamadapter/sink/metrics -count=1 -timeout=180s
PASS：6个包
GOMAXPROCS=4 go test -p 2 -race ./pkg/common/event -run 'TestTableSchemaStore|TestNeedTableNameStore' -count=1
PASS
GOMAXPROCS=4 go test -p 2 ./pkg/metrics -run TestDownstreamTypeFromSinkURI -count=1
PASS
GOMAXPROCS=4 tools/bin/golangci-lint run --timeout 5m ./downstreamadapter/sink/databend/... ./downstreamadapter/sink/metrics/...
0 issues
GOMAXPROCS=4 go vet -p 2 ./downstreamadapter/sink/databend ./downstreamadapter/sink ./pkg/sink/spool ./pkg/common/event ./pkg/errors ./downstreamadapter/sink/metrics
PASS
GOMAXPROCS=4 go test -p 2 ./downstreamadapter/sink/databend -run TestDatabendNativeLiveIntegration -v -count=1
PASS：显式配置用户授权staging URI，13.36s；独立随机库自动清理（URI不写命令/报告）
GOMAXPROCS=4 go build -p 2 -o /tmp/ticdc-databend-native ./cmd/cdc
PASS，临时产物不进入仓库；version子命令可执行，未注入发布版本元数据
```

新增Databend包为11个生产文件、11个测试文件、54个测试函数（包含3个显式opt-in live测试）；另有错误定义和sink metrics文件。factory/类型/schema store/配置scheme/错误分类/metrics/spool计量等现有文件仅有限接入改动。HTTP fixture仍仅是进程内协议测试；真实SQL/TiDB链路另见§8.3，生产故障恢复不据此宣称通过。
