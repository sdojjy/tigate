# bend-cdc 能力对照与 native sink DDL / 故障补充验收

日期：2026-10-25。结论：**已完成真实 Lake 能力对照、11 组真实源端矩阵、2 类 DML 故障及 19 个 DDL 提交边界恢复测试；发现并修复一个局部迁移退化。仍有明确未迁移能力，不宣称全部替代 bend-cdc。**

**后续更新**：下述对照矩阵是上一轮状态；按用户新要求，`conflict-key`现已实现，RENAME/DROP DATABASE改为跳过不阻塞。最新实现/真实测试见§8；分区保护仍保留。

## 1. 先纠正能力判断

固定基线为 bend-cdc `b6481621021e82aee659ad88e8e6c00f6dd11906`，不是另一个 HEAD。旧库保持干净，通过 Go overlay 添加临时测试，不改其源码。Native 为 TiCDC `2bf6dacd0aec9ac2fad464600e47341f9aa951f2` 加当前分支未提交实现；不改 collector/dispatcher/progress/调度框架。

| 场景 | bend-cdc 固定版实际证据 | native 当前结果 | 判定 |
|---|---|---|---|
| 分区表普通 DML | 真实 core 接受分区 CREATE 元数据，创建扁平目标，Canal 行 INSERT/Flush 后值存在 | 真实源 CREATE 分区表触发 unretryable，未建新目标 | **仍有迁移缺口**；旧 core 验证不等于已测完整 S3 分区文件/分区 DDL |
| 无显式 PK，但有 NOT NULL UNIQUE | native 与旧路径可使用 TiCDC 提供的隐式冲突键，不等于真正无键 | 真实 TiDB→CDC→Lake 的 INSERT、唯一键变更、DELETE 全通过，未开 force_replicate | **已支持**；之前笼统说“无 PK 都不支持”不准确 |
| 无可用 PK/非空唯一键 | 无 pkNames 且无 override 时拒绝；设置 `conflictKey=id` 后真实 ApplySchema、WriteBatch、Flush 通过 | force_replicate 后实际 INSERT，native 拒绝且 checkpoint 停止 | **仍缺显式 conflictKey override**；旧版依赖操作者保证所选键有效，不是任意无键表都安全 |
| RENAME TABLE | 真实旧 ApplySchema 返回成功，但旧表仍在、新表不存在；代码按 unsupported 跳过并推进 epoch | 拒绝，旧目标保留 | **旧版没有实际重命名支持**，不应把 nil error 当已支持 |
| DROP DATABASE | 旧 SchemaChanges 判 unsupported；给旧 ApplySchema 带表元数据探针后仍保留目标库/表 | 真实源 DROP DATABASE 后 native 拒绝，目标库表保留 | **旧版没有实际 DROP DATABASE 投影**；旧探针不是完整 S3 DROP DATABASE 测试 |
| 默认值/nullability | 真实旧 MODIFY 默认值、NOT NULL→NULL 通过 | 真实源多种组合通过，见§2 | 对应支持场景已补证据 |
| INT→VARCHAR(200) | 真实旧 core 通过，原值转文本保留 | 修复前真实 failed；局部修复后通过且原 feed 重放恢复 | **已修复迁移退化** |
| 多操作列 DDL | 旧 parser 对两列 ADD 等组合明确报 multi-operation 错误 | ADD+DROP+RENAME、CHANGE 完整定义等受测组合通过 | 不属于旧版全支持、新版遗漏；仍不承诺任意组合 |
| 缩窄/表达式默认值 | 旧版有明确限制，不是任意 DDL 都执行 | 本轮真实 source 探针验证 fail-closed | 拒绝测试通过≠功能支持 |

非空唯一键的源码依据是 [GetPrimaryKey 隐式键选择](../../pkg/common/table_info_helper.go#L593-L637)，native 复用该 helper，**不是本轮新增框架机制**。旧 unsupported 分类及处理见 [SchemaChanges](../../../webapi/bend-cdc/pkg/source/tidb/schema_ddl.go) 和 [ApplySchema](../../../webapi/bend-cdc/pkg/source/tidb/databend_merge_sink.go)。

## 2. 修复后全新库：11 / 11 PASS

真实环境：本地 TiUP TiDB/PD/TiKV v8.5.1、当前分支新架构单 capture CDC、用户授权 staging Lake。TiUP 仅用本轮独立实例；源为 UTC。CDC 注入的 `v8.5.1` 是本地兼容标签，不是官方 CDC 发布包；最终 git_hash 标签 `2bf6dacd-dirty-extended-e2e`。

最终随机库 `native_e2e_20261025_012759_8da770af`，结果及 cleanup 均 PASS。下面时间包括网络与收敛等待，不是性能指标。

| 测试 | 秒 / 结果 |
|---|---|
| 无显式 PK 的 NOT NULL UNIQUE 表，包含键变更/删除 | 11.240 / PASS |
| INT→VARCHAR(200)、INT 最小值保留、后续 `'0012'` 文本及默认值 | 16.797 / PASS |
| 同类型 MODIFY 字面默认值，已有行不被回填改写 | 8.122 / PASS |
| NOT NULL→NULL，显式 DEFAULT NULL | 7.830 / PASS |
| 清除源 NULL 后 NULL→NOT NULL，后续默认值写入 | 9.606 / PASS |
| MODIFY 完整定义省略 NOT NULL，使目标恢复可空 | 9.225 / PASS |
| MODIFY 省略 DEFAULT，删除旧默认值 | 8.022 / PASS |
| TINYINT→INT→BIGINT，VARCHAR→TEXT | 12.184 / PASS |
| ADD Decimal/文本/NULL 字面默认值，旧行回填及转义字符串 | 16.237 / PASS |
| CHANGE 重命名及完整默认值/nullability 定义 | 8.363 / PASS |
| 同一 DDL 的 ADD+DROP+RENAME，随后 DML | 9.161 / PASS |

不只比行数：按键比较实际值，并用真实目标 DESCRIBE 检查默认值和可空性。`VARCHAR(200)` 只存在于源定义，目标按既有 mapper 使用不定长 VARCHAR。

另有 **6 个独立真实拒绝探针**通过：分区表、真正无键表（显式 force_replicate 并写行）、表重命名、DROP DATABASE、INT→TINYINT 缩窄、CURRENT_TIMESTAMP 表达式默认值。断言为稳定 failed、checkpoint 不再推进、旧目标值未变/新目标未创建；不能将这些 PASS 计为支持了相应操作。

初始无键探针若不 force_replicate 会在上游被 eligibility 过滤，feed 可保持 normal，不能当 sink 拒绝证据。初始误把 NOT NULL UNIQUE 当“不支持无 PK”也导致测试预期失败；读取真实目标、追踪 helper 后已改为正向测试。保留这些原始结果，未把失败记录改成通过。

## 3. 两类真实 DML 故障：PASS

[实现与复现入口](../../downstreamadapter/sink/databend/live_fault_test.go)。SQL 都由真实 Lake 执行，不使用 HTTP fixture 伪造数据库成功。

### 3.1 部分 raw INSERT 成功

四行批次拆为两条 INSERT：第一条真实提交两行，第二条在发送前注入传输失败。

- 观察真实 raw=2、target=0，applyResult 未应用。
- writer 未重发未知请求、未继续 MERGE（匹配 raw POST 总数为2，第二条未送服务器）。
- 测试已知旧第一条成功、第二条未发，替换所有本地 manager/writer 状态，再完整重放四行。
- 最终真实 target=4、strict compaction 后 raw=4。

这是“部分子 SQL 成功”，不是声称同一条 INSERT 内部半提交。

### 3.2 MERGE 已提交，但成功响应丢失

委托真实 HTTP transport 执行，独立取得**相同 query_id 的 Succeeded**后，在 sink 看到该响应之前丢弃它。

- 真实 target 已有两行，但旧 sink 因 UNKNOWN 终止，源 flush callback=0。
- MERGE POST 只有1次，无内层/外层暗中重发。
- 测试方已独立取得旧 query 的终态且关闭旧 sink 后，重建 manager/writer、重放同一输入。
- 最终 target=2，compaction 后 raw=2，没有重复目标行。

原任意 UNKNOWN 请求在实际运维中仍必须取得终态及目标排他后才能恢复；测试夹具掌握的证据不等于新建了自动终态发现/HA 服务。

## 4. DDL 真实提交边界：19 / 19 PASS

每个断点均先在真实服务器提交该语句，再丢弃客户端响应；经实际 `Sink.WriteBlockEvent` 路径观察错误、Run 停止、DDL callback=0。随后在旧请求终态已知的前提下，用新 schemaManager 只读取真实 Lake 的 journal/表状态恢复，不继承旧内存状态。

| DDL | 丢失响应的关键提交边界 | 数量 |
|---|---|---|
| CREATE | pending journal、目标建表、raw建表、applied journal | 4 |
| ALTER | pending、ADD、RENAME、MODIFY、raw确保存在、applied | 6 |
| TRUNCATE | pending、DROP raw、TRUNCATE target、重建raw、applied | 5 |
| DROP | pending、DROP target、DROP raw、applied | 4 |

每例均验证恢复至 epoch=150/applied=true；非 DROP 场景再写 ts=160 的新数据并核对完整值，重放相同 DDL 不清新数据，ts=120 旧 DML 被 fence 跳过；DROP 后 target/raw 均不存在。ALTER 同时核对旧行 `label='old',extra=NULL` 和新行 `label='new',extra=7`，不只核对行数。

最终命令带 `-race`，2个父测试及21个故障子场景通过，package **199.112s**。19条匹配 query_id 的真实终态证据保留在 `fault-summary.json` 和原始日志。

**边界**：这是客户端真实响应丢失、旧 sink 生命周期终止和本地 manager 状态重建，不是服务端执行中崩溃、OS 任意指令点 SIGKILL、跨 capture 故障接管。没有声称“所有 DDL 的所有可能中断点”已覆盖，也没有对旧 S3/bend-cdc 整条链做同样故障注入。

## 5. 本轮修复与真实升级恢复

生产行为仅修改 [schema.go](../../downstreamadapter/sink/databend/schema.go) 的参数化字符转换判定：此前将所有参数化 VARCHAR MODIFY/CHANGE 一律拒绝，误挡住 bend-cdc 的 INT→VARCHAR(200)。现在仅放行 **INT32→VARCHAR(n>=11)**（足够完整容纳包括符号的 INT32 十进制域）；同字符类型未知原宽度、VARCHAR(10)、CHAR(200)、BIGINT→VARCHAR 等仍按保守规则拒绝。增加边界单测，不扩展通用 casting/框架协议。

真实复现先保留 failed feed，再停止旧 CDC、换本轮构建、同 data-dir 重启，从**原 checkpoint**手工 resume，不覆盖时间戳：

- 原 PID `67290` → 新 PID `68998`。
- checkpoint `469995752722530332` → `469995795137953793`。
- 原行 `-2147483648` 转文本保留；积压新行 `'0012'` 不丢前导零。
- 原错误 DDL 重放恢复成功，独立记录 `upgrade-recovery.json`；随后另起新库完整跑§2矩阵。

## 6. 验证、清理和可重复运行

新增/扩展测试入口：

- [extended.py](../../tests/integration_tests/databend_native/extended.py)：源端矩阵与拒绝探针。
- [baseline.py](../../tests/integration_tests/databend_native/baseline.py) / [固定版 Go overlay](../../tests/integration_tests/databend_native/baseline_parity_test.go.txt)：旧 core 对照。
- [live_fault_test.go](../../downstreamadapter/sink/databend/live_fault_test.go)：真实 Query API 故障注入。
- [README](../../tests/integration_tests/databend_native/README.md)：私有凭据、启动与命令。

固定 bend-cdc 的7个真实对照子测试及原有 IndexWarning/TRUNCATE 集成测试通过（package **38.659s**）；选定的原有分类/默认值/冲突键单测也通过。旧 checkout 未修改。

最终修改后，6包 race、sink lint（0 issues）、go vet、当前分支主程序构建通过。Databend包现49个测试函数，其中3个 opt-in live 父测试。全仓测试、生产部署、成本/长期负载没有执行。

本轮按**具体自有名字**确认16个源命名空间、26个远端命名空间（含baseline/fault随机库）均无残留，对应changefeed均已删除；没有操作原 `default` 中的已有表。自有本地CDC/TiUP停止，临时凭据清除；没有提交、推送或建PR。

私有证据根目录（仅本机可访问）：

```text
/var/folders/lq/3n41d_w97w55hrx2mzmd42w80000gn/T/ticdc-native-extended-il8v40pb
```

关键记录：`ddl-matrix-fixed-final/result.json`、`baseline-fixed-final/bend-live-parity.log`、`live-fault-final.log`、`fault-summary.json`、`upgrade-recovery.json`、`cleanup-verification.json`。首轮9组DML/32列/spool/排空重启仍见[首轮报告](./databend-native-sink-e2e-report.md)，与本轮不同运行分开记账。

## 7. 当前仍未闭合的事项

1. **分区普通 DML**与**显式 conflictKey override**仍是具体迁移差异，不能写成已覆盖；本轮没有用跳过错误或删除安全检查冒充支持。
2. 任意复杂/不安全DDL、所有类型/时区组合不是11组测试能够穷举；RENAME和DROP DATABASE也不能因旧版“成功返回”就宣称正确支持。
3. 服务端仍在执行时的 UNKNOWN 终态取得、自动HA、长期资源与性能成本继续保留未验收状态。

所以本轮关闭的是具体功能/恢复测试缺口及一个局部转换退化，不是宣称 bend-cdc 全功能、全部故障或生产就绪已验收。

## 8. 最新用户要求：conflict-key 配置、RENAME/DROP DATABASE 跳过

本节取代§1/§7中相应的“尚未迁移/拒绝”状态；不是改写上一轮实验记录。分区保护仍未开放。

### 8.1 配置及语义

bend-cdc 在 `DatabendConfig` 中保存单列 `conflict_key`，TiDB wrapper 的 `ConflictKey` 将其交给 raw identity / MERGE 作为业务键；**没有给源表自动添加 PRIMARY KEY 或唯一索引**。旧 `resolveConflictKeys` 允许覆盖无键或单列源键，拒绝覆盖复合源键。

新 sink 已按该规则增加 URI 参数：在原 URI 查询串追加：

```text
&exclusive=true&conflict-key=id
```

`exclusive=true`只应出现一次，已有时只追加`conflict-key`。真正没有可用源索引键的表，还需既有TiCDC选项：

```toml
force-replicate = true
```

API对应 `replica_config.force_replicate=true`。无需改TiCDC通用配置结构。

规则：

- 未配置时仍自动选择显式 PK / NOT NULL UNIQUE；配置后是feed级**单列override**，列名按可见元数据大小写归一。
- 不能覆盖复合源键；缺失/不可见列、NULL身份拒绝。此配置适合独立feed，不能随意把复合键表混入同一override任务。
- 业务方保证值唯一、非NULL且稳定。重复业务键会按同键更新处理，不会自动获得源端唯一约束。
- 任意未索引逻辑键不一定触发TiCDC拆分；未拆分UPDATE改变该键时native拒绝，不静默留下旧行。本轮没有新造通用变键协议。
- DML与DDL journal均使用相同的实际选取键；不要在原target/raw上直接更换键，须独立重新初始化。

生产改动集中于 sink 包的 `config.go`、`native.go`、`lane.go`、`schema.go`；不改collector、dispatcher、progress或调度算法。

### 8.2 跳过语义

`RENAME TABLE`（含多表类型）和 `DROP DATABASE` 在旧DML成功排空后 warning 并正常完成DDL回调；不发目标DDL，不创建虚假的applied epoch。不再因这些指定操作停任务；其他不安全DDL仍拒绝。

重要影响：目标库/旧表/旧数据保留；**重命名不会搬迁历史行**。后续DML使用新表名，按需创建新投影。DROP DATABASE不删除目标库；源库重建后的新表仍能同步。同名库表重建不会自动清理旧目标数据，需要空目标时应独立重新初始化。这是用户要求的跳过策略，不是完整DDL镜像。

如果前序DML的MERGE结果未知，屏障仍会失败并停止；不能以“跳过后续DDL”绕过旧写入的安全问题。

### 8.3 真实验收：3组PASS

使用本地TiUP v8.5.1、当前新架构CDC及同一授权staging Lake；CDC本地标签`2bf6dacd-dirty-options-e2e`，不是官方发布包。入口为[options.py](../../tests/integration_tests/databend_native/options.py)。

| 场景 | 结果 |
|---|---|
| 无任何PK/UK的表：force-replicate + `conflict-key=ID`，INSERT/UPDATE/DELETE/ADD COLUMN，journal键为`id`；另测覆盖不同的单列源PK | PASS / 32.927s |
| 真实RENAME后checkpoint跨过DDL、旧目标数据保留；对新表名继续UPDATE/INSERT，新投影值正确且旧表不变 | PASS / 16.873s |
| 真实DROP DATABASE后checkpoint继续、目标库和数据保留；重建源库、新建表的DML继续成功 | PASS / 16.143s |

单测/race同时验证：配置格式、大小写、空值、NULL身份、复合键拒绝、未拆分变键拒绝、配置贯穿DML/journal、跳过三种DDL无目标SQL/无虚构epoch、回调顺序、后续DML推进，以及旧UNKNOWN写不能被跳过掩盖。4包race、lint（0 issues）、go vet、主程序构建通过；全仓测试/HA/成本未跑。

私有证据根目录：

```text
/var/folders/lq/3n41d_w97w55hrx2mzmd42w80000gn/T/ticdc-native-options-gdj9c83o
```

确切路径由本机`/tmp/ticdc-native-options-current`指针记录；`manual-key-1/result.json`、`skip-ddl-1/result.json`和`cdc.log`分别保存结果及warning。两个run的status/cleanup均PASS；最终按具体名字核验源/目标库及feed清理，自有CDC/TiUP停止，临时凭据移除。未提交、推送或部署共享CDC。

### 8.4 为什么仍拒绝分区dispatcher

限制来自首版主动保护，不是Lake不能写分区数据。一个capture内，同一逻辑目标已经hash到同一lane串行；但不同物理分区可由不同capture承担，多个进程可能同时MERGE同一target。`enable_table_across_nodes=false`并不等于所有物理分区都在同一capture，`exclusive=true`也不是跨capture锁。当前仍拒绝分区输入，避免把“单进程串行”误当跨进程排他；这轮没有顺手删除该保护或引入框架锁协议。
