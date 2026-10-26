# TiCDC 原生 Lake Sink：本地 TiUP → staging Lake 实测报告

日期：2026-10-25（Asia/Shanghai）。状态：**本轮受控端到端验收通过；未提交、未推送、未部署到共享 CDC 集群。**

后续追加的bend-cdc对照、DDL默认值/nullability矩阵和真实响应丢失实验见[补充报告](./databend-native-sink-parity-fault-report.md)。下述9组是首轮记录，不替代后续实验；“无PK不支持”应精确为“无可用PK/非空唯一键且依赖显式conflictKey的表”。

## 1. 结论与证据边界

使用本地 TiUP TiDB/PD/TiKV、当前分支新架构 CDC，以及用户授权的真实 staging Lake，修复兼容性问题后，**从全新随机库重新执行的 9 组测试全部通过**。不是仅运行 mock，也没有使用 S3/Canal JSON/bend-cdc 代写。

- DML、有限列 DDL、TRUNCATE、DROP TABLE、32 个目标列的类型/值比对通过。
- 1,200 行事务超过 64 KiB 磁盘 spool 配额，仍能边应用边回收；实际发生 41 次 spool 磁盘读回。
- 排空、暂停、终止旧 CDC 后，制造源端 DML/DDL 积压，重启同一 data-dir 并从原 checkpoint 恢复，最终数据一致。
- 独立真实 Lake 核心测试验证旧 UPSERT 不复活已删除键、applied fence 与重复 TRUNCATE。
- **不代表**任意 DDL/列类型均已支持、在途 UNKNOWN 请求故障接管已通过、多 capture HA 已通过，或成本/吞吐已达到生产要求。

当前设计与限制见 [定稿设计 §10](./databend-native-sink-design-review.md)。可重复运行的测试见 [runner](../../tests/integration_tests/databend_native/run.py) 和 [操作说明](../../tests/integration_tests/databend_native/README.md)。

## 2. 实际环境与版本

| 组件 | 本次使用 |
|---|---|
| 源 TiDB / PD / TiKV | TiUP playground v8.5.1，各 1 个；TiDB `SELECT VERSION()` 返回 `8.0.11-TiDB-v8.5.1` |
| CDC 源码 | 分支 `feat/databend-native-sink`；基线 `2bf6dacd0aec9ac2fad464600e47341f9aa951f2` 加当前未提交实现 |
| CDC 运行 | 当前工作区构建，`--newarch --tz UTC`；单 capture，独占本次目标 |
| CDC 版本标签 | 本地注入 `v8.5.1` / `2bf6dacd-dirty-e2e`，仅用于源版本兼容检查，**不是官方 v8.5.1 发布包** |
| Lake | `tnww1vflk.gw.aliyun-ap-northeast-1.default.staging.lake.tidbcloud.com:443`，warehouse `default`，HTTPS Query API |
| Lake 实际版本 | `Lake Query v1.2.931-nightly-0ca7e6af79(rust-1.94.0-nightly-2026-08-02T22:24:51.279542089Z)` |
| 测试库 | `native_e2e_20261024_171016_bbf3e64f`（源、目标相同的独立随机库，测试后删除） |
| Changefeed | `native-e2e-20261024-171016-bbf3e64f`（测试后删除） |

用户原 URI 的 database 被替换为本次随机库；没有修改 `default` 内现有表。凭据仅从仓库外 0600 文件读取，不记录到本文或测试代码。

本地 TiUP SQL/PD/TiKV/CDC API 分别为 `127.0.0.1:14000/12379/30160/18300`。本机空间不足触发 TiDB 快速 ADD INDEX 的磁盘保护，故仅对本次独立 TiUP 关闭 `tidb_ddl_enable_fast_reorg` 和 `tidb_enable_dist_task` 后运行索引 DDL。这是**源端测试环境调整**，不属于 sink 修复，不能据此更改共享源配置。

## 3. 最终全新运行：9 / 9 PASS

耗时是该组 wall time，含远端 SQL 和等待收敛，不是吞吐基准。

| 测试组 | 具体断言 | 结果 / 秒 |
|---|---|---|
| DML | INSERT/UPDATE/DELETE；固定 PK 改二级 UK；非聚簇 PK 变键；同事务 DELETE+重插后最终键集合一致 | PASS / 16.571 |
| 复合键 | 包含分隔符/转义字符的复合主键不碰撞，源目标逐键一致 | PASS / 10.622 |
| 特殊标识符 | 表/列包含双引号；源列 `action`、`__ticdc_operation` 不覆盖内部动作列 | PASS / 10.246 |
| 类型/值 | 32 个投影列，初始、NULL、极值及更新后比对；VIRTUAL 不投影 | PASS / 12.801 |
| 列 DDL | ADD NOT NULL 字面默认值及旧行回填、RENAME、INT→BIGINT 后写大整数、DROP、TEXT NOT NULL 隐式空默认；二级 ADD/DROP INDEX | PASS / 42.352 |
| TRUNCATE | 旧行清空、旧 raw 代际清理后仅保留新 DML | PASS / 18.195 |
| 批处理/spool | 单事务 1,200 行×256 字符；随后 UPDATE 一行、DELETE 100 行；分别比对 1,200/1,100 行；低流量尾批排空 | PASS / 40.592 |
| 进程重启 | checkpoint 排空后暂停并终止旧 CDC；停机期间 UPDATE/INSERT/ADD COLUMN；同 data-dir 重启/恢复，完整比对积压结果 | PASS / 22.769 |
| DROP TABLE | 源 DROP 后目标表及对应 raw 表均不存在 | PASS / 12.387 |

### 类型覆盖与比较方法

| 源类型/边界 | 真实目标观察/比较 |
|---|---|
| TINY/SMALL/MEDIUM/INT/BIGINT，含 UNSIGNED | 覆盖有符号正负边界、`BIGINT` 最小值、`UINT64` 最大值；精确十进制文本比较，无 float64 中转 |
| DECIMAL(65,30) | 大正数、大负数、30 位小数和最小小数位更新；精确比较 |
| FLOAT/DOUBLE | 源端以足够精度读回后按浮点容差比对，不能用 MySQL CLI 默认短显示当精确值 |
| CHAR/TEXT/BOOLEAN | Unicode、emoji、引号、反斜线、换行、NULL；BOOLEAN 保留 TiDB TINYINT 表示 |
| BLOB/BINARY/VARBINARY | 0–255 全字节、定长补零、变长及更新；canonical hex 精确比较 |
| BIT(64) | 全 64 位值映射到 unsigned BIGINT，精确比较 |
| ENUM/SET | 数字 label `ENUM('2','x')` 不二次解码；64 项 SET 的 bit63 及多 label 更新 |
| JSON | 超过有符号范围的大整数、数组/布尔；显式 `is_null_value` 区分 JSON null 与 SQL NULL |
| DATETIME(6)/TIMESTAMP(6)/DATE | 微秒与日期保持；本轮源会话/CDC 为 UTC，不据此宣称任意时区组合已实测 |
| TIME(6)/YEAR | 负且超过 24h 的 TIME 精确文本、YEAR 数值 |
| 生成列 | STORED 实际值同步，含 BIGINT 最大值；VIRTUAL 列不在目标 DESCRIBE 中 |

按主键 canonical 化后比较，不依赖两端数值/VARCHAR 的不同排序规则。最终 Lake `DESCRIBE types_test` 为 **32 列**；此数字是测试表覆盖列数，不是“所有 MySQL 类型的数量”。

### spool 与恢复的实际证据

测试配置：`workers=1`、`batch-rows=64`、`batch-bytes=32768`、`flush-interval=200ms`、`spool-bytes=65536`、`memory-ratio=0.001`。真实 `ticdc_databend_spool_load_bytes_count` 累计 **41**，不是只根据阈值推测使用了磁盘。该测试证明本负载可在小配额下推进，不构成全进程 RSS/物理磁盘硬封顶或性能报告。

重启时旧 PID `41562` 已终止，新 PID `41917`；正数源事务读 TSO 为 `469988010726522887`，恢复后 checkpoint 为 `469988011722670087`。未覆盖 checkpoint，未跳过积压。**这是受控排空重启，不是在 MERGE/TRUNCATE 结果未知时直接启动新 owner。**

## 4. 本轮发现并修复的真实兼容问题

### 4.1 DESCRIBE 的 unsigned 类型别名

Lake 返回 `TINYINT/SMALLINT/INT/BIGINT UNSIGNED`，原校验只认内部 UINT8/16/32/64，导致合法目标被安全拒绝。`canonicalType` 统一别名及 Nullable 包装，不放宽 Decimal 精度检查；增加回归测试。真实极值类型测试已重跑通过。

### 4.2 双引号列名不是 JSONPath

原 SQL 的 `s.raw_data:"a""b"` 虽经过 SQL 标识符转义，仍触发真实 Lake `1006 InvalidKeyPath`。改为字面对象键 `get(s.raw_data, 'a"b')`，统一 SQL 字面量转义，并测试点号、引号、反斜线。没有替换 raw/MERGE 主算法，也没有引入 stream/manifest。

先核验旧错误请求为终态后，用 checkpoint 和 retained raw 恢复该失败案例，目标正确；随后使用修复后的完整代码，从新库完整重跑，9 组均通过。不是把多个失败运行的局部结果拼成一次全绿。

### 4.3 接入用户实际 URI

增加 `lake://` 作为同一 HTTPS native sink 的 scheme 别名，接通 factory/config/downstream metrics；`exclusive=true` 等首版限制保持不变。用户名/密码继续按 URI URL 编码，不降级明文。

测试脚本本身还修正了 reserved identifier、源 FLOAT 读出精度、hex 大小写、JSON null 观察、`/tmp` realpath 和 coordinator 启动等待等环境/fixture 问题，未将其计为数据复制缺陷。

## 5. 独立真实核心与本地回归

真实 Lake 核心测试 `TestDatabendNativeLiveIntegration`：**PASS，13.36s**（package 15.127s）。另外创建/清理独立随机库，验证 DELETE 后旧 UPSERT 不复活、applied epoch 过滤旧输入、相同 FinishedTs 的 TRUNCATE 重放不清空新数据。它不是 TiUP 源端完整链路的替代证明。

最终代码验证：

```sh
GOMAXPROCS=4 go test -p 2 -race ./downstreamadapter/sink/databend ./downstreamadapter/sink ./pkg/sink/spool ./pkg/errors ./pkg/config ./downstreamadapter/sink/metrics -count=1 -timeout=180s
# PASS，6 个包
GOMAXPROCS=4 tools/bin/golangci-lint run --timeout 5m ./downstreamadapter/sink/databend/... ./downstreamadapter/sink/metrics/...
# 0 issues
```

当前 Databend 包包含 45 个测试函数（含 1 个 opt-in 真实 Lake 测试）。历史本地回归还通过 TableSchemaStore 针对性测试、scheme metrics 测试、go vet 和主程序构建。未运行全仓测试；之前全量 common/event 曾因既有 TiDB 测试依赖重复注册 metrics panic，未改无关依赖，针对相关测试通过。

## 6. 清理与证据位置

- 最终 run 状态和 cleanup 均为 PASS。
- 对 4 次运行的**具体自有库名/feed ID**查询确认：Lake 测试库、源测试库、changefeed 均无残留；不是只看 DELETE 返回成功。
- 本轮启动的 CDC 与 TiUP 已停止；本地 14000/12379/30160/18300 端口均已关闭。
- 凭据不入 Git；临时 credential 文件在完成清理核验后删除。日志/result 保留在本机私有目录用于复核，不保证跨机器可访问。

证据根目录：

```text
/var/folders/lq/3n41d_w97w55hrx2mzmd42w80000gn/T/ticdc-native-e2e-0t2q351n
```

其中 `acceptance-4/result.json` / `acceptance-4.log` 为最终全新运行；`live-core-test-final.log` 为真实核心测试；`cleanup-verification.json` 为逐名字清理核验；`acceptance-3/quoted-recovery.json` 为特殊列名修复后恢复案例。最终测试 runner SHA-256：`81c753f3f38e471f7c6bb17a83d342215ea6352fb4147fbfca2c7056a7f25226`。

## 7. 尚未验收，不应扩大结论

- 在途 HTTP 断连/首页后失败、服务端仍执行的 UNKNOWN SQL、旧 owner 与新 owner 并发、进程任意中断点恢复：本地 fixture 覆盖部分分支，**本轮没有真实故障注入证明**。
- 多 capture、自动 HA、跨表事务原子性、分区表、无可用PK/非空唯一键、复杂/不安全 DDL、任意路由、非 UTC 时区矩阵不在本轮验收范围。
- 长期 retained raw 墓碑增长、多表高并发、成本/扫描量/SLO、长期满盘/IO 故障未做 benchmark/soak。
- 从既有 bend-cdc journal/目标在线无缝接管、历史全量初始化不在范围；不能把首次自动建空表当存量已同步。

因此本轮可以确认“首版支持范围内，真实 TiDB→当前 CDC→该 staging Lake 的功能纵切通过”，不能把 T1–T7 全部风险或生产就绪标为关闭。
