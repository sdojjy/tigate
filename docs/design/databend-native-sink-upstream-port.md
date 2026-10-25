# 官方 pingcap/ticdc 基线迁移与重新验收

日期：2026-10-25。状态：已迁移到官方master并完成独立验证；没有沿用旧基线结果冒充通过。

## 正确仓库与基线

- 用户指定上游：`https://github.com/pingcap/ticdc`。
- 本次从官方master抓取的固定HEAD：`b9179182f3c1c5db5a018029f41eb07eab804051`，`sink: honor first-match semantics in column selector validation (#6651)`。
- 独立目录：`/Users/jiangjianyuan/work/git/pingcap/ticdc-official`。
- 交付分支：`feat/databend-native-sink`。
- origin：`git@github.com:sdojjy/tigate.git`。GitHub元数据确认 `isFork=true`、parent=`pingcap/ticdc`；没有将旧tiflow当新repo推送。

## 迁移方法和边界

此前的 `sdojjy/tiflow:457c2f9bd` 保留作备份，不删除、不强推。旧基线2bf6dacd相对本次官方master有9个额外自定义提交，官方master则有41个新提交；不是只替换remote URL。

当前从官方master创建新分支，仅移入sink的50文件提交。确认旧2bf6dacd不是新分支祖先，未带入create-paused-changefeed等无关改动。保留上游go.mod/go.sum和Go1.26.9要求，没有覆盖为旧依赖或旧框架实现。

上游已包含当前sink接口、write gate、共用spool、PostEnqueue及DetachPostCallbacks；这些直接复用。与官方基线的生产代码diff仍仅新Databend包、scheme/type/factory/metrics/schema store/error注册和spool预接纳计量接口；不改collector/dynstream/progress/调度协议。

bend-cdc比较基线仍为b648162；sink范围和限制保持：full-raw MERGE、有界spool、conflict-key、指定RENAME/DROP DATABASE跳过；分区dispatcher、自动HA、全量初始化未开放。

## 官方基线独立验证

Go使用上游要求的1.26.9，不改go.mod/go.sum。与官方master的diff不包含api/coordinator/dispatcher/eventcollector/DDL协调算法等旧自定义变化。新checkout已独立打包对象并通过git fsck，不依赖旧工作区的object alternates；旧目录可保留为历史备份。

### 本地回归

```sh
GOMAXPROCS=4 go test -p 2 -race -tags=intest ./downstreamadapter/sink/databend ./downstreamadapter/sink ./pkg/sink/spool ./pkg/errors ./pkg/config ./downstreamadapter/sink/metrics ./downstreamadapter/sink/cloudstorage ./downstreamadapter/sink/mysql/causality -count=1 -timeout=240s
# PASS，8包，包含既有cloudstorage和MySQL causality相邻回归
GOMAXPROCS=4 go test -p 2 ./pkg/common/event -run 'TestTableSchemaStore|TestNeedTableNameStore' -count=1
# PASS
GOMAXPROCS=4 go test -p 2 ./pkg/metrics -run TestDownstreamTypeFromSinkURI -count=1
# PASS
```

定向golangci-lint：0 issues；go vet、主程序构建、make fmt（仅指定本次Go文件）、Python ruff/格式及文档检查通过。首次额外cloudstorage命令漏了项目要求的intest标签，TiDB testkit明确拒绝并引发后续fixture错误；改用上面的正确命令后8包通过，没有修改上游测试或依赖掩盖失败。

### 真实 TiUP→当前CDC→同一授权staging Lake

TiUP TiDB/PD/TiKV v8.5.1各1个；当前官方基线的本地构建标签为`b9179182-databend-official-e2e`，注入v8.5.1仅作源版本兼容标识，不是官方CDC发布包。源/会话UTC；仅对本轮自有TiUP关闭快速/分布式ADD INDEX，避免开发机剩余磁盘阈值影响小型DDL测试。

| 独立运行 | 结果 | 覆盖 |
|---|---|---|
| acceptance-2 | 9 / 9 PASS | DML/PK/UK、复合键、引号列名、32列类型和值、有限列DDL、TRUNCATE、spool、排空重启、DROP TABLE |
| ddl-matrix | 11 / 11 PASS | 非空UK无显式PK、INT→VARCHAR(200)、默认值/nullability、类型扩宽、字面默认值回填、CHANGE、多操作DDL |
| manual-key | 1 / 1 PASS | 真正无PK/UK的CRUD+加列，显式conflict-key、单列原生PK override |
| skipped-ddl | 2 / 2 PASS | RENAME和DROP DATABASE跳过后保留目标、checkpoint及后续DML继续 |

这些是23组分开运行的功能场景，有内容重叠，不表示23种互不重叠的能力。首次acceptance-1在只读version预检遇到HTTP读超时，没有创建feed或业务表；保留FAIL记录，随后在新随机库完整执行acceptance-2，未拼接失败结果。

新主链路实际spool磁盘读回 **41次**，测试配额64 KiB，1200行事务和尾批排空通过。排空重启PID `82754`→`88459`，checkpoint `469996705429585930`→`469996706412363793`，未覆盖checkpoint。

### 真实 Lake 故障注入

在当前官方源码上重新运行`TestDatabendNativeLiveRawPartialAndMergeResponseLoss`及`TestDatabendNativeLiveDDLInterruptionRecovery`，带race，**21个故障子场景PASS**（package 241.396s）：部分raw INSERT、MERGE提交后丢弃成功响应、CREATE/ALTER/TRUNCATE/DROP的19个关键提交边界。客户端未取得终态时不提前callback、不暗中重发；测试方独立取得匹配query_id的服务器Succeeded且旧sink已结束后，重建本地状态并验证恢复值、墓碑/epoch规则。

这是真实SQL+客户端响应丢失/状态重建，不是伪造SQL成功；不是服务端执行中崩溃、每个OS指令点SIGKILL或自动HA证明。

### 清理与证据

所有成功run的cleanup为PASS；按本轮具体名字核验5个源命名空间、7个远端命名空间（含2个fault随机库）及对应feed均无残留，没有修改用户default中的既有表。自有CDC/TiUP停止、测试tag数据清理、临时凭据删除。没有创建PR、部署共享CDC，HA/分区dispatcher/性能成本/全量初始化仍未验收或未支持。

私有证据目录（本机）：

```text
/var/folders/lq/3n41d_w97w55hrx2mzmd42w80000gn/T/ticdc-native-official-hstp1_7y
```

关键记录：`acceptance-2/result.json`、`ddl-matrix/result.json`、`manual-key/result.json`、`skipped-ddl/result.json`、`live-fault.log`、`fault-summary.json`、`repository-identity.json`、`cleanup-verification.json`。旧基线报告仅保留历史，当前验收以本文件和这些独立记录为准。

## 交付核验

新分支以官方b9179182f为merge-base，只包含sink提交及这次官方基线说明，不包含旧基线的9个额外自定义提交。推送目标是官方fork `sdojjy/tigate:feat/databend-native-sink`，不是`sdojjy/tiflow`。实际提交/远端一致性以Git记录为准；不会强推或删除旧备份分支。
