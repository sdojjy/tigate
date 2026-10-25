# Native Databend / Lake acceptance test

Opt-in integration test for **a local TiUP TiDB + current-branch new-architecture
TiCDC + a real, explicitly authorized Lake test warehouse**. It uses only the
Python standard library and a local `mysql` CLI; no Dumpling or S3 is involved.

The runner creates a random `native_e2e_*` database and a matching changefeed,
compares actual TiDB rows against actual Lake rows, and removes only those owned
resources after success. On failure, it keeps its isolated database/feed and a
private result file for diagnosis. Never point it at a production source/target.

Current upstream base: `pingcap/ticdc` master
`b9179182f3c1c5db5a018029f41eb07eab804051`; fork delivery is
`sdojjy/tigate`, not the legacy tiflow repository. Earlier test reports using
2bf6dacd are historical and must not be reused as new-base validation.

## Prepare

1. Build `cmd/cdc` from this branch. Use `--newarch` and UTC for the test capture.
   Any compatibility release label injected into this local build is **not** an
   official upstream release; record the source commit and dirty-tree status.
2. Start TiUP with one TiDB/PD/TiKV and no TiFlash/monitor. For example:

   ```sh
   tiup playground v8.5.1 --tag native-lake-test \
     --db 1 --pd 1 --kv 1 --tiflash 0 --ticdc 0 \
     --without-monitor --port-offset 10000
   ```

3. Start the built CDC with `--newarch --tz UTC`, local PD
   `http://127.0.0.1:12379`, API `127.0.0.1:18300`, and a private test data dir.
   The source is always `127.0.0.1`; `MYSQL_PORT` defaults to `14000`.
4. Put the authorized Lake URI in a **0600** file outside the repository. The
   runner reads `LAKE_URI_FILE`; never put the URI/password on a command line,
   into a shared report, or into Git. `lake://` is the HTTPS sink alias.
5. On low-free-space development laptops, TiDB v8.5.1's fast/distributed ADD
   INDEX can reject a tiny test table before CDC sees any DDL. For this **owned
   local TiUP only**, use legacy index construction if necessary:

   ```sql
   SET GLOBAL tidb_ddl_enable_fast_reorg=OFF;
   SET GLOBAL tidb_enable_dist_task=OFF;
   ```

   Record this source-environment adjustment. Do not change these settings on
   a shared/production source just to make this test pass.

## Run

```sh
umask 077
export LAKE_URI_FILE=/absolute/path/to/private/lake-uri
export E2E_RUN_DIR=/absolute/path/to/new/private/run-directory
export CDC_SERVER=http://127.0.0.1:18300
export MYSQL_PORT=14000
# Optional process-restart scenario: only the exact owned local binary is stopped.
export E2E_CDC_BINARY=/absolute/path/to/current-branch-cdc
export E2E_CDC_DATA_DIR=/absolute/path/to/the/cdc/data-dir
export E2E_PD=http://127.0.0.1:12379
python3 tests/integration_tests/databend_native/run.py
```

Use a fresh run directory each time. Dedicated source and target database names
are chosen automatically; the URI's original database (e.g. `default`) is not
used as the test database. Existing tables there are never altered or dropped.

The current suite covers:

- DML insert/update/delete, nonclustered PK updates, secondary UK updates,
  same-transaction delete/reinsert and composite keys.
- Quoted table/column names, `action` and internal-discriminator name collisions.
- Signed/unsigned integer boundaries, DECIMAL(65,30), FLOAT/DOUBLE, CHAR/TEXT,
  binary/BLOB/VARBINARY padding/bytes, BIT(64), numeric-label ENUM, 64-element SET,
  exact JSON integers, SQL NULL versus JSON null, dates, microsecond
  datetime/timestamp/time, YEAR, and visible STORED generated columns.
  VIRTUAL generated columns must not appear in the target.
- ADD/default/backfill, RENAME, integer widening, DROP column, secondary indexes,
  TRUNCATE/new DML and DROP TABLE/raw cleanup.
- A 1,200-row transaction larger than the **64 KiB** disk quota, with a small
  memory hot set. A live spool read histogram must show disk readback; this is
  not inferred solely from configured thresholds.
- Optional drained process restart: wait for a positive source transaction TSO
  checkpoint, pause, verify the owned CDC process, terminate it, create a source
  DML/DDL backlog, restart, wait through coordinator bootstrap, resume without
  overriding checkpoint, then compare the recovered state. This is **not**
  a claim of in-flight ambiguous-query failover or multi-capture HA.

Comparison is key-based, not dependent on differing numeric/VARCHAR ORDER BY
semantics. JSON null is observed explicitly with `is_null_value`; FLOAT is read
at sufficient source precision and compared with tolerance. All other integer,
Decimal, text and binary comparisons are exact (binary is canonicalized as hex).

## Failure inspection and cleanup

`result.json` records individual cases, versions, target schema and restart/spool
evidence. It excludes credentials. Logs and temporary credentials stay in the
private run directory, not the repository.

First inspect a failure and ensure the old CDC queries are terminated. To remove
just the owned failed run:

```sh
python3 tests/integration_tests/databend_native/run.py --cleanup
```

Use the same environment/run directory. Cleanup validates the generated namespace
and matching feed, pauses a healthy feed before deletion, and never drops the
URI's original database. The runner does not stop an externally launched TiUP or
CDC after the test: stop your owned local processes separately. Do not confuse a
successful source/target test with fault-injection, cost/SLO, unsupported-DDL or
production-readiness validation.

## Extended DDL and baseline parity probes

`extended.py` uses the same local services/private environment and a fresh
`E2E_RUN_DIR`. It verifies defaults and nullability with actual target DESCRIBE,
not only row counts. Its positive suite also covers a NOT NULL UNIQUE handle
without a declared PK and sufficiently wide `INT -> VARCHAR(200)`.

```sh
python3 tests/integration_tests/databend_native/extended.py
# Separate fresh run directories for each intentionally unsupported probe:
python3 tests/integration_tests/databend_native/extended.py --reject partition
python3 tests/integration_tests/databend_native/extended.py --reject no-pk
python3 tests/integration_tests/databend_native/extended.py --reject narrow
python3 tests/integration_tests/databend_native/extended.py --reject expression-default
```

A rejection PASS means **stable failed feed + nonadvancing checkpoint + unchanged
existing target (or no new projection)**, NOT feature support. The `no-pk` probe
forces replication and writes a source row, so upstream eligibility filtering
cannot masquerade as sink rejection. A NOT NULL UNIQUE table is a positive test,
not a `no-pk` rejection. Optional `--unique-only` / `--varchar-only` isolate those
positive cases for diagnosis.

`baseline.py` runs an ephemeral Go overlay against the **clean, fixed b648162**
independent bend-cdc module. It changes no baseline files. Set `BEND_CDC_DIR` to
that checkout, retain the same private URI-file environment, and use a fresh run
directory. `baseline_parity_test.go.txt` is the reviewable overlay fixture.

```sh
export BEND_CDC_DIR=/absolute/path/to/webapi/bend-cdc
python3 tests/integration_tests/databend_native/baseline.py
```

These baseline probes exercise its actual Canal/schema-to-Lake core, not the
entire S3 listing/decoding path. In particular they assert RENAME / DROP DATABASE
are **not projected**, rather than treating a nil error as feature support.

## Real warehouse response-loss injection

`live_fault_test.go` is opt-in. All SQL executes on the real authorized warehouse.
A delegating HTTP RoundTripper drops a matching real Succeeded response before
the sink sees it; it does NOT mock SQL success. For partial raw writes it blocks
the second INSERT before sending, after the first real INSERT committed.

```sh
python3 - <<'PY'
import os, pathlib, subprocess
p = pathlib.Path(os.environ['LAKE_URI_FILE'])
assert not p.stat().st_mode & 0o077

env = os.environ.copy()
env['DATABEND_NATIVE_TEST_URI'] = p.read_text().strip()
env.setdefault('GOMAXPROCS', '4')
subprocess.run([
    'go', 'test', '-p', '2', '-race', './downstreamadapter/sink/databend',
    '-run', 'TestDatabendNativeLive(RawPartialAndMergeResponseLoss|DDLInterruptionRecovery)$',
    '-v', '-count=1', '-timeout=20m',
], env=env, check=True)
PY
```

Coverage: partial raw recovery; committed MERGE with lost terminal response and
zero source flush; 19 DDL commit boundaries across CREATE/ALTER/TRUNCATE/DROP,
zero early DDL callbacks, durable pending/applied recovery, full recovered values,
and old-DML/applied-DDL replay fences. Each test creates/drops a random
`ticdc_fault_test_*` database.

Recovery is allowed because the test independently knows the old matching query
reached terminal success (or was never sent), and has ended the old sink
lifecycle. Schema-manager state is rebuilt inside the test process; this is not
a SIGKILL-at-every-instruction test, mid-execution server failure, or automatic
HA proof. A real arbitrary UNKNOWN still requires manual terminal/exclusivity
verification before resume.


## Explicit conflict key and skipped DDL

These are current positive acceptance scenarios (not the historical rejection
probes). Use the same local setup and a new E2E_RUN_DIR for each command:

```sh
python3 tests/integration_tests/databend_native/options.py --manual-key
python3 tests/integration_tests/databend_native/options.py
```

The first adds `conflict-key=ID` to the feed URI and enables API
`replica_config.force_replicate=true`. It verifies a genuinely keyless table's
INSERT/UPDATE/DELETE/ADD COLUMN, persisted effective identity, and an override of
a different single native PK. Source columns resolve case insensitively.

This is a feed-wide **single-column override**, matching bend-cdc, not automatic
uniqueness discovery: the operator must guarantee unique, non-NULL, stable values.
Do not change the key on an existing target without reinitialization. Composite
source keys cannot be replaced; absent/virtual columns and NULL row identities
are rejected. An unsplit UPDATE that changes the configured identity fails
closed; this version does not synthesize a new delete/insert protocol for arbitrary
mutable logical keys. A truly ineligible source table still needs TiCDC's existing
`force-replicate = true` (TOML) / `force_replicate` (API) setting.

The second proves RENAME TABLE / DROP DATABASE are explicitly skipped with a
warning, the checkpoint advances, previous target objects/data remain, and later
source DML/DDL still applies. Rename does not move historical rows: later DML
uses the new target name and may lazily create that projection. Dropping the
source database does not delete the target database; recreating the same names
does not automatically clear retained target rows. Reinitialize separately if
an empty projection is required. Only these requested DDLs
are skipped; unsafe column DDL and prior UNKNOWN writes still stop the task.

Partition dispatchers remain rejected: the existing lane only serializes within
one process, not between captures owning different physical partitions. These
options do not claim automatic multi-capture ownership/fencing support.
