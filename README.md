# spark-stage-doctor

[![CI](https://github.com/amritanshagrawal99/spark-stage-doctor/actions/workflows/ci.yml/badge.svg)](https://github.com/amritanshagrawal99/spark-stage-doctor/actions/workflows/ci.yml)
[![License: MIT](https://img.shields.io/badge/License-MIT-blue.svg)](LICENSE)

**Diagnose a Spark application from its event log: what was slow, why, and which setting or code change fixes it.**
Offline and pip-only (no JVM, no running cluster, no History Server). Reads Spark 3.3 to 4.0 logs in every layout
they come in. Every finding names the stage, shows the numbers behind it, and cites where its threshold comes from.

```text
$ spark-stage-doctor eventlogs/skew_join_unsplit
skew_join_unsplit  (Spark 4.0.4, 3.0 s)
  1 executor(s), 2 cores; 4 jobs, 4 stage attempts, 20 task attempts (0 failed); 1 SQL queries
  2 critical, 1 warn

[CRITICAL] SortMergeJoin could be a broadcast join
  where: SQL query 2: skew_join_unsplit
  The right side of this SortMergeJoin is 31.2 KiB in memory, but broadcast joins are switched off
  (autoBroadcastJoinThreshold=-1), so both sides were shuffled (167.8 MiB on the other side).
  fix: Remove spark.sql.autoBroadcastJoinThreshold=-1, or hint this join: `big.join(broadcast(small), ...)`

[WARN] Skewed SortMergeJoin (Inner) was not split by AQE
  where: stage 4 (job 2: skew_join_unsplit)
  The join stage's largest partition read 112.0 MiB against a median of 1.8 MiB. Why AQE left it: the largest
  partition (112.0 MiB) is below AQE's bar: max(skewedPartitionThresholdInBytes 256.0 MiB, skewedPartitionFactor
  5 x median 1.8 MiB) = 256.0 MiB.
  fix: spark.sql.adaptive.skewJoin.skewedPartitionThresholdInBytes=64m and/or ...skewedPartitionFactor=2
```

(Real output, abridged, on a log from the test suite; run with `--profile dev` because the job is tiny.)

## Install and run

```bash
pip install git+https://github.com/amritanshagrawal99/spark-stage-doctor
spark-stage-doctor /path/to/eventlog                      # text report
spark-stage-doctor /path/to/eventlog --format md          # Markdown, for a PR comment or job summary
spark-stage-doctor logs/ --format json --fail-on critical # every app in a directory; exit 1 on critical findings
spark-stage-doctor compare baseline.log current.log       # regression gate: exit 1 if the run got worse
```

Event logs are written when `spark.eventLog.enabled=true` (the directory is `spark.eventLog.dir`). Databricks writes
them through cluster log delivery under `<destination>/<cluster-id>/eventlog/`. The History Server serves them as a zip
at `/api/v1/applications/<app-id>/logs`.

| Input | Layout |
|---|---|
| Single file | `<app-id>[.zstd\|.lz4\|.snappy\|.lzf][.inprogress]`, the default up to Spark 3.5 |
| Rolling directory | `eventlog_v2_<app-id>/events_<n>_...` + `appstatus_...`, the **default in Spark 4.0** (with zstd) |
| History Server zip | any of the above inside a `.zip` |
| Databricks folder | `eventlog` + rolled `eventlog-YYYY-MM-DD--HH-MM.gz` parts (experimental, see Limitations) |
| A directory of logs | each application analysed separately |

Logs still being written are fine: a cut-off last line or compressed block is dropped and reported.

## What it checks

| Rule | Fires when | Threshold source |
|---|---|---|
| `task-skew` | the slowest task is >= 1.5x the 75th percentile and holds the stage open; split into **data skew** (it read >= 3x the median bytes) vs **straggler** (normal data: a slow node, so speculation helps) | Databricks Spark UI guide; Dr. Elephant severity ladder |
| `skew-join-not-split` | a shuffle join has a skewed partition that AQE did not split, and **which gate stopped it**: AQE off, skew join off, partition under `max(threshold, factor x median)`, a join type that can't be split, or AQE declining because splitting needs an extra shuffle | AQE's own `OptimizeSkewedJoin` rules |
| `aqe-skew-split` | (info) AQE did split skewed partitions, with its counts | AQE |
| `broadcast-candidate` | a sort-merge or shuffled-hash join whose small side (AQE's in-memory `data size`) fits the broadcast threshold, or is within 10x of it while the other side is 10x larger; knows which side each join type can broadcast | AQE threshold (10 MiB; 30 MiB adaptive on Databricks) |
| `small-files` | a file scan read >= 100 files averaging under 32 MiB (from the scan's driver-side metrics); suggests compaction and where to fix the writer | project; Spark split settings |
| `spill` | tasks spilled to disk; severity by spill relative to bytes read | Databricks (any spill); project share thresholds |
| `too-many-small-tasks` | thousands of tasks with a sub-second median; advice depends on whether they came from a shuffle, a file scan, or the code | Spark tuning guide (200 ms) |
| `too-few-tasks` | fewer tasks than cores on a long stage; points at `coalesce()` when it's in the lineage; skips small shuffles AQE coalesced on purpose | Databricks (one-task stage); tuning guide (2-3 tasks/core) |
| `gc-pressure` | GC >= 10% of task time | Spark UI (red at 10%); Dr. Elephant |
| `scheduler-delay` | median scheduler delay >= 100 ms, or a slow stage start | project |
| `task-failures` | failed attempts, grouped by error (the real exception line, also inside Python tracebacks); OOM / container-kill and fetch-failure advice; stage retries | Dr. Elephant (failure rate) |
| `speculation-waste` | killed speculative attempts burned >= 5% of task time | project |
| `shuffle-heavy` | >= 1 GiB shuffled with a high write / fetch-wait share | project |
| `low-utilisation` | tasks used < 50% of the cores while jobs ran | project |

Ratios are the same in both profiles. `--profile dev` only lowers the absolute floors (a 30 s slowest task, 100 MiB
partitions, 2000 tasks, 60 s stages, and so on), so small and test jobs can be checked too.

## Use it in CI

```yaml
- uses: amritanshagrawal99/spark-stage-doctor@main
  with:
    path: spark-events/           # written by your integration test's SparkSession
    baseline: baseline/app.log    # optional: compare with a known-good run instead
    fail-on: critical
```

The action writes a Markdown report to the job summary. It then fails the step on findings at or above `fail-on`,
or on any regression when `baseline` is set. A regression is a new or worsened finding (matched by rule and stage
name, since IDs change between runs) or a total that grew more than 20%: wall time, task time, shuffle written,
disk spill, failed tasks.

## How it reads a log correctly

These details decide whether the numbers are right. Each was checked against Spark's own source (`JsonProtocol`,
`SQLAppStatusListener`, `AppStatusUtils`) and then against the real logs in `tests/`:

- **Units are mixed.** `Executor CPU Time`, `Executor Deserialize CPU Time` and `Shuffle Write Time` are nanoseconds.
  Every other duration is milliseconds. Scheduler delay is the Spark UI's residual (task time minus the executor-side
  phases), floored at 0.
- **Task metrics come only from `TaskEnd` "Task Metrics"**, which is missing when an executor is lost. Never from
  `Accumulables`: Spark 4.0 writes them empty on start events and can drop the internal ones.
- **Stages are keyed by (stage, attempt).** Distributions use one successful attempt per partition, the first to
  finish, so speculative copies and retries aren't counted twice.
- **SQL operators are tied to stages only by accumulator ID.** Plan position and the `(N)` labels in `EXPLAIN`
  don't work for this. Under AQE each query has several plan snapshots with new IDs, so IDs from all of them are
  kept, and the final plan describes the operator. Per-task SQL metric values use the successful attempt's
  `Update`, never the running stage total in `Value`.
- **Configs are explicit-only.** "Spark Properties" lists what was set, so defaults are applied by version (AQE on
  since 3.2; speculation 1.5x / 0.75 in 3.x, 3x / 0.9 in 4.0). Per-query overrides (`modifiedConfigs`) win.
- **Spark's lz4 and snappy are Java block formats** (lz4-java's `LZ4Block`, snappy-java's xerial stream), not the
  standard LZ4 frame or Snappy framing formats, so generic Python readers can't open them. They are parsed by hand
  here, and LZF is decoded in pure Python.

## Tests against real Spark logs

`tools/gen_fixtures.py` runs on GitHub Actions with **pyspark 3.5.9 and 4.0.4**
([fixtures workflow](.github/workflows/fixtures.yml)). It writes one application per scenario: clean, plain-shuffle
skew, a skewed join AQE doesn't split, one it does split, broadcast disabled, forced spill, 1000 tiny tasks, a
`coalesce(1)` stage, injected task failures, 120 tiny CSV files, and the clean app under every event-log codec and Spark's default
layout. The logs are committed, so the tests need no JVM.

- **Every scenario must produce exactly its intended findings, and the clean baseline none,** on both Spark versions.
- **Spark's own test-suite logs** (3.1, 3.3, YARN, streaming, a real 53-second wasted speculative task) must parse
  and produce no unknown core events.
- Synthetic events cover what is impractical to generate: OOM container kills, fetch failures and stage retries,
  GC, Photon plan names, Databricks thresholds.
- Key branches were mutation-tested: each surviving mutant got a test.

Two things the real logs caught that the docs didn't:

- PySpark's `SparkSession.builder` keeps its options in a class-level dict, so generating several apps in one
  process silently leaked each scenario's settings into the next. A test now checks each log's properties against
  its manifest.
- The fixture workflow confirmed that Spark 4.0's default log is a rolling directory with zstd.

## Limitations

- **Databricks is experimental.** The folder layout and Photon operator names (`PhotonGroupingAgg`, ...) come from
  documentation and community reports. No real Databricks log is in the test suite yet, and contributions of a
  redacted one are welcome.
- Some executor-level metrics need `spark.eventLog.logStageExecutorMetrics=true` (off by default), and none of the
  rules depend on them. GC pressure uses task-level GC time, which is JVM-wide, so it is reported as a ratio.
- Skew is measured per task. When AQE coalesces a skewed stage down to a handful of tasks, there is too little
  distribution to call it skew (fewer than 10 successful tasks, or 5 with `--profile dev`).
- Structured streaming queries are parsed, but there are no streaming-specific rules yet.

## Prior art

[Sparklens](https://github.com/qubole/sparklens) and [Dr. Elephant](https://github.com/linkedin/dr-elephant) pioneered
this for Spark 2. Both are unmaintained, and both need a JVM build. [sparkMeasure](https://github.com/LucaCanali/sparkMeasure)
is an actively maintained live listener, not an offline log analyser. This project borrows their published
thresholds where they exist, and says so in each finding.

## License

[MIT](LICENSE). The four logs in `tests/fixtures/eventlogs/apache-spark/` are from Apache Spark (Apache License 2.0).
