"""Generate real Spark event logs, one application per scenario, for spark-stage-doctor's tests.

Runs on Linux with Java 17 (the GitHub Actions ubuntu-24.04 image), with any pyspark 3.5.x or 4.0.x:

    pip install pyspark==4.0.4 && python tools/gen_fixtures.py out/spark-4.0

Each scenario gets its own SparkSession (so its own event log) with only the settings it needs, and is named by
its job description so tests can find its stages. manifest.json records what each scenario is meant to trigger;
the tests assert the analyzer finds exactly that, and nothing on the clean baseline.

Not run on Windows: event logging needs winutils there (SparkContext fails at startup without it).
"""
from __future__ import annotations

import glob
import json
import os
import shutil
import sys
import time

from pyspark import SparkConf, SparkContext, TaskContext
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

OUT = os.path.abspath(sys.argv[1] if len(sys.argv) > 1 else "out")
ONLY = set(sys.argv[2].split(",")) if len(sys.argv) > 2 else None

BASE_CONF = {
    "spark.ui.enabled": "false",
    "spark.driver.memory": "2g",
    "spark.eventLog.enabled": "true",
    "spark.eventLog.logStageExecutorMetrics": "true",
    "spark.sql.shuffle.partitions": "8",
    "spark.sql.adaptive.enabled": "false",       # scenarios that need AQE turn it on explicitly
    "spark.sql.autoBroadcastJoinThreshold": "10485760",
}


def skewed_fact(spark, rows=2_000_000, hot_share=9):
    """`hot_share` tenths of the rows have key 0; the rest spread over 1000 keys. The payload is a SHA-256 hex
    string: incompressible, so shuffle bytes reflect row counts (a repeated literal compresses to almost nothing)."""
    return (spark.range(0, rows)
            .withColumn("key", F.when(F.col("id") % 10 < hot_share, F.lit(0)).otherwise(F.col("id") % 1000))
            .withColumn("payload", F.sha2(F.col("id").cast("string"), 256)))


def dim(spark, rows=1000):
    return spark.range(0, rows).withColumnRenamed("id", "key").withColumn("dim_name", F.concat(F.lit("d"), F.col("key")))


def noop(df):
    df.write.format("noop").mode("overwrite").save()


# ------------------------------------------------------------------ scenarios: (conf, body, expected)

def clean(spark):
    df = spark.range(0, 400_000).withColumn("g", F.col("id") % 64)
    noop(df.groupBy("g").agg(F.sum("id"), F.count("*")))


def skew_repartition(spark):
    """Data skew in a plain shuffle: AQE cannot split it (it only splits joins)."""
    noop(skewed_fact(spark).repartition(8, "key").sortWithinPartitions("payload")
         .withColumn("h", F.sha2(F.concat(F.col("payload"), F.col("payload")), 512)))   # per-row CPU work


def skew_join_unsplit(spark):
    """Skewed sort-merge join with AQE on at default thresholds: the hot partition is under 256 MB, so not split."""
    noop(skewed_fact(spark).join(dim(spark), "key").groupBy("dim_name").agg(F.max("payload")))


def skew_join_split(spark):
    """Same join with AQE skew thresholds lowered so that AQE does split it: SortMergeJoin(skew=true)."""
    noop(skewed_fact(spark).join(dim(spark), "key").groupBy("dim_name").agg(F.max("payload")))


def broadcast_disabled(spark):
    """autoBroadcastJoinThreshold=-1 forces a sort-merge join against a tiny dimension."""
    noop(skewed_fact(spark, rows=400_000, hot_share=0).join(dim(spark), "key"))


def spill(spark):
    """Force the sorter to spill (numElementsForceSpillThreshold) while sorting 2 large partitions."""
    noop(spark.range(0, 1_200_000).withColumn("r", F.rand(7)).repartition(2).sortWithinPartitions("r"))


def tiny_tasks(spark):
    """1000 partitions of 60 rows each: scheduling overhead dominates."""
    noop(spark.range(0, 60_000, numPartitions=1000).withColumn("x", F.col("id") * 2))


def single_task(spark):
    """coalesce(1) funnels a CPU-heavy stage into one task."""
    df = spark.range(0, 3_000_000, numPartitions=8).withColumn("h", F.sha2(F.col("id").cast("string"), 256))
    noop(df.coalesce(1).withColumn("h2", F.sha2(F.col("h"), 512)))


def task_failures(spark):
    """Half the tasks fail on their first attempt (ExceptionFailure) and succeed on retry."""
    def flaky(index, rows):
        if TaskContext.get().attemptNumber() == 0 and index % 2 == 0:
            raise RuntimeError(f"injected failure in partition {index}")
        yield sum(1 for _ in rows)
    spark.sparkContext.parallelize(range(80_000), 8).mapPartitionsWithIndex(flaky).collect()


def small_files(spark):
    """120 CSV files of ~2 KiB each, written by plain Python so that only the scan is in the log."""
    d = os.path.join(OUT, "_data", "small_csv")
    shutil.rmtree(d, ignore_errors=True)
    os.makedirs(d)
    for i in range(120):
        with open(os.path.join(d, f"part-{i:04d}.csv"), "w") as f:
            f.writelines(f"{i * 100 + j},{j % 7},row-{i}-{j}\n" for j in range(100))
    df = spark.read.schema("id LONG, grp INT, payload STRING").csv("file://" + d)
    noop(df.groupBy("grp").count())


def executor_lost(spark):
    """Partition 0's first attempt SIGKILLs its own executor JVM: ExecutorLostFailure, then a retry elsewhere."""
    def kill_executor(index, rows):
        if index == 0 and TaskContext.get().attemptNumber() == 0:
            daemon = os.getppid()                          # python worker -> pyspark daemon -> executor JVM
            with open(f"/proc/{daemon}/stat") as f:
                jvm = int(f.read().rsplit(")", 1)[1].split()[1])
            os.kill(jvm, 9)
            time.sleep(60)
        yield sum(1 for _ in rows)
    spark.sparkContext.parallelize(range(8000), 4).mapPartitionsWithIndex(kill_executor).collect()


def executor_memory(spark):
    """Cache ~200 MB on 4 GiB executors and aggregate it, with executor metrics polled every 100 ms so the peaks are
    recorded. Peak heap stays far below 4 GiB: an over-provisioning case."""
    df = (spark.range(0, 2_000_000, numPartitions=4)
          .withColumn("p", F.sha2(F.col("id").cast("string"), 256)).cache())
    df.count()
    noop(df.groupBy((F.col("id") % 1000).alias("g")).agg(F.countDistinct("p")))


CLUSTER = "local-cluster[2,1,1024]"   # 2 executor JVMs, 1 core and 1 GiB each (needs SPARK_HOME; Linux only)
OPTIONAL = {"executor_lost", "executor_memory"}

SCENARIOS = {
    "clean": ({}, clean, []),
    "skew_repartition": ({}, skew_repartition, ["task-skew"]),
    # Coalescing off: otherwise AQE merges the 7 small partitions and the stage has 2 tasks, too few for a median.
    "skew_join_unsplit": ({"spark.sql.adaptive.enabled": "true", "spark.sql.autoBroadcastJoinThreshold": "-1",
                           "spark.sql.adaptive.coalescePartitions.enabled": "false"},
                          skew_join_unsplit, ["task-skew", "skew-join-not-split"]),
    "skew_join_split": ({"spark.sql.adaptive.enabled": "true", "spark.sql.autoBroadcastJoinThreshold": "-1",
                         "spark.sql.adaptive.skewJoin.skewedPartitionFactor": "2",
                         "spark.sql.adaptive.skewJoin.skewedPartitionThresholdInBytes": "1MB",
                         "spark.sql.adaptive.advisoryPartitionSizeInBytes": "1MB"},
                        skew_join_split, ["aqe-skew-split"]),
    "broadcast_disabled": ({"spark.sql.autoBroadcastJoinThreshold": "-1"}, broadcast_disabled, ["broadcast-candidate"]),
    "spill": ({"spark.shuffle.spill.numElementsForceSpillThreshold": "20000"}, spill, ["spill"]),
    "tiny_tasks": ({}, tiny_tasks, ["too-many-small-tasks"]),
    "single_task": ({}, single_task, ["too-few-tasks"]),
    # local[N] ignores spark.task.maxFailures (always 1); local[N,F] allows F attempts per task.
    "task_failures": ({"master": "local[2,2]"}, task_failures, ["task-failures"]),
    "small_files": ({}, small_files, ["small-files"]),
    "executor_lost": ({"master": CLUSTER}, executor_lost, ["task-failures"]),
    # local-cluster's last number is the WORKER's memory; executors still default to spark.executor.memory=1g.
    "executor_memory": ({"master": "local-cluster[2,1,4096]", "spark.executor.memory": "3g", "spark.executor.metrics.pollingInterval": "100ms",
                         "spark.executor.processTreeMetrics.enabled": "true"}, executor_memory, ["executor-memory"]),
}

# The clean app again with each event-log codec (Spark 3.5 defaults to uncompressed; 4.0 to rolling + zstd).
CODEC_VARIANTS = {f"clean-{c}": c for c in ("zstd", "lz4", "snappy", "lzf")}


def run_app(name: str, conf: dict, body, log_dir: str) -> dict:
    os.makedirs(log_dir, exist_ok=True)
    if conf.get("master", "").startswith("local-cluster"):
        import pyspark
        os.environ.setdefault("SPARK_HOME", os.path.dirname(pyspark.__file__))   # executors are launched from here
        os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
    conf = dict(conf)
    # Not SparkSession.builder: its options live in a CLASS-level dict, so every scenario's settings would leak
    # into all later ones. A fresh SparkConf per app (without JVM system-property defaults) is isolated.
    sconf = SparkConf(loadDefaults=False).setMaster(conf.pop("master", "local[2]")).setAppName(name)
    sconf.setAll(list({**BASE_CONF, **conf, "spark.eventLog.dir": "file://" + log_dir}.items()))
    sc = SparkContext(conf=sconf)
    spark = SparkSession(sc)
    app_id, version = sc.applicationId, spark.version
    sc.setJobDescription(name)
    t = time.time()
    try:
        body(spark)
    finally:
        spark.stop()
        SparkSession._instantiatedSession = None   # noqa: SLF001 - so the next app does not reuse this session
    files = sorted(os.listdir(log_dir))
    return {"app_id": app_id, "spark_version": version, "seconds": round(time.time() - t, 1), "files": files}


def main() -> None:
    os.makedirs(OUT, exist_ok=True)
    manifest = {}
    jobs = [(n, c, b, e) for n, (c, b, e) in SCENARIOS.items()]
    jobs += [(n, {"spark.eventLog.compress": "true", "spark.eventLog.compression.codec": codec}, clean, [])
             for n, codec in CODEC_VARIANTS.items()]
    jobs += [("clean-defaults", {}, clean, [])]   # whatever this Spark version writes by default
    for name, conf, body, expected in jobs:
        if ONLY and name not in ONLY:
            continue
        log_dir = os.path.join(OUT, name)
        shutil.rmtree(log_dir, ignore_errors=True)
        if name != "clean-defaults":
            conf = {"spark.eventLog.rolling.enabled": "false", **conf}
            conf.setdefault("spark.eventLog.compress", "false")
        try:
            info = run_app(name, conf, body, log_dir)
        except Exception as e:  # noqa: BLE001 - optional scenarios depend on the runner (local-cluster, /proc)
            if name not in OPTIONAL:
                raise
            print(f"optional scenario {name} failed: {e!r}", flush=True)
            shutil.rmtree(log_dir, ignore_errors=True)
            continue
        info.update(expected=expected, conf=conf)
        manifest[name] = info
        print(json.dumps({name: info}), flush=True)
    shutil.rmtree(os.path.join(OUT, "_data"), ignore_errors=True)
    with open(os.path.join(OUT, "manifest.json"), "w") as f:
        json.dump(manifest, f, indent=2, sort_keys=True)
    print("sizes:", {p: os.path.getsize(p) for p in glob.glob(os.path.join(OUT, "*", "*")) if os.path.isfile(p)})


if __name__ == "__main__":
    main()
