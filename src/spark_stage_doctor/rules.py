"""Diagnoses. Each rule reads the model, applies a threshold whose source is named in the finding, and says what to
change. Ratios come from published sources where they exist (Databricks' Spark UI guide, Dr. Elephant, the Spark
tuning guide, AQE's own OptimizeSkewedJoin rule); everything else is labelled "project default".

Two profiles share the same ratios and differ only in absolute floors (seconds, bytes, task counts), so that a
1-second test job isn't held to production floors: DEFAULT for real workloads, DEV for small or test runs.
"""
from __future__ import annotations

import datetime as dt
import math
import re
from collections import Counter
from dataclasses import asdict, dataclass, field
from typing import Any

from .model import FAILED_REASONS, App, PlanNode, Stage
from .stats import human_bytes, human_ms, median, quantile, ratio

MiB, GiB = 1 << 20, 1 << 30
SEVERITIES = ("info", "warn", "critical")


@dataclass(frozen=True)
class Thresholds:
    min_tasks: int = 10                       # fewer successful tasks than this: no distribution-based rule
    skew_min_max_task_ms: int = 30_000        # the slowest task must take at least this long to matter
    data_skew_min_bytes: int = 100 * MiB      # RAPIDS tools' floor for calling a partition skewed
    small_tasks_min_count: int = 2000
    few_tasks_min_wall_ms: int = 60_000
    gc_min_run_ms: int = 60_000
    shuffle_heavy_min_bytes: int = GiB
    utilisation_min_app_ms: int = 5 * 60_000
    small_files_min_count: int = 100
    memory_min_task_ms: int = 10 * 60_000     # total task time before memory sizing is worth judging
    stream_min_batches: int = 10


DEFAULT = Thresholds()
DEV = Thresholds(min_tasks=5, skew_min_max_task_ms=1000, data_skew_min_bytes=MiB, small_tasks_min_count=200,
                 few_tasks_min_wall_ms=1000, gc_min_run_ms=2000, shuffle_heavy_min_bytes=16 * MiB,
                 utilisation_min_app_ms=5000, small_files_min_count=50, memory_min_task_ms=2000,
                 stream_min_batches=5)
PROFILES = {"default": DEFAULT, "dev": DEV}


@dataclass
class Finding:
    rule: str
    severity: str
    title: str
    detail: str
    fixes: list[str] = field(default_factory=list)
    evidence: dict[str, Any] = field(default_factory=dict)
    source: str = "project default"
    stage: tuple[int, int] | None = None
    sql_id: int | None = None
    where: str = ""                            # "stage 3 (job 2: <description>)"

    def to_dict(self) -> dict:
        d = asdict(self)
        d["stage"] = list(self.stage) if self.stage else None
        return d


# ------------------------------------------------------------------ helpers

_SIZE = re.compile(r"^\s*(-?\d+)\s*([kmgtp]?)(i?b)?\s*$", re.I)


def parse_bytes(v: str | None, default: int) -> int:
    """Spark byte strings: "10485760", "10m", "10MB", "64k", "-1" (binary units, as JavaUtils.byteStringAsBytes)."""
    if v is None:
        return default
    m = _SIZE.match(str(v))
    if not m:
        return default
    n, unit = int(m.group(1)), m.group(2).lower()
    return n * (1 << (10 * " kmgtp".index(unit))) if unit else n


def spark_size(n: float) -> str:
    """Bytes as a Spark config value, rounded down to a whole unit: 67108864 -> "64m"."""
    for unit, shift in (("g", 30), ("m", 20), ("k", 10)):
        if n >= 1 << shift:
            return f"{int(n) >> shift}{unit}"
    return str(int(n))


def memory_size(n: float) -> str:
    """A memory setting to suggest: rounded UP to 256 MiB, so a suggestion never undercuts what was measured."""
    q = 256 * MiB
    n = -(-int(n) // q) * q
    return f"{n >> 30}g" if n % (1 << 30) == 0 else f"{n >> 20}m"


def _bool(v: str | None, default: bool) -> bool:
    return default if v is None else str(v).strip().lower() == "true"


def _where(app: App, s: Stage) -> str:
    desc = s.description or (app.sql[s.sql_id].description if s.sql_id in app.sql else "") or s.name
    desc = " ".join(desc.split())        # streaming job descriptions span several lines
    job = f"job {s.job_id}" if s.job_id is not None else "no job"
    return f"{s.label} ({job}{': ' + desc[:80] if desc else ''})"


def _finding(app: App, s: Stage | None, **kw) -> Finding:
    if s is not None:
        kw.setdefault("stage", s.key)
        kw.setdefault("sql_id", s.sql_id)
        kw.setdefault("where", _where(app, s))
    return Finding(**kw)


def _partitions_for(total_bytes: int, cores: int) -> int:
    return max(2 * max(cores, 1), math.ceil(total_bytes / (128 * MiB)))


def _ops(app: App, s: Stage) -> set[str]:
    return {n.name for n in app.stage_operators(s)}


JOINS = {"SortMergeJoin", "ShuffledHashJoin"}


# ------------------------------------------------------------------ stage rules

def task_skew(app: App, s: Stage, th: Thresholds) -> list[Finding]:
    ok = s.successful
    if len(ok) < th.min_tasks or not s.wall_ms:
        return []
    runs = [t.run_ms for t in ok]
    mx, med, p75 = max(runs), median(runs), quantile(runs, 0.75)
    if mx < th.skew_min_max_task_ms or mx < 1.5 * p75 or mx - med < 0.2 * s.wall_ms:
        return []
    r = ratio(mx, med)
    severity = "critical" if r >= 10 else "warn" if r >= 3.16 else "info"
    slow = max(ok, key=lambda t: t.run_ms)
    by_shuffle = sum(t.shuffle_read_bytes for t in ok) >= sum(t.input_bytes for t in ok)
    size = (lambda t: t.shuffle_read_bytes) if by_shuffle else (lambda t: t.input_bytes)
    kind = "shuffle read" if by_shuffle else "input"
    sizes = [size(t) for t in ok]
    med_size, slow_size = median(sizes), size(slow)
    data_skew = slow_size >= 3 * med_size and slow_size >= th.data_skew_min_bytes
    ev = {"tasks": len(ok), "max_run_ms": mx, "median_run_ms": med, "p75_run_ms": p75, "max_over_median": round(r, 2),
          "stage_wall_ms": s.wall_ms, "slowest_partition": slow.index, f"slowest_{kind.replace(' ', '_')}_bytes": slow_size,
          f"median_{kind.replace(' ', '_')}_bytes": med_size, "slowest_host": slow.host}
    if data_skew:
        ops = _ops(app, s)
        fixes = []
        if ops & JOINS:
            fixes.append("This stage runs a shuffle join: see the skew-join finding for why AQE did not split it.")
        else:
            fixes += ["AQE only splits skewed JOIN partitions; this stage's skew is in "
                      f"{', '.join(sorted(ops)) or 'a non-join shuffle'}, so it has to be fixed in the query:",
                      "Salt the hot key: add `salt = (rand() * N).cast('int')`, aggregate on (key, salt), then on key.",
                      "Before a write, `df.hint('rebalance')` / `/*+ REBALANCE(col) */` lets AQE split large partitions "
                      "(needs spark.sql.adaptive.enabled=true).",
                      "Handle the hot keys separately (filter them out, process or broadcast them, union the rest); "
                      "check whether the hot key is NULL or a placeholder that should be dropped."]
        detail = (f"Partition {slow.index} read {human_bytes(slow_size)} ({kind}) against a median of "
                  f"{human_bytes(med_size)}, and took {human_ms(mx)} against a median of {human_ms(med)}. "
                  "This is data skew: one partition holds a disproportionate share of the rows.")
        title = f"Data skew: one task ran {r:.1f}x the median"
    else:
        fixes = ["The slow task did not read more data than the others: look at its host for a slow or overloaded node, "
                 "GC (see gc-pressure), or a straggling disk.",
                 "Speculation re-runs straggling tasks elsewhere: spark.speculation=true "
                 f"(defaults {'3x / 0.9' if app.version >= (4, 0) else '1.5x / 0.75'} for multiplier / quantile). "
                 "It does not help data skew."]
        detail = (f"Partition {slow.index} took {human_ms(mx)} against a median of {human_ms(med)}, but read a "
                  f"normal amount of data ({human_bytes(slow_size)} vs median {human_bytes(med_size)}).")
        title = f"Straggler task: {r:.1f}x the median run time"
    return [_finding(app, s, rule="task-skew", severity=severity, title=title, detail=detail, fixes=fixes, evidence=ev,
                     source="Databricks Spark UI guide (max > 1.5x p75); Dr. Elephant severity ladder (max/median)")]


def _join_kind(node: PlanNode) -> str:
    m = re.search(r"\b(Inner|LeftOuter|RightOuter|FullOuter|LeftSemi|LeftAnti|Cross|ExistenceJoin)\b", node.simple)
    return m.group(1) if m else "Inner"


def _splittable_sides(kind: str) -> str:
    return {"Inner": "both", "Cross": "both", "LeftOuter": "left", "LeftSemi": "left", "LeftAnti": "left",
            "RightOuter": "right"}.get(kind, "none")


def skew_join(app: App, s: Stage, th: Thresholds) -> list[Finding]:
    ops = [n for n in app.stage_operators(s) if n.name in JOINS]
    if not ops:
        return []
    ex = app.sql.get(s.sql_id)
    split = [n for n in (ex.plan.walk() if ex and ex.plan else []) if n.name in JOINS and n.skew_split]
    if split:
        counts = {}
        for n in ex.plan.walk():
            for m in n.metrics:
                if m.name in ("number of skewed partitions", "number of skewed partition splits"):
                    counts[m.name] = counts.get(m.name, 0) + sum(app.sql_metric_values(ex.id, m.accumulator_id))
        return [_finding(app, s, rule="aqe-skew-split", severity="info",
                         title=f"AQE split skewed partitions of {split[0].name}",
                         detail="Adaptive query execution detected skewed join partitions and split them "
                                f"({', '.join(f'{k}: {v}' for k, v in counts.items()) or 'counts not recorded'}).",
                         evidence={"join": split[0].raw_name, **counts}, source="AQE (OptimizeSkewedJoin)")]
    ok = s.successful
    if len(ok) < th.min_tasks:
        return []
    sizes = [t.shuffle_read_bytes for t in ok]
    mx, med = max(sizes), median(sizes)
    if mx < th.data_skew_min_bytes or mx < 3 * med:
        return []
    sid = s.sql_id
    join = ops[0]
    kind = _join_kind(join)
    aqe = _bool(app.conf("spark.sql.adaptive.enabled", sid), app.version >= (3, 2))
    skew_on = _bool(app.conf("spark.sql.adaptive.skewJoin.enabled", sid), True)
    thr = parse_bytes(app.conf("spark.sql.adaptive.skewJoin.skewedPartitionThresholdInBytes", sid), 256 * MiB)
    factor = float(app.conf("spark.sql.adaptive.skewJoin.skewedPartitionFactor", sid) or 5.0)
    bar = max(thr, factor * med)
    gates, fixes = [], []
    if not aqe:
        gates.append("adaptive query execution is off (spark.sql.adaptive.enabled=false)")
        fixes.append("spark.sql.adaptive.enabled=true")
    if not skew_on:
        gates.append("spark.sql.adaptive.skewJoin.enabled=false")
        fixes.append("spark.sql.adaptive.skewJoin.enabled=true")
    if _splittable_sides(kind) == "none":
        gates.append(f"a {kind} join cannot be split on either side")
        fixes.append("Salt the join key, or split the hot keys into a separate join.")
    if mx <= bar:
        gates.append(f"the largest partition ({human_bytes(mx)}) is below AQE's bar: max(skewedPartitionThresholdInBytes "
                     f"{human_bytes(thr)}, skewedPartitionFactor {factor:g} x median {human_bytes(med)}) = {human_bytes(bar)}")
        new_thr = max(64 * MiB, int(med * 2)) if mx > 64 * MiB else max(MiB, int(mx // 2))
        fixes.append(f"spark.sql.adaptive.skewJoin.skewedPartitionThresholdInBytes={spark_size(new_thr)} "
                     "and/or spark.sql.adaptive.skewJoin.skewedPartitionFactor=2 so this partition qualifies")
    if not gates:
        gates.append("AQE saw the skew but declined to split it, which happens when splitting would need an extra "
                     "shuffle (e.g. the join output is repartitioned or aggregated on the join key)")
        fixes.append("spark.sql.adaptive.forceOptimizeSkewedJoin=true (accepts the extra shuffle)")
    fixes.append("If the hot key is NULL or a placeholder, filter it out before the join.")
    return [_finding(app, s, rule="skew-join-not-split", severity="warn",
                     title=f"Skewed {join.name} ({kind}) was not split by AQE",
                     detail=f"The join stage's largest partition read {human_bytes(mx)} against a median of "
                            f"{human_bytes(med)}. Why AQE left it: " + "; ".join(gates) + ".",
                     fixes=fixes, evidence={"join": join.raw_name, "join_type": kind, "max_partition_bytes": mx,
                                            "median_partition_bytes": med, "aqe": aqe, "threshold_bytes": thr,
                                            "factor": factor}, source="AQE (OptimizeSkewedJoin) gates")]


def spill(app: App, s: Stage, th: Thresholds) -> list[Finding]:
    ok = s.successful
    disk = sum(t.disk_spill for t in ok)
    if not ok or disk <= 0:
        return []
    spilling = sum(1 for t in ok if t.disk_spill > 0)
    sr, sw, inp = (sum(t.shuffle_read_bytes for t in ok), sum(t.shuffle_write_bytes for t in ok),
                   sum(t.input_bytes for t in ok))
    base = max(sr, inp, 1)
    share = disk / base
    severity = "critical" if share >= 0.5 else "warn" if share >= 0.1 or spilling / len(ok) >= 0.25 else "info"
    ops = sorted({n.name for n in app.stage_operators(s) for m in n.metrics if m.name == "spill size"
                  and sum(app.sql_metric_values(s.sql_id, m.accumulator_id)) > 0}) if s.sql_id is not None else []
    cores = app.stage_cores(s)
    target = _partitions_for(max(sr, sw, inp), cores)
    fixes = [f"More, smaller partitions: spark.sql.shuffle.partitions={target} (about 128 MiB each)"
             + (", or spark.sql.shuffle.partitions=auto on Databricks" if app.databricks else ""),
             "Or let AQE size them: spark.sql.adaptive.coalescePartitions.parallelismFirst=false and "
             "spark.sql.adaptive.advisoryPartitionSizeInBytes=128m",
             "More memory per task: raise spark.executor.memory or lower spark.executor.cores.",
             "Shuffle less: select only the needed columns and filter before the shuffle."]
    return [_finding(app, s, rule="spill", severity=severity, title=f"Spill to disk: {human_bytes(disk)}",
                     detail=f"{spilling} of {len(ok)} tasks spilled {human_bytes(disk)} to disk, "
                            f"{share:.0%} of the {human_bytes(base)} the stage read"
                            + (f", in {', '.join(ops)}" if ops else "") + ". Spilled data is written, read back "
                            "and merged, which costs I/O and CPU. Memory spill figures are page-granular estimates, "
                            "so only the disk figure is used.",
                     fixes=fixes, evidence={"disk_spill_bytes": disk, "memory_spill_bytes": sum(t.mem_spill for t in ok),
                                            "spilling_tasks": spilling, "tasks": len(ok), "read_bytes": base,
                                            "operators": ops},
                     source="Databricks Spark UI guide (any spill); share thresholds are project defaults")]


def small_tasks(app: App, s: Stage, th: Thresholds) -> list[Finding]:
    ok = s.successful
    cores = app.stage_cores(s)
    if len(ok) < max(th.small_tasks_min_count, 10 * cores):
        return []
    runs = [t.run_ms for t in ok]
    med = median(runs)
    dur = sum(t.duration_ms for t in ok)
    overhead = sum(t.deser_ms + t.ser_ms + t.scheduler_delay_ms for t in ok)
    share = ratio(overhead, dur)
    if med >= 1000 and share < 0.2:
        return []
    severity = "critical" if med < 200 else "warn"
    sr, inp = sum(t.shuffle_read_bytes for t in ok), sum(t.input_bytes for t in ok)
    if sr >= inp and sr > 0:
        fixes = [f"spark.sql.shuffle.partitions={_partitions_for(sr, cores)} for this shuffle "
                 f"({human_bytes(sr)} read), or spark.sql.adaptive.coalescePartitions.parallelismFirst=false with "
                 "spark.sql.adaptive.advisoryPartitionSizeInBytes=128m (AQE otherwise favours many partitions)."]
    elif inp > 0:
        fixes = ["The stage reads many small splits: raise spark.sql.files.maxPartitionBytes (e.g. 256m) and compact "
                 "small files (Delta OPTIMIZE, or rewrite with fewer files)."]
    else:
        fixes = ["The partition count comes from the code (parallelize / range / repartition): create fewer "
                 f"partitions, about {_partitions_for(0, cores)} (2-3 per core) for this cluster."]
    return [_finding(app, s, rule="too-many-small-tasks", severity=severity,
                     title=f"{len(ok)} tiny tasks (median {human_ms(med)})",
                     detail=f"{len(ok)} tasks on {cores} cores, median run time {human_ms(med)}; scheduling, "
                            f"deserialisation and result handling took {share:.0%} of task time. The Spark tuning guide "
                            "puts the floor for efficient tasks at about 200 ms.",
                     fixes=fixes, evidence={"tasks": len(ok), "cores": cores, "median_run_ms": med,
                                            "overhead_share": round(share, 3)},
                     source="Spark tuning guide (200 ms tasks); count and share thresholds are project defaults")]


def few_tasks(app: App, s: Stage, th: Thresholds) -> list[Finding]:
    ok = s.successful
    cores = app.stage_cores(s)
    if not ok or s.wall_ms < th.few_tasks_min_wall_ms or cores <= 1 or len(ok) >= cores:
        return []
    sr = sum(t.shuffle_read_bytes for t in ok)
    advisory = parse_bytes(app.conf("spark.sql.adaptive.advisoryPartitionSizeInBytes", s.sql_id), 64 * MiB)
    aqe = _bool(app.conf("spark.sql.adaptive.enabled", s.sql_id), app.version >= (3, 2))
    if aqe and 0 < sr < len(ok) * advisory:
        return []   # AQE coalesced a small shuffle into few partitions on purpose: splitting it would not help
    severity = "critical" if len(ok) == 1 else "warn"
    rdds = " ".join(f"{r.get('Name', '')} {r.get('Callsite', '')}" for r in s.rdds).lower()
    fixes = []
    if "coalesce" in rdds:
        fixes.append("The stage's lineage has a coalesce(): coalesce(n) also shrinks the parallelism of the work "
                     "BEFORE it. Use repartition(n) instead (a shuffle), or move the coalesce to just before the write.")
    fixes += [f"Aim for 2-3 tasks per core: repartition({2 * cores}) before the expensive step.",
              "Other usual causes: a window without PARTITION BY, a gzip (non-splittable) input, a JDBC read without "
              "partitionColumn/numPartitions."]
    return [_finding(app, s, rule="too-few-tasks", severity=severity,
                     title=f"{len(ok)} task{'s' if len(ok) > 1 else ''} for {cores} cores over {human_ms(s.wall_ms)}",
                     detail=f"The stage ran {len(ok)} task{'s' if len(ok) > 1 else ''} on a cluster with {cores} cores, "
                            f"so at most {len(ok) / cores:.0%} of it was busy for {human_ms(s.wall_ms)}.",
                     fixes=fixes, evidence={"tasks": len(ok), "cores": cores, "stage_wall_ms": s.wall_ms},
                     source="Databricks Spark UI guide (long stage with one task); Spark tuning guide (2-3 tasks per core)")]


def gc_pressure(app: App, s: Stage, th: Thresholds) -> list[Finding]:
    ok = s.successful
    run, gc = sum(t.run_ms for t in ok), sum(t.gc_ms for t in ok)
    if run < th.gc_min_run_ms:
        return []
    share = gc / run
    if share < 0.10:
        return []
    severity = "critical" if share >= 0.20 else "warn"
    return [_finding(app, s, rule="gc-pressure", severity=severity, title=f"GC took {share:.0%} of task time",
                     detail=f"Tasks spent {share:.0%} of their run time in JVM garbage collection. The Spark UI marks "
                            "10% in red. (Per-task GC time is JVM-wide, so the ratio is what counts.)",
                     fixes=["More memory per task: raise spark.executor.memory or lower spark.executor.cores.",
                            "Unpersist cached data you no longer need; heavy caching shrinks execution memory.",
                            "Prefer built-in functions to Python/Scala UDFs that allocate per row."],
                     evidence={"gc_ms": gc, "run_ms": run, "gc_share": round(share, 3)},
                     source="Spark UI (10% red); Dr. Elephant (0.10 / 0.15 / 0.20)")]


def scheduler_delay(app: App, s: Stage, th: Thresholds) -> list[Finding]:
    ok = s.successful
    if len(ok) < th.min_tasks:
        return []
    cores = app.stage_cores(s)
    if len(ok) >= max(th.small_tasks_min_count, 10 * cores) and median([t.run_ms for t in ok]) < 1000:
        return []   # reported as too-many-small-tasks
    delays = [t.scheduler_delay_ms for t in ok]
    med, share = median(delays), ratio(sum(delays), sum(t.duration_ms for t in ok))
    start = min(t.launch for t in ok) - s.submitted if s.submitted else 0
    # Delays under ~50 ms are timing noise (the value is a floored residual), whatever share they make up.
    if med < 100 and (share < 0.1 or med < 50) and start < 5000:
        return []
    return [_finding(app, s, rule="scheduler-delay", severity="warn",
                     title=f"Scheduler delay: median {human_ms(med)} per task",
                     detail=f"Median scheduler delay {human_ms(med)} ({share:.0%} of task time); first task started "
                            f"{human_ms(start)} after the stage was submitted.",
                     fixes=["Large task closures: broadcast big objects instead of capturing them.",
                            "Locality waits: for object-store data, spark.locality.wait=0s.",
                            "A busy driver (many concurrent jobs, large collect()) delays scheduling."],
                     evidence={"median_delay_ms": med, "delay_share": round(share, 3), "start_latency_ms": start})]


_EXC_LINE = re.compile(r"^[A-Za-z_][\w.$]*(Error|Exception|Exit|Failure)\w*(:|$)")


def error_line(text: str | None) -> str:
    """The informative line of a task error. Python tracebacks (PythonException) end with the real exception
    ("RuntimeError: ..."); JVM errors start with it."""
    lines = [ln.strip() for ln in (text or "").splitlines() if ln.strip()]
    if not lines:
        return ""
    hits = [ln for ln in lines if _EXC_LINE.match(ln) and not ln.startswith("org.apache.spark.api.python.PythonException")]
    pick = (hits[-1] if "Traceback (most recent call last)" in text else hits[0]) if hits else lines[0]
    return pick[:200]


# "Command exited with code 137" is what a SIGKILLed executor reports (seen on a real log): the usual sender is the
# OS / YARN / Kubernetes OOM killer.
_CANCELLED = re.compile(r"\bcancell?ed\b", re.I)    # job group / query stop / SparkContext shutdown
_OOM = re.compile(r"OutOfMemory|exceeding memory limits|Container killed|exit(ed)? (with )?code 137|killed by YARN|"
                  r"memoryOverhead|OOMKilled", re.I)


def failures(app: App, s: Stage, th: Thresholds) -> list[Finding]:
    failed = [t for t in s.tasks if t.reason in FAILED_REASONS]
    if s.failure and not failed and _CANCELLED.search(s.failure):
        return []   # cancelled (job group cancelled, query stopped, SparkContext shut down): not a failure
    killed = [t for t in s.tasks if t.reason == "TaskKilled" and (t.kill_reason or "").startswith("another attempt")]
    out: list[Finding] = []
    if failed or s.failure or s.attempt > 0:
        total = max(s.num_tasks, 1)
        rate = len(failed) / total
        lost = [t for t in failed if t.reason == "ExecutorLostFailure"]
        severity = ("critical" if s.failure or rate >= 0.5 else "warn" if rate >= 0.3 or s.attempt > 0 or lost
                    or any(t.reason == "FetchFailed" for t in failed) else "info")
        groups = Counter((t.reason, t.error_class or "") for t in failed)
        sample = {k: next(t.error for t in failed if (t.reason, t.error_class or "") == k) for k in groups}
        lines = [f"{n} x {r}{' ' + c if c else ''}: {error_line(sample[(r, c)])}" for (r, c), n in groups.most_common(3)]
        fixes = []
        if any(_OOM.search(f"{t.error_class} {t.error}") for t in failed):
            fixes.append("Out-of-memory failures: raise spark.executor.memoryOverhead (to ~20% of executor memory; "
                         "spark.executor.pyspark.memory for Python workers), and make partitions smaller (see spill).")
        if lost:
            fixes.append(f"{len(lost)} attempt(s) died with their executor (lost executors: "
                         f"{', '.join(sorted({t.executor for t in lost}))}). Exit code 137 means the process was "
                         "SIGKILLed, usually by the OOM killer or the container limit; check the cluster manager's "
                         "logs for that executor. Every task on it, and its shuffle output, had to be redone.")
        if any(t.reason == "FetchFailed" for t in failed) or s.attempt > 0:
            fixes.append("Shuffle fetch failures re-run parent stages: usually a lost executor (memory, preemption). "
                         "Check the executor-loss reasons; an external shuffle service keeps shuffle files alive.")
        if not fixes:
            fixes.append("Fix the exception above; retries only hide it (each costs a re-run of the task).")
        detail = (f"{len(failed)} failed task attempts out of {s.num_tasks} tasks ({rate:.0%})"
                  + (f"; the stage FAILED: {s.failure.splitlines()[0][:200]}" if s.failure else "")
                  + (f"; this is retry attempt {s.attempt} of the stage" if s.attempt else "") + ". "
                  + " | ".join(lines))
        out.append(_finding(app, s, rule="task-failures", severity=severity,
                            title=f"{len(failed)} failed task attempts" + (" (stage failed)" if s.failure else ""),
                            detail=detail, fixes=fixes,
                            evidence={"failed_attempts": len(failed), "tasks": s.num_tasks, "failure_rate": round(rate, 3),
                                      "errors": dict(Counter(f"{r} {c}".strip() for r, c in groups.elements())),
                                      "stage_failed": bool(s.failure)},
                            source="Dr. Elephant (task failure rate 0.1 / 0.3 / 0.5)"))
    # "another attempt succeeded" kills the LOSER. A killed speculative copy is pure waste; a killed original means
    # the copy won, i.e. speculation rescued a straggler.
    run = sum(t.run_ms for t in s.tasks)
    lost_copies = [t for t in killed if t.speculative]
    won = [t for t in killed if not t.speculative]
    wasted = sum(t.run_ms for t in lost_copies)
    if lost_copies and ratio(wasted, run) >= 0.05:
        out.append(_finding(app, s, rule="speculation-waste", severity="warn",
                            title=f"Speculation wasted {ratio(wasted, run):.0%} of task time",
                            detail=f"{len(lost_copies)} speculative copies ran {human_ms(wasted)} and were killed because "
                                   "the original attempt finished first: the originals were not stragglers.",
                            fixes=["Make speculation pickier: raise spark.speculation.multiplier / spark.speculation.quantile "
                                   "(or spark.speculation.minTaskRuntime); disable it for this job if slow tasks are data "
                                   "skew, since a copy reads the same data."],
                            evidence={"killed_copies": len(lost_copies), "wasted_run_ms": wasted, "run_ms": run}))
    if won:
        out.append(_finding(app, s, rule="speculation-helped", severity="info",
                            title=f"Speculation rescued {len(won)} straggling task{'s' if len(won) > 1 else ''}",
                            detail=f"For {len(won)} partition{'s' if len(won) > 1 else ''} a speculative copy finished first "
                                   f"and the original was killed after {human_ms(sum(t.run_ms for t in won))}. Look at the "
                                   f"hosts of the originals ({', '.join(sorted({t.host for t in won}))}) for slow nodes.",
                            evidence={"rescued": len(won), "original_run_ms": sum(t.run_ms for t in won)}))
    return out


def shuffle_heavy(app: App, s: Stage, th: Thresholds) -> list[Finding]:
    ok = s.successful
    sw = sum(t.shuffle_write_bytes for t in ok)
    if sw < th.shuffle_heavy_min_bytes:
        return []
    inp, sr, run = sum(t.input_bytes for t in ok), sum(t.shuffle_read_bytes for t in ok), sum(t.run_ms for t in ok)
    expansion = sw / (inp + sr) if inp + sr else 0.0    # a generated source (range, parallelize) reads nothing
    write_share = ratio(sum(t.shuffle_write_ns for t in ok) / 1e6, run)
    fetch_share = ratio(sum(t.fetch_wait_ms for t in ok), run)
    if expansion < 1 and write_share < 0.2 and fetch_share < 0.2:
        return []
    return [_finding(app, s, rule="shuffle-heavy", severity="warn", title=f"Heavy shuffle: {human_bytes(sw)} written",
                     detail=f"The stage wrote {human_bytes(sw)} of shuffle data ({expansion:.1f}x what it read); shuffle "
                            f"writes took {write_share:.0%} and fetch waits {fetch_share:.0%} of task time.",
                     fixes=["Filter rows and select columns before the shuffle; pre-aggregate where possible.",
                            "Broadcast the small side of a join instead of shuffling both sides (see broadcast-candidate).",
                            "Avoid a repartition() right before another shuffle (join/groupBy)."],
                     evidence={"shuffle_write_bytes": sw, "expansion": round(expansion, 2),
                               "write_share": round(write_share, 3), "fetch_share": round(fetch_share, 3)})]


STAGE_RULES = (task_skew, skew_join, spill, small_tasks, few_tasks, gc_pressure, scheduler_delay, failures, shuffle_heavy)


# ------------------------------------------------------------------ SQL / app rules

def _exchange_below(node: PlanNode) -> PlanNode | None:
    for n in node.walk():
        if n.name in ("Exchange", "ShuffleExchange") or n.name.startswith("ShuffleExchange"):
            return n
    return None


# Which child may be broadcast (become the build side) for each join type: 0 = left, 1 = right.
_BROADCASTABLE = {"Inner": [0, 1], "Cross": [0, 1], "LeftOuter": [1], "LeftSemi": [1], "LeftAnti": [1],
                  "ExistenceJoin": [1], "RightOuter": [0]}


def broadcast_candidates(app: App, th: Thresholds) -> list[Finding]:
    out = []
    for ex in app.sql.values():
        if ex.plan is None:
            continue
        # The adaptive threshold defaults to the static one (OSS) or to 30 MiB (Databricks), unless broadcasts are off.
        static = parse_bytes(app.conf("spark.sql.autoBroadcastJoinThreshold", ex.id), 10 * MiB)
        adaptive = app.conf("spark.sql.adaptive.autoBroadcastJoinThreshold", ex.id)
        if adaptive is not None:
            thr = parse_bytes(adaptive, static)
        else:
            thr = 30 * MiB if app.databricks and static > 0 else static
        for join in (n for n in ex.plan.walk() if n.name in JOINS and len(n.children) == 2):
            sizes = []
            for child in join.children:
                exch = _exchange_below(child)
                m = exch.metric("data size") if exch else None
                sizes.append(sum(app.sql_metric_values(ex.id, m.accumulator_id)) if m else None)
            if None in sizes:
                continue
            kind = _join_kind(join)
            sides = _BROADCASTABLE.get(kind, [])
            if not sides:
                continue
            small = min(sides, key=lambda i: sizes[i])
            size, other = sizes[small], sizes[1 - small]
            limit = thr if thr > 0 else 10 * MiB
            ev = {"join": join.raw_name, "join_type": kind, "small_side": "left" if small == 0 else "right",
                  "small_side_bytes": size, "other_side_bytes": other, "broadcast_threshold": thr}
            if size <= limit:
                if thr <= 0:
                    detail = (f"The {ev['small_side']} side of this {join.name} is {human_bytes(size)} in memory, but "
                              "broadcast joins are switched off (autoBroadcastJoinThreshold=-1), so both sides were "
                              f"shuffled ({human_bytes(other)} on the other side).")
                    fixes = ["Remove spark.sql.autoBroadcastJoinThreshold=-1, or hint this join: "
                             "`big.join(broadcast(small), ...)` / `/*+ BROADCAST(small) */`."]
                    severity = "critical"
                else:
                    detail = (f"The {ev['small_side']} side is {human_bytes(size)}, under the {human_bytes(thr)} broadcast "
                              "threshold, yet the join stayed a shuffle join.")
                    fixes = ["Check spark.sql.adaptive.enabled (AQE converts joins at runtime from actual sizes); a "
                             "NO_BROADCAST_HASH / MERGE hint; or a side that is mostly empty partitions "
                             "(spark.sql.adaptive.nonEmptyPartitionRatioForBroadcastJoin).",
                             "Hint it: `broadcast(small)`."]
                    severity = "warn"
            elif size <= 10 * limit and other >= 10 * size:
                detail = (f"The {ev['small_side']} side is {human_bytes(size)}, {size / limit:.1f}x the broadcast threshold, "
                          f"and the other side is {human_bytes(other)}: broadcasting it would avoid shuffling the big side.")
                fixes = [f"Hint it (`broadcast(small)`) or raise spark.sql.autoBroadcastJoinThreshold to about "
                         f"{spark_size(size * 1.5)}; keep broadcasts well under the driver's memory "
                         "(sizes here are in-memory row sizes, as AQE measures them)."]
                severity = "warn"
            else:
                continue
            out.append(Finding("broadcast-candidate", severity, f"{join.name} could be a broadcast join", detail, fixes,
                               ev, "AQE broadcast threshold (default 10 MiB; 30 MiB adaptive on Databricks)",
                               sql_id=ex.id, where=f"SQL query {ex.id}" + (f": {ex.description[:80]}" if ex.description else "")))
    return out


def small_files(app: App, th: Thresholds) -> list[Finding]:
    """File scans over many small files, from the scan node's driver-side metrics (files and bytes read)."""
    out = []
    for ex in app.sql.values():
        if ex.plan is None:
            continue
        for scan in ex.plan.walk():
            files_m, size_m = scan.metric("number of files read"), scan.metric("size of files read")
            if not (files_m and size_m) or not scan.name.startswith(("Scan", "FileScan", "BatchScan")):
                continue
            files = sum(app.sql_metric_values(ex.id, files_m.accumulator_id))
            size = sum(app.sql_metric_values(ex.id, size_m.accumulator_id))
            if files < th.small_files_min_count:
                continue
            avg = size / files
            if avg >= 32 * MiB:
                continue
            open_cost = parse_bytes(app.conf("spark.sql.files.openCostInBytes", ex.id), 4 * MiB)
            max_part = parse_bytes(app.conf("spark.sql.files.maxPartitionBytes", ex.id), 128 * MiB)
            fmt = scan.metadata.get("Format") or scan.name.removeprefix("Scan ").strip()
            location = scan.metadata.get("Location", "")
            severity = "critical" if avg < MiB else "warn"
            fixes = ["Compact the input into files of 128 MiB-1 GiB: Delta `OPTIMIZE`, Iceberg `rewrite_data_files`, "
                     "or a one-off rewrite with fewer, larger files.",
                     "Fix it where the files are produced: repartition (or `/*+ REBALANCE */`) before the write"
                     + ("; on Databricks, delta.autoOptimize.optimizeWrite / autoCompact" if app.databricks else "")
                     + "; for streaming sinks, a longer trigger interval."]
            if avg < open_cost:
                fixes.append(f"Meanwhile, each file costs Spark at least spark.sql.files.openCostInBytes "
                             f"({spark_size(open_cost)}) when it packs splits, so tasks are padded with tiny files.")
            out.append(Finding("small-files", severity, f"{fmt} scan read {files} small files (avg {human_bytes(avg)})",
                               f"The scan read {files} files totalling {human_bytes(size)}, {human_bytes(avg)} on average. "
                               "Every file costs a listing entry and an open (and a footer read for Parquet/ORC), so small files make "
                               f"scans slow far beyond their size. Splits are packed up to {spark_size(max_part)} "
                               "(spark.sql.files.maxPartitionBytes)."
                               + (f" Location: {location[:120]}" if location else ""),
                               fixes, {"files": files, "bytes": size, "avg_file_bytes": round(avg),
                                       "open_cost_bytes": open_cost, "max_partition_bytes": max_part, "format": fmt},
                               "project defaults (100 files, 32 MiB average); Spark file-split settings",
                               sql_id=ex.id, where=f"SQL query {ex.id}" + (f": {ex.description[:80]}" if ex.description else "")))
    return out


def executor_memory(app: App, th: Thresholds) -> list[Finding]:
    """Executor heap and container sizing from recorded peaks (needs spark.eventLog.logStageExecutorMetrics=true and
    a metrics poll during the stages; otherwise there is nothing to judge and the rule stays silent)."""
    local = (app.properties.get("spark.master") or "").startswith("local[") or app.properties.get("spark.master") == "local"
    peaks = {e: p for e, p in app.executor_peaks.items() if (e == "driver") == local and p.get("JVMHeapMemory", 0) > 0}
    task_ms = sum(t.run_ms for s in app.stages.values() for t in s.tasks)
    if not peaks or task_ms < th.memory_min_task_ms:
        return []
    if local:
        heap = parse_bytes(app.properties.get("spark.driver.memory"), 1 << 30)
    elif app.profile_memory_mib:
        heap = app.profile_memory_mib * MiB
    else:
        heap = parse_bytes(app.properties.get("spark.executor.memory"), 1 << 30)
    overhead = (app.profile_overhead_mib * MiB if app.profile_overhead_mib else
                parse_bytes(app.properties.get("spark.executor.memoryOverhead"), max(384 * MiB, heap // 10)))
    peak_heap = max(p["JVMHeapMemory"] for p in peaks.values())
    rss = max(p.get("ProcessTreeJVMRSSMemory", 0) + p.get("ProcessTreePythonRSSMemory", 0)
              + p.get("ProcessTreeOtherRSSMemory", 0) for p in peaks.values())
    gc = ratio(sum(t.gc_ms for s in app.stages.values() for t in s.tasks), task_ms)
    use = peak_heap / heap
    # Spark's managed memory (execution + storage) against its unified region: what Spark itself needed. Used heap
    # alone mostly measures uncollected garbage, which grows with whatever heap is available.
    unified = {e: (p.get("OnHeapExecutionMemory", 0) + p.get("OnHeapStorageMemory", 0)) for e, p in peaks.items()}
    regions = {e: app.executors[e].max_onheap for e in peaks if e in app.executors and app.executors[e].max_onheap}
    managed_use = max((unified[e] / regions[e] for e in regions), default=None)
    ev = {"executors": len(peaks), "heap_bytes": heap, "peak_heap_bytes": peak_heap, "heap_use": round(use, 3),
          "peak_managed_use": round(managed_use, 3) if managed_use is not None else None,
          "peak_rss_bytes": rss, "container_bytes": heap + overhead, "gc_share": round(gc, 3)}
    where = "application" + (" (local mode: the driver is the executor)" if local else "")
    out = []
    if rss and rss >= 0.9 * (heap + overhead):
        out.append(Finding("container-memory", "warn", f"Executor processes reached {rss / (heap + overhead):.0%} of the "
                           "container", f"Peak resident memory of an executor process tree was {human_bytes(rss)} against "
                           f"{human_bytes(heap + overhead)} (heap {human_bytes(heap)} + overhead {human_bytes(overhead)}). "
                           "Past the limit, YARN / Kubernetes kill the container (exit code 137) and its tasks are lost.",
                           ["Raise spark.executor.memoryOverhead (off-heap buffers, Python workers, native libraries), "
                            "or spark.executor.pyspark.memory for Python-heavy jobs."], ev, where=where))
    if use >= 0.9 and gc >= 0.10:
        out.append(Finding("executor-memory", "warn", f"Executor heap peaked at {use:.0%} with {gc:.0%} GC",
                           f"Used heap reached {human_bytes(peak_heap)} of {human_bytes(heap)} while tasks spent {gc:.0%} "
                           "of their time in GC: the executors are short of memory.",
                           ["Raise spark.executor.memory, or lower spark.executor.cores so each task gets more memory.",
                            "Make partitions smaller (more shuffle partitions) so less data is held per task."],
                           ev, "Spark UI (GC red at 10%); project", where=where))
    elif (not local and managed_use is not None and managed_use < 0.3 and use < 0.5
          and heap - max(1 << 30, int(peak_heap * 1.5)) >= 1 << 30):        # worth it only if it frees >= 1 GiB
        suggested = max(1 << 30, int(peak_heap * 1.5))
        out.append(Finding("executor-memory", "info",
                           f"Executors used {managed_use:.0%} of Spark's memory: likely over-provisioned",
                           f"Spark's execution + storage memory peaked at {managed_use:.0%} of its unified region, and "
                           f"used heap (including uncollected garbage) at {human_bytes(peak_heap)} of {human_bytes(heap)}, "
                           f"on {len(peaks)} executor(s). Peaks are sampled, so treat this as a size to test, not a guarantee.",
                           [f"Try spark.executor.memory={memory_size(suggested)}, or more cores per executor with the same "
                            "memory; re-check after the change."], ev, "project", where=where))
    return out


def _num(v) -> float | None:
    """Progress JSON numbers; rates can be the string "NaN" (first batch) or missing."""
    try:
        f = float(v)
    except (TypeError, ValueError):
        return None
    return None if math.isnan(f) or math.isinf(f) else f


def _ts_ms(p: dict) -> float | None:
    try:
        return dt.datetime.fromisoformat(str(p.get("timestamp", "")).replace("Z", "+00:00")).timestamp() * 1000
    except ValueError:
        return None


def _backlog(p: dict) -> bool:
    """A source reported more data available (latestOffset, 3.1+) than the batch took (endOffset): a rate limit
    such as maxOffsetsPerTrigger is holding data back."""
    return any(x.get("latestOffset") is not None and x.get("endOffset") is not None
               and x.get("latestOffset") != x.get("endOffset") for x in p.get("sources") or [])


def _falling_behind(q, prog: list[dict]) -> list[Finding]:
    """Falling behind = no idle time between batches AND a growing backlog.

    Spark's own inputRowsPerSecond vs processedRowsPerSecond (what the UI plots) is NOT used: input rate divides by
    the time since the previous trigger, which a quantised source (the rate source emits whole seconds) or trigger
    alignment inflates. On a real log it showed input 1.5M/s vs processed 0.6M/s for a query with 30% idle time and
    no backlog. Instead: busy share = batch duration / time to the next trigger, plus batch-size growth or offsets
    left behind."""
    recent = prog[max(1, len(prog) // 3):]              # past the start-up batches
    busy = []
    for a, b in zip(recent, recent[1:], strict=False):
        ta, tb = _ts_ms(a), _ts_ms(b)
        if ta is not None and tb is not None and tb > ta:
            busy.append((_num(a.get("batchDuration")) or 0) / (tb - ta))
    if len(busy) < 3 or median(busy) < 0.95:
        return []
    rows = [r for r in (sum(x.get("numInputRows", 0) for x in p.get("sources") or []) for p in recent) if r]
    third = max(1, len(rows) // 3)
    growth = (sum(rows[-third:]) / third) / max(sum(rows[:third]) / third, 1) if rows else 1.0
    backlog = sum(1 for p in recent if _backlog(p))
    ev = {"batches": len(prog), "median_busy_share": round(median(busy), 2), "batch_rows_growth": round(growth, 2),
          "batches_with_backlog": backlog, "first_rows": rows[0] if rows else 0, "last_rows": rows[-1] if rows else 0}
    fixes = ["Add parallelism: more cores / executors, and more source partitions (Kafka minPartitions, the rate "
             "source's numPartitions).",
             "Bound each batch (maxOffsetsPerTrigger for Kafka, maxFilesPerTrigger for files) so latency stays "
             "predictable while it catches up.",
             "Cut per-row work (UDFs, repeated hashing, wide projections) or move it after a filter."]
    if growth >= 1.2 or backlog:
        why = (f"batch sizes grew {growth:.1f}x (from {ev['first_rows']:,} to {ev['last_rows']:,} rows)" if growth >= 1.2
               else f"{backlog} batches left data behind (latestOffset ahead of endOffset)")
        return [Finding("stream-falling-behind", "critical" if growth >= 2 else "warn", "Streaming query is falling behind",
                        f"Batches ran back to back (median {median(busy):.0%} of the time between triggers was spent "
                        f"processing) and {why}, so latency keeps growing.", fixes, ev,
                        "project (Spark's input vs processed rate is not reliable: see docs)", where=q.label)]
    return [Finding("stream-saturated", "info", "Streaming query has no headroom",
                    f"Batches ran back to back (median {median(busy):.0%} of the time between triggers was spent "
                    "processing), though the backlog is not growing yet. Any increase in input will make it fall behind.",
                    fixes[:1], ev, "project", where=q.label)]


def _state_growth(q, prog: list[dict]) -> list[Finding]:
    """State that only grows. Without a watermark, Spark keeps every key's state forever."""
    out = []
    watermark = any((p.get("eventTime") or {}).get("watermark") for p in prog)
    n_ops = max((len(p.get("stateOperators") or []) for p in prog), default=0)
    for k in range(n_ops):
        series = [(p.get("stateOperators") or [])[k] if k < len(p.get("stateOperators") or []) else {} for p in prog]
        totals = [_num(x.get("numRowsTotal")) or 0 for x in series]
        mid, last = totals[len(totals) // 2], totals[-1]
        growing = sum(1 for a, b in zip(totals, totals[1:], strict=False) if b >= a) / max(len(totals) - 1, 1)
        # Steady growth: up (or flat) in >= 90% of batches and 1.5x the midpoint by the end. Linear growth from zero
        # ends at ~2x the midpoint; bounded state oscillates around a level.
        if not (last > 0 and last >= 1.5 * max(mid, 1) and growing >= 0.9):
            continue
        mem = _num(series[-1].get("memoryUsedBytes")) or 0
        op = series[-1].get("operatorName") or f"state operator {k}"
        if watermark:
            fixes = ["A watermark is set but state still grows: its delay may be longer than this run, or the grouping "
                     "key has no event-time window, so nothing ever expires."]
        else:
            fixes = ["Add a watermark (`withWatermark('event_time', '10 minutes')`) and group by an event-time window so "
                     "old state can be dropped; for dropDuplicates use dropDuplicatesWithinWatermark (3.5+).",
                     "For arbitrary state, expire it explicitly: timeouts in (flat)MapGroupsWithState, or TTL in "
                     "transformWithState (4.0)."]
        fixes.append("Large state belongs in RocksDB: spark.sql.streaming.stateStore.providerClass="
                     "org.apache.spark.sql.execution.streaming.state.RocksDBStateStoreProvider.")
        out.append(Finding("stream-state-growth", "info" if watermark else "warn",
                           f"Streaming state keeps growing: {int(last):,} rows" + ("" if watermark else ", no watermark"),
                           f"{op} held {int(mid):,} rows halfway through and {int(last):,} at the end "
                           f"({human_bytes(mem)} in memory), growing in {growing:.0%} of batches."
                           + ("" if watermark else " The query has no watermark, so this state is never dropped."),
                           fixes, {"operator": op, "rows_mid": int(mid), "rows_last": int(last), "memory_bytes": int(mem),
                                   "watermark": watermark}, "project", where=q.label))
    return out


def _late_rows_and_commits(q, prog: list[dict]) -> list[Finding]:
    out = []
    dropped = sum(_num(x.get("numRowsDroppedByWatermark")) or 0 for p in prog for x in p.get("stateOperators") or [])
    if dropped:
        out.append(Finding("stream-late-rows", "info", f"{int(dropped):,} late rows dropped by the watermark",
                           f"State operators dropped {int(dropped):,} rows that arrived later than the watermark allows.",
                           ["If those rows matter, lengthen the watermark delay (at the cost of more state)."],
                           {"dropped_rows": int(dropped)}, "project", where=q.label))
    # Checkpoint commits dominating batch time: slow checkpoint storage (an object store). Local-disk commits take
    # tens of ms, so the 1 s mean keeps tiny batches with cheap commits from tripping this.
    trig = sum(_num((p.get("durationMs") or {}).get("triggerExecution")) or 0 for p in prog)
    commits = [(_num((p.get("durationMs") or {}).get("walCommit")) or 0) + (_num((p.get("durationMs") or {}).get("commit")) or 0)
               for p in prog]
    mean = sum(commits) / len(commits)
    if trig and sum(commits) / trig >= 0.3 and mean >= 1000:
        out.append(Finding("stream-slow-commits", "warn", f"Checkpoint commits take {sum(commits) / trig:.0%} of batch time",
                           f"Writing the offset log and commit log averaged {human_ms(mean)} per batch. On object stores "
                           "each commit is several small writes.",
                           ["Put the checkpoint on faster storage, or trigger less often so the fixed cost is shared by "
                            "more data.",
                            "With RocksDB state: spark.sql.streaming.stateStore.rocksdb.changelogCheckpointing.enabled=true."],
                           {"commit_share": round(sum(commits) / trig, 3), "mean_commit_ms": round(mean)}, "project",
                           where=q.label))
    if q.exception:
        out.append(Finding("stream-failed", "critical", "Streaming query terminated with an error", error_line(q.exception),
                           ["Fix the error; the query restarts from its checkpoint."], {"error": q.exception[:500]},
                           where=q.label))
    return out


def streaming(app: App, th: Thresholds) -> list[Finding]:
    out = []
    for q in app.streams.values():
        prog = sorted(q.progress, key=lambda p: p.get("batchId", 0))
        if q.exception or len(prog) >= th.stream_min_batches:
            if len(prog) >= th.stream_min_batches:
                out += _falling_behind(q, prog) + _state_growth(q, prog)
            out += _late_rows_and_commits(q, prog) if prog else []
    return out


def utilisation(app: App, th: Thresholds) -> list[Finding]:
    spans = sorted((j.submitted, j.completed) for j in app.jobs.values() if j.completed)
    if not spans or not app.total_cores:
        return []
    merged: list[list[int]] = []
    for a, b in spans:
        if merged and a <= merged[-1][1]:
            merged[-1][1] = max(merged[-1][1], b)
        else:
            merged.append([a, b])
    busy_wall = sum(b - a for a, b in merged)
    if busy_wall < th.utilisation_min_app_ms:
        return []
    task_ms = sum(t.duration_ms for s in app.stages.values() for t in s.tasks)
    capacity = sum(app.cores_at((a + b) // 2) * (b - a) for a, b in merged)
    util = ratio(task_ms, capacity)
    if util >= 0.5:
        return []
    severity = "critical" if util < 0.3 else "warn"
    return [Finding("low-utilisation", severity, f"Executors were {util:.0%} busy while jobs ran",
                    f"Tasks used {util:.0%} of the {app.total_cores} cores' capacity during the {human_ms(busy_wall)} that "
                    "jobs were running. Idle cores usually mean too few tasks per stage, a few long tasks (skew) "
                    "holding a stage open, or an over-sized cluster.",
                    ["Fix the skew / too-few-tasks findings first; then size the cluster down or enable "
                     "spark.dynamicAllocation.enabled=true."],
                    {"utilisation": round(util, 3), "cores": app.total_cores, "busy_wall_ms": busy_wall}, where="application")]


def diagnose(app: App, th: Thresholds = DEFAULT) -> list[Finding]:
    out: list[Finding] = []
    for s in sorted(app.stages.values(), key=lambda s: s.key):
        for rule in STAGE_RULES:
            out += rule(app, s, th)
    out += broadcast_candidates(app, th)
    out += small_files(app, th)
    out += executor_memory(app, th)
    out += streaming(app, th)
    out += utilisation(app, th)
    order = {k: i for i, k in enumerate(reversed(SEVERITIES))}
    return sorted(out, key=lambda f: (order[f.severity], f.stage or (1 << 30, 0), f.rule))


__all__ = ["Thresholds", "DEFAULT", "DEV", "PROFILES", "Finding", "diagnose", "parse_bytes"]
