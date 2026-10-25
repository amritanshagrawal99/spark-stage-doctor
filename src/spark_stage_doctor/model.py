"""Build an application model from Spark listener events.

Rules that decide correctness (all checked against Spark's JsonProtocol / SQLAppStatusListener sources, 3.3-4.0):

* Stages are keyed by (Stage ID, Stage Attempt ID): a retried stage is a new attempt, never merged.
* Task metrics come only from TaskEnd "Task Metrics", which can be missing (lost executor, fetch failure).
  Never from Accumulables: Spark 4.0 writes them empty on start events, and can drop internal ones entirely.
* Units: "Executor CPU Time", "Executor Deserialize CPU Time" and "Shuffle Write Time" are nanoseconds. Every other
  duration is milliseconds, and timestamps are epoch milliseconds.
* SQL operators are tied to stages ONLY by accumulator id (plan metric accumulatorId == Accumulables ID). Under AQE
  each execution has several plan snapshots with new ids, so ids from every snapshot are kept; the last one is the
  operator tree.
* Non-core events are named by their fully-qualified class ("org.apache.spark.sql.execution.ui.Spark..."); they
  are matched on the last dotted component. Unknown events are counted and skipped.
"""
from __future__ import annotations

import re
from collections import Counter
from dataclasses import dataclass, field

# ------------------------------------------------------------------ plan


@dataclass
class Metric:
    name: str
    accumulator_id: int
    metric_type: str          # sum | size | timing (ms) | nsTiming (ns) | average (x10) | v2Custom_*


@dataclass
class PlanNode:
    name: str                 # normalised: "Photon" prefix stripped, Photon aliases mapped, "(skew=true)" removed
    raw_name: str
    simple: str
    metadata: dict
    metrics: list[Metric]
    children: list[PlanNode]

    @property
    def skew_split(self) -> bool:
        return "(skew=true)" in self.raw_name

    def walk(self):
        yield self
        for c in self.children:
            yield from c.walk()

    def metric(self, name: str) -> Metric | None:
        return next((m for m in self.metrics if m.name == name), None)


_PHOTON = {"GroupingAgg": "HashAggregate", "ShuffleExchangeSink": "Exchange", "ShuffleExchangeSource": "Exchange"}


def _node(d: dict) -> PlanNode:
    raw = d.get("nodeName", "")
    name = raw.split("(")[0].strip()
    if name.startswith("Photon") and len(name) > 6:
        name = _PHOTON.get(name[6:], name[6:])
    metrics = [Metric(m.get("name", ""), int(m["accumulatorId"]), m.get("metricType", "sum"))
               for m in d.get("metrics", []) if "accumulatorId" in m]
    return PlanNode(name, raw, d.get("simpleString", ""), d.get("metadata") or {}, metrics,
                    [_node(c) for c in d.get("children", [])])


@dataclass
class SqlExecution:
    id: int
    root_id: int | None
    description: str
    start: int
    end: int | None = None
    error: str | None = None
    modified_configs: dict = field(default_factory=dict)
    plans: list[PlanNode] = field(default_factory=list)          # start + every AQE update, in order
    acc: dict[int, tuple[PlanNode, Metric]] = field(default_factory=dict)
    driver_updates: dict[int, list[int]] = field(default_factory=dict)   # AQE posts several values per id

    @property
    def plan(self) -> PlanNode | None:
        return self.plans[-1] if self.plans else None

    @property
    def initial_plan(self) -> PlanNode | None:
        return self.plans[0] if self.plans else None

    def add_plan(self, info: dict) -> None:
        node = _node(info)
        self.plans.append(node)
        for n in node.walk():
            for m in n.metrics:
                self.acc[m.accumulator_id] = (n, m)   # later snapshots win: they describe the final operator


# ------------------------------------------------------------------ tasks and stages


@dataclass
class Task:
    task_id: int
    stage: tuple[int, int]
    index: int
    attempt: int
    executor: str
    host: str
    locality: str
    speculative: bool
    launch: int
    finish: int
    getting_result: int
    reason: str
    kill_reason: str | None = None
    error_class: str | None = None
    error: str | None = None
    has_metrics: bool = False
    run_ms: int = 0
    cpu_ns: int = 0
    deser_ms: int = 0
    ser_ms: int = 0
    gc_ms: int = 0
    result_bytes: int = 0
    mem_spill: int = 0
    disk_spill: int = 0
    peak_mem: int = 0
    input_bytes: int = 0
    input_records: int = 0
    output_bytes: int = 0
    shuffle_read_bytes: int = 0
    shuffle_read_records: int = 0
    fetch_wait_ms: int = 0
    shuffle_write_bytes: int = 0
    shuffle_write_ns: int = 0
    shuffle_write_records: int = 0
    sql_updates: dict[int, int] = field(default_factory=dict)

    @property
    def success(self) -> bool:
        return self.reason == "Success"

    @property
    def duration_ms(self) -> int:
        return max(0, self.finish - self.launch)

    @property
    def scheduler_delay_ms(self) -> int:
        """The Spark UI's definition: what's left of the task's wall time after the executor-side phases."""
        fetching_result = self.finish - self.getting_result if self.getting_result > 0 else 0
        return max(0, self.duration_ms - self.run_ms - self.deser_ms - self.ser_ms - fetching_result)


FAILED_REASONS = {"ExceptionFailure", "FetchFailed", "ExecutorLostFailure", "TaskResultLost", "Resubmitted",
                  "UnknownReason", "TaskCommitDenied"}


@dataclass
class Stage:
    stage_id: int
    attempt: int
    name: str
    num_tasks: int
    submitted: int | None = None
    completed: int | None = None
    failure: str | None = None
    parent_ids: list[int] = field(default_factory=list)
    job_id: int | None = None
    sql_id: int | None = None
    description: str | None = None
    rdds: list[dict] = field(default_factory=list)
    sql_acc_ids: set[int] = field(default_factory=set)
    tasks: list[Task] = field(default_factory=list)

    @property
    def key(self) -> tuple[int, int]:
        return (self.stage_id, self.attempt)

    @property
    def label(self) -> str:
        return f"stage {self.stage_id}" + (f" (attempt {self.attempt})" if self.attempt else "")

    @property
    def wall_ms(self) -> int:
        if self.submitted is None or self.completed is None:
            return 0
        return max(0, self.completed - self.submitted)

    @property
    def successful(self) -> list[Task]:
        """One successful task per partition index (a speculative copy can also succeed; keep the first)."""
        seen: dict[int, Task] = {}
        for t in sorted(self.tasks, key=lambda t: t.finish):
            if t.success and t.index not in seen:
                seen[t.index] = t
        return list(seen.values())


@dataclass
class Job:
    job_id: int
    submitted: int
    stage_ids: list[int]
    sql_id: int | None
    root_sql_id: int | None
    description: str | None
    completed: int | None = None
    succeeded: bool | None = None


@dataclass
class Executor:
    executor_id: str
    host: str
    cores: int
    added: int
    removed: int | None = None
    removed_reason: str | None = None


@dataclass
class App:
    spark_version: str = ""
    name: str = ""
    app_id: str = ""
    start: int | None = None
    end: int | None = None
    properties: dict[str, str] = field(default_factory=dict)
    executors: dict[str, Executor] = field(default_factory=dict)
    jobs: dict[int, Job] = field(default_factory=dict)
    stages: dict[tuple[int, int], Stage] = field(default_factory=dict)
    sql: dict[int, SqlExecution] = field(default_factory=dict)
    unknown_events: Counter = field(default_factory=Counter)
    notes: list[str] = field(default_factory=list)

    @property
    def version(self) -> tuple[int, int]:
        m = re.match(r"(\d+)\.(\d+)", self.spark_version)   # "3.3.0-SNAPSHOT", "3.1.1.119"
        return (int(m.group(1)), int(m.group(2))) if m else (0, 0)

    @property
    def databricks(self) -> bool:
        return any(k.startswith("spark.databricks.") for k in self.properties)

    def conf(self, key: str, sql_id: int | None = None) -> str | None:
        """A config value as the query saw it: per-query override (modifiedConfigs) first, then Spark Properties."""
        if sql_id is not None and sql_id in self.sql and key in self.sql[sql_id].modified_configs:
            return self.sql[sql_id].modified_configs[key]
        return self.properties.get(key)

    def cores_at(self, t: int) -> int:
        return sum(e.cores for e in self.executors.values()
                   if e.added <= t and (e.removed is None or e.removed > t)) or 0

    def stage_cores(self, s: Stage) -> int:
        if s.submitted is None:
            return self.total_cores
        mid = s.submitted + s.wall_ms // 2
        return self.cores_at(mid) or self.total_cores

    @property
    def total_cores(self) -> int:
        return max((self.cores_at(e.added) for e in self.executors.values()), default=0)

    def stage_operators(self, s: Stage) -> list[PlanNode]:
        """SQL operators whose metrics this stage updated (by accumulator id)."""
        ex = self.sql.get(s.sql_id) if s.sql_id is not None else None
        if ex is None:
            return []
        seen, out = set(), []
        for a in sorted(s.sql_acc_ids):
            hit = ex.acc.get(a)
            if hit and id(hit[0]) not in seen:
                seen.add(id(hit[0]))
                out.append(hit[0])
        return out

    def sql_metric_values(self, sql_id: int, accumulator_id: int) -> list[int]:
        """Per-task values of one SQL metric: for each (stage attempt, partition), the update of the successful
        attempt (Spark's own rule), with -1 ("not set") dropped; plus any driver-side update."""
        out = []
        for s in self.stages.values():
            if s.sql_id != sql_id:
                continue
            for t in s.successful:
                v = t.sql_updates.get(accumulator_id)
                if v is not None and v >= 0:
                    out.append(v)
        ex = self.sql.get(sql_id)
        if ex:
            out += [v for v in ex.driver_updates.get(accumulator_id, []) if v >= 0]
        return out


# ------------------------------------------------------------------ building


def _int(v, default=0) -> int:
    try:
        return int(v)
    except (TypeError, ValueError):
        return default


def _task(ev: dict) -> Task:
    info, reason = ev.get("Task Info", {}), ev.get("Task End Reason", {})
    t = Task(task_id=_int(info.get("Task ID")), stage=(_int(ev.get("Stage ID")), _int(ev.get("Stage Attempt ID"))),
             index=_int(info.get("Index")), attempt=_int(info.get("Attempt")), executor=str(info.get("Executor ID", "")),
             host=str(info.get("Host", "")), locality=str(info.get("Locality", "")),
             speculative=bool(info.get("Speculative", False)), launch=_int(info.get("Launch Time")),
             finish=_int(info.get("Finish Time")), getting_result=_int(info.get("Getting Result Time")),
             reason=reason.get("Reason", "UnknownReason"), kill_reason=reason.get("Kill Reason"),
             error_class=reason.get("Class Name"), error=reason.get("Description") or reason.get("Loss Reason")
             or reason.get("Message"))
    m = ev.get("Task Metrics")
    if m:
        sr, sw = m.get("Shuffle Read Metrics") or {}, m.get("Shuffle Write Metrics") or {}
        t.has_metrics = True
        t.run_ms, t.cpu_ns = _int(m.get("Executor Run Time")), _int(m.get("Executor CPU Time"))
        t.deser_ms, t.ser_ms = _int(m.get("Executor Deserialize Time")), _int(m.get("Result Serialization Time"))
        t.gc_ms, t.result_bytes = _int(m.get("JVM GC Time")), _int(m.get("Result Size"))
        t.mem_spill, t.disk_spill = _int(m.get("Memory Bytes Spilled")), _int(m.get("Disk Bytes Spilled"))
        t.peak_mem = _int(m.get("Peak Execution Memory"))
        t.input_bytes = _int((m.get("Input Metrics") or {}).get("Bytes Read"))
        t.input_records = _int((m.get("Input Metrics") or {}).get("Records Read"))
        t.output_bytes = _int((m.get("Output Metrics") or {}).get("Bytes Written"))
        t.shuffle_read_bytes = _int(sr.get("Local Bytes Read")) + _int(sr.get("Remote Bytes Read"))
        t.shuffle_read_records, t.fetch_wait_ms = _int(sr.get("Total Records Read")), _int(sr.get("Fetch Wait Time"))
        t.shuffle_write_bytes, t.shuffle_write_ns = _int(sw.get("Shuffle Bytes Written")), _int(sw.get("Shuffle Write Time"))
        t.shuffle_write_records = _int(sw.get("Shuffle Records Written"))
    for a in info.get("Accumulables") or []:
        if a.get("Metadata") == "sql" and "Update" in a:   # SQL metric updates are JSON strings
            t.sql_updates[_int(a.get("ID"), -1)] = _int(a["Update"], -1)
    return t


def _stage(info: dict) -> Stage:
    return Stage(_int(info.get("Stage ID")), _int(info.get("Stage Attempt ID")), info.get("Stage Name", ""),
                 _int(info.get("Number of Tasks")), parent_ids=list(info.get("Parent IDs") or []))


def build(events: list[dict]) -> App:
    app = App()
    stage_job: dict[int, int] = {}
    for ev in events:
        name = ev["Event"].rsplit(".", 1)[-1].rsplit("$", 1)[-1]
        if name == "SparkListenerLogStart":
            app.spark_version = ev.get("Spark Version", "")
        elif name == "SparkListenerApplicationStart":
            app.name, app.app_id, app.start = ev.get("App Name", ""), ev.get("App ID", ""), ev.get("Timestamp")
        elif name == "SparkListenerApplicationEnd":
            app.end = ev.get("Timestamp")
        elif name == "SparkListenerEnvironmentUpdate":
            app.properties = {str(k): str(v) for k, v in (ev.get("Spark Properties") or {}).items()} \
                if isinstance(ev.get("Spark Properties"), dict) else {str(k): str(v) for k, v in ev.get("Spark Properties") or []}
        elif name == "SparkListenerExecutorAdded":
            info = ev.get("Executor Info", {})
            app.executors[str(ev.get("Executor ID"))] = Executor(str(ev.get("Executor ID")), info.get("Host", ""),
                                                                 _int(info.get("Total Cores")), _int(ev.get("Timestamp")))
        elif name == "SparkListenerExecutorRemoved":
            e = app.executors.get(str(ev.get("Executor ID")))
            if e:
                e.removed, e.removed_reason = _int(ev.get("Timestamp")), ev.get("Removed Reason")
        elif name == "SparkListenerJobStart":
            props = ev.get("Properties") or {}
            sid = props.get("spark.sql.execution.id")
            root = props.get("spark.sql.execution.root.id")
            job = Job(_int(ev.get("Job ID")), _int(ev.get("Submission Time")), list(ev.get("Stage IDs") or []),
                      _int(sid, None) if sid is not None else None, _int(root, None) if root is not None else None,
                      props.get("spark.job.description"))
            app.jobs[job.job_id] = job
            for s in job.stage_ids:
                stage_job.setdefault(s, job.job_id)
        elif name == "SparkListenerJobEnd":
            job = app.jobs.get(_int(ev.get("Job ID")))
            if job:
                job.completed = _int(ev.get("Completion Time"))
                job.succeeded = (ev.get("Job Result") or {}).get("Result") == "JobSucceeded"
        elif name in ("SparkListenerStageSubmitted", "SparkListenerStageCompleted"):
            info = ev.get("Stage Info", {})
            key = (_int(info.get("Stage ID")), _int(info.get("Stage Attempt ID")))
            s = app.stages.setdefault(key, _stage(info))
            s.num_tasks = _int(info.get("Number of Tasks"), s.num_tasks)
            s.submitted = info.get("Submission Time", s.submitted)
            if name == "SparkListenerStageCompleted":
                s.completed = info.get("Completion Time")
                s.failure = info.get("Failure Reason")
                s.rdds = info.get("RDD Info") or []
                s.sql_acc_ids |= {_int(a.get("ID"), -1) for a in info.get("Accumulables") or [] if a.get("Metadata") == "sql"}
        elif name == "SparkListenerTaskEnd":
            t = _task(ev)
            s = app.stages.get(t.stage)
            if s is None:
                s = app.stages[t.stage] = Stage(t.stage[0], t.stage[1], "", 0)
            s.tasks.append(t)
            s.sql_acc_ids |= set(t.sql_updates)
        elif name == "SparkListenerSQLExecutionStart":
            eid = _int(ev.get("executionId"))
            ex = app.sql[eid] = SqlExecution(eid, ev.get("rootExecutionId"), ev.get("description", ""),
                                             _int(ev.get("time")), modified_configs=ev.get("modifiedConfigs") or {})
            if ev.get("sparkPlanInfo"):
                ex.add_plan(ev["sparkPlanInfo"])
        elif name == "SparkListenerSQLAdaptiveExecutionUpdate":
            ex = app.sql.get(_int(ev.get("executionId")))
            if ex and ev.get("sparkPlanInfo"):
                ex.add_plan(ev["sparkPlanInfo"])
        elif name == "SparkListenerSQLAdaptiveSQLMetricUpdates":
            ex = app.sql.get(_int(ev.get("executionId")))
            if ex and ex.plan:
                for m in ev.get("sqlPlanMetrics") or []:
                    ex.acc.setdefault(int(m["accumulatorId"]), (ex.plan, Metric(m.get("name", ""), int(m["accumulatorId"]),
                                                                               m.get("metricType", "sum"))))
        elif name == "SparkListenerDriverAccumUpdates":
            ex = app.sql.get(_int(ev.get("executionId")))
            if ex:
                for pair in ev.get("accumUpdates") or []:
                    if isinstance(pair, (list, tuple)) and len(pair) == 2:
                        ex.driver_updates.setdefault(_int(pair[0], -1), []).append(_int(pair[1], -1))
        elif name == "SparkListenerSQLExecutionEnd":
            ex = app.sql.get(_int(ev.get("executionId")))
            if ex:
                ex.end, ex.error = _int(ev.get("time")), ev.get("errorMessage") or None
        elif name in _IGNORED:
            pass
        else:
            app.unknown_events[ev["Event"]] += 1
    for s in app.stages.values():
        s.job_id = stage_job.get(s.stage_id)
        job = app.jobs.get(s.job_id) if s.job_id is not None else None
        if job:
            s.sql_id = job.sql_id
            s.description = job.description
    return app


_IGNORED = {"SparkListenerTaskStart", "SparkListenerTaskGettingResult", "SparkListenerBlockManagerAdded",
            "SparkListenerBlockManagerRemoved", "SparkListenerUnpersistRDD", "SparkListenerExecutorMetricsUpdate",
            "SparkListenerStageExecutorMetrics", "SparkListenerBlockUpdated", "SparkListenerResourceProfileAdded",
            "SparkListenerSpeculativeTaskSubmitted", "SparkListenerExecutorExcluded", "SparkListenerExecutorUnexcluded",
            "SparkListenerNodeExcluded", "SparkListenerNodeUnexcluded", "SparkListenerExecutorBlacklisted",
            "SparkListenerExecutorUnblacklisted", "SparkListenerNodeBlacklisted", "SparkListenerNodeUnblacklisted",
            "SparkListenerExecutorExcludedForStage", "SparkListenerNodeExcludedForStage", "SparkListenerUnschedulableTaskSetAdded",
            "SparkListenerUnschedulableTaskSetRemoved", "SparkListenerMiscellaneousProcessAdded", "QueryStartedEvent",
            "QueryProgressEvent", "QueryTerminatedEvent", "QueryIdleEvent"}


def plan_json(node: PlanNode) -> dict:
    """Compact JSON view of a plan (for reports and debugging)."""
    return {"name": node.raw_name, "children": [plan_json(c) for c in node.children]}


__all__ = ["App", "Stage", "Task", "Job", "Executor", "SqlExecution", "PlanNode", "Metric", "build", "FAILED_REASONS",
           "plan_json"]
