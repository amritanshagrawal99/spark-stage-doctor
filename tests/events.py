"""Build minimal Spark listener events, shaped exactly like JsonProtocol's output, for rule unit tests.
(The scenario tests use real logs from Spark 3.5 / 4.0; these cover cases that are impractical to generate.)"""
from __future__ import annotations

import itertools

_ids = itertools.count(1)
T0 = 1_700_000_000_000


def log_start(version="3.5.1"):
    return {"Event": "SparkListenerLogStart", "Spark Version": version}


def env(**props):
    return {"Event": "SparkListenerEnvironmentUpdate", "Spark Properties": props, "JVM Information": {},
            "System Properties": {}, "Classpath Entries": {}}


def executor(eid="1", cores=4, ts=T0, host="h1"):
    return {"Event": "SparkListenerExecutorAdded", "Timestamp": ts, "Executor ID": eid,
            "Executor Info": {"Host": host, "Total Cores": cores, "Log Urls": {}}}


def job(job_id, stage_ids, sql_id=None, ts=T0, end=None, desc=None):
    props = {}
    if sql_id is not None:
        props["spark.sql.execution.id"] = str(sql_id)
    if desc:
        props["spark.job.description"] = desc
    out = [{"Event": "SparkListenerJobStart", "Job ID": job_id, "Submission Time": ts, "Stage Infos": [],
            "Stage IDs": stage_ids, "Properties": props}]
    if end is not None:
        out.append({"Event": "SparkListenerJobEnd", "Job ID": job_id, "Completion Time": end,
                    "Job Result": {"Result": "JobSucceeded"}})
    return out


def stage(stage_id, n_tasks, submitted=T0, completed=None, attempt=0, failure=None, rdds=(), sql_accs=()):
    info = {"Stage ID": stage_id, "Stage Attempt ID": attempt, "Stage Name": f"stage {stage_id}",
            "Number of Tasks": n_tasks, "RDD Info": list(rdds), "Parent IDs": [], "Details": "",
            "Submission Time": submitted, "Accumulables": [{"ID": a, "Name": "m", "Value": "0", "Internal": True,
                                                            "Count Failed Values": True, "Metadata": "sql"}
                                                           for a in sql_accs]}
    out = [{"Event": "SparkListenerStageSubmitted", "Stage Info": dict(info), "Properties": {}}]
    done = dict(info)
    done["Completion Time"] = completed if completed is not None else submitted + 60_000
    if failure:
        done["Failure Reason"] = failure
    out.append({"Event": "SparkListenerStageCompleted", "Stage Info": done})
    return out


def task(stage_id, index, run_ms, attempt=0, stage_attempt=0, launch=T0, executor="1", host="h1", reason="Success",
         speculative=False, gc_ms=0, sr_bytes=0, in_bytes=0, sw_bytes=0, disk_spill=0, mem_spill=0, deser_ms=1,
         ser_ms=0, extra_ms=2, metrics=True, cpu_ns=None, sql=None, **reason_fields):
    tid = next(_ids)
    finish = launch + run_ms + deser_ms + ser_ms + extra_ms
    ev = {"Event": "SparkListenerTaskEnd", "Stage ID": stage_id, "Stage Attempt ID": stage_attempt,
          "Task Type": "ResultTask", "Task End Reason": {"Reason": reason, **reason_fields},
          "Task Info": {"Task ID": tid, "Index": index, "Attempt": attempt, "Partition ID": index, "Launch Time": launch,
                        "Executor ID": executor, "Host": host, "Locality": "PROCESS_LOCAL", "Speculative": speculative,
                        "Getting Result Time": 0, "Finish Time": finish, "Failed": reason != "Success",
                        "Killed": reason == "TaskKilled",
                        "Accumulables": [{"ID": a, "Name": "m", "Update": str(v), "Value": str(v), "Internal": True,
                                          "Count Failed Values": True, "Metadata": "sql"} for a, v in (sql or {}).items()]}}
    if metrics:
        ev["Task Metrics"] = {
            "Executor Deserialize Time": deser_ms, "Executor Deserialize CPU Time": deser_ms * 1_000_000,
            "Executor Run Time": run_ms, "Executor CPU Time": run_ms * 1_000_000 if cpu_ns is None else cpu_ns,
            "Peak Execution Memory": 0, "Result Size": 1000, "JVM GC Time": gc_ms, "Result Serialization Time": ser_ms,
            "Memory Bytes Spilled": mem_spill, "Disk Bytes Spilled": disk_spill,
            "Shuffle Read Metrics": {"Remote Blocks Fetched": 0, "Local Blocks Fetched": 1, "Fetch Wait Time": 0,
                                     "Remote Bytes Read": sr_bytes // 2, "Remote Bytes Read To Disk": 0,
                                     "Local Bytes Read": sr_bytes - sr_bytes // 2, "Total Records Read": 10},
            "Shuffle Write Metrics": {"Shuffle Bytes Written": sw_bytes, "Shuffle Write Time": 0,
                                      "Shuffle Records Written": 0},
            "Input Metrics": {"Bytes Read": in_bytes, "Records Read": 0},
            "Output Metrics": {"Bytes Written": 0, "Records Written": 0}, "Updated Blocks": []}
    return ev


def plan(name, children=(), metrics=(), simple=None):
    return {"nodeName": name, "simpleString": simple or name, "children": list(children), "metadata": {},
            "metrics": [{"name": n, "accumulatorId": a, "metricType": t} for n, a, t in metrics]}


def sql_start(eid, plan_info, desc="query", modified=None, ts=T0):
    return {"Event": "org.apache.spark.sql.execution.ui.SparkListenerSQLExecutionStart", "executionId": eid,
            "description": desc, "details": "", "physicalPlanDescription": "", "sparkPlanInfo": plan_info, "time": ts,
            "modifiedConfigs": modified or {}}


def sql_update(eid, plan_info):
    return {"Event": "org.apache.spark.sql.execution.ui.SparkListenerSQLAdaptiveExecutionUpdate", "executionId": eid,
            "physicalPlanDescription": "", "sparkPlanInfo": plan_info}


def driver_accum(eid, *pairs):
    return {"Event": "org.apache.spark.sql.execution.ui.SparkListenerDriverAccumUpdates", "executionId": eid,
            "accumUpdates": [list(p) for p in pairs]}


def app(*parts, version="3.5.1", props=None, cores=4):
    """A complete event list: header, one executor, then `parts` (events or lists of events)."""
    out = [log_start(version), env(**(props or {})), executor(cores=cores),
           {"Event": "SparkListenerApplicationStart", "App Name": "t", "App ID": "app-1", "Timestamp": T0, "User": "u"}]
    for p in parts:
        out += p if isinstance(p, list) else [p]
    return out
