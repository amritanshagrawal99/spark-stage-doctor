"""Rules on synthetic events (shaped like Spark's JsonProtocol output) for cases real fixtures can't produce."""
import events as E
import pytest

from spark_stage_doctor import model, rules
from spark_stage_doctor.rules import DEFAULT, parse_bytes, spark_size

MiB = 1 << 20


def diagnose(*parts, th=DEFAULT, **kw):
    return rules.diagnose(model.build(E.app(*parts, **kw)), th)


def by_rule(findings, rule):
    return [f for f in findings if f.rule == rule]


def tasks(stage_id, runs, **kw):
    return [E.task(stage_id, i, r, **kw) for i, r in enumerate(runs)]


# ------------------------------------------------------------------ parsing helpers

@pytest.mark.parametrize("text, expected", [("10485760", 10485760), ("10m", 10 * MiB), ("10MB", 10 * MiB),
                                            ("64k", 64 << 10), ("1g", 1 << 30), ("-1", -1), ("junk", 7), (None, 7)])
def test_parse_bytes(text, expected):
    assert parse_bytes(text, 7) == expected


def test_spark_size():
    assert (spark_size(64 * MiB), spark_size(3 << 30), spark_size(1536), spark_size(10)) == ("64m", "3g", "1k", "10")


@pytest.mark.parametrize("v, expected", [("3.3.0-SNAPSHOT", (3, 3)), ("3.1.1.119", (3, 1)), ("4.0.4", (4, 0)), ("", (0, 0))])
def test_version(v, expected):
    assert model.App(spark_version=v).version == expected


# ------------------------------------------------------------------ model

def test_units_and_scheduler_delay():
    t = E.task(0, 0, 1000, deser_ms=50, ser_ms=20, extra_ms=130, cpu_ns=400_000_000)
    app = model.build(E.app(E.stage(0, 1), t))
    task = app.stages[(0, 0)].tasks[0]
    assert task.cpu_ns / 1e6 == 400 and task.scheduler_delay_ms == 130 and task.duration_ms == 1200


def test_successful_keeps_one_attempt_per_partition():
    evs = [E.task(0, 0, 100), E.task(0, 0, 100, attempt=1, speculative=True, launch=E.T0 + 5),
           E.task(0, 1, 100, reason="ExceptionFailure", **{"Class Name": "x.Boom", "Description": "boom"}),
           E.task(0, 1, 100, attempt=1)]
    s = model.build(E.app(E.stage(0, 2), evs)).stages[(0, 0)]
    assert len(s.tasks) == 4 and sorted(t.index for t in s.successful) == [0, 1]


def test_stage_attempts_stay_separate_and_task_without_metrics_is_kept():
    evs = [*E.stage(0, 2), E.task(0, 0, 10, reason="FetchFailed", metrics=False, Message="fetch"),
           *E.stage(0, 2, attempt=1), E.task(0, 0, 10, stage_attempt=1), E.task(0, 1, 10, stage_attempt=1)]
    app = model.build(E.app(evs))
    assert set(app.stages) == {(0, 0), (0, 1)}
    assert not app.stages[(0, 0)].tasks[0].has_metrics


def test_sql_metric_ignores_failed_attempts_and_unset_values():
    evs = [*E.job(0, [0], sql_id=1), *E.stage(0, 2),
           E.task(0, 0, 10, sql={7: 999}, reason="ExceptionFailure", **{"Class Name": "x", "Description": "y"}),
           E.task(0, 0, 10, attempt=1, sql={7: 100}), E.task(0, 1, 10, sql={7: -1})]
    app = model.build(E.app(E.sql_start(1, E.plan("Exchange", metrics=[("data size", 7, "size")])), evs))
    assert app.sql_metric_values(1, 7) == [100]


def test_aqe_snapshots_union_accumulators_and_photon_names_are_normalised():
    first = E.plan("PhotonShuffleExchangeSink", metrics=[("data size", 1, "size")])
    final = E.plan("SortMergeJoin(skew=true)", [E.plan("PhotonGroupingAgg", metrics=[("rows", 2, "sum")])])
    app = model.build(E.app(E.sql_start(3, first), E.sql_update(3, final)))
    ex = app.sql[3]
    assert ex.acc[1][0].name == "Exchange" and ex.acc[2][0].name == "HashAggregate"
    assert ex.plan.name == "SortMergeJoin" and ex.plan.skew_split and not ex.initial_plan.skew_split


def test_modified_configs_override_spark_properties():
    app = model.build(E.app(E.sql_start(1, E.plan("X"), modified={"spark.sql.shuffle.partitions": "50"}),
                            props={"spark.sql.shuffle.partitions": "200"}))
    assert app.conf("spark.sql.shuffle.partitions", 1) == "50" and app.conf("spark.sql.shuffle.partitions") == "200"


# ------------------------------------------------------------------ rules

def test_data_skew_vs_straggler():
    runs = [5_000] * 11 + [60_000]
    skew = diagnose(E.stage(0, 12), [E.task(0, i, r, sr_bytes=(900 if r > 5000 else 10) * MiB) for i, r in enumerate(runs)])
    strag = diagnose(E.stage(0, 12), [E.task(0, i, r, sr_bytes=200 * MiB, host="slow" if r > 5000 else "h1")
                                      for i, r in enumerate(runs)])
    [a], [b] = by_rule(skew, "task-skew"), by_rule(strag, "task-skew")
    assert a.title.startswith("Data skew") and a.severity == "critical"
    assert b.title.startswith("Straggler") and b.evidence["slowest_host"] == "slow"
    assert any("spark.speculation=true" in x and "1.5x" in x for x in b.fixes)


def test_speculation_defaults_change_in_spark_4():
    runs = [5_000] * 11 + [60_000]
    f = by_rule(diagnose(E.stage(0, 12), tasks(0, runs, sr_bytes=200 * MiB), version="4.0.0"), "task-skew")[0]
    assert any("3x / 0.9" in x for x in f.fixes)


def test_no_skew_below_the_floor_or_with_few_tasks():
    assert not by_rule(diagnose(E.stage(0, 12), tasks(0, [100] * 11 + [2_000])), "task-skew")    # max under 30 s
    assert not by_rule(diagnose(E.stage(0, 4), tasks(0, [5_000] * 3 + [60_000])), "task-skew")   # 4 tasks


def test_gc_pressure():
    f = by_rule(diagnose(E.stage(0, 4), tasks(0, [30_000] * 4, gc_ms=7_000)), "gc-pressure")
    assert f and f[0].severity == "critical" and f[0].evidence["gc_share"] == pytest.approx(0.233, abs=0.01)
    assert not by_rule(diagnose(E.stage(0, 4), tasks(0, [30_000] * 4, gc_ms=1_000)), "gc-pressure")


def test_scheduler_delay_noise_is_ignored_but_real_delay_is_not():
    noise = diagnose(E.stage(0, 20), tasks(0, [10] * 20, extra_ms=5))           # 5 ms of 17: large share, tiny value
    real = diagnose(E.stage(0, 20), tasks(0, [2_000] * 20, extra_ms=400))
    assert not by_rule(noise, "scheduler-delay") and by_rule(real, "scheduler-delay")


def test_oom_executor_loss_gets_memory_advice():
    evs = [E.task(0, 0, 10, reason="ExecutorLostFailure", metrics=False,
                  **{"Executor ID": "3", "Exit Caused By App": True,
                     "Loss Reason": "Container killed by YARN for exceeding memory limits. 9.1 GB of 9 GB used"}),
           E.task(0, 0, 10, attempt=1)]
    [f] = by_rule(diagnose(E.stage(0, 1), evs), "task-failures")
    assert any("memoryOverhead" in x for x in f.fixes) and "exceeding memory limits" in f.detail


def test_fetch_failure_and_stage_retry():
    evs = [*E.stage(1, 2, failure="org.apache.spark.shuffle.FetchFailedException: Failed to connect"),
           E.task(1, 0, 10, reason="FetchFailed", metrics=False, Message="Failed to connect to host"),
           *E.stage(1, 2, attempt=1), E.task(1, 0, 10, stage_attempt=1), E.task(1, 1, 10, stage_attempt=1)]
    fs = by_rule(diagnose(evs), "task-failures")
    assert [f.severity for f in fs] == ["critical", "warn"]          # failed attempt 0; retry attempt 1
    assert "stage FAILED" in fs[0].detail and "retry attempt 1" in fs[1].detail


def test_speculation_waste():
    evs = [E.task(0, 0, 40_000, reason="TaskKilled", speculative=True, **{"Kill Reason": "another attempt succeeded"}),
           E.task(0, 0, 50_000, attempt=1)]
    assert by_rule(diagnose(E.stage(0, 1), evs), "speculation-waste")


def test_small_tasks_advice_depends_on_the_source():
    shuffle = by_rule(diagnose(E.stage(0, 3000), tasks(0, [50] * 3000, sr_bytes=MiB)), "too-many-small-tasks")[0]
    scan = by_rule(diagnose(E.stage(0, 3000), tasks(0, [50] * 3000, in_bytes=MiB)), "too-many-small-tasks")[0]
    assert "spark.sql.shuffle.partitions=" in shuffle.fixes[0] and "maxPartitionBytes" in scan.fixes[0]
    assert shuffle.severity == "critical"


def test_few_tasks_skips_a_small_shuffle_aqe_coalesced_on_purpose():
    big = diagnose(E.stage(0, 1, completed=E.T0 + 120_000), E.task(0, 0, 110_000, in_bytes=5 << 30))
    small = diagnose(E.stage(0, 1, completed=E.T0 + 120_000), E.task(0, 0, 110_000, sr_bytes=10 * MiB))
    assert by_rule(big, "too-few-tasks") and not by_rule(small, "too-few-tasks")


def _join_app(left_bytes, right_bytes, kind="Inner", props=None, version="3.5.1"):
    join = E.plan("SortMergeJoin", [E.plan("Exchange", metrics=[("data size", 1, "size")]),
                                    E.plan("Exchange", metrics=[("data size", 2, "size")])],
                  simple=f"SortMergeJoin [k#1], [k#2], {kind}")
    evs = [E.sql_start(1, join), *E.job(0, [0], sql_id=1), *E.stage(0, 2),
           E.task(0, 0, 10, sql={1: left_bytes}), E.task(0, 1, 10, sql={2: right_bytes})]
    return evs, props or {}, version


@pytest.mark.parametrize("left, right, kind, props, severity", [
    (5 << 30, 2 * MiB, "Inner", {"spark.sql.autoBroadcastJoinThreshold": "-1"}, "critical"),
    (5 << 30, 2 * MiB, "Inner", {}, "warn"),                       # under 10 MiB yet a shuffle join: a gate blocked it
    (5 << 30, 40 * MiB, "Inner", {}, "warn"),                      # 4x the threshold, other side 128x bigger
    (5 << 30, 400 * MiB, "Inner", {}, None),                       # too big to broadcast
    (2 * MiB, 5 << 30, "LeftOuter", {}, None),                     # a left outer join can only broadcast the right
    (5 << 30, 2 * MiB, "FullOuter", {}, None),
])
def test_broadcast_candidate(left, right, kind, props, severity):
    evs, props, version = _join_app(left, right, kind, props)
    fs = by_rule(diagnose(evs, props=props, version=version), "broadcast-candidate")
    assert [f.severity for f in fs] == ([severity] if severity else [])


def test_databricks_uses_its_30mb_adaptive_threshold():
    evs, _, _ = _join_app(5 << 30, 25 * MiB)
    oss = by_rule(diagnose(evs), "broadcast-candidate")[0]
    dbx = by_rule(diagnose(evs, props={"spark.databricks.clusterUsageTags.clusterId": "x"}), "broadcast-candidate")[0]
    assert "2.5x the broadcast threshold" in oss.detail and oss.evidence["broadcast_threshold"] == 10 * MiB
    assert dbx.evidence["broadcast_threshold"] == 30 * MiB and "yet the join stayed a shuffle join" in dbx.detail


def test_skew_join_gates():
    join = E.plan("SortMergeJoin", [E.plan("Exchange"), E.plan("Exchange")], simple="SortMergeJoin [k], [k], Inner",
                  metrics=[("number of output rows", 9, "sum")])
    def app(props, hot):
        evs = [E.sql_start(1, join), *E.job(0, [0], sql_id=1), *E.stage(0, 10, sql_accs=[9]),
               *[E.task(0, i, 1000, sr_bytes=(hot if i == 0 else 10 * MiB), sql={9: 1}) for i in range(10)]]
        return rules.diagnose(model.build(E.app(evs, props=props)), DEFAULT)
    below = by_rule(app({}, 200 * MiB), "skew-join-not-split")[0]
    off = by_rule(app({"spark.sql.adaptive.enabled": "false"}, 2 << 30), "skew-join-not-split")[0]
    declined = by_rule(app({}, 2 << 30), "skew-join-not-split")[0]
    assert "below AQE's bar" in below.detail
    assert "spark.sql.adaptive.enabled=true" in off.fixes
    assert any("forceOptimizeSkewedJoin=true" in x for x in declined.fixes)


def test_low_utilisation():
    evs = [*E.job(0, [0], end=E.T0 + 600_000), *E.stage(0, 1, completed=E.T0 + 600_000), E.task(0, 0, 100_000)]
    [f] = by_rule(diagnose(evs, cores=16), "low-utilisation")
    assert f.severity == "critical" and f.evidence["utilisation"] < 0.3


def test_findings_are_sorted_most_severe_first():
    evs = [*E.stage(0, 4), *tasks(0, [30_000] * 4, gc_ms=3_200),               # gc 10.7%: warn
           *E.stage(1, 1), E.task(1, 0, 10, reason="ExceptionFailure", **{"Class Name": "a", "Description": "b"}),
           E.task(1, 0, 10, attempt=1)]
    sev = [f.severity for f in diagnose(evs)]
    assert sev == sorted(sev, key=lambda s: -rules.SEVERITIES.index(s))


def test_a_metric_in_several_aqe_snapshots_belongs_to_the_final_plans_node():
    initial = E.plan("Exchange", metrics=[("data size", 5, "size")])
    final = E.plan("ShuffleQueryStage", [E.plan("Exchange", metrics=[("data size", 5, "size")])])
    ex = model.build(E.app(E.sql_start(1, initial), E.sql_update(1, final))).sql[1]
    assert ex.acc[5][0] is ex.plan.children[0]


def test_the_first_successful_attempt_is_kept_when_a_speculative_copy_also_succeeds():
    first = E.task(0, 0, 100, launch=E.T0)
    late_copy = E.task(0, 0, 100, attempt=1, speculative=True, launch=E.T0 + 50)
    s = model.build(E.app(E.stage(0, 1), late_copy, first)).stages[(0, 0)]
    [kept] = s.successful
    assert not kept.speculative
