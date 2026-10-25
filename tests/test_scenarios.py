"""Real event logs from Spark 3.5.9 and 4.0.4 (tools/gen_fixtures.py, run on GitHub Actions): each scenario must
produce exactly its intended findings, and the clean baseline none."""
import json
from pathlib import Path

import pytest

from spark_stage_doctor import rules
from spark_stage_doctor.cli import analyse

FIX = Path(__file__).parent / "fixtures" / "eventlogs"
VERSIONS = ["spark-3.5", "spark-4.0"]

# Rule ids each scenario must produce under the dev profile, and nothing else. The joins run with broadcasts
# switched off (autoBroadcastJoinThreshold=-1) against a 1000-row dimension, so broadcast-candidate is also right.
EXPECTED = {
    "clean": set(),
    "clean-defaults": set(),
    "clean-zstd": set(), "clean-lz4": set(), "clean-snappy": set(), "clean-lzf": set(),
    "skew_repartition": {"task-skew"},
    "skew_join_unsplit": {"task-skew", "skew-join-not-split", "broadcast-candidate"},
    "skew_join_split": {"aqe-skew-split", "broadcast-candidate"},
    "broadcast_disabled": {"broadcast-candidate"},
    "spill": {"spill"},
    "tiny_tasks": {"too-many-small-tasks"},
    "single_task": {"too-few-tasks"},
    "task_failures": {"task-failures"},
    "small_files": {"small-files"},
    # local-cluster[2,1,...]: two real executor JVMs
    "executor_lost": {"task-failures"},
    "executor_memory": {"executor-memory"},
}


def run(version, scenario, th=rules.DEV):
    diags = analyse(FIX / version / scenario, th)
    assert len(diags) == 1
    return diags[0]


@pytest.mark.parametrize("version", VERSIONS)
@pytest.mark.parametrize("scenario", sorted(EXPECTED))
def test_scenario_produces_exactly_its_findings(version, scenario):
    d = run(version, scenario)
    assert {f.rule for f in d.findings} == EXPECTED[scenario], [(f.rule, f.title) for f in d.findings]


@pytest.mark.parametrize("version", VERSIONS)
@pytest.mark.parametrize("scenario", sorted(EXPECTED))
def test_default_profile_is_quiet_on_tiny_jobs(version, scenario):
    """Production floors (30 s tasks, 100 MiB partitions, 2000 tasks...) must not fire on 1-second test jobs,
    except where the evidence is not size-dependent (failed tasks, broadcast disabled for a tiny side, any spill, 100+ tiny files)."""
    rules_hit = {f.rule for f in run(version, scenario, rules.DEFAULT).findings}
    assert rules_hit <= {"task-failures", "broadcast-candidate", "spill", "aqe-skew-split", "small-files"}, rules_hit


@pytest.mark.parametrize("version", VERSIONS)
def test_fixture_configs_did_not_leak_between_apps(version):
    """The generator's first version reused SparkSession.builder, whose options are class-level: every app inherited
    the previous apps' settings. Each log's Spark Properties must carry exactly its own scenario's overrides."""
    manifest = json.loads((FIX / version / "manifest.json").read_text())
    scenario_keys = {k for info in manifest.values() for k in info["conf"] if k.startswith("spark.")}
    for name, info in manifest.items():
        props = run(version, name).app.properties
        own = {k: v for k, v in info["conf"].items() if k.startswith("spark.")}
        for k in scenario_keys - own.keys() - {"spark.eventLog.compress", "spark.eventLog.rolling.enabled",
                                                "spark.eventLog.compression.codec", "spark.sql.adaptive.enabled",
                                                "spark.sql.autoBroadcastJoinThreshold"}:
            assert k not in props, f"{name} inherited {k}={props[k]}"
        for k, v in own.items():
            assert props.get(k) == v, (name, k)


def test_spark4_default_log_is_rolling_zstd():
    d = run("spark-4.0", "clean-defaults")
    assert d.layout == "rolling" and d.codecs == ["zstd"]
    assert run("spark-3.5", "clean-defaults").codecs == ["plain"]


@pytest.mark.parametrize("version", VERSIONS)
@pytest.mark.parametrize("codec", ["zstd", "lz4", "snappy", "lzf"])
def test_every_spark_codec_decodes_to_the_same_events(version, codec):
    plain = run(version, "clean").app
    other = run(version, f"clean-{codec}")
    assert other.codecs == [codec]
    assert len(other.app.stages) == len(plain.stages) and other.app.spark_version == plain.spark_version
    assert sum(len(s.tasks) for s in other.app.stages.values()) == sum(len(s.tasks) for s in plain.stages.values())


@pytest.mark.parametrize("version", VERSIONS)
def test_skew_finding_evidence(version):
    f = next(f for f in run(version, "skew_repartition").findings if f.rule == "task-skew")
    assert f.title.startswith("Data skew")
    assert f.evidence["slowest_shuffle_read_bytes"] > 50 * f.evidence["median_shuffle_read_bytes"]
    assert any("Salt" in x for x in f.fixes) and any("REBALANCE" in x for x in f.fixes)   # not a join: AQE can't help


@pytest.mark.parametrize("version", VERSIONS)
def test_unsplit_skew_join_names_the_gate(version):
    f = next(f for f in run(version, "skew_join_unsplit").findings if f.rule == "skew-join-not-split")
    assert "below AQE's bar" in f.detail and f.evidence["threshold_bytes"] == 256 << 20
    assert any("skewedPartitionThresholdInBytes=64m" in x for x in f.fixes)


@pytest.mark.parametrize("version", VERSIONS)
def test_split_skew_join_reports_aqe_counts(version):
    f = next(f for f in run(version, "skew_join_split").findings if f.rule == "aqe-skew-split")
    assert f.evidence.get("number of skewed partitions", 0) >= 1


@pytest.mark.parametrize("version", VERSIONS)
def test_broadcast_candidate_sizes(version):
    f = next(f for f in run(version, "broadcast_disabled").findings if f.rule == "broadcast-candidate")
    assert f.severity == "critical" and f.evidence["broadcast_threshold"] == -1
    assert f.evidence["small_side_bytes"] < 1 << 20 < f.evidence["other_side_bytes"]


@pytest.mark.parametrize("version", VERSIONS)
def test_failures_are_grouped_by_error(version):
    f = next(f for f in run(version, "task_failures").findings if f.rule == "task-failures")
    assert f.evidence["failed_attempts"] == 4 and "injected failure" in f.detail
    assert not run(version, "task_failures").findings[0].evidence.get("stage_failed")


@pytest.mark.parametrize("version", VERSIONS)
def test_single_task_points_at_coalesce(version):
    f = next(f for f in run(version, "single_task").findings if f.rule == "too-few-tasks")
    assert f.severity == "critical" and any("coalesce" in x for x in f.fixes)


APACHE = sorted((FIX / "apache-spark").glob("*.json"))


@pytest.mark.parametrize("path", APACHE, ids=[p.name for p in APACHE])
def test_apache_spark_test_logs_parse(path):
    """Spark's own test-suite logs (3.1, 3.3; YARN, streaming): parse fully, no unknown core events."""
    d = analyse(path)[0]
    assert d.app.spark_version and d.app.stages
    assert not [e for e in d.app.unknown_events if e.startswith("SparkListener")], d.app.unknown_events


def test_real_speculation_in_apache_log():
    d = analyse(FIX / "apache-spark" / "application_1628109047826_1317105.json")[0]
    f = next(f for f in d.findings if f.rule == "speculation-waste")
    assert f.evidence["killed_copies"] == 1 and f.evidence["wasted_run_ms"] > 50_000


@pytest.mark.parametrize("version", VERSIONS)
def test_small_files_reads_the_scan_driver_metrics(version):
    f = next(f for f in run(version, "small_files").findings if f.rule == "small-files")
    assert f.evidence["files"] == 120 and f.evidence["format"] == "CSV" and f.evidence["avg_file_bytes"] < 4096
    assert f.severity == "critical" and any("openCostInBytes (4m)" in x for x in f.fixes)


@pytest.mark.parametrize("version", VERSIONS)
def test_killed_executor_is_explained(version):
    """The task SIGKILLs its executor JVM: exactly what the OOM killer does. Spark reports 'Command exited with
    code 137' (the first rule version only matched 'exit code 137')."""
    d = run(version, "executor_lost")
    f = next(f for f in d.findings if f.rule == "task-failures")
    assert f.severity == "warn" and "ExecutorLostFailure: Command exited with code 137" in f.detail
    assert any("memoryOverhead" in x for x in f.fixes) and any("SIGKILLed" in x for x in f.fixes)
    assert any(e.removed for e in d.app.executors.values())


@pytest.mark.parametrize("version", VERSIONS)
def test_executor_memory_uses_spark_managed_memory(version):
    f = next(f for f in run(version, "executor_memory").findings if f.rule == "executor-memory")
    assert f.evidence["heap_bytes"] == 3 << 30 and f.evidence["executors"] == 2
    assert f.evidence["peak_managed_use"] < 0.3 and f.evidence["heap_use"] < 0.5
