import json
from pathlib import Path

from spark_stage_doctor.cli import main

F = Path(__file__).parent / "fixtures" / "eventlogs" / "spark-3.5"


def test_text_report_and_exit_codes(capsys):
    assert main([str(F / "spill"), "--profile", "dev"]) == 0
    out = capsys.readouterr().out
    assert "[CRITICAL] Spill to disk" in out and "fix: More, smaller partitions" in out
    assert main([str(F / "spill"), "--profile", "dev", "--fail-on", "critical"]) == 1
    assert main([str(F / "clean"), "--profile", "dev", "--fail-on", "info"]) == 0


def test_json_report(capsys):
    main([str(F / "tiny_tasks"), "--profile", "dev", "--format", "json"])
    [doc] = json.loads(capsys.readouterr().out)
    assert doc["summary"]["spark_version"] == "3.5.9" and doc["summary"]["findings"]["critical"] == 1
    assert doc["findings"][0]["rule"] == "too-many-small-tasks" and doc["findings"][0]["stage"] == [0, 0]


def test_markdown_report_to_file(tmp_path):
    out = tmp_path / "r.md"
    main([str(F / "broadcast_disabled"), "--profile", "dev", "--format", "md", "-o", str(out)])
    md = out.read_text(encoding="utf-8")
    assert md.startswith("## Spark stage doctor") and "| 🔴 | SortMergeJoin could be a broadcast join |" in md
    assert "<details>" in md and "Evidence:" in md


def test_min_severity_filters_the_text_report(capsys):
    main([str(F / "skew_join_split"), "--profile", "dev", "--min-severity", "warn"])
    out = capsys.readouterr().out
    assert "broadcast join" in out and "AQE split" not in out


def test_missing_path_exits_2(capsys):
    assert main(["no/such/log"]) == 2
    assert "spark-stage-doctor:" in capsys.readouterr().err


def test_compare_flags_new_findings_and_resolved_ones(capsys):
    assert main(["compare", str(F / "clean"), str(F / "spill"), "--profile", "dev"]) == 1
    assert "new: [critical] Spill to disk" in capsys.readouterr().out
    assert main(["compare", str(F / "spill"), str(F / "clean"), "--profile", "dev"]) == 0
    assert "resolved: Spill to disk" in capsys.readouterr().out
    assert main(["compare", str(F / "clean"), str(F / "spill"), "--profile", "dev", "--no-fail"]) == 0


def test_compare_same_run_is_not_a_regression(capsys):
    assert main(["compare", str(F / "skew_repartition"), str(F / "skew_repartition"), "--profile", "dev"]) == 0
    assert "no regression" in capsys.readouterr().out


def test_compare_markdown_and_json(capsys):
    main(["compare", str(F / "clean"), str(F / "task_failures"), "--profile", "dev", "--format", "md", "--no-fail"])
    md = capsys.readouterr().out
    assert "| failed_tasks | 0 | 4 |" in md and "**Regressed**" in md
    main(["compare", str(F / "clean"), str(F / "task_failures"), "--profile", "dev", "--format", "json", "--no-fail"])
    doc = json.loads(capsys.readouterr().out)
    assert doc["regressed"] and any(r.startswith("failed_tasks: 0 -> 4") for r in doc["regressions"])


def test_compare_needs_one_application_per_side(capsys):
    assert main(["compare", str(F), str(F / "clean")]) == 2
    assert "expected one application" in capsys.readouterr().err
