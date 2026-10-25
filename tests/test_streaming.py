"""Streaming rules on synthetic StreamingQueryProgress events (shaped like Spark's JSON) and on the real fixtures."""
import datetime as dt
from pathlib import Path

import events as E
import pytest

from spark_stage_doctor import model, rules
from spark_stage_doctor.cli import analyse

FIX = Path(__file__).parent / "fixtures" / "eventlogs"


def progress(batch, t_ms, duration, rows, state_rows=None, watermark=None, end=None, latest=None, commit_ms=10,
             dropped=0, in_rate="NaN", out_rate=0.0):
    ts = dt.datetime(2026, 1, 1, tzinfo=dt.timezone.utc) + dt.timedelta(milliseconds=t_ms)
    p = {"id": "q1", "runId": "r1", "name": "orders", "timestamp": ts.isoformat().replace("+00:00", "Z"),
         "batchId": batch, "batchDuration": duration,
         "durationMs": {"triggerExecution": duration, "walCommit": commit_ms, "commit": commit_ms, "addBatch": duration // 2},
         "eventTime": {"watermark": watermark} if watermark else {},
         "sources": [{"description": "Kafka", "numInputRows": rows, "inputRowsPerSecond": in_rate,
                      "processedRowsPerSecond": out_rate, "endOffset": end, "latestOffset": latest}],
         "sink": {"description": "noop", "numOutputRows": rows}}
    if state_rows is not None:
        p["stateOperators"] = [{"operatorName": "stateStoreSave", "numRowsTotal": state_rows, "numRowsUpdated": 1,
                                "memoryUsedBytes": state_rows * 100, "numRowsDroppedByWatermark": dropped}]
    return {"Event": "org.apache.spark.sql.streaming.StreamingQueryListener$QueryProgressEvent", "progress": p}


def diagnose(evs, th=rules.DEFAULT):
    return {f.rule: f for f in rules.diagnose(model.build(E.app(evs)), th)}


def run_batches(n, gap, durations, rows, **kw):
    t, out = 0, []
    for i in range(n):
        out.append(progress(i, t, durations(i), rows(i), **kw))
        t += max(gap, durations(i))
    return out


def test_falling_behind_needs_back_to_back_batches_and_growth():
    fs = diagnose(run_batches(12, 500, lambda i: 1000 * 1.3 ** i, lambda i: int(1000 * 1.3 ** i)))
    assert fs["stream-falling-behind"].severity == "critical"


def test_idle_time_between_batches_means_keeping_up_whatever_the_rates_say():
    """The real-log trap: Spark's input rate looked 2.5x the processed rate, yet 30% of each interval was idle."""
    fs = diagnose(run_batches(12, 1000, lambda i: 700, lambda i: 400_000, in_rate=1_500_000.0, out_rate=600_000.0))
    assert not {"stream-falling-behind", "stream-saturated"} & fs.keys()


def test_saturated_without_growth_is_info():
    fs = diagnose(run_batches(12, 500, lambda i: 1000, lambda i: 240_000))
    assert fs["stream-saturated"].severity == "info" and "stream-falling-behind" not in fs


def test_offsets_left_behind_count_as_backlog():
    fs = diagnose(run_batches(12, 500, lambda i: 1000, lambda i: 10_000, end='{"t":{"0":10}}', latest='{"t":{"0":99}}'))
    assert fs["stream-falling-behind"].evidence["batches_with_backlog"] > 0


def test_state_growth_with_and_without_watermark():
    grow = diagnose(run_batches(12, 1000, lambda i: 100, lambda i: 10, state_rows=None))
    assert "stream-state-growth" not in grow
    nowm = diagnose([progress(i, i * 1000, 100, 10, state_rows=1000 * (i + 1)) for i in range(12)])
    wm = diagnose([progress(i, i * 1000, 100, 10, state_rows=1000 * (i + 1), watermark="2026-01-01T00:00:00.000Z")
                   for i in range(12)])
    bounded = diagnose([progress(i, i * 1000, 100, 10, state_rows=30 + 10 * (i % 2)) for i in range(12)])
    assert nowm["stream-state-growth"].severity == "warn" and "no watermark" in nowm["stream-state-growth"].title
    assert wm["stream-state-growth"].severity == "info"
    assert "stream-state-growth" not in bounded
    sawtooth = diagnose([progress(i, i * 1000, 100, 10, state_rows=1000 if i % 2 else 10 + i) for i in range(12)])
    assert "stream-state-growth" not in sawtooth      # evicted every other batch; ends on a peak by chance


def test_late_rows_and_slow_commits():
    fs = diagnose([progress(i, i * 5000, 4000, 10, state_rows=50, dropped=3, commit_ms=800) for i in range(12)])
    assert fs["stream-late-rows"].evidence["dropped_rows"] == 36
    assert fs["stream-slow-commits"].evidence["mean_commit_ms"] == 1600
    cheap = diagnose([progress(i, i * 500, 60, 10, commit_ms=20) for i in range(12)])   # 66% share but 40 ms
    assert "stream-slow-commits" not in cheap


def test_failed_query():
    evs = [{"Event": "org.apache.spark.sql.streaming.StreamingQueryListener$QueryStartedEvent", "id": "q1", "runId": "r1",
            "name": "orders", "timestamp": "2026-01-01T00:00:00.000Z"},
           progress(0, 0, 100, 10),
           {"Event": "org.apache.spark.sql.streaming.StreamingQueryListener$QueryTerminatedEvent", "id": "q1", "runId": "r1",
            "exception": "org.apache.spark.SparkException: Job aborted.\n\tat org.apache..."}]
    f = diagnose(evs)["stream-failed"]
    assert f.severity == "critical" and f.where == "streaming query orders" and "Job aborted" in f.detail


@pytest.mark.parametrize("version", ["spark-3.5", "spark-4.0"])
def test_real_unbounded_state_fixture(version):
    [d] = analyse(FIX / version / "stream_unbounded_state", rules.DEV)
    f = next(f for f in d.findings if f.rule == "stream-state-growth")
    assert f.evidence["rows_last"] == 26_000 and not f.evidence["watermark"]
    assert d.app.streams and len(next(iter(d.app.streams.values())).progress) >= 10


@pytest.mark.parametrize("version", ["spark-3.5", "spark-4.0"])
def test_stopping_a_query_cancels_a_batch_and_that_is_not_a_failure(version):
    """Stopping a query cancels its in-flight batch job: 3.5 says 'Job N cancelled part of cancelled job group',
    4.0 '[SPARK_JOB_CANCELLED] Job N cancelled Query ...'. The first rule version reported a critical stage failure."""
    [d] = analyse(FIX / version / "stream_falling_behind", rules.DEV)
    assert any(s.failure and "cancelled" in s.failure for s in d.app.stages.values())
    assert not [f for f in d.findings if f.rule == "task-failures"]
