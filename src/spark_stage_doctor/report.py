"""Render a diagnosis as terminal text, Markdown (PR comments, job summaries) or JSON."""
from __future__ import annotations

import json
from dataclasses import dataclass, field

from .model import App
from .rules import SEVERITIES, Finding
from .stats import human_bytes, human_ms

ICON = {"critical": "🔴", "warn": "🟠", "info": "🔵"}


@dataclass
class Diagnosis:
    source: str
    app: App
    findings: list[Finding]
    codecs: list[str] = field(default_factory=list)
    layout: str = "file"
    notes: list[str] = field(default_factory=list)

    def summary(self) -> dict:
        a = self.app
        tasks = [t for s in a.stages.values() for t in s.tasks]
        wall = (a.end - a.start) if a.start and a.end else None
        top = sorted(a.stages.values(), key=lambda s: s.wall_ms, reverse=True)[:5]
        return {
            "source": self.source, "layout": self.layout, "codecs": self.codecs,
            "app_name": a.name, "app_id": a.app_id, "spark_version": a.spark_version,
            "databricks": a.databricks, "duration_ms": wall, "executors": len(a.executors), "cores": a.total_cores,
            "jobs": len(a.jobs), "stages": len(a.stages), "sql_queries": len(a.sql), "task_attempts": len(tasks),
            "failed_task_attempts": sum(1 for t in tasks if not t.success and t.reason != "TaskKilled"),
            "findings": {sev: sum(1 for f in self.findings if f.severity == sev) for sev in SEVERITIES},
            "slowest_stages": [{"stage": list(s.key), "name": s.name, "wall_ms": s.wall_ms, "tasks": s.num_tasks}
                               for s in top if s.wall_ms],
            "notes": self.notes + a.notes,
            "unknown_events": dict(a.unknown_events),
        }


def to_json(diags: list[Diagnosis]) -> str:
    return json.dumps([{"summary": d.summary(), "findings": [f.to_dict() for f in d.findings]} for d in diags], indent=2)


def _header(d: Diagnosis) -> list[str]:
    s = d.summary()
    dur = human_ms(s["duration_ms"]) if s["duration_ms"] else "unknown duration"
    counts = ", ".join(f"{n} {sev}" for sev, n in reversed(list(s["findings"].items())) if n) or "no findings"
    return [f"{s['app_name'] or s['app_id'] or d.source}  (Spark {s['spark_version'] or '?'}"
            + (", Databricks" if s["databricks"] else "") + f", {dur})",
            f"  {s['executors']} executor(s), {s['cores']} cores; {s['jobs']} jobs, {s['stages']} stage attempts, "
            f"{s['task_attempts']} task attempts ({s['failed_task_attempts']} failed); {s['sql_queries']} SQL queries",
            f"  log: {d.source} [{d.layout}, {'/'.join(d.codecs) or 'plain'}]",
            f"  {counts}"]


def to_text(diags: list[Diagnosis], min_severity: str = "info") -> str:
    floor = SEVERITIES.index(min_severity)
    out = []
    for d in diags:
        out += _header(d)
        for note in d.notes + d.app.notes:
            out.append(f"  note: {note}")
        for f in d.findings:
            if SEVERITIES.index(f.severity) < floor:
                continue
            out += ["", f"[{f.severity.upper()}] {f.title}", f"  where: {f.where}", f"  {f.detail}"]
            out += [f"  fix: {x}" for x in f.fixes]
            out.append(f"  rule: {f.rule} | threshold source: {f.source}")
        out.append("")
    return "\n".join(out)


def to_markdown(diags: list[Diagnosis], min_severity: str = "info") -> str:
    floor = SEVERITIES.index(min_severity)
    out = ["## Spark stage doctor", ""]
    for d in diags:
        s = d.summary()
        out += [f"**{s['app_name'] or s['app_id']}**: Spark {s['spark_version']}, "
                f"{human_ms(s['duration_ms']) if s['duration_ms'] else 'unknown duration'}, {s['cores']} cores, "
                f"{s['stages']} stage attempts, {s['task_attempts']} task attempts", ""]
        shown = [f for f in d.findings if SEVERITIES.index(f.severity) >= floor]
        if not shown:
            out += ["No findings. ✅", ""]
            continue
        out += ["| | Finding | Where |", "|---|---|---|"]
        out += [f"| {ICON[f.severity]} | {f.title} | {f.where.replace('|', '/')} |" for f in shown]
        out.append("")
        for f in shown:
            out += [f"<details><summary>{ICON[f.severity]} <b>{f.title}</b> ({f.rule})</summary>", "", f.detail, ""]
            out += [f"- {x}" for x in f.fixes]
            ev = ", ".join(f"{k}={_fmt(k, v)}" for k, v in f.evidence.items())
            out += ["", f"<sub>Evidence: {ev}. Threshold source: {f.source}.</sub>", "", "</details>", ""]
    return "\n".join(out)


def _fmt(k: str, v) -> str:
    if isinstance(v, (int, float)) and not isinstance(v, bool) and k.endswith("bytes"):
        return human_bytes(v)
    if isinstance(v, (int, float)) and not isinstance(v, bool) and k.endswith("_ms"):
        return human_ms(v)
    return str(v)
