"""Compare two runs of the same job: what got worse (CI regression gate).

Stage and job ids change between runs, so findings are matched by rule + stage NAME (Spark's call-site name,
e.g. "save at etl.py:42", stable while the code is) or SQL description. Totals are compared as ratios, with an
absolute floor so that noise on tiny values doesn't count.
"""
from __future__ import annotations

from dataclasses import dataclass, field

from .report import Diagnosis
from .rules import SEVERITIES, Finding
from .stats import human_bytes, human_ms

METRICS = {   # name: (how to compute, absolute floor below which a change is ignored, formatter)
    "wall_ms": (lambda a: (a.end - a.start) if a.start and a.end else 0, 30_000, human_ms),
    "task_time_ms": (lambda a: sum(t.run_ms for s in a.stages.values() for t in s.tasks), 60_000, human_ms),
    "shuffle_write_bytes": (lambda a: sum(t.shuffle_write_bytes for s in a.stages.values() for t in s.tasks), 1 << 30,
                            human_bytes),
    "disk_spill_bytes": (lambda a: sum(t.disk_spill for s in a.stages.values() for t in s.tasks), 1 << 28, human_bytes),
    "failed_tasks": (lambda a: sum(1 for s in a.stages.values() for t in s.tasks if not t.success
                                   and t.reason != "TaskKilled"), 1, str),
}


@dataclass
class Comparison:
    baseline: str
    current: str
    metrics: dict[str, tuple[float, float]] = field(default_factory=dict)
    regressions: list[str] = field(default_factory=list)
    new_findings: list[Finding] = field(default_factory=list)
    resolved: list[Finding] = field(default_factory=list)

    @property
    def regressed(self) -> bool:
        return bool(self.regressions or self.new_findings)

    def to_dict(self) -> dict:
        return {"baseline": self.baseline, "current": self.current, "metrics": self.metrics,
                "regressions": self.regressions, "new_findings": [f.to_dict() for f in self.new_findings],
                "resolved": [f.to_dict() for f in self.resolved], "regressed": self.regressed}


def _key(d: Diagnosis, f: Finding) -> tuple:
    if f.stage is not None:
        s = d.app.stages.get(f.stage)
        return (f.rule, s.name if s else f.where)
    ex = d.app.sql.get(f.sql_id) if f.sql_id is not None else None
    return (f.rule, ex.description if ex else f.where)


def compare(base: Diagnosis, cur: Diagnosis, tolerance: float = 0.2, min_severity: str = "warn") -> Comparison:
    c = Comparison(base.source, cur.source)
    for name, (fn, floor, fmt) in METRICS.items():
        b, n = fn(base.app), fn(cur.app)
        c.metrics[name] = (b, n)
        if n - b >= floor and n > b * (1 + tolerance):
            pct = f" (+{(n - b) / b:.0%})" if b else ""
            c.regressions.append(f"{name}: {fmt(b)} -> {fmt(n)}{pct}")
    floor = SEVERITIES.index(min_severity)
    relevant = [f for f in cur.findings if SEVERITIES.index(f.severity) >= floor]
    before = {_key(base, f): f for f in base.findings}
    after = {_key(cur, f): f for f in relevant}
    for k, f in after.items():
        old = before.get(k)
        if old is None or SEVERITIES.index(f.severity) > SEVERITIES.index(old.severity):
            c.new_findings.append(f)
    c.resolved = [f for k, f in before.items() if k not in {_key(cur, x): x for x in cur.findings}
                  and SEVERITIES.index(f.severity) >= floor]
    return c


def to_text(c: Comparison) -> str:
    out = [f"baseline: {c.baseline}", f"current:  {c.current}",
           "REGRESSED" if c.regressed else "no regression", ""]
    out += [f"  regression: {r}" for r in c.regressions]
    out += [f"  new: [{f.severity}] {f.title} ({f.where})" for f in c.new_findings]
    out += [f"  resolved: {f.title} ({f.where})" for f in c.resolved]
    return "\n".join(out)


def to_markdown(c: Comparison) -> str:
    out = ["## Spark stage doctor: run comparison", "",
           f"**{'Regressed' if c.regressed else 'No regression'}** ({c.baseline} -> {c.current})", "",
           "| Metric | Baseline | Current |", "|---|---|---|"]
    for name, (b, n) in c.metrics.items():
        fmt = METRICS[name][2]
        out.append(f"| {name} | {fmt(b)} | {fmt(n)} |")
    if c.regressions or c.new_findings or c.resolved:
        out.append("")
    out += [f"- 🔺 {r}" for r in c.regressions]
    out += [f"- 🆕 **{f.title}** ({f.where}): {f.detail}" for f in c.new_findings]
    out += [f"- ✅ resolved: {f.title} ({f.where})" for f in c.resolved]
    return "\n".join(out)
