"""spark-stage-doctor command line."""
from __future__ import annotations

import argparse
import sys
from pathlib import Path

from . import __version__, model, reader, rules
from .report import Diagnosis, to_json, to_markdown, to_text


def analyse(path: str | Path, thresholds: rules.Thresholds = rules.DEFAULT) -> list[Diagnosis]:
    """Every application under `path`, diagnosed."""
    out = []
    for src in reader.resolve(path):
        res = reader.read(src)
        apps = list(reader.split_applications(res.events))
        for i, events in enumerate(apps):
            app = model.build(events)
            name = src.name if len(apps) == 1 else f"{src.name}#{i + 1}"
            notes = list(res.notes) + (["the application was still running when the log was copied"]
                                       if src.in_progress else [])
            out.append(Diagnosis(name, app, rules.diagnose(app, thresholds), sorted(res.codecs), src.layout, notes))
    return out


def _one(path: str, th: rules.Thresholds) -> Diagnosis:
    diags = analyse(path, th)
    if len(diags) != 1:
        raise ValueError(f"{path}: expected one application, found {len(diags)}")
    return diags[0]


def compare_main(argv: list[str]) -> int:
    from . import compare as cmp

    p = argparse.ArgumentParser(prog="spark-stage-doctor compare",
                                description="Compare two runs of the same job; exit 1 if the current run regressed.")
    p.add_argument("baseline")
    p.add_argument("current")
    p.add_argument("--format", choices=("text", "md", "json"), default="text")
    p.add_argument("--profile", choices=tuple(rules.PROFILES), default="default")
    p.add_argument("--tolerance", type=float, default=0.2, help="relative increase that counts (default 0.2 = 20%%)")
    p.add_argument("--min-severity", choices=rules.SEVERITIES, default="warn", help="new findings below this are ignored")
    p.add_argument("--no-fail", action="store_true", help="always exit 0 (report only)")
    p.add_argument("-o", "--output")
    a = p.parse_args(argv)
    th = rules.PROFILES[a.profile]
    try:
        c = cmp.compare(_one(a.baseline, th), _one(a.current, th), a.tolerance, a.min_severity)
    except (FileNotFoundError, ValueError) as e:
        print(f"spark-stage-doctor: {e}", file=sys.stderr)
        return 2
    import json
    rendered = {"text": cmp.to_text, "md": cmp.to_markdown}.get(a.format, lambda c: json.dumps(c.to_dict(), indent=2))(c)
    _emit(rendered, a.output)
    return 1 if c.regressed and not a.no_fail else 0


def _emit(rendered: str, output: str | None) -> None:
    if output:
        Path(output).write_text(rendered, encoding="utf-8")
    else:
        sys.stdout.reconfigure(encoding="utf-8")
        print(rendered)


def main(argv: list[str] | None = None) -> int:
    argv = sys.argv[1:] if argv is None else argv
    if argv and argv[0] == "compare":
        return compare_main(argv[1:])
    p = argparse.ArgumentParser(prog="spark-stage-doctor",
                                description="Diagnose Spark applications from their event logs (Spark 3.3-4.0).")
    p.epilog = "Compare two runs: spark-stage-doctor compare BASELINE CURRENT [--format md]"
    p.add_argument("paths", nargs="+", help="event log file, rolling dir (eventlog_v2_*), History Server .zip, "
                                            "Databricks eventlog folder, or a directory of logs")
    p.add_argument("--format", choices=("text", "md", "json"), default="text")
    p.add_argument("--profile", choices=tuple(rules.PROFILES), default="default",
                   help="'dev' lowers the absolute floors (seconds, bytes, task counts) for small or test jobs")
    p.add_argument("--min-severity", choices=rules.SEVERITIES, default="info")
    p.add_argument("--fail-on", choices=(*rules.SEVERITIES, "never"), default="never",
                   help="exit 1 if any finding is at or above this severity (for CI)")
    p.add_argument("-o", "--output", help="write the report to this file instead of stdout")
    p.add_argument("--version", action="version", version=f"%(prog)s {__version__}")
    a = p.parse_args(argv)

    diags: list[Diagnosis] = []
    try:
        for path in a.paths:
            diags += analyse(path, rules.PROFILES[a.profile])
    except (FileNotFoundError, ValueError) as e:
        print(f"spark-stage-doctor: {e}", file=sys.stderr)
        return 2
    if not diags:
        print("spark-stage-doctor: no event logs found", file=sys.stderr)
        return 2
    text = {"text": to_text, "md": to_markdown}.get(a.format)
    rendered = text(diags, a.min_severity) if text else to_json(diags)
    _emit(rendered, a.output)
    if a.fail_on != "never":
        floor = rules.SEVERITIES.index(a.fail_on)
        if any(rules.SEVERITIES.index(f.severity) >= floor for d in diags for f in d.findings):
            return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
