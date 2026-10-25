"""Find and read Spark event logs in every layout they come in.

    single file        <app-id>[.<codec>][.inprogress]                      (Spark default up to 3.5)
    rolling directory  eventlog_v2_<app-id>/ events_<n>_<app-id>[.<codec>] + appstatus_<app-id>[.inprogress]
                       (Spark 4.0 default; files are read in NUMERIC order of n: events_10 comes after events_2)
    History Server     .zip from /api/v1/applications/<app-id>/logs (any of the above inside)
    Databricks         a folder with the live file `eventlog` and rolled `eventlog-YYYY-MM-DD--HH-MM.gz` parts,
                       read oldest first and `eventlog` last
    a directory of any of these (e.g. spark.eventLog.dir) is several applications

One file can also hold several applications: every SparkListenerLogStart starts a new one.
"""
from __future__ import annotations

import json
import re
import tempfile
import zipfile
from collections.abc import Iterator
from dataclasses import dataclass, field
from pathlib import Path

from .codecs import read_lines

_ROLLED = re.compile(r"^events_(\d+)_")
_DATABRICKS_PART = re.compile(r"^eventlog-(\d{4}-\d{2}-\d{2}--\d{2}-\d{2})")


@dataclass
class Source:
    """One application's log: its files in reading order."""
    name: str
    files: list[Path]
    layout: str                      # file | rolling | databricks
    in_progress: bool = False


@dataclass
class ReadResult:
    events: list[dict]
    codecs: set[str] = field(default_factory=set)
    notes: list[str] = field(default_factory=list)   # truncation, unparseable lines


def resolve(path: str | Path) -> list[Source]:
    """Every application log under `path`, each with its files in the order they must be read."""
    p = Path(path)
    if not p.exists():
        raise FileNotFoundError(p)
    if p.is_file() and p.suffix.lower() == ".zip":
        return _from_zip(p)
    if p.is_file():
        return [Source(p.name, [p], "file", p.name.endswith(".inprogress"))]
    if (src := _as_rolling(p)) or (src := _as_databricks(p)):
        return [src]
    found: list[Source] = []
    for child in sorted(p.iterdir()):
        if child.name.startswith(".") or child.name == "manifest.json":
            continue
        if child.is_dir():
            found += resolve(child)
        elif child.suffix.lower() == ".zip":
            found += _from_zip(child)
        elif not child.name.startswith("appstatus_"):
            found.append(Source(child.name, [child], "file", child.name.endswith(".inprogress")))
    return found


def _as_rolling(d: Path) -> Source | None:
    parts = [(int(m.group(1)), f) for f in d.iterdir() if f.is_file() and (m := _ROLLED.match(f.name))]
    if not parts:
        return None
    status = [f for f in d.iterdir() if f.name.startswith("appstatus_")]
    in_progress = any(f.name.endswith(".inprogress") for f in status)
    return Source(d.name, [f for _, f in sorted(parts)], "rolling", in_progress)


def _as_databricks(d: Path) -> Source | None:
    live = d / "eventlog"
    rolled = sorted((m.group(1), f) for f in d.iterdir() if f.is_file() and (m := _DATABRICKS_PART.match(f.name)))
    if not live.is_file() and not rolled:
        return None
    files = [f for _, f in rolled] + ([live] if live.is_file() else [])
    return Source(d.name, files, "databricks", live.is_file())


def _from_zip(z: Path) -> list[Source]:
    # Extracted to a private temp dir that lives as long as the process (the Sources point into it).
    out = Path(tempfile.mkdtemp(prefix="ssd-zip-"))
    with zipfile.ZipFile(z) as zf:
        for info in zf.infolist():
            target = (out / info.filename).resolve()
            if not str(target).startswith(str(out.resolve())):   # no path traversal out of the temp dir
                raise ValueError(f"{z.name}: unsafe member path {info.filename!r}")
        zf.extractall(out)
    sources = resolve(out)
    for s in sources:
        s.name = f"{z.name}:{s.name}"
    return sources


def read(source: Source) -> ReadResult:
    """All events of a source, in order. A final line cut off mid-write (in-progress logs) is dropped with a note;
    an unparseable line elsewhere is skipped with a note."""
    res = ReadResult([])
    for f in source.files:
        lines, codec, note = read_lines(f)
        res.codecs.add(codec)
        if note:
            res.notes.append(note)
        buffered = list(lines)
        for i, line in enumerate(buffered):
            if not line.strip():
                continue
            try:
                ev = json.loads(line)
            except json.JSONDecodeError:
                last = i == len(buffered) - 1
                res.notes.append(f"{f.name}: {'incomplete last line dropped' if last else f'unparseable line {i + 1} skipped'}")
                continue
            if isinstance(ev, dict) and "Event" in ev:
                res.events.append(ev)
    return res


def split_applications(events: list[dict]) -> Iterator[list[dict]]:
    """A new SparkListenerLogStart begins a new application (Databricks folders can hold several SparkContexts)."""
    current: list[dict] = []
    for ev in events:
        if ev["Event"] == "SparkListenerLogStart" and current:
            yield current
            current = []
        current.append(ev)
    if current:
        yield current
