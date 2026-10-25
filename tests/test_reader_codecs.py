import gzip
import json
import shutil
import struct
import zipfile
from pathlib import Path

import pytest

from spark_stage_doctor import codecs, reader
from spark_stage_doctor.cli import analyse

FIX = Path(__file__).parent / "fixtures" / "eventlogs"


def only_file(d: Path) -> Path:
    return next(p for p in d.rglob("*") if p.is_file())


def clean_lines(version="spark-3.5") -> list[str]:
    return only_file(FIX / version / "clean").read_text(encoding="utf-8").splitlines()


# ------------------------------------------------------------------ codecs

@pytest.mark.parametrize("codec", ["zstd", "lz4", "snappy", "lzf"])
def test_truncated_compressed_log_keeps_what_decodes(tmp_path, codec):
    """An .inprogress copy can end mid-block: decode the complete blocks, report the cut, don't fail."""
    src = only_file(FIX / "spark-4.0" / f"clean-{codec}")
    raw = src.read_bytes()
    cut = tmp_path / (src.name + ".inprogress")
    cut.write_bytes(raw[: int(len(raw) * 0.6)])
    lines, used, note = codecs.read_lines(cut)
    assert used == codec and note is not None
    full = codecs.decompress(raw, codec).decode().splitlines()
    got = list(lines)
    assert got and len(got) <= len(full) and got[:-1] == full[: len(got) - 1]


def test_codec_is_sniffed_when_the_name_has_no_extension(tmp_path):
    for codec in ("zstd", "lz4", "snappy", "lzf"):
        src = only_file(FIX / "spark-3.5" / f"clean-{codec}")
        renamed = tmp_path / f"renamed-{codec}"
        shutil.copy(src, renamed)
        assert codecs.codec_for(renamed, renamed.read_bytes()[:16]) == codec
    g = tmp_path / "eventlog"
    g.write_bytes(gzip.compress(b'{"Event":"x"}\n'))
    assert codecs.codec_for(g, g.read_bytes()[:16]) == "gzip"


def test_lz4_stored_block_and_end_marker():
    payload = b"hello\n"
    block = b"LZ4Block" + bytes([0x10]) + struct.pack("<iii", len(payload), len(payload), 0) + payload
    end = b"LZ4Block" + bytes([0x10]) + struct.pack("<iii", 0, 0, 0)
    assert codecs.decompress(block + end, "lz4") == payload


def test_lzf_back_reference_overlapping_its_output():
    # literal "ab" (ctrl 1), then a back reference of length 6 at distance 2: "abababab"
    block = bytes([1]) + b"ab" + bytes([(4 << 5) | 0, 1])
    chunk = b"ZV\x01" + struct.pack(">HH", len(block), 8) + block
    assert codecs.decompress(chunk, "lzf") == b"abababab"


def test_concatenated_gzip_members():
    assert codecs.decompress(gzip.compress(b"a\n") + gzip.compress(b"b\n"), "gzip") == b"a\nb\n"


# ------------------------------------------------------------------ layouts

def write_rolling(d: Path, lines: list[str], parts=3, in_progress=False) -> Path:
    """A Spark 4-style rolling dir. Indexes 1, 2, 10 make lexical order differ from numeric order."""
    d.mkdir()
    chunk = len(lines) // parts + 1
    for n, i in zip((1, 2, 10), range(0, len(lines), chunk), strict=False):
        (d / f"events_{n}_app-1").write_text("\n".join(lines[i:i + chunk]) + "\n", encoding="utf-8")
    (d / ("appstatus_app-1" + (".inprogress" if in_progress else ""))).write_text("")
    return d


def test_rolling_files_are_read_in_numeric_order(tmp_path):
    lines = clean_lines()
    d = write_rolling(tmp_path / "eventlog_v2_app-1", lines)
    [src] = reader.resolve(d)
    assert src.layout == "rolling" and [f.name.split("_")[1] for f in src.files] == ["1", "2", "10"]
    assert [json.dumps(e, sort_keys=True) for e in reader.read(src).events] == \
        [json.dumps(json.loads(x), sort_keys=True) for x in lines if x.strip()]


def test_in_progress_rolling_log_with_a_cut_last_line(tmp_path):
    lines = clean_lines()
    d = write_rolling(tmp_path / "eventlog_v2_app-1", lines, in_progress=True)
    last = sorted(d.glob("events_10_*"))[0]
    last.write_text(last.read_text()[:-40], encoding="utf-8")    # cut mid-line
    [diag] = analyse(d)
    assert any("incomplete last line" in n for n in diag.notes)
    assert any("still running" in n for n in diag.notes)


def test_history_server_zip(tmp_path):
    d = write_rolling(tmp_path / "eventlog_v2_app-1", clean_lines())
    z = tmp_path / "app-1.zip"
    with zipfile.ZipFile(z, "w") as zf:
        for f in d.iterdir():
            zf.write(f, f"eventlog_v2_app-1/{f.name}")
    [diag] = analyse(z)
    assert diag.layout == "rolling" and diag.source.startswith("app-1.zip:") and diag.app.stages


def test_zip_with_path_traversal_is_rejected(tmp_path):
    z = tmp_path / "evil.zip"
    with zipfile.ZipFile(z, "w") as zf:
        zf.writestr("../escape", "x")
    with pytest.raises(ValueError, match="unsafe"):
        reader.resolve(z)


def test_databricks_folder_reads_rolled_parts_oldest_first_then_live_file(tmp_path):
    """Synthetic layout (no real Databricks log is available): `eventlog` is live, parts are hourly .gz."""
    lines = clean_lines()
    d = tmp_path / "1234-567890-abcde_10_0_0_1" / "5555555555555555555"
    d.mkdir(parents=True)
    third = len(lines) // 3
    (d / "eventlog-2026-10-25--06-00.gz").write_bytes(gzip.compress(("\n".join(lines[third:2 * third]) + "\n").encode()))
    (d / "eventlog-2026-10-25--05-00.gz").write_bytes(gzip.compress(("\n".join(lines[:third]) + "\n").encode()))
    (d / "eventlog").write_text("\n".join(lines[2 * third:]) + "\n", encoding="utf-8")
    [src] = reader.resolve(tmp_path)
    assert src.layout == "databricks" and [f.name for f in src.files] == \
        ["eventlog-2026-10-25--05-00.gz", "eventlog-2026-10-25--06-00.gz", "eventlog"]
    assert len(reader.read(src).events) == len([x for x in lines if x.strip()])


def test_directory_of_many_logs(tmp_path):
    for name in ("clean", "spill"):
        shutil.copy(only_file(FIX / "spark-3.5" / name), tmp_path / name)
    (tmp_path / "appstatus_x").write_text("")
    assert sorted(d.source for d in analyse(tmp_path)) == ["clean", "spill"]


def test_two_applications_in_one_file_are_split(tmp_path):
    f = tmp_path / "two"
    f.write_text("\n".join(clean_lines() + clean_lines("spark-4.0")), encoding="utf-8")
    diags = analyse(f)
    assert [d.app.spark_version for d in diags] == ["3.5.9", "4.0.4"]


def test_unparseable_line_in_the_middle_is_skipped_with_a_note(tmp_path):
    lines = clean_lines()
    f = tmp_path / "broken"
    f.write_text("\n".join(lines[:5] + ["{not json"] + lines[5:]), encoding="utf-8")
    [diag] = analyse(f)
    assert any("unparseable line 6" in n for n in diag.notes) and diag.app.stages


def test_missing_path_is_an_error():
    with pytest.raises(FileNotFoundError):
        reader.resolve("does/not/exist")
