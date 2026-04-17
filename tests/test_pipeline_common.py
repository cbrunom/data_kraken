from pathlib import Path
import json

from golgg.pipeline.common import log_step_end, log_step_start, tournament_dirs


def test_tournament_dirs_returns_empty_for_missing_path():
    missing = "this/path/does/not/exist"
    assert tournament_dirs(missing) == []


def test_tournament_dirs_returns_only_directories_sorted_case_insensitive(tmp_path: Path):
    (tmp_path / "bETA").mkdir()
    (tmp_path / "alpha").mkdir()
    (tmp_path / "Gamma").mkdir()
    (tmp_path / "note.txt").write_text("x", encoding="ascii")

    result = tournament_dirs(str(tmp_path))
    assert [p.name for p in result] == ["alpha", "bETA", "Gamma"]


def test_step_timing_log_is_persisted(tmp_path: Path, monkeypatch):
    log_path = tmp_path / "step_timing_runs.jsonl"
    monkeypatch.setenv("GOLGG_STEP_TIMING_LOG", str(log_path))

    started_at = log_step_start("test_step")
    log_step_end("test_step", started_at)

    lines = log_path.read_text(encoding="utf-8").strip().splitlines()
    assert len(lines) == 2

    start_record = json.loads(lines[0])
    end_record = json.loads(lines[1])
    assert start_record["event"] == "start"
    assert end_record["event"] == "end"
    assert start_record["step"] == "test_step"
    assert end_record["step"] == "test_step"
