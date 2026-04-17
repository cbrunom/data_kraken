import json

from golgg.observability.pipeline_runs import (
    RUN_LOG_FILE,
    append_run_record,
    build_run_record,
    summarize_generated_files,
)


def test_build_run_record_include_expected_fields():
    record = build_run_record(
        pipeline_name="step007_infographic_dataset",
        tournament_key="CBLOL_2026_Split_1",
        status="written",
        generated_files=["a.csv", "b.csv"],
        contract_errors={"a.csv": ["missing_required_column:tournament_key"]},
    )

    assert record["pipeline_name"] == "step007_infographic_dataset"
    assert record["tournament_key"] == "CBLOL_2026_Split_1"
    assert record["status"] == "written"
    assert record["generated_files"] == ["a.csv", "b.csv"]
    assert record["contract_errors"] == {"a.csv": ["missing_required_column:tournament_key"]}
    assert record["timestamp_utc"].endswith("+00:00") or record["timestamp_utc"].endswith("Z")


def test_append_run_record_write_jsonl_line(tmp_path):
    log_dir = tmp_path / "logs"
    record = build_run_record(
        pipeline_name="step007_infographic_dataset",
        tournament_key="all",
        status="contract_passed",
    )

    path = append_run_record(str(log_dir), record)

    assert path.endswith(RUN_LOG_FILE)
    content = (log_dir / RUN_LOG_FILE).read_text(encoding="utf-8").strip()
    assert json.loads(content) == record


def test_summarize_generated_files_return_expected_standardized_paths():
    files = summarize_generated_files("golgg/data/gold/infographic_ready/standardized", "CBLOL_2026_Split_1")

    assert files[0].endswith("CBLOL_2026_Split_1_player_match_highlights.csv")
    assert files[-1].endswith("CBLOL_2026_Split_1_missing_metrics.csv")
    assert len(files) == 8

