# -*- coding: utf-8 -*-

"""Helpers to persist lightweight execution logs for pipeline runs."""

from __future__ import annotations

from datetime import datetime, timezone
import json
import os
from pathlib import Path


RUN_LOG_FILE = "pipeline_runs.jsonl"


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def build_run_record(
    *,
    pipeline_name: str,
    tournament_key: str,
    status: str,
    generated_files: list[str] | None = None,
    contract_errors: dict[str, list[str]] | None = None,
) -> dict[str, object]:
    return {
        "timestamp_utc": utc_now_iso(),
        "pipeline_name": pipeline_name,
        "tournament_key": tournament_key,
        "status": status,
        "generated_files": generated_files or [],
        "contract_errors": contract_errors or {},
    }


def append_run_record(log_dir: str, record: dict[str, object]) -> str:
    Path(log_dir).mkdir(parents=True, exist_ok=True)
    path = os.path.join(log_dir, RUN_LOG_FILE)
    with open(path, "a", encoding="utf-8") as file_handle:
        file_handle.write(json.dumps(record, ensure_ascii=False) + "\n")
    return path


def summarize_generated_files(out_dir: str, safe_name: str) -> list[str]:
    suffixes = [
        "player_match_highlights",
        "champion_summary",
        "most_played_champions",
        "team_match_highlights",
        "best_players_performance",
        "top_kda",
        "most_kills_single_game",
        "missing_metrics",
    ]
    return [os.path.join(out_dir, f"{safe_name}_{suffix}.csv") for suffix in suffixes]
