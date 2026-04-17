# -*- coding: utf-8 -*-

"""Shared helpers used by pipeline stages."""

from pathlib import Path
from datetime import datetime, timezone
import json
import os
import re
import time
import unicodedata


USER_AGENT = (
    "Mozilla/5.0 (X11; Linux x86_64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/75.0.3770.80 Safari/537.36"
)

HEADERS = {"User-Agent": USER_AGENT}
DEFAULT_STEP_TIMING_LOG = Path("golgg/observability/step_timing_runs.jsonl")
CHAMPION_NAME_OVERRIDES = {
    "kai": "KaiSa",
    "k": "KSante",
    "rek": "RekSai",
    "jarvaniv": "JarvanIV",
}


def elapsed_label(start_time: float) -> str:
    """Return elapsed seconds label for progress messages."""
    return f"{time.time() - start_time:.2f} seconds"


def _step_timing_log_path() -> Path:
    custom_path = os.getenv("GOLGG_STEP_TIMING_LOG", "").strip()
    if custom_path:
        return Path(custom_path)
    return DEFAULT_STEP_TIMING_LOG


def _append_step_timing_log(record: dict) -> None:
    try:
        log_path = _step_timing_log_path()
        log_path.parent.mkdir(parents=True, exist_ok=True)
        with log_path.open("a", encoding="utf-8") as log_file:
            log_file.write(json.dumps(record, ensure_ascii=True) + "\n")
    except Exception:
        # Logging must never break the pipeline execution.
        return


def log_step_start(step_name: str) -> float:
    """Print a standard start banner and return the start timestamp."""
    start_time = time.time()
    _append_step_timing_log(
        {
            "event": "start",
            "step": step_name,
            "timestamp_utc": datetime.now(timezone.utc).isoformat(),
            "start_time_epoch": start_time,
        }
    )
    print(f"[START] {step_name}", flush=True)
    return start_time


def log_step_end(step_name: str, start_time: float) -> None:
    """Print a standard end banner with total elapsed time."""
    elapsed_seconds = round(time.time() - start_time, 2)
    _append_step_timing_log(
        {
            "event": "end",
            "step": step_name,
            "timestamp_utc": datetime.now(timezone.utc).isoformat(),
            "start_time_epoch": start_time,
            "elapsed_seconds": elapsed_seconds,
        }
    )
    print(f"[DONE] {step_name} in {elapsed_label(start_time)}", flush=True)


def tournament_dirs(base_dir: str):
    """Yield tournament directories in deterministic order."""
    base = Path(base_dir)
    if not base.exists():
        return []
    return [entry for entry in sorted(base.iterdir(), key=lambda p: p.name.lower()) if entry.is_dir()]


def normalize_champion_name(value: object) -> str:
    """Return a canonical champion name without punctuation.

    Examples: K'Sante -> KSante, Cho'Gath -> ChoGath, Dr. Mundo -> DrMundo.
    """
    raw = str(value).strip()
    if not raw:
        return ""

    normalized = unicodedata.normalize("NFKD", raw).encode("ascii", "ignore").decode("ascii")
    normalized = re.sub(r"[^A-Za-z0-9]+", "", normalized).strip()
    if not normalized:
        return raw

    override = CHAMPION_NAME_OVERRIDES.get(normalized.lower())
    if override:
        return override

    return normalized


def normalize_champion_square_filename(file_name: str) -> str:
    """Normalize a champion square filename while keeping the OriginalSquare suffix."""
    path = Path(file_name)
    stem = path.stem
    suffix = path.suffix
    original_square_suffix = "_OriginalSquare"

    if stem.endswith(original_square_suffix):
        champion_part = stem[: -len(original_square_suffix)]
        normalized_champion = normalize_champion_name(champion_part)
        if normalized_champion:
            return f"{normalized_champion}{original_square_suffix}{suffix}"

    normalized_stem = normalize_champion_name(stem)
    if normalized_stem:
        return f"{normalized_stem}{suffix}"

    return file_name
