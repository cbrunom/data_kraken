# -*- coding: utf-8 -*-

"""Lightweight contracts for standardized infographic datasets."""

from __future__ import annotations

import os
from dataclasses import dataclass, field

import pandas as pd


@dataclass
class DatasetContract:
    required_columns: list[str]
    numeric_columns: list[str] = field(default_factory=list)
    non_null_columns: list[str] = field(default_factory=list)
    unique_key: list[str] | None = None
    min_values: dict[str, float] = field(default_factory=dict)
    max_values: dict[str, float] = field(default_factory=dict)


CONTRACTS: dict[str, DatasetContract] = {
    "most_played_champions": DatasetContract(
        required_columns=["tournament_key", "rank", "champ", "games", "winrate_pct", "kda", "kda_open"],
        numeric_columns=["rank", "games", "winrate_pct", "kda"],
        non_null_columns=["tournament_key", "champ"],
        unique_key=["tournament_key", "rank"],
        min_values={"rank": 1, "games": 1, "kda": 0, "winrate_pct": 0},
        max_values={"winrate_pct": 100},
    ),
    "player_match_highlights": DatasetContract(
        required_columns=["tournament_key", "highlight", "metric", "player", "team", "champ", "partida", "value"],
        non_null_columns=["tournament_key", "highlight", "metric"],
    ),
    "team_match_highlights": DatasetContract(
        required_columns=["tournament_key", "highlight", "metric", "team", "partida", "stage", "game", "duration", "value"],
        numeric_columns=["game", "value"],
        non_null_columns=["tournament_key", "highlight", "metric"],
    ),
    "best_players_performance": DatasetContract(
        required_columns=["tournament_key", "scope", "split", "metric", "player", "team", "value"],
        non_null_columns=["tournament_key", "metric"],
    ),
    "top_kda": DatasetContract(
        required_columns=["tournament_key", "rank", "player", "team", "role", "kills", "deaths", "assists", "kda", "kda_open", "match_count"],
        numeric_columns=["rank", "kills", "deaths", "assists", "kda", "match_count"],
        non_null_columns=["tournament_key", "player", "team", "role"],
        unique_key=["tournament_key", "rank"],
        min_values={"rank": 1, "kda": 0, "kills": 0, "deaths": 0, "assists": 0, "match_count": 1},
    ),
    "most_kills_single_game": DatasetContract(
        required_columns=["tournament_key", "rank", "player", "team", "champ", "partida", "game", "kills", "deaths", "assists"],
        numeric_columns=["rank", "game", "kills", "deaths", "assists"],
        non_null_columns=["tournament_key", "player", "team", "champ", "partida"],
        unique_key=["tournament_key", "rank"],
        min_values={"rank": 1, "game": 1, "kills": 0, "deaths": 0, "assists": 0},
    ),
    "missing_metrics": DatasetContract(
        required_columns=["tournament_key", "metric", "reason"],
        non_null_columns=["tournament_key", "metric", "reason"],
    ),
    "champion_summary": DatasetContract(
        required_columns=["tournament_key", "different_champions"],
        numeric_columns=["different_champions"],
        non_null_columns=["tournament_key"],
        unique_key=["tournament_key"],
        min_values={"different_champions": 1},
    ),
}


def infer_section_name(file_path: str) -> str:
    name = os.path.basename(file_path).replace(".csv", "")
    suffixes = sorted(CONTRACTS.keys(), key=len, reverse=True)
    for suffix in suffixes:
        if name.endswith("_" + suffix):
            return suffix
    return ""


def _is_numeric_like(series: pd.Series) -> bool:
    coerced = pd.to_numeric(series, errors="coerce")
    return coerced.notna().all()


def validate_dataframe(df: pd.DataFrame, contract: DatasetContract) -> list[str]:
    errors: list[str] = []

    for col in contract.required_columns:
        if col not in df.columns:
            errors.append(f"missing_required_column:{col}")

    if errors:
        return errors

    for col in contract.non_null_columns:
        if df[col].isna().any():
            errors.append(f"null_not_allowed:{col}")

    for col in contract.numeric_columns:
        if col in df.columns:
            non_null = df[col].dropna()
            if not non_null.empty and not _is_numeric_like(non_null):
                errors.append(f"invalid_numeric_column:{col}")
            else:
                numeric_values = pd.to_numeric(non_null, errors="coerce")
                if col in contract.min_values:
                    min_value = contract.min_values[col]
                    if (numeric_values < min_value).any():
                        errors.append(f"min_value_violation:{col}:{min_value}")
                if col in contract.max_values:
                    max_value = contract.max_values[col]
                    if (numeric_values > max_value).any():
                        errors.append(f"max_value_violation:{col}:{max_value}")

    if contract.unique_key:
        duplicates = df.duplicated(subset=contract.unique_key, keep=False)
        if duplicates.any():
            errors.append("duplicate_key:" + ",".join(contract.unique_key))

    return errors


def validate_standardized_file(file_path: str) -> list[str]:
    section = infer_section_name(file_path)
    if not section or section not in CONTRACTS:
        return ["unknown_section_contract"]
    df = pd.read_csv(file_path)
    return validate_dataframe(df, CONTRACTS[section])


def validate_standardized_directory(directory: str) -> dict[str, list[str]]:
    results: dict[str, list[str]] = {}
    for name in sorted(os.listdir(directory)):
        if not name.endswith(".csv"):
            continue
        path = os.path.join(directory, name)
        errs = validate_standardized_file(path)
        if errs:
            results[name] = errs
    return results
