# -*- coding: utf-8 -*-

"""Matador metric computation helpers."""

from __future__ import annotations

import pandas as pd


MATADOR_INPUT_COLUMNS = [
    "Player",
    "Team",
    "Role",
    "Kills",
    "Deaths",
    "Assists",
    "KDA",
    "DPM",
    "GPM",
    "KP%",
    "Objectives Stolen",
    "Match_count",
]


def _minmax(series: pd.Series, *, invert: bool = False) -> pd.Series:
    values = pd.to_numeric(series, errors="coerce").fillna(0.0)
    min_value = float(values.min())
    max_value = float(values.max())
    if max_value == min_value:
        normalized = pd.Series([0.5] * len(values), index=values.index)
    else:
        normalized = (values - min_value) / (max_value - min_value)
    if invert:
        normalized = 1.0 - normalized
    return normalized


def _tier_from_score(score: float) -> str:
    if score >= 85:
        return "S"
    if score >= 70:
        return "A"
    if score >= 55:
        return "B"
    return "C"


def compute_matador(raw_df: pd.DataFrame) -> pd.DataFrame:
    """Compute Matador ranking from tournament-level raw player stats."""
    required = set(MATADOR_INPUT_COLUMNS)
    if not required.issubset(set(raw_df.columns)):
        return pd.DataFrame(
            columns=[
                "Rank",
                "Player",
                "Team",
                "Role",
                "Matador Score",
                "Tier",
                "Kills",
                "Deaths",
                "Assists",
                "KDA",
                "DPM",
                "GPM",
                "KP%",
                "Match_count",
            ]
        )

    df = raw_df[MATADOR_INPUT_COLUMNS].copy()
    for column in ["Kills", "Deaths", "Assists", "KDA", "DPM", "GPM", "KP%", "Objectives Stolen", "Match_count"]:
        df[column] = pd.to_numeric(df[column], errors="coerce").fillna(0.0)

    score_components = {
        "Kills": _minmax(df["Kills"]) * 0.22,
        "Assists": _minmax(df["Assists"]) * 0.10,
        "KDA": _minmax(df["KDA"]) * 0.20,
        "DPM": _minmax(df["DPM"]) * 0.18,
        "GPM": _minmax(df["GPM"]) * 0.10,
        "KP%": _minmax(df["KP%"]) * 0.08,
        "Objectives Stolen": _minmax(df["Objectives Stolen"]) * 0.04,
        "Deaths": _minmax(df["Deaths"], invert=True) * 0.08,
    }

    base_score = sum(score_components.values())
    match_max = float(df["Match_count"].max()) if not df.empty else 0.0
    if match_max > 0:
        consistency_factor = 0.70 + 0.30 * (df["Match_count"] / match_max)
    else:
        consistency_factor = 1.0

    df["Matador Score"] = (base_score * consistency_factor * 100).round(2)
    df = df.sort_values("Matador Score", ascending=False).reset_index(drop=True)
    df.insert(0, "Rank", range(1, len(df) + 1))
    df["Tier"] = df["Matador Score"].apply(_tier_from_score)

    return df[
        [
            "Rank",
            "Player",
            "Team",
            "Role",
            "Matador Score",
            "Tier",
            "Kills",
            "Deaths",
            "Assists",
            "KDA",
            "DPM",
            "GPM",
            "KP%",
            "Match_count",
        ]
    ]
