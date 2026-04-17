# -*- coding: utf-8 -*-

"""Shared transformations for player and match stats."""

from __future__ import annotations

import pandas as pd


DEFAULT_ROLE_OVERRIDES = {
}


def normalize_kda_column(df: pd.DataFrame) -> pd.DataFrame:
    """Convert K/D/A columns to numeric and normalize 'Perfect KDA' rows.

    For rows with KDA == 'Perfect KDA', KDA becomes Kills + Assists.
    """
    out = df.copy()
    out["Kills"] = pd.to_numeric(out["Kills"], errors="coerce")
    out["Deaths"] = pd.to_numeric(out["Deaths"], errors="coerce")
    out["Assists"] = pd.to_numeric(out["Assists"], errors="coerce")
    out["KDA"] = out["KDA"].astype("object")

    perfect_mask = out["KDA"] == "Perfect KDA"
    out.loc[perfect_mask, "KDA"] = out.loc[perfect_mask, "Kills"] + out.loc[perfect_mask, "Assists"]
    return out


def apply_role_overrides(df: pd.DataFrame, overrides: dict[str, str] | None = None) -> pd.DataFrame:
    """Apply known role overrides based on player names."""
    out = df.copy()
    role_overrides = overrides or DEFAULT_ROLE_OVERRIDES
    for player, role in role_overrides.items():
        out.loc[out["Player"] == player, "Role"] = role
    return out


def strip_percentage_columns(df: pd.DataFrame, columns: list[str]) -> pd.DataFrame:
    """Remove trailing % from provided columns."""
    out = df.copy()
    for column in columns:
        out[column] = out[column].astype(str).str.replace("%", "", regex=False)
    return out


def recompute_kda_from_averages(player_avg: pd.DataFrame) -> pd.DataFrame:
    """Recompute KDA from averaged K/D/A values.

    Formula: (Kills + Assists) / Deaths, with fallback to Kills + Assists when Deaths == 0.
    """
    out = player_avg.copy()
    out["KDA"] = (out["Kills"] + out["Assists"]) / out["Deaths"].replace(0, pd.NA)
    out["KDA"] = out["KDA"].fillna(out["Kills"] + out["Assists"])
    return out
