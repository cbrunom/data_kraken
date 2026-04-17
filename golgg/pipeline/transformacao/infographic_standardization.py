# -*- coding: utf-8 -*-

"""Standardization helpers for infographic-ready datasets."""

import os
import re

from golgg.pipeline.common import normalize_champion_name


INVALID_TEAM_TOKENS = {"", "0", "0.0", "nan", "none", "null", "n/a"}
PLAYER_TEAM_OVERRIDES = {
    "stepz": "RED Canids",
    "cody": "Leviatan",
}


def slug_column(name: str) -> str:
    """Convert display-style column names to snake_case canonical names."""
    value = str(name).strip().lower()
    value = value.replace("%", " pct ")
    value = value.replace("@", " at ")
    value = re.sub(r"[^a-z0-9]+", "_", value)
    value = re.sub(r"_+", "_", value).strip("_")
    return value


def _normalize_token(value) -> str:
    return str(value).strip().lower()


def _is_invalid_team(value) -> bool:
    return _normalize_token(value) in INVALID_TEAM_TOKENS


def sanitize_standardized_values(df):
    """Fix known data-quality issues before persisting standardized files."""
    out = df.copy()

    for champion_column in ["champ", "champion"]:
        if champion_column in out.columns:
            out[champion_column] = out[champion_column].apply(normalize_champion_name)

    if "team" not in out.columns:
        return out

    inferred_player_teams = {}
    if {"player", "team"}.issubset(set(out.columns)):
        valid_player_rows = out[~out["team"].apply(_is_invalid_team)]
        if not valid_player_rows.empty:
            for player_name, player_rows in valid_player_rows.groupby("player"):
                modes = player_rows["team"].mode()
                inferred_player_teams[_normalize_token(player_name)] = modes.iloc[0] if not modes.empty else player_rows["team"].iloc[0]

    if "player" in out.columns:
        def _fix_team(row):
            current_team = row.get("team")
            if not _is_invalid_team(current_team):
                return current_team

            player_key = _normalize_token(row.get("player", ""))
            if player_key in inferred_player_teams:
                return inferred_player_teams[player_key]
            if player_key in PLAYER_TEAM_OVERRIDES:
                return PLAYER_TEAM_OVERRIDES[player_key]
            return current_team

        out["team"] = out.apply(_fix_team, axis=1)

    return out


def standardize_dataframe(df, tournament_key: str):
    """Return standardized dataframe copy with canonical columns."""
    out = df.copy()
    out.columns = [slug_column(c) for c in out.columns]
    out = sanitize_standardized_values(out)
    out.insert(0, "tournament_key", tournament_key)
    return out


def write_standardized_outputs(base_out_dir, safe_name, outputs, consolidated_df=None):
    """Persist standardized versions of section outputs and consolidated file."""
    std_dir = os.path.join(base_out_dir, "standardized")
    os.makedirs(std_dir, exist_ok=True)

    for section_name, section_df in outputs.items():
        std_df = standardize_dataframe(section_df, safe_name)
        std_df.to_csv(os.path.join(std_dir, f"{safe_name}_{section_name}.csv"), index=False)

    if consolidated_df is not None:
        std_all = standardize_dataframe(consolidated_df, safe_name)
        std_all.to_csv(os.path.join(std_dir, f"{safe_name}_all_sections.csv"), index=False)
