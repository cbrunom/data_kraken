from __future__ import annotations

import pandas as pd

from golgg.pipeline.transformacao.player_stats import recompute_kda_from_averages, strip_percentage_columns


NUMERIC_COLS = [
    "Level",
    "Kills",
    "Deaths",
    "Assists",
    "KDA",
    "CS",
    "CSM",
    "Golds",
    "GPM",
    "GOLD%",
    "Vision Score",
    "Wards placed",
    "Wards destroyed",
    "Control Wards Purchased",
    "Detector Wards Placed",
    "VSPM",
    "WPM",
    "VWPM",
    "WCPM",
    "VS%",
    "Total damage to Champion",
    "Physical Damage",
    "Magic Damage",
    "True Damage",
    "DPM",
    "DMG%",
    "K+A Per Minute",
    "KP%",
    "Solo kills",
    "Double kills",
    "Triple kills",
    "Quadra kills",
    "Penta kills",
    "GD@15",
    "CSD@15",
    "XPD@15",
    "LVLD@15",
    "Objectives Stolen",
    "Damage dealt to turrets",
    "Damage dealt to buildings",
    "Total heal",
    "Total Heals On Teammates",
    "Damage self mitigated",
    "Total Damage Shielded On Teammates",
    "Time ccing others",
    "Total Time CC Dealt",
    "Total damage taken",
    "Total Time Spent Dead",
    "Consumables purchased",
    "Items Purchased",
    "Shutdown bounty collected",
    "Shutdown bounty lost",
]

TOTAL_METRIC_COLS = [
    "Solo kills",
    "Double kills",
    "Triple kills",
    "Quadra kills",
    "Penta kills",
    "Objectives Stolen",
]


def calculate_grade(series: pd.Series) -> pd.Series:
    return round((series - series.min()) / (series.max() - series.min()) * 100, 2)


def prepare_fullstats_for_ranking(fullstats_df: pd.DataFrame) -> pd.DataFrame:
    out = fullstats_df.copy()
    out = out[~out["Torneio"].isin(["Torneio"])]
    out = strip_percentage_columns(out, ["GOLD%", "VS%", "DMG%", "KP%"])
    out = out.fillna(0)
    out[NUMERIC_COLS] = out[NUMERIC_COLS].apply(pd.to_numeric, errors="coerce")
    return out


def build_raw_player_stats(fullstats_df: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    player_avg = fullstats_df.groupby(["Player", "Team", "Role"]).mean(numeric_only=True)
    player_avg = recompute_kda_from_averages(player_avg)

    player_match_count = fullstats_df.groupby(["Player", "Team", "Role"]).size().reset_index(name="Match_count")
    player_totals = fullstats_df.groupby(["Player", "Team", "Role"])[TOTAL_METRIC_COLS].sum().reset_index()
    player_totals = player_totals.rename(columns={col: f"{col} Total" for col in TOTAL_METRIC_COLS})

    raw_player_stats = player_avg.reset_index()
    raw_player_stats = pd.merge(raw_player_stats, player_match_count, on=["Player", "Team", "Role"], how="left")
    raw_player_stats = pd.merge(raw_player_stats, player_totals, on=["Player", "Team", "Role"], how="left")
    return raw_player_stats, player_avg, player_match_count


def build_player_grades(player_avg: pd.DataFrame, player_match_count: pd.DataFrame) -> pd.DataFrame:
    player_grade = player_avg.apply(calculate_grade, axis=0).reset_index(["Player", "Role", "Team"])
    player_grade = pd.merge(player_grade, player_match_count, on=["Player", "Team", "Role"], how="left")

    player_grade["Overall_Rank"] = player_grade.sum(axis=1, numeric_only=True).rank(ascending=False)
    player_grade = player_grade.sort_values(by="Overall_Rank", ascending=True)
    player_grade = player_grade.drop_duplicates()

    cols = list(player_grade)
    player_grade = player_grade[cols]

    dfroles = pd.DataFrame(columns=["Player", "Role_Rank"])
    for role in player_grade["Role"].unique():
        player_grade_role = player_grade[player_grade["Role"] == role].copy()
        player_grade_role["Role_Rank"] = player_grade_role.sum(numeric_only=True, axis=1).rank(ascending=False)
        player_grade_role.loc[:, "Role_Rank"] = player_grade_role["Role_Rank"]
        player_grade_role.reset_index(inplace=True)
        player_grade_role = player_grade_role[["Player", "Role_Rank"]]
        dfroles = pd.concat([dfroles, player_grade_role], ignore_index=True)

    player_grade = pd.merge(player_grade, dfroles, on="Player")
    player_grade = player_grade.fillna(0)
    return player_grade
