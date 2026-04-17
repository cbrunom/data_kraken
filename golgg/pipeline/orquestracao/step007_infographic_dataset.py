# -*- coding: utf-8 -*-
"""
Build infographic-ready datasets from existing pipeline outputs.

MVP focus:
- Objective player highlights (single best game values)
- Most played champions (top 10 + open KDA)
- General infos totals
- Team match highlights
- Best player performance (objective metrics)
- Objectives summary (available data only)
- Game highlights (most kills + placeholders for unavailable duration metrics)
- Missing metrics report
"""

import glob
import os
import pandas as pd

from golgg.pipeline.common import log_step_end, log_step_start
from golgg.pipeline.publicacao import layered_output_path, write_csv_with_compat
from golgg.observability.pipeline_runs import append_run_record, build_run_record, summarize_generated_files
from golgg.pipeline.transformacao.infographic_standardization import write_standardized_outputs
from golgg.contracts.infographic_contracts import validate_standardized_directory


RAW_DIR = "golgg/data/silver/player_raw_torneios"
FULLSTATS_DIR = "golgg/data/silver/fullstats"
TORNEIOS_DIR = "golgg/data/bronze/torneios"
OUT_DIR = "golgg/infographic_ready"
GOLD_OUT_DIR = str(layered_output_path(OUT_DIR, "gold"))
RUN_LOG_DIR = os.path.join(GOLD_OUT_DIR, "logs")


DEPRECATED_SECTION_SUFFIXES = ["summary", "matador", "objectives", "game_highlights", "all_sections"]


def safe_to_name(safe_name):
    return safe_name.replace("_", " ")


def to_numeric(df, cols):
    present = [c for c in cols if c in df.columns]
    if present:
        df[present] = df[present].apply(pd.to_numeric, errors="coerce")
    return df


def format_br_int(value):
    return f"{int(round(value)):,}".replace(",", ".")


def format_duration_from_minutes(total_minutes):
    total_seconds = int(round(float(total_minutes) * 60))
    days = total_seconds // 86400
    rem = total_seconds % 86400
    hours = rem // 3600
    rem = rem % 3600
    minutes = rem // 60
    seconds = rem % 60
    return f"{days}d {hours}h {minutes}m {seconds}s"


def format_compact_number(value, decimals=2):
    if pd.isna(value):
        return "N/A"

    try:
        numeric_value = float(value)
    except (TypeError, ValueError):
        return str(value)

    rounded_value = round(numeric_value, decimals)
    if rounded_value.is_integer():
        return str(int(rounded_value))

    return f"{rounded_value:.{decimals}f}".rstrip("0").rstrip(".")


def format_percentage(value):
    if pd.isna(value):
        return "N/A"

    try:
        numeric_value = float(value)
    except (TypeError, ValueError):
        text = str(value)
        return text if text.endswith("%") else f"{text}%"

    return f"{int(round(numeric_value))}%"


def estimate_total_time_played(fullstats_df):
    # Estimate game duration from Golds/GPM, then average per match and sum all matches.
    required = {"Partida", "Golds", "GPM"}
    if not required.issubset(set(fullstats_df.columns)):
        return None

    tmp = fullstats_df[["Partida", "Golds", "GPM"]].copy()
    to_numeric(tmp, ["Golds", "GPM"])
    tmp = tmp[tmp["GPM"] > 0]
    if tmp.empty:
        return None

    tmp["GameMinutesEst"] = tmp["Golds"] / tmp["GPM"]
    per_match = tmp.groupby("Partida", as_index=False).agg(GameMinutesEst=("GameMinutesEst", "mean"))
    return float(per_match["GameMinutesEst"].sum())


def infer_match_winner_by_gold(fullstats_df):
    # Infer winner as team with highest total gold in each match.
    required = {"Partida", "Team", "Golds", "Kills"}
    if not required.issubset(set(fullstats_df.columns)):
        return pd.DataFrame(columns=["Partida", "WinnerTeam"])

    team_game = (
        fullstats_df[["Partida", "Team", "Golds", "Kills"]]
        .groupby(["Partida", "Team"], as_index=False)
        .sum(numeric_only=True)
    )
    winners = (
        team_game.sort_values(["Partida", "Golds", "Kills"], ascending=[True, False, False])
        .drop_duplicates(subset=["Partida"], keep="first")
        [["Partida", "Team"]]
        .rename(columns={"Team": "WinnerTeam"})
    )
    return winners


def infer_match_winners(fullstats_df):
    key_columns = []
    if "GameLink" in fullstats_df.columns:
        key_columns = ["GameLink"]
    elif {"Partida", "Game"}.issubset(set(fullstats_df.columns)):
        key_columns = ["Partida", "Game"]
    elif "Partida" in fullstats_df.columns:
        key_columns = ["Partida"]

    if key_columns and {"WinnerTeam"}.issubset(set(fullstats_df.columns)):
        winner_cols = key_columns + ["WinnerTeam"]
        winners = fullstats_df[winner_cols].copy()
        winners = winners[winners["WinnerTeam"].notna() & (winners["WinnerTeam"].astype(str) != "N/A")]
        winners = winners.drop_duplicates()
        if not winners.empty:
            return winners

    winners = infer_match_winner_by_gold(fullstats_df)
    if winners.empty:
        return winners

    if key_columns and "Partida" in winners.columns:
        if len(key_columns) == 1 and key_columns[0] == "GameLink" and "GameLink" in fullstats_df.columns:
            game_lookup = fullstats_df[["GameLink"]].drop_duplicates()
            return game_lookup.merge(winners, on="GameLink", how="left")
        if {"Partida", "Game"}.issubset(set(fullstats_df.columns)):
            game_lookup = fullstats_df[["Partida", "Game"]].drop_duplicates()
            return game_lookup.merge(winners, on="Partida", how="left")

    return winners


def get_total_games(safe_name):
    matches = glob.glob(os.path.join(TORNEIOS_DIR, "*", f"{safe_name}.csv"))
    if not matches:
        return None
    dfm = pd.read_csv(matches[0])
    if "Link" in dfm.columns:
        return int((dfm["Link"] != "-").sum())
    return int(len(dfm))


def build_general_infos(safe_name, tournament_name, fullstats_df):
    cols = ["Kills", "Deaths", "Assists", "CS", "Golds", "Total damage to Champion"]
    to_numeric(fullstats_df, cols)

    total_games = get_total_games(safe_name)
    total_time_minutes = estimate_total_time_played(fullstats_df)

    out = {
        "Tournament": tournament_name,
        "Total Games": total_games,
        "Total Kills": int(fullstats_df["Kills"].sum()),
        "Total Deaths": int(fullstats_df["Deaths"].sum()),
        "Total Assists": int(fullstats_df["Assists"].sum()),
        "Total CS": int(fullstats_df["CS"].sum()),
        "Total Gold": format_br_int(fullstats_df["Golds"].sum()),
        "Total DMG": format_br_int(fullstats_df["Total damage to Champion"].sum()),
        "Total Time Played": format_duration_from_minutes(total_time_minutes)
        if total_time_minutes is not None
        else "N/A (duration not collected)",
    }
    return pd.DataFrame([out])


def build_player_match_highlights(fullstats_df):
    metrics = [
        ("Best KDA", "KDA", "max"),
        ("Most Kills", "Kills", "max"),
        ("Most Solo KILLS", "Solo kills", "max"),
        ("Most Assists", "Assists", "max"),
        ("Best DPM", "DPM", "max"),
        ("Best CSM", "CSM", "max"),
        ("Best GD@15", "GD@15", "max"),
        ("Best VSPM", "VSPM", "max"),
    ]

    to_numeric(fullstats_df, [m[1] for m in metrics if m[1] in fullstats_df.columns])
    rows = []

    for label, col, direction in metrics:
        if col not in fullstats_df.columns or fullstats_df[col].dropna().empty:
            rows.append(
                {
                    "Highlight": label,
                    "Metric": col,
                    "Player": "N/A",
                    "Team": "N/A",
                    "Champ": "N/A",
                    "Partida": "N/A",
                    "Game": "N/A",
                    "Value": "N/A (metric not collected)",
                }
            )
            continue

        idx = fullstats_df[col].idxmax() if direction == "max" else fullstats_df[col].idxmin()
        row = fullstats_df.loc[idx]
        rows.append(
            {
                "Highlight": label,
                "Metric": col,
                "Player": row.get("Player", "N/A"),
                "Team": row.get("Team", "N/A"),
                "Champ": row.get("Champ", "N/A"),
                "Partida": row.get("Partida", "N/A"),
                "Game": row.get("Game", "N/A"),
                "Value": round(float(row[col]), 2),
            }
        )
    return pd.DataFrame(rows)


def build_top_kda(fullstats_df):
    cols = ["Kills", "Deaths", "Assists"]
    to_numeric(fullstats_df, cols)

    required = {"Player", "Team", "Role"}
    if not required.issubset(set(fullstats_df.columns)):
        return pd.DataFrame(columns=["Rank", "Player", "Team", "Role", "Kills", "Deaths", "Assists", "KDA", "KDA_Open", "Match_count"])

    grouped = (
        fullstats_df[["Player", "Team", "Role", "Kills", "Deaths", "Assists"]]
        .groupby(["Player", "Team", "Role"], as_index=False)
        .sum(numeric_only=True)
    )
    grouped["KDA"] = (grouped["Kills"] + grouped["Assists"]) / grouped["Deaths"].replace(0, pd.NA)
    grouped["KDA"] = grouped["KDA"].fillna(grouped["Kills"] + grouped["Assists"])
    grouped["KDA_Open"] = (
        grouped["Kills"].round(0).astype(int).astype(str)
        + "/"
        + grouped["Deaths"].round(0).astype(int).astype(str)
        + "/"
        + grouped["Assists"].round(0).astype(int).astype(str)
    )
    match_counts = fullstats_df.groupby(["Player", "Team", "Role"], as_index=False).size().rename(columns={"size": "Match_count"})
    grouped = grouped.merge(match_counts, on=["Player", "Team", "Role"], how="left")
    grouped = grouped.sort_values(["KDA", "Kills", "Assists"], ascending=[False, False, False]).head(5).copy()
    grouped.insert(0, "Rank", range(1, len(grouped) + 1))
    return grouped[["Rank", "Player", "Team", "Role", "Kills", "Deaths", "Assists", "KDA", "KDA_Open", "Match_count"]]


def build_most_kills_single_game(fullstats_df):
    to_numeric(fullstats_df, ["Kills", "Deaths", "Assists", "DPM"])
    required = {"Player", "Team", "Champ", "Partida"}
    if not required.issubset(set(fullstats_df.columns)):
        return pd.DataFrame(columns=["Rank", "Player", "Team", "Champ", "Partida", "Game", "Kills", "Deaths", "Assists"])

    ranked = fullstats_df.sort_values(["Kills", "Assists", "DPM"], ascending=[False, False, False]).head(5).copy()
    ranked.insert(0, "Rank", range(1, len(ranked) + 1))
    columns = ["Rank", "Player", "Team", "Champ", "Partida", "Game", "Kills", "Deaths", "Assists"]
    available = [column for column in columns if column in ranked.columns]
    return ranked[available]


def build_champion_outputs(fullstats_df):
    base_columns = ["Champ", "Kills", "Deaths", "Assists"]
    for optional_column in ["Team", "Partida", "Game", "GameLink"]:
        if optional_column in fullstats_df.columns:
            base_columns.append(optional_column)

    champs = fullstats_df[base_columns].copy()
    to_numeric(champs, ["Kills", "Deaths", "Assists"])

    grouped = champs.groupby("Champ", as_index=False).agg(
        Games=("Champ", "size"),
        Kills=("Kills", "sum"),
        Deaths=("Deaths", "sum"),
        Assists=("Assists", "sum"),
    )
    grouped["KDA"] = (grouped["Kills"] + grouped["Assists"]) / grouped["Deaths"].replace(0, pd.NA)
    grouped["KDA"] = grouped["KDA"].fillna(grouped["Kills"] + grouped["Assists"])
    grouped["KDA_Open"] = (
        grouped["KDA"].round(2).astype(str)
        + " - "
        + grouped["Kills"].astype(int).astype(str)
        + "/"
        + grouped["Deaths"].astype(int).astype(str)
        + "/"
        + grouped["Assists"].astype(int).astype(str)
    )
    if {"Team", "GameLink"}.issubset(set(champs.columns)):
        winners = infer_match_winners(fullstats_df)
        champ_match = champs[["Champ", "Team", "GameLink"]].drop_duplicates().rename(columns={"Team": "TeamMatch"})
        champ_match = pd.merge(champ_match, winners, on=["GameLink"], how="left")
        champ_match["Win"] = (champ_match["TeamMatch"] == champ_match["WinnerTeam"]).astype(int)
        wr = champ_match.groupby("Champ", as_index=False).agg(Wins=("Win", "sum"), GamesWR=("GameLink", "count"))
        wr["WinRate%"] = round((wr["Wins"] / wr["GamesWR"]).fillna(0) * 100, 2)
        grouped = pd.merge(grouped, wr[["Champ", "WinRate%"]], on="Champ", how="left")
    elif {"Team", "Partida", "Game"}.issubset(set(champs.columns)):
        winners = infer_match_winners(fullstats_df)
        champ_match = champs[["Champ", "Team", "Partida", "Game"]].drop_duplicates().rename(columns={"Team": "TeamMatch"})
        champ_match = pd.merge(champ_match, winners, on=["Partida", "Game"], how="left")
        champ_match["Win"] = (champ_match["TeamMatch"] == champ_match["WinnerTeam"]).astype(int)
        wr = champ_match.groupby("Champ", as_index=False).agg(Wins=("Win", "sum"), GamesWR=("Game", "count"))
        wr["WinRate%"] = round((wr["Wins"] / wr["GamesWR"]).fillna(0) * 100, 2)
        grouped = pd.merge(grouped, wr[["Champ", "WinRate%"]], on="Champ", how="left")
    else:
        grouped["WinRate%"] = "N/A"

    grouped = grouped.sort_values("Games", ascending=False)
    top10 = grouped.head(10).copy()
    top10.insert(0, "Rank", range(1, len(top10) + 1))
    top10 = top10[["Rank", "Champ", "Games", "WinRate%", "KDA", "KDA_Open"]]

    summary = pd.DataFrame(
        [
            {
                "Different Champions": int(grouped["Champ"].nunique()),
            }
        ]
    )
    return summary, top10.round(2)


def build_team_match_highlights(fullstats_df):
    required = {"Partida", "Game", "Team", "Kills"}
    if not required.issubset(set(fullstats_df.columns)):
        return pd.DataFrame()

    to_numeric(fullstats_df, ["Kills"])
    team_game = fullstats_df[["Partida", "Game", "Team", "Kills"]].groupby(["Partida", "Game", "Team"], as_index=False).sum(numeric_only=True)
    game_totals = team_game.groupby(["Partida", "Game"], as_index=False).agg(TotalKills=("Kills", "sum"))

    context_cols = ["Partida", "Game", "WinnerTeam", "Stage", "GameDuration"]
    available_context_cols = [column for column in context_cols if column in fullstats_df.columns]
    context_df = fullstats_df[available_context_cols].drop_duplicates(subset=["Partida", "Game"])
    game_totals = game_totals.merge(context_df, on=["Partida", "Game"], how="left")

    if game_totals.empty:
        return pd.DataFrame()

    top_row = game_totals.sort_values(["TotalKills"], ascending=False).iloc[0]
    return pd.DataFrame(
        [
            {
                "Highlight": "Game with the most kills",
                "Metric": "Total Kills",
                "Team": top_row.get("WinnerTeam", "N/A"),
                "Partida": top_row.get("Partida", "N/A"),
                "Stage": top_row.get("Stage", "N/A"),
                "Game": top_row.get("Game", "N/A"),
                "Duration": top_row.get("GameDuration", "N/A"),
                "Value": int(top_row.get("TotalKills", 0)),
            }
        ]
    )


def build_best_players_performance(raw_df):
    metrics = [
        ("KDA", "KDA", "max"),
        ("Kills", "Kills", "max"),
        ("Deaths", "Deaths", "min"),
        ("Assists", "Assists", "max"),
        ("CSM", "CSM", "max"),
        ("DPM", "DPM", "max"),
        ("GPM", "GPM", "max"),
    ]
    to_numeric(raw_df, [m[1] for m in metrics if m[1] in raw_df.columns])

    rows = []
    for label, col, direction in metrics:
        if col not in raw_df.columns or raw_df[col].dropna().empty:
            rows.append({"Metric": label, "Player": "N/A", "Team": "N/A", "Value": "N/A"})
            continue
        idx = raw_df[col].idxmax() if direction == "max" else raw_df[col].idxmin()
        row = raw_df.loc[idx]
        rows.append(
            {
                "Metric": label,
                "Player": row.get("Player", "N/A"),
                "Team": row.get("Team", "N/A"),
                "Value": round(float(row[col]), 2),
            }
        )
    out = pd.DataFrame(rows)
    out.insert(0, "Scope", "Tournament")
    out.insert(1, "Split", "N/A (single tournament dataset)")
    return out


def build_objectives_summary(fullstats_df):
    to_numeric(fullstats_df, ["Objectives Stolen", "Damage dealt to turrets", "Damage dealt to buildings"])

    rows = [
        {"Objective": "Dragons", "Value": "N/A (not collected)"},
        {"Objective": "Barons", "Value": "N/A (not collected)"},
        {"Objective": "Towers", "Value": "N/A (tower count not collected)"},
        {"Objective": "Voidgrubs", "Value": "N/A (not collected)"},
    ]

    if "Objectives Stolen" in fullstats_df.columns:
        rows.append({"Objective": "Objectives Stolen (total)", "Value": int(fullstats_df["Objectives Stolen"].sum())})
    if "Damage dealt to turrets" in fullstats_df.columns:
        rows.append(
            {"Objective": "Turret Damage (total)", "Value": format_br_int(fullstats_df["Damage dealt to turrets"].sum())}
        )
    return pd.DataFrame(rows)


def build_game_highlights(fullstats_df):
    to_numeric(fullstats_df, ["Kills"])

    team_game_kills = (
        fullstats_df[["Partida", "Team", "Kills"]]
        .groupby(["Partida", "Team"], as_index=False)
        .sum(numeric_only=True)
    )
    game_kills = team_game_kills.groupby("Partida", as_index=False).agg(Total_Kills=("Kills", "sum"))
    idx = game_kills["Total_Kills"].idxmax()
    mk = game_kills.loc[idx]

    rows = [
        {"Highlight": "Shortest Game", "Partida": "N/A", "Value": "N/A (duration not collected)"},
        {"Highlight": "Longest Game", "Partida": "N/A", "Value": "N/A (duration not collected)"},
        {"Highlight": "Most Kills Game", "Partida": mk["Partida"], "Value": int(mk["Total_Kills"])},
        {"Highlight": "Blue Side WR", "Partida": "N/A", "Value": "N/A (side/winner not collected)"},
        {"Highlight": "Red Side WR", "Partida": "N/A", "Value": "N/A (side/winner not collected)"},
    ]
    return pd.DataFrame(rows)


def build_missing_metrics_report():
    rows = [
        {"Metric": "Total Time Played", "Reason": "Estimated by Golds/GPM per match"},
        {"Metric": "Shortest/Longest Game", "Reason": "Game duration not collected"},
        {"Metric": "Blue/Red Side WR", "Reason": "Side and winner per game not collected"},
        {"Metric": "Dragon/Baron/Voidgrub counts", "Reason": "Objective event counts not collected"},
        {"Metric": "Tower count", "Reason": "Only tower damage is available"},
        {"Metric": "Best Players per split (Spring/Summer)", "Reason": "Split-level grouped dataset not configured"},
    ]
    return pd.DataFrame(rows)


def format_root_outputs_for_display(outputs):
    display_outputs = {section_name: section_df.copy() for section_name, section_df in outputs.items()}

    player_df = display_outputs.get("player_match_highlights")
    if player_df is not None:
        if "Value" in player_df.columns:
            player_df["Value"] = player_df["Value"].apply(lambda value: format_compact_number(value, decimals=2))
        if "Game" in player_df.columns:
            player_df["Game"] = player_df["Game"].apply(lambda value: format_compact_number(value, decimals=0))

    champ_summary_df = display_outputs.get("champion_summary")
    if champ_summary_df is not None and "Different Champions" in champ_summary_df.columns:
        champ_summary_df["Different Champions"] = champ_summary_df["Different Champions"].apply(lambda value: format_compact_number(value, decimals=0))

    champions_df = display_outputs.get("most_played_champions")
    if champions_df is not None:
        if "Games" in champions_df.columns:
            champions_df["Games"] = champions_df["Games"].apply(lambda value: format_compact_number(value, decimals=0))
        if "WinRate%" in champions_df.columns:
            champions_df["WinRate%"] = champions_df["WinRate%"].apply(format_percentage)
        if "KDA" in champions_df.columns:
            champions_df["KDA"] = champions_df["KDA"].apply(lambda value: format_compact_number(value, decimals=2))

    team_df = display_outputs.get("team_match_highlights")
    if team_df is not None and "Value" in team_df.columns:
        team_df["Value"] = team_df["Value"].apply(lambda value: format_compact_number(value, decimals=2))
        if "Game" in team_df.columns:
            team_df["Game"] = team_df["Game"].apply(lambda value: format_compact_number(value, decimals=0))

    best_df = display_outputs.get("best_players_performance")
    if best_df is not None and "Value" in best_df.columns:
        best_df["Value"] = best_df["Value"].apply(lambda value: format_compact_number(value, decimals=2))

    top_kda_df = display_outputs.get("top_kda")
    if top_kda_df is not None:
        for column in ["Rank", "Kills", "Deaths", "Assists", "Match_count"]:
            if column in top_kda_df.columns:
                top_kda_df[column] = top_kda_df[column].apply(lambda value: format_compact_number(value, decimals=0))
        for column in ["KDA"]:
            if column in top_kda_df.columns:
                top_kda_df[column] = top_kda_df[column].apply(lambda value: format_compact_number(value, decimals=2))

    kills_df = display_outputs.get("most_kills_single_game")
    if kills_df is not None:
        for column in ["Rank", "Kills", "Deaths", "Assists", "Game"]:
            if column in kills_df.columns:
                kills_df[column] = kills_df[column].apply(lambda value: format_compact_number(value, decimals=0))

    return display_outputs


def build_outputs_bundle(safe_name, tournament_name, raw_df, fullstats_df):
    """Build all section dataframes for a tournament."""
    player_match_highlights_df = build_player_match_highlights(fullstats_df)
    champion_summary_df, champions_df = build_champion_outputs(fullstats_df)
    team_match_highlights_df = build_team_match_highlights(fullstats_df)
    best_players_df = build_best_players_performance(raw_df)
    top_kda_df = build_top_kda(fullstats_df)
    most_kills_single_game_df = build_most_kills_single_game(fullstats_df)
    missing_df = build_missing_metrics_report()

    return {
        "player_match_highlights": player_match_highlights_df,
        "champion_summary": champion_summary_df,
        "most_played_champions": champions_df,
        "team_match_highlights": team_match_highlights_df,
        "best_players_performance": best_players_df,
        "top_kda": top_kda_df,
        "most_kills_single_game": most_kills_single_game_df,
        "missing_metrics": missing_df,
    }


def write_outputs_bundle(out_dir, safe_name, outputs):
    """Persist section outputs and consolidated dataframe to disk."""
    display_outputs = format_root_outputs_for_display(outputs)

    write_csv_with_compat(
        display_outputs["player_match_highlights"],
        os.path.join(out_dir, f"{safe_name}_player_match_highlights.csv"),
        "gold",
        index=False,
    )
    write_csv_with_compat(
        display_outputs["champion_summary"],
        os.path.join(out_dir, f"{safe_name}_champion_summary.csv"),
        "gold",
        index=False,
    )
    write_csv_with_compat(
        display_outputs["most_played_champions"],
        os.path.join(out_dir, f"{safe_name}_most_played_champions.csv"),
        "gold",
        index=False,
    )
    write_csv_with_compat(
        display_outputs["team_match_highlights"],
        os.path.join(out_dir, f"{safe_name}_team_match_highlights.csv"),
        "gold",
        index=False,
    )
    write_csv_with_compat(
        display_outputs["best_players_performance"],
        os.path.join(out_dir, f"{safe_name}_best_players_performance.csv"),
        "gold",
        index=False,
    )
    write_csv_with_compat(
        display_outputs["top_kda"],
        os.path.join(out_dir, f"{safe_name}_top_kda.csv"),
        "gold",
        index=False,
    )
    write_csv_with_compat(
        display_outputs["most_kills_single_game"],
        os.path.join(out_dir, f"{safe_name}_most_kills_single_game.csv"),
        "gold",
        index=False,
    )
    write_csv_with_compat(
        display_outputs["missing_metrics"],
        os.path.join(out_dir, f"{safe_name}_missing_metrics.csv"),
        "gold",
        index=False,
    )


def cleanup_deprecated_outputs(base_out_dir, safe_name):
    for suffix in DEPRECATED_SECTION_SUFFIXES:
        legacy_root_file = os.path.join(base_out_dir, f"{safe_name}_{suffix}.csv")
        legacy_std_file = os.path.join(base_out_dir, "standardized", f"{safe_name}_{suffix}.csv")
        for path in [legacy_root_file, legacy_std_file]:
            if os.path.exists(path):
                os.remove(path)


def main():
    start_time = log_step_start("step007_infographic_dataset")
    os.makedirs(OUT_DIR, exist_ok=True)
    os.makedirs(GOLD_OUT_DIR, exist_ok=True)
    os.makedirs(RUN_LOG_DIR, exist_ok=True)

    raw_files = sorted(glob.glob(os.path.join(RAW_DIR, "raw_player_stats_*.csv")))
    if not raw_files:
        print("No raw player files found. Run step5 first.")
        return

    for raw_path in raw_files:
        safe_name = os.path.basename(raw_path).replace("raw_player_stats_", "").replace(".csv", "")
        tournament_name = safe_to_name(safe_name)
        fullstats_path = os.path.join(FULLSTATS_DIR, f"fullstats_{safe_name}.csv")
        if not os.path.exists(fullstats_path):
            print(f"Skipping {safe_name}: missing fullstats file")
            continue

        raw_df = pd.read_csv(raw_path)
        fullstats_df = pd.read_csv(fullstats_path)
        fullstats_df = fullstats_df[~fullstats_df["Torneio"].isin(["Torneio"])].copy()

        outputs = build_outputs_bundle(safe_name, tournament_name, raw_df, fullstats_df)
        write_outputs_bundle(OUT_DIR, safe_name, outputs)
        write_standardized_outputs(GOLD_OUT_DIR, safe_name, outputs, None)
        cleanup_deprecated_outputs(GOLD_OUT_DIR, safe_name)

        append_run_record(
            RUN_LOG_DIR,
            build_run_record(
                pipeline_name="step007_infographic_dataset",
                tournament_key=safe_name,
                status="written",
                generated_files=summarize_generated_files(os.path.join(GOLD_OUT_DIR, "standardized"), safe_name),
            ),
        )

        print(f"Generated infographic dataset for {tournament_name}")

    # Contract gate: fail fast if standardized outputs violate expected schema rules.
    standardized_dir = os.path.join(GOLD_OUT_DIR, "standardized")
    errors = validate_standardized_directory(standardized_dir)
    if errors:
        append_run_record(
            RUN_LOG_DIR,
            build_run_record(
                pipeline_name="step007_infographic_dataset",
                tournament_key="all",
                status="contract_failed",
                contract_errors=errors,
            ),
        )
        lines = []
        for file_name, file_errors in errors.items():
            lines.append(f"{file_name}: {', '.join(file_errors)}")
        raise ValueError("Standardized dataset contract validation failed:\n" + "\n".join(lines))

    append_run_record(
        RUN_LOG_DIR,
        build_run_record(
            pipeline_name="step007_infographic_dataset",
            tournament_key="all",
            status="contract_passed",
            generated_files=sorted(os.listdir(standardized_dir)),
        ),
    )

    log_step_end("step007_infographic_dataset", start_time)


if __name__ == "__main__":
    main()

