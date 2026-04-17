"""Reusable infographic section builders shared by pipeline and UI."""

from __future__ import annotations

import pandas as pd


def to_numeric(df: pd.DataFrame, cols: list[str]) -> pd.DataFrame:
    present = [c for c in cols if c in df.columns]
    if present:
        df[present] = df[present].apply(pd.to_numeric, errors="coerce")
    return df


def infer_match_winner_by_gold(fullstats_df: pd.DataFrame) -> pd.DataFrame:
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


def infer_match_winners(fullstats_df: pd.DataFrame) -> pd.DataFrame:
    key_columns: list[str] = []
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


def build_player_match_highlights(fullstats_df: pd.DataFrame) -> pd.DataFrame:
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


def build_top_kda(fullstats_df: pd.DataFrame) -> pd.DataFrame:
    cols = ["Kills", "Deaths", "Assists"]
    to_numeric(fullstats_df, cols)

    required = {"Player", "Team", "Role"}
    if not required.issubset(set(fullstats_df.columns)):
        return pd.DataFrame(
            columns=["Rank", "Player", "Team", "Role", "Kills", "Deaths", "Assists", "KDA", "KDA_Open", "Match_count"]
        )

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


def build_most_kills_single_game(fullstats_df: pd.DataFrame) -> pd.DataFrame:
    to_numeric(fullstats_df, ["Kills", "Deaths", "Assists", "DPM"])
    required = {"Player", "Team", "Champ", "Partida"}
    if not required.issubset(set(fullstats_df.columns)):
        return pd.DataFrame(columns=["Rank", "Player", "Team", "Champ", "Partida", "Game", "Kills", "Deaths", "Assists"])

    ranked = fullstats_df.sort_values(["Kills", "Assists", "DPM"], ascending=[False, False, False]).head(5).copy()
    ranked.insert(0, "Rank", range(1, len(ranked) + 1))
    columns = ["Rank", "Player", "Team", "Champ", "Partida", "Game", "Kills", "Deaths", "Assists"]
    available = [column for column in columns if column in ranked.columns]
    return ranked[available]


def build_champion_outputs(fullstats_df: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
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
