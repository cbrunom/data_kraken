import pandas as pd

from golgg.pipeline.transformacao.player_stats import apply_role_overrides, normalize_kda_column


BASE_COLUMN_ORDER = [
    "Torneio",
    "Season",
    "Partida",
    "Game",
    "GameLink",
    "Stage",
    "GameDuration",
    "Player",
    "Role",
    "Team",
    "WinnerTeam",
    "LoserTeam",
    "SeriesWinner",
    "SeriesScore",
    "SeriesLoser",
]


def prepare_info_teams_for_merge(info_teams: pd.DataFrame) -> pd.DataFrame:
    info_merge = info_teams.copy()
    if "Tournament" in info_merge.columns:
        info_merge = info_merge.rename(columns={"Tournament": "Torneio"})
    else:
        info_merge["Torneio"] = "N/A"

    for required_col in ["Player", "Season", "Torneio", "Team"]:
        if required_col not in info_merge.columns:
            info_merge[required_col] = "N/A"

    return info_merge[["Player", "Season", "Torneio", "Team"]].drop_duplicates()


def merge_team_mapping(fullstats_df: pd.DataFrame, info_merge: pd.DataFrame) -> pd.DataFrame:
    merged = pd.merge(
        fullstats_df,
        info_merge,
        on=["Player", "Season", "Torneio"],
        how="left",
    )

    if merged["Team"].isna().any():
        season_unique = (
            info_merge.groupby(["Player", "Season"], as_index=False)["Team"]
            .agg(lambda values: values.iloc[0] if len(set(values)) == 1 else None)
        )
        season_unique = season_unique[season_unique["Team"].notna()]
        merged = pd.merge(
            merged,
            season_unique,
            on=["Player", "Season"],
            how="left",
            suffixes=("", "_season"),
        )
        merged["Team"] = merged["Team"].fillna(merged["Team_season"])
        merged = merged.drop(columns=["Team_season"])

    merged["Team"] = merged["Team"].fillna("N/A")
    return merged


def reorder_fullstats_columns(fullstats_df: pd.DataFrame) -> pd.DataFrame:
    ordered_columns = BASE_COLUMN_ORDER + [col for col in fullstats_df.columns if col not in BASE_COLUMN_ORDER]
    return fullstats_df.reindex(columns=ordered_columns)


def apply_fullstats_transformations(fullstats_df: pd.DataFrame, info_teams: pd.DataFrame) -> pd.DataFrame:
    transformed = fullstats_df[~fullstats_df["Torneio"].isin(["Torneio"])].copy()
    transformed = normalize_kda_column(transformed)
    transformed = apply_role_overrides(transformed)

    info_merge = prepare_info_teams_for_merge(info_teams)
    transformed = merge_team_mapping(transformed, info_merge)

    numeric_cols = transformed.select_dtypes(include=["number"]).columns
    transformed[numeric_cols] = transformed[numeric_cols].fillna(0)
    return reorder_fullstats_columns(transformed)
