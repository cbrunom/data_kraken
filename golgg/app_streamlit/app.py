# -*- coding: utf-8 -*-

"""Streamlit app for browsing standardized infographic datasets."""

from __future__ import annotations

import re
import unicodedata
from pathlib import Path
from urllib.parse import quote, unquote

import pandas as pd
from PIL import Image, ImageOps

from golgg.pipeline.transformacao.infographic_sections import (
    build_champion_outputs,
    build_most_kills_single_game,
    build_player_match_highlights,
    build_top_kda,
)


LEGACY_STANDARDIZED_DIR = Path("golgg/infographic_ready/standardized")
GOLD_STANDARDIZED_DIR = Path("golgg/data/gold/infographic_ready/standardized")
STANDARDIZED_DIR = GOLD_STANDARDIZED_DIR
LEGACY_FULLSTATS_DIR = Path("golgg/fullstats")
SILVER_FULLSTATS_DIR = Path("golgg/data/silver/fullstats")
LEGACY_INFO_TEAMS_PATH = Path("golgg/source/info_teams.csv")
SILVER_INFO_TEAMS_PATH = Path("golgg/data/silver/info_teams.csv")
PLAYER_IMAGES_DIR = Path("golgg/images/player_images")
TEAM_LOGOS_DIR = Path("golgg/images/team_logos")
CHAMPION_SQUARES_DIR = Path("golgg/images/champion_squares")
REPO_URL = "https://github.com/cbrunom/data_kraken"
DATA_KRAKEN_LOGO_PATH = Path("golgg/assets/data_kraken_logo.png")
DATA_KRAKEN_LOGO_URL = "https://raw.githubusercontent.com/cbrunom/data_kraken/main/golgg/assets/data_kraken_logo.png"
PLAYER_MAPPING_FILE = PLAYER_IMAGES_DIR / "player_images_mapping.csv"
TEAM_MAPPING_FILE = TEAM_LOGOS_DIR / "team_logos_mapping.csv"
DISPLAY_IMAGE_SIZE = (123, 123)
SECTION_ICON_SIZE = 84
TOP5_ICON_SIZE = 64
TEAM_KEY_ALIASES = {
    "los_grandes": "los",
    "vks": "vivo_keyd_stars",
    "vivo_keyd": "vivo_keyd_stars",
    "pain": "pain_gaming",
    "pain_gaming": "pain_gaming",
    "red": "red_canids",
    "red_canids_kalunga": "red_canids",
}
CHAMPION_KEY_ALIASES = {
    "kai": "kai_sa",
    "ksante": "k_sante",
    "kaisa": "kai_sa",
    "rek": "rek_sai",
    "reksai": "rek_sai",
    "velkoz": "vel_koz",
    "chogath": "cho_gath",
    "drmundo": "dr_mundo",
    "jarvaniv": "jarvan_iv",
    "masteryi": "master_yi",
    "missfortune": "miss_fortune",
    "renata": "renata_glasc",
    "tahmkench": "tahm_kench",
    "twistedfate": "twisted_fate",
    "xinzhao": "xin_zhao",
    "aurelionsol": "aurelion_sol",
    "belveth": "bel_veth",
    "kogmaw": "kog_maw",
}
PLAYER_IMAGE_URL_FALLBACKS = {
    ("red_canids", "stepz"): "https://static.wikia.nocookie.net/lolesports_gamepedia_en/images/1/18/RED_STEPZ_2026_Split_1.png/revision/latest?cb=20260327042536",
    ("red_canids", "zynts"): "https://static.wikia.nocookie.net/lolesports_gamepedia_en/images/7/78/RED_Zynts_2025_Split_1.png/revision/latest/scale-to-width-down/220?cb=20250319184040",
    ("leviatan", "cody"): "https://static.wikia.nocookie.net/lolesports_gamepedia_en/images/3/3b/FX_Cody_2026_Split_1.png/revision/latest",
}
SECTION_ORDER = [
    "champion_summary",
    "most_played_champions",
    "player_match_highlights",
    "team_match_highlights",
    "best_players_performance",
    "top_kda",
    "most_kills_single_game",
    "missing_metrics",
]

PARITY_SECTIONS = [
    "player_match_highlights",
    "champion_summary",
    "most_played_champions",
    "team_match_highlights",
    "best_players_performance",
    "top_kda",
    "most_kills_single_game",
]

SECTION_TITLES = {
    "champion_summary": "Champion Summary",
    "most_played_champions": "Most Played Champions",
    "player_match_highlights": "Player Match Highlights",
    "team_match_highlights": "Team Match Highlights",
    "best_players_performance": "Best Players Performance",
    "top_kda": "Top KDA",
    "most_kills_single_game": "Most Kills in a Single Game",
    "missing_metrics": "Missing Metrics",
}

SECTION_DESCRIPTIONS = {
    "champion_summary": "How many different champions appeared.",
    "most_played_champions": "Most used champions, with KDA and win rate.",
    "player_match_highlights": "Best individual match moments by metric.",
    "team_match_highlights": "Game with the most kills by match context.",
    "best_players_performance": "Best players per tournament metric.",
    "top_kda": "Top 5 players by tournament KDA, with raw K/D/A totals.",
    "most_kills_single_game": "Top 5 player-game rows by kills.",
    "missing_metrics": "Known metrics not collected yet.",
}

ROLE_DISPLAY_ORDER = ["TOP", "JUNGLE", "MID", "ADC", "SUPPORT"]
ROLE_DISPLAY_NAMES = {
    "TOP": "Top",
    "JUNGLE": "Jungle",
    "MID": "Mid",
    "ADC": "ADC",
    "SUPPORT": "Support",
}
ALL_LANES_LABEL = "All lanes"

HIDDEN_UI_SECTIONS = {
    "champion_summary",
    "team_match_highlights",
    "player_match_highlights",
    "best_players_performance",
}


def read_csv_if_exists(path: Path) -> pd.DataFrame:
    if path.exists():
        return pd.read_csv(path)
    return pd.DataFrame()


def normalize_entity_name(value: str | object) -> str:
    text = unicodedata.normalize("NFKD", str(value)).encode("ascii", "ignore").decode("ascii")
    text = text.lower().replace("&", " and ")
    text = re.sub(r"[^a-z0-9]+", "_", text)
    return text.strip("_")


def load_display_image(image_path: Path, size: tuple[int, int] = DISPLAY_IMAGE_SIZE) -> Image.Image:
    image = Image.open(image_path).convert("RGBA")
    resampling = getattr(Image, "Resampling", Image)
    contained = ImageOps.contain(image, size, method=resampling.LANCZOS)
    canvas = Image.new("RGBA", size, (0, 0, 0, 0))
    offset_x = (size[0] - contained.width) // 2
    offset_y = (size[1] - contained.height) // 2
    canvas.paste(contained, (offset_x, offset_y), contained)
    return canvas


def render_media_image(
    st,
    media: Path | str,
    *,
    caption: str | None = None,
    use_container_width: bool = True,
    width: int | None = None,
) -> None:
    image_kwargs = {"caption": caption}
    if width is not None:
        image_kwargs["width"] = width
        image_kwargs["use_container_width"] = False
    else:
        image_kwargs["use_container_width"] = use_container_width

    if isinstance(media, Path):
        st.image(load_display_image(media), **image_kwargs)
        return
    st.image(media, **image_kwargs)


def format_compact_number(value: object, *, decimals: int = 2) -> str:
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


def format_percentage(value: object, *, decimals: int = 0) -> str:
    if pd.isna(value):
        return "N/A"

    try:
        numeric_value = float(value)
    except (TypeError, ValueError):
        text = str(value)
        if not text or text.upper().startswith("N/A"):
            return "N/A"
        return text if text.endswith("%") else f"{text}%"

    if decimals == 0:
        return f"{int(round(numeric_value))}%"
    return f"{numeric_value:.{decimals}f}%"


def format_section_for_display(section_name: str, df: pd.DataFrame) -> pd.DataFrame:
    display_df = df.copy()

    if section_name == "most_played_champions":
        if "winrate_pct" in display_df.columns:
            display_df["winrate_pct"] = display_df["winrate_pct"].apply(format_percentage)
        if "kda" in display_df.columns:
            display_df["kda"] = display_df["kda"].apply(lambda value: format_compact_number(value, decimals=2))
        if "rank" in display_df.columns:
            display_df["rank"] = display_df["rank"].apply(lambda value: format_compact_number(value, decimals=0))
        if "games" in display_df.columns:
            display_df["games"] = display_df["games"].apply(lambda value: format_compact_number(value, decimals=0))

    if section_name in {"player_match_highlights", "team_match_highlights", "best_players_performance"}:
        if "Value" in display_df.columns:
            display_df["Value"] = display_df["Value"].apply(lambda value: format_compact_number(value, decimals=2))
        if "value" in display_df.columns:
            display_df["value"] = display_df["value"].apply(lambda value: format_compact_number(value, decimals=2))

    if section_name == "player_match_highlights":
        if "Game" in display_df.columns:
            display_df["Game"] = display_df["Game"].apply(lambda value: format_compact_number(value, decimals=0))

    if section_name == "team_match_highlights":
        if "Game" in display_df.columns:
            display_df["Game"] = display_df["Game"].apply(lambda value: format_compact_number(value, decimals=0))

    if section_name == "top_kda":
        for column in ["rank", "kills", "deaths", "assists", "match_count"]:
            if column in display_df.columns:
                display_df[column] = display_df[column].apply(lambda value: format_compact_number(value, decimals=0))
        if "kda" in display_df.columns:
            display_df["kda"] = display_df["kda"].apply(lambda value: format_compact_number(value, decimals=2))

    if section_name == "most_kills_single_game":
        for column in ["rank", "kills", "deaths", "assists", "game"]:
            if column in display_df.columns:
                display_df[column] = display_df[column].apply(lambda value: format_compact_number(value, decimals=0))

    if section_name == "champion_summary" and "different_champions" in display_df.columns:
        display_df["different_champions"] = display_df["different_champions"].apply(lambda value: format_compact_number(value, decimals=0))

    return display_df


def load_fullstats_for_tournament(tournament_key: str) -> pd.DataFrame:
    fullstats_path = SILVER_FULLSTATS_DIR / f"fullstats_{tournament_key}.csv"
    if fullstats_path.exists():
        return pd.read_csv(fullstats_path)

    legacy_fullstats_path = LEGACY_FULLSTATS_DIR / f"fullstats_{tournament_key}.csv"
    return read_csv_if_exists(legacy_fullstats_path)


def normalize_role_value(value: object) -> str:
    return str(value).strip().upper()


def discover_role_labels(fullstats_df: pd.DataFrame) -> list[str]:
    labels = [ALL_LANES_LABEL]
    if fullstats_df.empty or "Role" not in fullstats_df.columns:
        return labels

    available_roles: list[str] = []
    seen_roles: set[str] = set()
    for raw_role in fullstats_df["Role"].dropna().astype(str):
        role_value = normalize_role_value(raw_role)
        if role_value and role_value not in seen_roles:
            seen_roles.add(role_value)
            available_roles.append(role_value)

    for role_value in ROLE_DISPLAY_ORDER:
        if role_value in seen_roles:
            labels.append(ROLE_DISPLAY_NAMES[role_value])

    for role_value in available_roles:
        if role_value not in ROLE_DISPLAY_ORDER:
            labels.append(role_value.title())

    return labels


def selected_role_value(selected_role_label: str) -> str | None:
    if selected_role_label == ALL_LANES_LABEL:
        return None

    for role_value, display_name in ROLE_DISPLAY_NAMES.items():
        if display_name == selected_role_label:
            return role_value

    return normalize_role_value(selected_role_label)


def filter_fullstats_by_role(fullstats_df: pd.DataFrame, role_label: str) -> pd.DataFrame:
    role_value = selected_role_value(role_label)
    if role_value is None or fullstats_df.empty or "Role" not in fullstats_df.columns:
        return fullstats_df

    role_mask = fullstats_df["Role"].astype(str).map(normalize_role_value) == role_value
    return fullstats_df.loc[role_mask].copy()


def standardize_app_columns(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df.copy()

    rename_map = {
        "Rank": "rank",
        "Player": "player",
        "Team": "team",
        "Role": "role",
        "Kills": "kills",
        "Deaths": "deaths",
        "Assists": "assists",
        "KDA": "kda",
        "KDA_Open": "kda_open",
        "Match_count": "match_count",
        "Champ": "champ",
        "Games": "games",
        "WinRate%": "winrate_pct",
        "Different Champions": "different_champions",
        "Partida": "partida",
        "Game": "game",
        "Highlight": "highlight",
        "Metric": "metric",
        "Value": "value",
    }
    return df.rename(columns=rename_map).copy()


def build_top5_from_fullstats(fullstats_df: pd.DataFrame, source_col: str, title: str, metric_name: str) -> pd.DataFrame:
    if fullstats_df.empty or source_col not in fullstats_df.columns:
        return pd.DataFrame()

    work_df = fullstats_df.copy()
    work_df["metric_value"] = pd.to_numeric(work_df[source_col], errors="coerce")
    work_df = work_df.dropna(subset=["metric_value"])
    if work_df.empty:
        return pd.DataFrame()

    work_df = work_df.sort_values("metric_value", ascending=False).head(5)

    return pd.DataFrame(
        {
            "highlight": title,
            "metric": metric_name,
            "player": work_df.get("Player", "N/A"),
            "team": work_df.get("Team", "N/A"),
            "champ": work_df.get("Champ", "N/A"),
            "partida": work_df.get("Partida", "N/A"),
            "game": work_df.get("Game", "N/A"),
            "value": work_df["metric_value"],
        }
    )


def build_role_filtered_views(fullstats_df: pd.DataFrame) -> dict[str, pd.DataFrame]:
    if fullstats_df.empty:
        empty = pd.DataFrame()
        return {
            "champion_summary": empty,
            "most_played_champions": empty,
            "top_kda": empty,
            "most_kills_single_game": empty,
            "top_solo_kills": empty,
            "top_assists": empty,
            "top_dpm": empty,
            "top_csm": empty,
        }

    champion_summary_df, most_played_df = build_champion_outputs(fullstats_df)
    top_kda_df = standardize_app_columns(build_top_kda(fullstats_df))
    most_kills_df = standardize_app_columns(build_most_kills_single_game(fullstats_df))

    top_solo_df = standardize_app_columns(
        build_top5_from_fullstats(fullstats_df, "Solo kills", "TOP 5 Most Solo KILLS", "Solo kills")
    )
    top_assists_df = standardize_app_columns(
        build_top5_from_fullstats(fullstats_df, "Assists", "TOP 5 Most ASSISTS", "Assists")
    )
    top_dpm_df = standardize_app_columns(
        build_top5_from_fullstats(fullstats_df, "DPM", "TOP 5 Best DPM", "DPM")
    )
    top_csm_df = standardize_app_columns(
        build_top5_from_fullstats(fullstats_df, "CSM", "TOP 5 Best CSM", "CSM")
    )

    return {
        "champion_summary": standardize_app_columns(champion_summary_df),
        "most_played_champions": standardize_app_columns(most_played_df),
        "top_kda": top_kda_df,
        "most_kills_single_game": most_kills_df,
        "top_solo_kills": top_solo_df,
        "top_assists": top_assists_df,
        "top_dpm": top_dpm_df,
        "top_csm": top_csm_df,
    }


def build_player_image_index(
    mapping_file: Path = PLAYER_MAPPING_FILE,
) -> dict[tuple[str, str], Path | str]:
    if not mapping_file.exists():
        return dict(PLAYER_IMAGE_URL_FALLBACKS)

    mapping_df = pd.read_csv(mapping_file)
    index: dict[tuple[str, str], Path | str] = {}

    def team_variants(team_key: str) -> set[str]:
        variants = {team_key}
        canonical = TEAM_KEY_ALIASES.get(team_key, team_key)
        variants.add(canonical)
        for alias_key, canonical_key in TEAM_KEY_ALIASES.items():
            if canonical_key in variants:
                variants.add(alias_key)
        return variants

    for _, row in mapping_df.iterrows():
        team_key = normalize_entity_name(row.get("team_key", ""))
        player_key = normalize_entity_name(row.get("player_key", row.get("player_name", "")))
        if not team_key or not player_key:
            continue

        source: Path | str | None = None
        target_file = row.get("target_file", "")
        target_name = Path(str(target_file).replace("\\", "/")).name
        image_path = PLAYER_IMAGES_DIR / target_name
        if image_path.exists():
            source = image_path
        else:
            image_url = str(row.get("image_url", "")).strip()
            if image_url.startswith("http"):
                source = image_url

        if source is None:
            continue

        for variant_team_key in team_variants(team_key):
            index[(variant_team_key, player_key)] = source

    for fallback_key, fallback_url in PLAYER_IMAGE_URL_FALLBACKS.items():
        if fallback_key not in index:
            index[fallback_key] = fallback_url
    return index


def build_team_logo_index(mapping_file: Path = TEAM_MAPPING_FILE) -> dict[str, Path]:
    if not mapping_file.exists():
        return {}

    mapping_df = pd.read_csv(mapping_file)
    index: dict[str, Path] = {}
    for _, row in mapping_df.iterrows():
        team_key = normalize_entity_name(row.get("team_key", ""))
        if not team_key:
            continue

        target_file = row.get("target_file", "")
        target_name = Path(str(target_file).replace("\\", "/")).name
        image_path = TEAM_LOGOS_DIR / target_name
        if image_path.exists():
            index[team_key] = image_path
            stem_key = normalize_entity_name(Path(target_name).stem)
            if stem_key and stem_key not in index:
                index[stem_key] = image_path

    for alias_key, canonical_key in TEAM_KEY_ALIASES.items():
        if canonical_key in index and alias_key not in index:
            index[alias_key] = index[canonical_key]
    return index


def build_champion_image_index(champion_dir: Path = CHAMPION_SQUARES_DIR) -> dict[str, Path]:
    if not champion_dir.exists():
        return {}

    index: dict[str, Path] = {}
    for image_path in sorted(champion_dir.glob("*_OriginalSquare.png")):
        champ_name = image_path.name.removesuffix("_OriginalSquare.png")
        candidate_keys = []
        normalized = normalize_entity_name(champ_name)
        if normalized:
            candidate_keys.append(normalized)

        compact_normalized = normalized.replace("_", "") if normalized else ""
        if compact_normalized and compact_normalized not in candidate_keys:
            candidate_keys.append(compact_normalized)

        stripped_name = champ_name.replace("'", "").replace("â€™", "")
        stripped_normalized = normalize_entity_name(stripped_name)
        if stripped_normalized and stripped_normalized not in candidate_keys:
            candidate_keys.append(stripped_normalized)

        for candidate_key in candidate_keys:
            if candidate_key not in index:
                index[candidate_key] = image_path
    return index


def resolve_player_image(
    team_name: object,
    player_name: object,
    player_index: dict[tuple[str, str], Path | str],
) -> Path | str | None:
    team_key = normalize_entity_name(team_name)
    player_key = normalize_entity_name(player_name)
    if not team_key or not player_key:
        return None

    candidate_teams = [team_key]
    if team_key in TEAM_KEY_ALIASES:
        candidate_teams.append(TEAM_KEY_ALIASES[team_key])
    for alias_key, canonical_key in TEAM_KEY_ALIASES.items():
        if canonical_key == team_key and alias_key not in candidate_teams:
            candidate_teams.append(alias_key)

    for candidate_team_key in candidate_teams:
        value = player_index.get((candidate_team_key, player_key))
        if value:
            return value

    # Fallback: if team mapping misses, try matching by player name across any team.
    for (indexed_team_key, indexed_player_key), value in player_index.items():
        if indexed_player_key == player_key and value:
            return value

    return PLAYER_IMAGE_URL_FALLBACKS.get((team_key, player_key))


def resolve_team_logo(team_name: object, team_index: dict[str, Path]) -> Path | None:
    team_key = normalize_entity_name(team_name)
    if not team_key:
        return None
    if team_key in TEAM_KEY_ALIASES:
        team_key = TEAM_KEY_ALIASES[team_key]
    return team_index.get(team_key)


def champion_to_ddragon_id(champion_name: object) -> str:
    raw = str(champion_name).strip()
    if not raw:
        return ""

    special = {
        "wukong": "MonkeyKing",
        "dr mundo": "DrMundo",
        "dr. mundo": "DrMundo",
        "jarvan iv": "JarvanIV",
        "ksante": "KSante",
        "k sante": "KSante",
        "rek": "RekSai",
        "rek sai": "RekSai",
        "cho gath": "Chogath",
        "kai sa": "KaiSa",
        "vel koz": "Velkoz",
        "bel veth": "Belveth",
        "kog maw": "KogMaw",
        "lee sin": "LeeSin",
        "master yi": "MasterYi",
        "miss fortune": "MissFortune",
        "renata glasc": "Renata",
        "tahm kench": "TahmKench",
        "twisted fate": "TwistedFate",
        "xin zhao": "XinZhao",
        "aurelion sol": "AurelionSol",
    }
    normalized = normalize_entity_name(raw).replace("_", " ")
    if normalized in special:
        return special[normalized]

    tokenized = re.sub(r"[^A-Za-z0-9]+", " ", raw).strip().split()
    if not tokenized:
        return ""
    return "".join(token.capitalize() for token in tokenized)


def champion_ddragon_url(champion_name: object) -> str | None:
    champion_id = champion_to_ddragon_id(champion_name)
    if not champion_id:
        return None
    return f"https://ddragon.leagueoflegends.com/cdn/14.24.1/img/champion/{quote(champion_id)}.png"


def resolve_champion_image(champion_name: object, champion_index: dict[str, Path]) -> Path | str | None:
    champion_key = normalize_entity_name(champion_name)
    if not champion_key:
        return None

    if champion_key in {"na", "n_a", "none", "nan", "0"}:
        return None

    # Handle name variants like K'Sante/KSante, Kai'Sa/KaiSa, Rek'Sai/RekSai.
    candidate_keys = [
        champion_key,
        champion_key.replace("_", ""),
        champion_key.replace("'", ""),
        champion_key.replace("â€™", ""),
        champion_key.replace("_", "").replace("'", ""),
        champion_key.replace("_", "").replace("â€™", ""),
    ]

    if champion_key in CHAMPION_KEY_ALIASES:
        alias_key = CHAMPION_KEY_ALIASES[champion_key]
        if alias_key not in candidate_keys:
            candidate_keys.append(alias_key)

    if champion_key.replace("_", "") in CHAMPION_KEY_ALIASES:
        alias_key = CHAMPION_KEY_ALIASES[champion_key.replace("_", "")]
        if alias_key not in candidate_keys:
            candidate_keys.append(alias_key)

    for candidate_key in candidate_keys:
        local_path = champion_index.get(candidate_key)
        if local_path:
            return local_path

    return champion_ddragon_url(champion_name)


def collect_section_media(
    section_name: str,
    df: pd.DataFrame,
    player_index: dict[tuple[str, str], Path | str],
    team_index: dict[str, Path],
    champion_index: dict[str, Path],
) -> list[tuple[str, Path | str]]:
    if df.empty:
        return []

    media_items: list[tuple[str, Path | str]] = []

    if section_name in {"player_match_highlights", "best_players_performance"}:
        for _, row in df.head(8).iterrows():
            player_path = resolve_player_image(row.get("team", ""), row.get("player", ""), player_index)
            if player_path:
                media_items.append((f"Player: {row.get('player', 'N/A')}", player_path))

    if section_name in {"player_match_highlights", "most_played_champions"}:
        column_name = "champ" if "champ" in df.columns else "player"
        for _, row in df.head(8).iterrows():
            champ_path = resolve_champion_image(row.get(column_name, ""), champion_index)
            if champ_path:
                media_items.append((f"Champion: {row.get(column_name, 'N/A')}", champ_path))

    if section_name in {"team_match_highlights", "best_players_performance", "player_match_highlights", "top_kda", "most_kills_single_game"}:
        for _, row in df.head(8).iterrows():
            team_path = resolve_team_logo(row.get("team", ""), team_index)
            if team_path:
                media_items.append((f"Team: {row.get('team', 'N/A')}", team_path))

    unique: list[tuple[str, Path | str]] = []
    seen = set()
    for label, image_path in media_items:
        key = str(image_path)
        if key in seen:
            continue
        seen.add(key)
        unique.append((label, image_path))
    return unique


def render_section_media(st, section_name: str, df: pd.DataFrame) -> None:
    player_index = build_player_image_index()
    team_index = build_team_logo_index()
    champion_index = build_champion_image_index()

    media_items = collect_section_media(section_name, df, player_index, team_index, champion_index)
    if not media_items:
        st.info("No media found for this section. Showing data only.")
        return

    st.caption("Media associated with entities in this section")
    columns = st.columns(4)
    for idx, (label, image_path) in enumerate(media_items[:8]):
        with columns[idx % 4]:
            render_media_image(st, image_path, caption=label, use_container_width=True)


def resolve_player_image_any_team(player_name: object, player_index: dict[tuple[str, str], Path | str]) -> Path | str | None:
    player_key = normalize_entity_name(player_name)
    if not player_key:
        return None

    for (indexed_team_key, indexed_player_key), value in player_index.items():
        if indexed_player_key == player_key and value:
            return value

    return None


def collect_unique_values(df: pd.DataFrame, candidate_columns: list[str]) -> list[str]:
    if df.empty:
        return []

    values: list[str] = []
    seen: set[str] = set()
    for column_name in candidate_columns:
        if column_name not in df.columns:
            continue
        for raw_value in df[column_name].dropna().astype(str).tolist():
            value = str(raw_value).strip()
            if not value:
                continue
            key = normalize_entity_name(value)
            if key in seen:
                continue
            seen.add(key)
            values.append(value)
    return sorted(values, key=lambda value: normalize_entity_name(value))


def render_inventory_grid(
    st,
    title: str,
    entities: list[str],
    resolver,
    index_builder,
    *,
    missing_label: str,
    width: int = SECTION_ICON_SIZE,
) -> None:
    st.markdown(f'<div class="inventory-section-title">{title}</div>', unsafe_allow_html=True)
    st.caption(f"{len(entities)} items")
    if not entities:
        st.info("No items found for this inventory.")
        return

    image_index = index_builder()
    missing_entities: list[str] = []
    columns_per_row = 4
    for start in range(0, len(entities), columns_per_row):
        row_entities = entities[start:start + columns_per_row]
        columns = st.columns(columns_per_row)
        for idx, entity_name in enumerate(row_entities):
            with columns[idx]:
                image_path = resolver(entity_name, image_index)
                if image_path:
                    render_media_image(st, image_path, caption=entity_name, use_container_width=False, width=width)
                else:
                    missing_entities.append(entity_name)
                    st.markdown(
                        f'<div class="inventory-missing-box">{entity_name[:3].upper()}</div>',
                        unsafe_allow_html=True,
                    )
                    st.caption(entity_name)

    if missing_entities:
        st.caption(f"{missing_label}: " + ", ".join(missing_entities))


def render_debug_inventory_section(
    st,
    tournament_key: str,
    bundle: dict[str, pd.DataFrame],
    fullstats_df: pd.DataFrame,
) -> None:
    players = collect_unique_values(fullstats_df, ["Player", "player"])
    champions = collect_unique_values(fullstats_df, ["Champ", "champ"])
    teams = collect_all_tournament_teams(tournament_key, bundle)

    st.divider()
    with st.expander("Debug inventory: players, champions, teams", expanded=False):
        st.caption("This section is for asset validation. It uses all unique names from the selected tournament and shows whether each item resolves to a local image.")

        with st.expander("Players", expanded=True):
            render_inventory_grid(
                st,
                "All players",
                players,
                resolve_player_image_any_team,
                build_player_image_index,
                missing_label="Players without image",
                width=72,
            )

        with st.expander("Champions", expanded=True):
            render_inventory_grid(
                st,
                "All champions",
                champions,
                resolve_champion_image,
                build_champion_image_index,
                missing_label="Champions without image",
                width=72,
            )

        with st.expander("Teams", expanded=True):
            render_inventory_grid(
                st,
                "All teams",
                teams,
                resolve_team_logo,
                build_team_logo_index,
                missing_label="Teams without image",
                width=72,
            )


def discover_tournaments(standardized_dir: Path = STANDARDIZED_DIR) -> list[str]:
    if not standardized_dir.exists():
        return []

    tournaments = []
    for champions_path in sorted(standardized_dir.glob("*_most_played_champions.csv")):
        tournaments.append(champions_path.name.removesuffix("_most_played_champions.csv"))
    return tournaments


def resolve_standardized_dir() -> Path:
    if GOLD_STANDARDIZED_DIR.exists() and any(GOLD_STANDARDIZED_DIR.glob("*_most_played_champions.csv")):
        return GOLD_STANDARDIZED_DIR
    return LEGACY_STANDARDIZED_DIR


def load_tournament_section(standardized_dir: Path, tournament_key: str, section_name: str) -> pd.DataFrame:
    return read_csv_if_exists(standardized_dir / f"{tournament_key}_{section_name}.csv")


def load_tournament_bundle(standardized_dir: Path, tournament_key: str) -> dict[str, pd.DataFrame]:
    return {
        section_name: load_tournament_section(standardized_dir, tournament_key, section_name)
        for section_name in SECTION_ORDER
        if (standardized_dir / f"{tournament_key}_{section_name}.csv").exists()
    }


def discover_sections(bundle: dict[str, pd.DataFrame]) -> list[str]:
    return [section_name for section_name in SECTION_ORDER if section_name in bundle]


def discover_parity_sections(bundle: dict[str, pd.DataFrame]) -> list[str]:
    return [section_name for section_name in PARITY_SECTIONS if section_name in bundle]


def missing_parity_sections(bundle: dict[str, pd.DataFrame]) -> list[str]:
    return [section_name for section_name in PARITY_SECTIONS if section_name not in bundle]


def tournament_display_name(bundle: dict[str, pd.DataFrame], tournament_key: str) -> str:
    return tournament_key.replace("_", " ")


def tournament_metrics(bundle: dict[str, pd.DataFrame]) -> dict[str, str]:
    champion_df = bundle.get("champion_summary", pd.DataFrame())
    top_df = bundle.get("most_played_champions", pd.DataFrame())

    metrics = {
        "Unique Champions": "N/A",
        "Top Champions Listed": "N/A",
        "Available Sections": str(len(discover_sections(bundle))),
    }

    if not champion_df.empty and "different_champions" in champion_df.columns:
        metrics["Unique Champions"] = str(champion_df.iloc[0].get("different_champions", "N/A"))

    if not top_df.empty:
        metrics["Top Champions Listed"] = str(len(top_df))

    return metrics


def section_title(section_name: str) -> str:
    return SECTION_TITLES.get(section_name, section_name.replace("_", " ").title())


def section_description(section_name: str) -> str:
    return SECTION_DESCRIPTIONS.get(section_name, "")


def render_hero(st, tournament_name: str, tournament_key: str) -> None:
    st.markdown(
        f"""
        <div class="recap-hero">
            <div class="recap-title-main">{tournament_name}</div>
            <div class="recap-subtitle-main">{tournament_key.replace('_', ' ')} recap</div>
            <div class="recap-kicker"><a class="repo-link" href="{REPO_URL}" target="_blank" rel="noopener noreferrer">data_kraken</a></div>
        </div>
        """,
        unsafe_allow_html=True,
    )


def render_top_banner(st, tournament_name: str, tournament_key: str, bundle: dict[str, pd.DataFrame]) -> None:
    left_col, right_col = st.columns([1.15, 2.85], gap="medium")
    with left_col:
        render_hero(st, tournament_name, tournament_key)
        if DATA_KRAKEN_LOGO_PATH.exists():
            st.image(DATA_KRAKEN_LOGO_PATH, width=260)
        else:
            st.image(DATA_KRAKEN_LOGO_URL, width=260)
    with right_col:
        render_team_banner(st, tournament_key, bundle)


def collect_all_tournament_teams(
    tournament_key: str,
    bundle: dict[str, pd.DataFrame] | None = None,
    info_teams_path: Path = SILVER_INFO_TEAMS_PATH,
) -> list[str]:
    """Load all participating teams for a tournament. Combines info_teams.csv and bundle data for complete coverage."""
    teams: list[str] = []
    all_available_teams: set[str] = set()

    active_info_teams_path = info_teams_path if info_teams_path.exists() else LEGACY_INFO_TEAMS_PATH
    if active_info_teams_path.exists():
        try:
            info_teams_df = pd.read_csv(active_info_teams_path)
            tournament_display = tournament_key.replace("_", " ")

            for _, row in info_teams_df.iterrows():
                player_page = str(row.get("player_page", ""))
                team_name = str(row.get("Team", "")).strip()

                if not team_name or not player_page:
                    continue

                all_available_teams.add(team_name)

                if tournament_display in unquote(player_page) and team_name not in teams:
                    teams.append(team_name)
        except Exception:
            pass

    if bundle:
        for df in bundle.values():
            if df.empty:
                continue

            for column_name in df.columns:
                normalized_column = str(column_name).lower()
                if "team" not in normalized_column:
                    continue

                for value in df[column_name].dropna().astype(str).tolist():
                    if value and value not in teams:
                        teams.append(value)

    for team in all_available_teams:
        if team not in teams:
            teams.append(team)

    return teams


def render_team_banner(st, tournament_key: str, bundle: dict[str, pd.DataFrame] | None = None) -> None:
    team_index = build_team_logo_index()
    teams = collect_all_tournament_teams(tournament_key, bundle)
    if not teams:
        return

    st.markdown('<div class="team-strip-title">Teams</div>', unsafe_allow_html=True)
    columns_per_row = 4
    for start in range(0, len(teams), columns_per_row):
        row_teams = teams[start:start + columns_per_row]
        columns = st.columns(columns_per_row)
        for idx, team_name in enumerate(row_teams):
            with columns[idx]:
                image_path = resolve_team_logo(team_name, team_index)
                if image_path:
                    st.image(image_path, caption=team_name, width=123)
                else:
                    st.markdown(
                        f'<div class="fallback-logo">{team_name[:3].upper()}</div>',
                        unsafe_allow_html=True,
                    )


def render_player_highlights_strip(st, bundle: dict[str, pd.DataFrame]) -> None:
    df = bundle.get("player_match_highlights", pd.DataFrame())
    if df.empty:
        return

    player_index = build_player_image_index()
    team_index = build_team_logo_index()
    champion_index = build_champion_image_index()

    st.markdown('<div class="recap-strip-title">Matches Highlights - Player</div>', unsafe_allow_html=True)
    rows = list(df.head(6).iterrows())
    columns = st.columns(len(rows))

    for column, (_, row) in zip(columns, rows, strict=True):
        metric = str(row.get("highlight", row.get("metric", "Metric")))
        player_name = str(row.get("player", "N/A"))
        team_name = row.get("team", "")
        value = str(row.get("value", "N/A"))
        champion_name = row.get("champ", "")
        partida_name = str(row.get("partida", row.get("Partida", "N/A")))
        game_number = row.get("game", row.get("Game", ""))
        if pd.notna(game_number) and str(game_number) not in {"", "N/A"}:
            partida_name = f"{partida_name} - Game {format_compact_number(game_number, decimals=0)}"

        player_image_path = resolve_player_image(team_name, player_name, player_index)
        champion_image_path = resolve_champion_image(champion_name, champion_index)
        if player_image_path is None:
            player_image_path = resolve_team_logo(team_name, team_index)

        with column:
            st.markdown(f'<div class="mini-title">{metric}</div>', unsafe_allow_html=True)
            left_img_col, right_img_col = st.columns(2)
            with left_img_col:
                if player_image_path:
                    render_media_image(st, player_image_path, use_container_width=False, width=TOP5_ICON_SIZE)
            with right_img_col:
                if champion_image_path:
                    render_media_image(st, champion_image_path, use_container_width=False, width=TOP5_ICON_SIZE)
            st.markdown(f'<div class="mini-name">{player_name}</div>', unsafe_allow_html=True)
            st.markdown(f'<div class="mini-match">{partida_name}</div>', unsafe_allow_html=True)
            st.markdown(f'<div class="mini-value">{format_compact_number(value, decimals=2)}</div>', unsafe_allow_html=True)


def render_champion_spotlight(st, champion_summary_df: pd.DataFrame, df: pd.DataFrame) -> None:
    if df.empty:
        return

    champion_index = build_champion_image_index()
    st.markdown('<div class="recap-strip-title">Most Played Champions</div>', unsafe_allow_html=True)
    if not champion_summary_df.empty and "different_champions" in champion_summary_df.columns:
        unique_champions = champion_summary_df.iloc[0].get("different_champions", "N/A")
        st.caption(f"{format_compact_number(unique_champions, decimals=0)} different champions")

    for _, row in df.head(5).iterrows():
        champion_name = str(row.get("champ", "N/A"))
        games = str(row.get("games", "N/A"))
        winrate = str(row.get("winrate_pct", "N/A"))
        kda = str(row.get("kda", "N/A"))
        image_path = resolve_champion_image(champion_name, champion_index)

        name_col, games_col, wr_col, kda_col = st.columns([2.2, 1, 1, 1.5])
        with name_col:
            if image_path:
                icon_col, text_col = st.columns([1, 3])
                with icon_col:
                    render_media_image(st, image_path, use_container_width=False, width=SECTION_ICON_SIZE)
                with text_col:
                    st.markdown(f'<div class="champion-name">{champion_name}</div>', unsafe_allow_html=True)
            else:
                st.markdown(f'<div class="champion-name">{champion_name}</div>', unsafe_allow_html=True)
        games_col.markdown(f'<div class="champion-stat">{format_compact_number(games, decimals=0)}<br/>Games</div>', unsafe_allow_html=True)
        wr_col.markdown(f'<div class="champion-stat">{format_percentage(winrate)}<br/>WR</div>', unsafe_allow_html=True)
        kda_col.markdown(f'<div class="champion-stat">{format_compact_number(kda, decimals=2)}<br/>KDA</div>', unsafe_allow_html=True)


def top5_row_match_and_value(section_name: str, row: pd.Series) -> tuple[str, str]:
    if section_name == "top_kda":
        match_text = f"{row.get('team', '')} - {row.get('role', '')}"
        value_text = (
            f"KDA {format_compact_number(row.get('kda', 'N/A'), decimals=2)}"
            f" - {row.get('kda_open', '')}"
        )
        return match_text.strip(" -"), value_text

    if section_name == "most_kills_single_game":
        partida_name = str(row.get("partida", row.get("Partida", "N/A")))
        game_number = row.get("game", row.get("Game", ""))
        if pd.notna(game_number) and str(game_number) not in {"", "N/A"}:
            match_text = f"{partida_name} - Game {format_compact_number(game_number, decimals=0)}"
        else:
            match_text = partida_name
        rank_value = format_compact_number(row.get("rank", row.get("Rank", "N/A")), decimals=0)
        value_text = (
            f"{rank_value} - "
            f"{format_compact_number(row.get('kills', 'N/A'), decimals=0)}"
            f"/{format_compact_number(row.get('deaths', 'N/A'), decimals=0)}"
            f"/{format_compact_number(row.get('assists', 'N/A'), decimals=0)}"
        )
        return match_text, value_text

    if section_name == "best_players_performance":
        metric_name = str(row.get("metric", "")).strip()
        match_text = f"{row.get('team', '')} - {metric_name}".strip(" -")
        value_text = format_compact_number(row.get("value", "N/A"), decimals=2)
        return match_text, value_text

    partida_name = str(row.get("partida", "N/A"))
    game_number = row.get("game", "")
    if pd.notna(game_number) and str(game_number) not in {"", "N/A"}:
        match_text = f"{partida_name} - Game {format_compact_number(game_number, decimals=0)}"
    else:
        match_text = partida_name
    value_text = format_compact_number(row.get("value", "N/A"), decimals=2)
    return match_text, value_text


def render_recap_cards_section(st, section_name: str, df: pd.DataFrame, bundle: dict[str, pd.DataFrame]) -> bool:
    if df.empty:
        st.info("No data available for this section.")
        return True

    player_index = build_player_image_index()
    team_index = build_team_logo_index()
    champion_index = build_champion_image_index()

    if section_name == "most_played_champions":
        champion_summary_df = bundle.get("champion_summary", pd.DataFrame())
        if not champion_summary_df.empty and "different_champions" in champion_summary_df.columns:
            unique_champions = champion_summary_df.iloc[0].get("different_champions", "N/A")
            st.caption(f"{format_compact_number(unique_champions, decimals=0)} different champions")

        for _, row in df.head(10).iterrows():
            champion_name = str(row.get("champ", "N/A"))
            image_path = resolve_champion_image(champion_name, champion_index)

            c1, c2, c3, c4 = st.columns([2.2, 1, 1, 1.5])
            with c1:
                if image_path:
                    image_col, name_col = st.columns([1, 3])
                    with image_col:
                        render_media_image(st, image_path, use_container_width=False, width=SECTION_ICON_SIZE)
                    with name_col:
                        st.markdown(f'<div class="champion-name">{champion_name}</div>', unsafe_allow_html=True)
                else:
                    st.markdown(f'<div class="champion-name">{champion_name}</div>', unsafe_allow_html=True)
            c2.markdown(f'<div class="champion-stat">{format_compact_number(row.get("games", "N/A"), decimals=0)}<br/>Games</div>', unsafe_allow_html=True)
            c3.markdown(f'<div class="champion-stat">{format_percentage(row.get("winrate_pct", "N/A"))}<br/>WR</div>', unsafe_allow_html=True)
            c4.markdown(
                f'<div class="champion-stat">{format_compact_number(row.get("kda", "N/A"), decimals=2)}<br/>{row.get("kda_open", "KDA")}</div>',
                unsafe_allow_html=True,
            )
        return True

    if section_name in {"player_match_highlights", "best_players_performance", "top_kda", "most_kills_single_game"}:
        rows = list(df.head(5).iterrows())
        for _, row in rows:
            player_name = str(row.get("player", "N/A"))
            team_name = row.get("team", "")
            champion_name = row.get("champ", "")

            player_image_path = resolve_player_image(team_name, player_name, player_index)
            champion_image_path = resolve_champion_image(champion_name, champion_index)
            if player_image_path is None:
                player_image_path = resolve_team_logo(team_name, team_index)

            match_text, value_text = top5_row_match_and_value(section_name, row)

            # Use a single compact row template across all Top 5 blocks to keep widths aligned.
            row_cols = st.columns([0.65, 0.65, 1.3, 1.8, 1.3])
            with row_cols[0]:
                if player_image_path:
                    render_media_image(st, player_image_path, use_container_width=False, width=TOP5_ICON_SIZE)
                else:
                    team_logo_path = resolve_team_logo(team_name, team_index)
                    if team_logo_path:
                        render_media_image(st, team_logo_path, use_container_width=False, width=TOP5_ICON_SIZE)
            with row_cols[1]:
                if champion_image_path:
                    render_media_image(st, champion_image_path, use_container_width=False, width=TOP5_ICON_SIZE)
            with row_cols[2]:
                st.markdown(f'<div class="mini-name">{player_name}</div>', unsafe_allow_html=True)
            with row_cols[3]:
                st.markdown(f'<div class="mini-match">{match_text}</div>', unsafe_allow_html=True)
            with row_cols[4]:
                st.markdown(f'<div class="mini-value">{value_text}</div>', unsafe_allow_html=True)

            st.markdown('<div class="top5-row-divider"></div>', unsafe_allow_html=True)
        return True

    return False


def render_section(st, section_name: str, df: pd.DataFrame, bundle: dict[str, pd.DataFrame]) -> None:
    st.subheader(section_title(section_name))
    description = section_description(section_name)
    if description:
        st.caption(description)

    if df.empty:
        st.info("No data available for this section.")
        return

    if render_recap_cards_section(st, section_name, df, bundle):
        return

    st.dataframe(format_section_for_display(section_name, df), use_container_width=True, hide_index=True)
    render_section_media(st, section_name, df)


def render_top5_sections_side_by_side(st, sections: list[str], role_views: dict[str, pd.DataFrame], bundle: dict[str, pd.DataFrame]) -> list[str]:
    top5_solo_df = role_views.get("top_solo_kills", pd.DataFrame())
    top5_assists_df = role_views.get("top_assists", pd.DataFrame())
    top5_dpm_df = role_views.get("top_dpm", pd.DataFrame())
    top5_csm_df = role_views.get("top_csm", pd.DataFrame())
    top_kda_df = role_views.get("top_kda", pd.DataFrame())
    most_kills_df = role_views.get("most_kills_single_game", pd.DataFrame())

    pair_specs: list[tuple[tuple[str, str, str, pd.DataFrame], tuple[str, str, str, pd.DataFrame]]] = [
        (
            ("TOP 5 Most Kills in a Single Game", section_description("most_kills_single_game"), "most_kills_single_game", most_kills_df.head(5)),
            (
                "TOP 5 Most Solo KILLS",
                "Top 5 player-game rows by solo kills.",
                "player_match_highlights",
                top5_solo_df,
            ),
        ),
        (
            ("TOP 5 Best KDA", section_description("top_kda"), "top_kda", top_kda_df.head(5)),
            (
                "TOP 5 Most ASSISTS",
                "Top 5 player-game rows by assists.",
                "player_match_highlights",
                top5_assists_df,
            ),
        ),
        (
            (
                "TOP 5 Best DPM",
                "Top 5 player-game rows by damage per minute.",
                "player_match_highlights",
                top5_dpm_df,
            ),
            (
                "TOP 5 Best CSM",
                "Top 5 player-game rows by cs per minute.",
                "player_match_highlights",
                top5_csm_df,
            ),
        ),
    ]

    for left_spec, right_spec in pair_specs:
        cols = st.columns(2, gap="medium")
        for col, (title, desc, base_section, df) in zip(cols, [left_spec, right_spec], strict=True):
            with col:
                st.subheader(title)
                if desc:
                    st.caption(desc)
                if df.empty:
                    st.info("No data available for this section.")
                else:
                    render_recap_cards_section(st, base_section, df, bundle)
        st.divider()

    return [section_name for section_name in sections if section_name == "most_played_champions"]


def render_metrics(st, metrics: dict[str, str]) -> None:
    columns = st.columns(len(metrics))
    for column, (label, value) in zip(columns, metrics.items(), strict=True):
        column.metric(label, value)


def apply_page_style(st) -> None:
    st.markdown(
        """
        <style>
        @import url('https://fonts.googleapis.com/css2?family=Rajdhani:wght@500;600;700&family=Barlow+Condensed:wght@600;700&display=swap');

        .stApp {
            background: radial-gradient(circle at top, #10243d 0%, #081421 55%, #050b12 100%);
            color: #f5f7fa;
            font-family: 'Rajdhani', sans-serif;
        }
        section[data-testid="stSidebar"] {
            background: #07111b;
            border-right: 1px solid rgba(244, 199, 106, 0.08);
        }
        h1, h2, h3 {
            font-family: 'Barlow Condensed', sans-serif;
            letter-spacing: 0.02em;
            text-transform: uppercase;
        }
        .recap-hero {
            border: 1px solid rgba(244, 199, 106, 0.18);
            background: linear-gradient(135deg, rgba(9, 21, 34, 0.95) 0%, rgba(6, 14, 24, 0.95) 100%);
            border-radius: 14px;
            padding: 0.9rem 1rem 0.8rem 1rem;
            margin: 0.2rem 0 0 0;
            box-shadow: 0 0 0 1px rgba(13, 28, 45, 0.25) inset;
        }
        .stApp hr {
            border-top: 1px solid rgba(160, 186, 214, 0.12);
            margin: 0.5rem 0 0.7rem 0;
        }
        /* Hide media fullscreen controls on Streamlit images */
        .stImage button,
        [data-testid="stImage"] button,
        [data-testid="stImageContainer"] button,
        button[title="Fullscreen"],
        button[title="View fullscreen"],
        button[title*="fullscreen" i],
        button[aria-label*="fullscreen" i],
        [data-testid="StyledFullScreenButton"] {
            display: none !important;
            visibility: hidden !important;
            pointer-events: none !important;
        }
        .recap-kicker {
            color: #d2c39a;
            font-size: 1rem;
            font-weight: 700;
            letter-spacing: 0.05em;
            margin-top: 0.45rem;
        }
        .repo-link {
            color: #d2c39a;
            text-decoration: none;
        }
        .repo-link:hover {
            text-decoration: underline;
        }
        .recap-title-main {
            color: #f3ede2;
            font-size: 1.8rem;
            font-family: 'Barlow Condensed', sans-serif;
            font-weight: 700;
            line-height: 1;
        }
        .recap-subtitle-main {
            color: #8ea2b8;
            font-size: 1rem;
            text-transform: uppercase;
            letter-spacing: 0.06em;
            font-weight: 600;
        }
        .recap-strip-title {
            margin-top: 0.25rem;
            margin-bottom: 0.05rem;
            color: #d9c38f;
            font-size: 1rem;
            font-family: 'Barlow Condensed', sans-serif;
            text-transform: uppercase;
            letter-spacing: 0.04em;
        }
        .team-strip-title {
            margin-top: 0;
            margin-bottom: 0.1rem;
            color: #d9c38f;
            font-size: 1rem;
            font-family: 'Barlow Condensed', sans-serif;
            text-transform: uppercase;
            letter-spacing: 0.04em;
        }
        .mini-title {
            color: #f0ddaf;
            text-align: center;
            font-weight: 700;
            margin-bottom: 0.05rem;
            font-size: 0.84rem;
            text-transform: uppercase;
        }
        .mini-name {
            color: #f7f8fa;
            text-align: center;
            font-weight: 700;
            margin-top: 0.05rem;
            font-size: 0.84rem;
            text-transform: uppercase;
        }
        .mini-match {
            color: #9ec3f0;
            text-align: center;
            font-weight: 600;
            margin-top: 0.03rem;
            margin-bottom: 0.03rem;
            font-size: 0.72rem;
        }
        .mini-value {
            color: #9ec3f0;
            text-align: center;
            font-weight: 600;
            margin-top: 0;
            margin-bottom: 0.15rem;
            font-size: 0.82rem;
        }
        .top5-row-divider {
            border-bottom: 1px solid rgba(160, 186, 214, 0.16);
            margin: 0.08rem 0 0.3rem 0;
        }
        .champion-name {
            color: #f4f6f9;
            font-weight: 700;
            margin-top: 0.2rem;
            text-transform: uppercase;
            font-size: 0.86rem;
        }
        .champion-stat {
            text-align: center;
            color: #e6edf7;
            font-weight: 600;
            margin-top: 0.2rem;
            font-size: 0.82rem;
            line-height: 1.2;
        }
        .fallback-logo {
            border: 1px solid rgba(160, 186, 214, 0.28);
            border-radius: 999px;
            height: 48px;
            width: 48px;
            display: flex;
            align-items: center;
            justify-content: center;
            color: #d9e8ff;
            font-weight: 700;
            margin: 0 auto 0.4rem auto;
            background: rgba(41, 66, 97, 0.28);
        }
        .inventory-section-title {
            margin-top: 0.35rem;
            margin-bottom: 0.15rem;
            color: #d9c38f;
            font-size: 1rem;
            font-family: 'Barlow Condensed', sans-serif;
            text-transform: uppercase;
            letter-spacing: 0.04em;
        }
        .inventory-missing-box {
            border: 1px solid rgba(255, 170, 170, 0.35);
            border-radius: 14px;
            height: 72px;
            display: flex;
            align-items: center;
            justify-content: center;
            color: #ffcccc;
            font-weight: 700;
            margin: 0 auto 0.25rem auto;
            background: rgba(120, 40, 40, 0.22);
        }
        </style>
        """,
        unsafe_allow_html=True,
    )


def main() -> None:
    import streamlit as st

    st.set_page_config(page_title="Gol.gg Streamlit", page_icon="LoL", layout="wide")
    apply_page_style(st)

    standardized_dir = resolve_standardized_dir()
    tournaments = discover_tournaments(standardized_dir)
    if not tournaments:
        st.warning("No standardized datasets found. Run step7 first.")
        return

    with st.sidebar:
        st.header("Navigation")
        selected_tournament = st.selectbox("Tournament", tournaments)
        bundle = load_tournament_bundle(standardized_dir, selected_tournament)

    fullstats_df = load_fullstats_for_tournament(selected_tournament)
    role_options = discover_role_labels(fullstats_df)
    with st.sidebar:
        selected_role_label = st.selectbox("Lane", role_options)

    if not bundle:
        st.warning("No datasets found for the selected tournament.")
        return

    role_filtered_fullstats = filter_fullstats_by_role(fullstats_df, selected_role_label)
    role_views = build_role_filtered_views(role_filtered_fullstats)
    filtered_bundle = dict(bundle)
    filtered_bundle.update(
        {
            "player_match_highlights": standardize_app_columns(build_player_match_highlights(role_filtered_fullstats)),
            "champion_summary": role_views["champion_summary"],
            "most_played_champions": role_views["most_played_champions"],
            "top_kda": role_views["top_kda"],
            "most_kills_single_game": role_views["most_kills_single_game"],
        }
    )

    display_name = tournament_display_name(bundle, selected_tournament)
    render_top_banner(st, display_name, selected_tournament, bundle)
    st.caption(f"Lane filter: {selected_role_label}")
    render_player_highlights_strip(st, filtered_bundle)
    st.divider()

    missing_sections = [section_name for section_name in missing_parity_sections(bundle) if section_name not in HIDDEN_UI_SECTIONS]
    if missing_sections:
        st.warning(
            "Missing sections for full infograph parity: "
            + ", ".join(section_title(section_name) for section_name in missing_sections)
        )

    sections = [section_name for section_name in discover_parity_sections(bundle) if section_name not in HIDDEN_UI_SECTIONS]
    if "most_played_champions" in bundle and "most_played_champions" not in sections:
        sections.append("most_played_champions")
    if not sections:
        st.warning("No parity sections found for the selected tournament.")
        return

    sections = render_top5_sections_side_by_side(st, sections, role_views, filtered_bundle)

    for section_name in sections:
        render_section(st, section_name, filtered_bundle.get(section_name, pd.DataFrame()), filtered_bundle)
        st.divider()

    render_debug_inventory_section(st, selected_tournament, bundle, fullstats_df)

    # Consolidated dataset intentionally removed from UI.


if __name__ == "__main__":
    main()

