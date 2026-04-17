import pandas as pd
from pathlib import Path
from PIL import Image

from golgg.app_streamlit.app import (
    DISPLAY_IMAGE_SIZE,
    GOLD_STANDARDIZED_DIR,
    LEGACY_STANDARDIZED_DIR,
    PARITY_SECTIONS,
    build_champion_image_index,
    build_player_image_index,
    build_team_logo_index,
    collect_section_media,
    discover_sections,
    discover_parity_sections,
    discover_tournaments,
    load_tournament_bundle,
    load_display_image,
    format_compact_number,
    format_percentage,
    format_section_for_display,
    missing_parity_sections,
    normalize_entity_name,
    resolve_champion_image,
    resolve_player_image,
    resolve_team_logo,
    resolve_standardized_dir,
    section_description,
    section_title,
    standardize_app_columns,
    tournament_display_name,
    tournament_metrics,
)


def test_discover_tournaments_ignore_champion_summary_files(tmp_path):
    standardized = tmp_path / "standardized"
    standardized.mkdir()
    (standardized / "CBLOL_2026_Split_1_most_played_champions.csv").write_text("rank,champ,games\n1,A,2\n", encoding="utf-8")
    (standardized / "CBLOL_2026_Split_1_champion_summary.csv").write_text("tournament_key\nA\n", encoding="utf-8")
    (standardized / "CBLOL_Cup_2026_most_played_champions.csv").write_text("rank,champ,games\n1,B,2\n", encoding="utf-8")

    tournaments = discover_tournaments(standardized)

    assert tournaments == ["CBLOL_2026_Split_1", "CBLOL_Cup_2026"]


def test_resolve_standardized_dir_prefer_gold_when_available(tmp_path, monkeypatch):
    gold = tmp_path / "gold_std"
    legacy = tmp_path / "legacy_std"
    gold.mkdir()
    legacy.mkdir()
    (gold / "CBLOL_2026_Split_1_most_played_champions.csv").write_text("rank,champ,games\n1,A,2\n", encoding="utf-8")

    from golgg.app_streamlit import app as app_module

    monkeypatch.setattr(app_module, "GOLD_STANDARDIZED_DIR", gold)
    monkeypatch.setattr(app_module, "LEGACY_STANDARDIZED_DIR", legacy)

    assert resolve_standardized_dir() == gold


def test_resolve_standardized_dir_fallback_legacy_when_gold_missing_files(tmp_path, monkeypatch):
    gold = tmp_path / "gold_std"
    legacy = tmp_path / "legacy_std"
    gold.mkdir()
    legacy.mkdir()
    (legacy / "CBLOL_2026_Split_1_most_played_champions.csv").write_text("rank,champ,games\n1,A,2\n", encoding="utf-8")

    from golgg.app_streamlit import app as app_module

    monkeypatch.setattr(app_module, "GOLD_STANDARDIZED_DIR", gold)
    monkeypatch.setattr(app_module, "LEGACY_STANDARDIZED_DIR", legacy)

    assert resolve_standardized_dir() == legacy


def test_load_tournament_bundle_include_expected_sections(tmp_path):
    standardized = tmp_path / "standardized"
    standardized.mkdir()
    (standardized / "TOURN_champion_summary.csv").write_text(
        "tournament_key,different_champions\nTOURN,3\n",
        encoding="utf-8",
    )
    (standardized / "TOURN_most_played_champions.csv").write_text(
        "rank,champ,games,winrate_pct,kda,kda_open\n1,Aatrox,4,38,11.5,11.5 - 1/2/3\n",
        encoding="utf-8",
    )

    bundle = load_tournament_bundle(standardized, "TOURN")

    assert list(bundle.keys()) == ["champion_summary", "most_played_champions"]


def test_tournament_display_name_falls_back_to_safe_name_when_summary_missing():
    bundle = {"champion_summary": pd.DataFrame(), "most_played_champions": pd.DataFrame()}
    assert tournament_display_name(bundle, "CBLOL_2026_Split_1") == "CBLOL 2026 Split 1"


def test_section_helpers_return_title_and_description():
    assert section_title("most_played_champions") == "Most Played Champions"
    assert section_description("team_match_highlights") == "Game with the most kills by match context."
    assert section_title("top_kda") == "Top KDA"
    assert section_title("most_kills_single_game") == "Most Kills in a Single Game"


def test_tournament_metrics_use_available_summary_values():
    bundle = {
        "champion_summary": pd.DataFrame([{"different_champions": 3}]),
        "most_played_champions": pd.DataFrame([{"rank": 1}, {"rank": 2}]),
    }

    metrics = tournament_metrics(bundle)

    assert metrics["Unique Champions"] == "3"
    assert metrics["Top Champions Listed"] == "2"
    assert metrics["Available Sections"] == "2"


def test_discover_parity_sections_follow_expected_order():
    bundle = {
        "most_played_champions": pd.DataFrame(),
        "player_match_highlights": pd.DataFrame(),
        "top_kda": pd.DataFrame(),
    }

    sections = discover_parity_sections(bundle)

    assert sections == ["player_match_highlights", "most_played_champions", "top_kda"]


def test_missing_parity_sections_detect_gaps_for_incomplete_bundle():
    bundle = {
        "player_match_highlights": pd.DataFrame(),
    }

    missing = missing_parity_sections(bundle)

    assert "player_match_highlights" not in missing
    assert "champion_summary" in missing
    assert len(missing) == len(PARITY_SECTIONS) - 1


def test_parity_sections_include_matador_and_skip_missing_metrics():
    assert "matador" not in PARITY_SECTIONS
    assert "missing_metrics" not in PARITY_SECTIONS


def test_format_helpers_compact_numbers_and_percentages():
    assert format_percentage("38.0") == "38%"
    assert format_compact_number("22.0") == "22"
    assert format_compact_number("11.555", decimals=2) == "11.55"


def test_format_section_for_display_normalizes_common_columns():
    df = pd.DataFrame(
        [
            {"rank": 1, "champ": "Aatrox", "games": 4, "winrate_pct": 38.0, "kda": 11.5, "kda_open": "11.5 - 1/2/3"},
        ]
    )

    out = format_section_for_display("most_played_champions", df)

    assert out.loc[0, "rank"] == "1"
    assert out.loc[0, "games"] == "4"
    assert out.loc[0, "winrate_pct"] == "38%"
    assert out.loc[0, "kda"] == "11.5"


def test_format_section_for_display_handles_new_sections():
    top_kda_df = pd.DataFrame([
        {"rank": 1, "player": "JoJo", "team": "FURIA", "role": "MID", "kills": 17, "deaths": 38, "assists": 260, "kda": 7.29, "kda_open": "17/38/260", "match_count": 22},
    ])
    kills_df = pd.DataFrame([
        {"rank": 1, "player": "JoJo", "team": "FURIA", "champ": "Azir", "partida": "FURIA vs LOUD", "game": 3, "kills": 18, "deaths": 2, "assists": 6},
    ])

    top_out = format_section_for_display("top_kda", top_kda_df)
    kills_out = format_section_for_display("most_kills_single_game", kills_df)

    assert top_out.loc[0, "kda"] == "7.29"
    assert top_out.loc[0, "match_count"] == "22"
    assert kills_out.loc[0, "kills"] == "18"
    assert kills_out.loc[0, "game"] == "3"


def test_standardize_app_columns_maps_partida_and_game_columns():
    df = pd.DataFrame([
        {"Partida": "paiN Gaming vs Leviatan", "Game": 4, "Player": "tinowns"}
    ])

    out = standardize_app_columns(df)

    assert "partida" in out.columns
    assert "game" in out.columns
    assert out.loc[0, "partida"] == "paiN Gaming vs Leviatan"
    assert out.loc[0, "game"] == 4


def test_load_display_image_return_fixed_size_canvas(tmp_path: Path):
    image_path = tmp_path / "logo.png"
    Image.new("RGBA", (64, 32), (255, 0, 0, 255)).save(image_path)

    out = load_display_image(image_path)

    assert out.size == DISPLAY_IMAGE_SIZE


def test_normalize_entity_name_remove_accents_and_special_chars():
    assert normalize_entity_name("K'Sante") == "k_sante"
    assert normalize_entity_name("Brandão") == "brandao"
    assert normalize_entity_name("paiN Gaming") == "pain_gaming"


def test_build_indexes_and_resolve_entity_images(tmp_path):
    player_dir = tmp_path / "players"
    team_dir = tmp_path / "teams"
    champ_dir = tmp_path / "champs"
    player_dir.mkdir()
    team_dir.mkdir()
    champ_dir.mkdir()

    player_image = player_dir / "pain_gaming__tinowns.png"
    player_image.write_bytes(b"fake")
    player_map = player_dir / "player_images_mapping.csv"
    player_map.write_text(
        "team_key,player_name,player_key,target_file\n"
        "pain_gaming,tinowns,tinowns,golgg\\images\\player_images\\pain_gaming__tinowns.png\n",
        encoding="utf-8",
    )

    team_image = team_dir / "pain_gaming.png"
    team_image.write_bytes(b"fake")
    team_map = team_dir / "team_logos_mapping.csv"
    team_map.write_text(
        "team_key,target_file\n"
        "pain_gaming,golgg\\images\\team_logos\\pain_gaming.png\n",
        encoding="utf-8",
    )

    champ_image = champ_dir / "K_Sante_OriginalSquare.png"
    champ_image.write_bytes(b"fake")

    from golgg.app_streamlit import app as app_module

    original_player_dir = app_module.PLAYER_IMAGES_DIR
    original_team_dir = app_module.TEAM_LOGOS_DIR
    try:
        app_module.PLAYER_IMAGES_DIR = player_dir
        app_module.TEAM_LOGOS_DIR = team_dir

        player_index = build_player_image_index(player_map)
        team_index = build_team_logo_index(team_map)
        champ_index = build_champion_image_index(champ_dir)
    finally:
        app_module.PLAYER_IMAGES_DIR = original_player_dir
        app_module.TEAM_LOGOS_DIR = original_team_dir

    assert resolve_player_image("paiN Gaming", "tinowns", player_index) == player_image
    assert resolve_team_logo("paiN Gaming", team_index) == team_image
    assert resolve_champion_image("K'Sante", champ_index) == champ_image
    assert resolve_champion_image("KSante", champ_index) == champ_image


def test_collect_section_media_return_deduplicated_items():
    df = pd.DataFrame(
        [
            {"team": "paiN Gaming", "player": "tinowns", "champ": "K'Sante"},
            {"team": "paiN Gaming", "player": "tinowns", "champ": "K'Sante"},
        ]
    )

    player_index = {("pain_gaming", "tinowns"): Path("/tmp/player.png")}
    team_index = {"pain_gaming": Path("/tmp/team.png")}
    champ_index = {"k_sante": Path("/tmp/champ.png")}

    items = collect_section_media("player_match_highlights", df, player_index, team_index, champ_index)

    assert len(items) == 3


def test_resolve_player_image_supports_team_aliases_in_index():
    player_index = {("los", "ackerman"): Path("/tmp/los__ackerman.png")}

    out = resolve_player_image("Los Grandes", "Ackerman", player_index)

    assert out == Path("/tmp/los__ackerman.png")


def test_resolve_player_image_has_stepz_url_fallback():
    out = resolve_player_image("RED Canids", "Stepz", {})

    assert isinstance(out, str)
    assert "RED_STEPZ_2026_Split_1" in out


def test_resolve_player_image_has_cody_url_fallback():
    out = resolve_player_image("Leviatan", "cody", {})

    assert isinstance(out, str)
    assert "FX_Cody_2026_Split_1" in out
