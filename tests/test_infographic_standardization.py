import pandas as pd

from golgg.pipeline.transformacao.infographic_standardization import (
    slug_column,
    standardize_dataframe,
    write_standardized_outputs,
)
from golgg.pipeline.common import normalize_champion_name, normalize_champion_square_filename
from golgg.step009x_download_champion_squares import normalize_existing_champion_square_files


def test_slug_column_convert_percent_and_at_to_canonical_tokens():
    assert slug_column("WinRate%") == "winrate_pct"
    assert slug_column("GD@15") == "gd_at_15"
    assert slug_column("Total Time Played") == "total_time_played"


def test_standardize_dataframe_add_tournament_key_and_snake_case_columns():
    df = pd.DataFrame([{"Total Gold": "1.234", "KDA_Open": "5.0 - 5/1/2"}])
    out = standardize_dataframe(df, "CBLOL_2026_SPLIT_1")

    assert list(out.columns) == ["tournament_key", "total_gold", "kda_open"]
    assert out.loc[0, "tournament_key"] == "CBLOL_2026_SPLIT_1"


def test_standardize_dataframe_fix_invalid_team_by_player_override():
    df = pd.DataFrame([{"Player": "Stepz", "Team": 0, "Role": "JUNGLE"}])

    out = standardize_dataframe(df, "CBLOL_2026_SPLIT_1")

    assert out.loc[0, "team"] == "RED Canids"


def test_standardize_dataframe_fix_champion_truncation_k_to_ksante():
    df = pd.DataFrame([{"Champ": "K", "Games": 6}])

    out = standardize_dataframe(df, "CBLOL_2026_SPLIT_1")

    assert out.loc[0, "champ"] == "KSante"


def test_normalize_champion_name_removes_punctuation():
    assert normalize_champion_name("Cho'Gath") == "ChoGath"
    assert normalize_champion_name("Dr. Mundo") == "DrMundo"
    assert normalize_champion_name("Bel'Veth") == "BelVeth"
    assert normalize_champion_name("K'Sante") == "KSante"


def test_normalize_champion_square_filename_uses_canonical_stem():
    assert normalize_champion_square_filename("K'Sante_OriginalSquare.png") == "KSante_OriginalSquare.png"
    assert normalize_champion_square_filename("Dr._Mundo_OriginalSquare.png") == "DrMundo_OriginalSquare.png"


def test_normalize_existing_champion_square_files_fix_case_only_rename(tmp_path):
    image_path = tmp_path / "JarvanIv_OriginalSquare.png"
    image_path.write_bytes(b"fake")

    normalize_existing_champion_square_files(tmp_path)

    names = sorted(path.name for path in tmp_path.iterdir())
    assert names == ["JarvanIV_OriginalSquare.png"]


def test_write_standardized_outputs_create_section_and_all_sections_files(tmp_path):
    outputs = {
        "summary": pd.DataFrame([{"Total Gold": "1.234"}]),
        "objectives": pd.DataFrame([{"Objective": "Dragons", "Value": "N/A"}]),
    }
    consolidated = pd.DataFrame([{"Section": "summary", "Total Gold": "1.234"}])

    write_standardized_outputs(str(tmp_path), "TOURN_X", outputs, consolidated)

    assert (tmp_path / "standardized" / "TOURN_X_summary.csv").exists()
    assert (tmp_path / "standardized" / "TOURN_X_objectives.csv").exists()
    assert (tmp_path / "standardized" / "TOURN_X_all_sections.csv").exists()

