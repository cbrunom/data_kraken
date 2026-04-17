import pandas as pd

from golgg.contracts.infographic_contracts import (
    CONTRACTS,
    infer_section_name,
    validate_dataframe,
)


def test_infer_section_name_detect_suffix_from_standardized_filename():
    path = "golgg/data/gold/infographic_ready/standardized/CBLOL_2026_Split_1_most_played_champions.csv"
    assert infer_section_name(path) == "most_played_champions"


def test_validate_dataframe_pass_for_valid_team_match_highlights_contract():
    df = pd.DataFrame(
        [
            {
                "tournament_key": "CBLOL_2026_Split_1",
                "highlight": "Game with the most kills",
                "metric": "Total Kills",
                "team": "RED Canids",
                "partida": "RED Canids vs LOUD",
                "stage": "WEEK2",
                "game": 1,
                "duration": "44:17",
                "value": 48,
            }
        ]
    )

    errors = validate_dataframe(df, CONTRACTS["team_match_highlights"])
    assert errors == []


def test_validate_dataframe_fail_when_required_column_missing():
    df = pd.DataFrame([{"tournament_key": "X"}])
    errors = validate_dataframe(df, CONTRACTS["most_played_champions"])
    assert any(e.startswith("missing_required_column:") for e in errors)


def test_validate_dataframe_fail_when_numeric_column_invalid():
    df = pd.DataFrame(
        [
            {
                "tournament_key": "CBLOL_2026_Split_1",
                "rank": 1,
                "champ": "Aatrox",
                "games": "abc",
                "winrate_pct": 50,
                "kda": 3.5,
                "kda_open": "3.5 - 7/2/0",
            }
        ]
    )
    errors = validate_dataframe(df, CONTRACTS["most_played_champions"])
    assert "invalid_numeric_column:games" in errors


def test_validate_dataframe_fail_when_rank_or_winrate_out_of_domain():
    df = pd.DataFrame(
        [
            {
                "tournament_key": "CBLOL_2026_Split_1",
                "rank": 0,
                "champ": "Aatrox",
                "games": 4,
                "winrate_pct": 120,
                "kda": 11.5,
                "kda_open": "11.5 - 17/4/29",
            }
        ]
    )

    errors = validate_dataframe(df, CONTRACTS["most_played_champions"])

    assert "min_value_violation:rank:1" in errors
    assert "max_value_violation:winrate_pct:100" in errors


def test_validate_dataframe_fail_when_different_champions_is_not_positive():
    df = pd.DataFrame([{"tournament_key": "CBLOL_2026_Split_1", "different_champions": 0}])

    errors = validate_dataframe(df, CONTRACTS["champion_summary"])
    assert "min_value_violation:different_champions:1" in errors


def test_validate_dataframe_pass_for_valid_top_kda_contract():
    df = pd.DataFrame(
        [
            {
                "tournament_key": "CBLOL_2026_Split_1",
                "rank": 1,
                "player": "tinowns",
                "team": "pain_gaming",
                "role": "MID",
                "kills": 7.0,
                "deaths": 2.0,
                "assists": 8.0,
                "kda": 7.5,
                "kda_open": "7/2/8",
                "match_count": 4,
            }
        ]
    )

    errors = validate_dataframe(df, CONTRACTS["top_kda"])
    assert errors == []
