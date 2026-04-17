import pandas as pd
from unittest.mock import patch

from golgg.pipeline.orquestracao.step007_infographic_dataset import (
    build_outputs_bundle,
    build_missing_metrics_report,
    format_br_int,
    format_duration_from_minutes,
    safe_to_name,
    write_outputs_bundle,
)


def test_safe_to_name_replace_underscore_with_space():
    assert safe_to_name("CBLOL_Cup_2026") == "CBLOL Cup 2026"


def test_format_br_int_apply_thousand_separator_ptbr_style():
    assert format_br_int(1234567) == "1.234.567"


def test_format_duration_from_minutes_convert_to_day_hour_minute_second():
    # 1500 minutes = 1d 1h 0m 0s
    assert format_duration_from_minutes(1500) == "1d 1h 0m 0s"


def test_build_missing_metrics_report_include_expected_columns_and_rows():
    out = build_missing_metrics_report()
    assert set(["Metric", "Reason"]).issubset(set(out.columns))
    assert len(out) >= 5


def test_build_outputs_bundle_return_expected_section_keys_with_mocked_builders():
    raw_df = pd.DataFrame([{"x": 1}])
    fullstats_df = pd.DataFrame([{"x": 1}])
    one = pd.DataFrame([{"A": 1}])
    two = pd.DataFrame([{"B": 2}])

    with patch("golgg.pipeline.orquestracao.step007_infographic_dataset.build_player_match_highlights", return_value=one
    ), patch("golgg.pipeline.orquestracao.step007_infographic_dataset.build_champion_outputs", return_value=(one, two)), patch(
        "golgg.pipeline.orquestracao.step007_infographic_dataset.build_team_match_highlights", return_value=one
    ), patch("golgg.pipeline.orquestracao.step007_infographic_dataset.build_best_players_performance", return_value=one), patch(
        "golgg.pipeline.orquestracao.step007_infographic_dataset.build_top_kda", return_value=one
    ), patch(
        "golgg.pipeline.orquestracao.step007_infographic_dataset.build_most_kills_single_game", return_value=one
    ), patch(
        "golgg.pipeline.orquestracao.step007_infographic_dataset.build_missing_metrics_report", return_value=one
    ):
        out = build_outputs_bundle("SAFE", "TOURN", raw_df, fullstats_df)

    assert set(out.keys()) == {
        "player_match_highlights",
        "champion_summary",
        "most_played_champions",
        "team_match_highlights",
        "best_players_performance",
        "top_kda",
        "most_kills_single_game",
        "missing_metrics",
    }


def test_write_outputs_bundle_create_expected_files(tmp_path):
    outputs = {
        "player_match_highlights": pd.DataFrame([{"A": 1}]),
        "champion_summary": pd.DataFrame([{"A": 1}]),
        "most_played_champions": pd.DataFrame([{"A": 1}]),
        "team_match_highlights": pd.DataFrame([{"A": 1}]),
        "best_players_performance": pd.DataFrame([{"A": 1}]),
        "top_kda": pd.DataFrame([{"A": 1}]),
        "most_kills_single_game": pd.DataFrame([{"A": 1}]),
        "missing_metrics": pd.DataFrame([{"A": 1}]),
    }

    write_outputs_bundle(str(tmp_path), "TEST_TOURN", outputs)

    expected = [
        "TEST_TOURN_player_match_highlights.csv",
        "TEST_TOURN_champion_summary.csv",
        "TEST_TOURN_most_played_champions.csv",
        "TEST_TOURN_team_match_highlights.csv",
        "TEST_TOURN_best_players_performance.csv",
        "TEST_TOURN_top_kda.csv",
        "TEST_TOURN_most_kills_single_game.csv",
        "TEST_TOURN_missing_metrics.csv",
    ]

    for name in expected:
        assert (tmp_path / name).exists()


def test_write_outputs_bundle_formats_human_readable_csv_values(tmp_path):
    outputs = {
        "player_match_highlights": pd.DataFrame([{"Highlight": "Best KDA", "Metric": "KDA", "Player": "Tinowns", "Team": "paiN Gaming", "Champ": "Galio", "Partida": "paiN Gaming vs Leviatan", "Game": 2, "Value": 22.0}]),
        "champion_summary": pd.DataFrame([{"Different Champions": 88}]),
        "most_played_champions": pd.DataFrame([{"Rank": 1, "Champ": "Aatrox", "Games": 4, "WinRate%": 38.0, "KDA": 11.5, "KDA_Open": "11.5 - 17/4/29"}]),
        "team_match_highlights": pd.DataFrame([{"Highlight": "Game with the most kills", "Metric": "Total Kills", "Team": "LOUD", "Partida": "RED Canids vs LOUD", "Stage": "WEEK2", "Game": 1, "Duration": "44:17", "Value": 48.0}]),
        "best_players_performance": pd.DataFrame([{"Scope": "Tournament", "Split": "N/A (single tournament dataset)", "Metric": "KDA", "Player": "JoJo", "Team": "FURIA", "Value": 7.29}]),
        "top_kda": pd.DataFrame([{"Rank": 1, "Player": "JoJo", "Team": "FURIA", "Role": "MID", "Kills": 17, "Deaths": 38, "Assists": 260, "KDA": 7.29, "KDA_Open": "17/38/260", "Match_count": 22}]),
        "most_kills_single_game": pd.DataFrame([{"Rank": 1, "Player": "JoJo", "Team": "FURIA", "Champ": "Azir", "Partida": "FURIA vs LOUD", "Game": 3, "Kills": 18, "Deaths": 2, "Assists": 6}]),
        "missing_metrics": pd.DataFrame([{"Metric": "Tower count", "Reason": "Only tower damage is available"}]),
    }

    write_outputs_bundle(str(tmp_path), "TEST_TOURN", outputs)

    champs_text = (tmp_path / "TEST_TOURN_most_played_champions.csv").read_text(encoding="utf-8")
    team_text = (tmp_path / "TEST_TOURN_team_match_highlights.csv").read_text(encoding="utf-8")
    kda_text = (tmp_path / "TEST_TOURN_top_kda.csv").read_text(encoding="utf-8")
    kills_text = (tmp_path / "TEST_TOURN_most_kills_single_game.csv").read_text(encoding="utf-8")

    assert "38%" in champs_text
    assert "11.5" in champs_text
    assert "44:17" in team_text
    assert "48" in team_text
    assert "17/38/260" in kda_text
    assert "FURIA vs LOUD" in kills_text


def test_build_champion_outputs_uses_game_winners_for_winrate():
    from golgg.pipeline.orquestracao.step007_infographic_dataset import build_champion_outputs

    fullstats_df = pd.DataFrame(
        [
            {"Champ": "Rumble", "Team": "LOUD", "Partida": "LOUD vs RED", "Game": 1, "Kills": 1, "Deaths": 1, "Assists": 0, "WinnerTeam": "LOUD"},
            {"Champ": "Rumble", "Team": "LOUD", "Partida": "LOUD vs RED", "Game": 2, "Kills": 2, "Deaths": 1, "Assists": 0, "WinnerTeam": "LOUD"},
            {"Champ": "Rumble", "Team": "RED Canids", "Partida": "LOUD vs RED", "Game": 3, "Kills": 3, "Deaths": 1, "Assists": 0, "WinnerTeam": "LOUD"},
        ]
    )

    summary, top10 = build_champion_outputs(fullstats_df)

    assert summary.loc[0, "Different Champions"] == 1
    assert top10.loc[0, "Champ"] == "Rumble"
    assert round(float(top10.loc[0, "WinRate%"]), 2) == 66.67

