import pandas as pd

from golgg.pipeline.transformacao.player_stats import (
    apply_role_overrides,
    normalize_kda_column,
    recompute_kda_from_averages,
    strip_percentage_columns,
)


def test_normalize_kda_column_convert_perfect_kda_to_kills_plus_assists():
    df = pd.DataFrame(
        [
            {"Player": "A", "Kills": "3", "Deaths": "1", "Assists": "5", "KDA": "Perfect KDA"},
            {"Player": "B", "Kills": "2", "Deaths": "2", "Assists": "4", "KDA": "3.0"},
        ]
    )

    out = normalize_kda_column(df)

    assert out.loc[0, "KDA"] == 8
    assert out.loc[1, "KDA"] == "3.0"
    assert str(out["Kills"].dtype).startswith(("int", "float"))


def test_apply_role_overrides_apply_known_players_roles():
    df = pd.DataFrame(
        [
            {"Player": "Ayu", "Role": "MID"},
            {"Player": "Outro", "Role": "TOP"},
        ]
    )

    out = apply_role_overrides(df)

    assert out.loc[0, "Role"] == "MID"
    assert out.loc[1, "Role"] == "TOP"


def test_strip_percentage_columns_remove_percent_suffix_from_columns():
    df = pd.DataFrame([{"GOLD%": "12%", "KP%": "40%", "VS%": "5%", "DMG%": "30%"}])

    out = strip_percentage_columns(df, ["GOLD%", "KP%", "VS%", "DMG%"])

    assert out.loc[0, "GOLD%"] == "12"
    assert out.loc[0, "KP%"] == "40"
    assert out.loc[0, "VS%"] == "5"
    assert out.loc[0, "DMG%"] == "30"


def test_recompute_kda_from_averages_divide_by_deaths_and_fallback_when_zero():
    player_avg = pd.DataFrame(
        [
            {"Kills": 4.0, "Assists": 6.0, "Deaths": 2.0, "KDA": 0.0},
            {"Kills": 3.0, "Assists": 7.0, "Deaths": 0.0, "KDA": 0.0},
        ]
    )

    out = recompute_kda_from_averages(player_avg)

    assert out.loc[0, "KDA"] == 5.0
    assert out.loc[1, "KDA"] == 10.0
