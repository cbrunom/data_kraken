import pandas as pd

from golgg.pipeline.enriquecimento.matador import compute_matador


def test_compute_matador_return_ranked_dataset_with_expected_columns():
    raw_df = pd.DataFrame(
        [
            {
                "Player": "p1",
                "Team": "A",
                "Role": "MID",
                "Kills": 7,
                "Deaths": 2,
                "Assists": 8,
                "KDA": 7.5,
                "DPM": 800,
                "GPM": 450,
                "KP%": 75,
                "Objectives Stolen": 1,
                "Match_count": 4,
            },
            {
                "Player": "p2",
                "Team": "B",
                "Role": "ADC",
                "Kills": 4,
                "Deaths": 5,
                "Assists": 4,
                "KDA": 1.6,
                "DPM": 450,
                "GPM": 380,
                "KP%": 60,
                "Objectives Stolen": 0,
                "Match_count": 3,
            },
        ]
    )

    out = compute_matador(raw_df)

    assert list(out.columns) == [
        "Rank",
        "Player",
        "Team",
        "Role",
        "Matador Score",
        "Tier",
        "Kills",
        "Deaths",
        "Assists",
        "KDA",
        "DPM",
        "GPM",
        "KP%",
        "Match_count",
    ]
    assert out.iloc[0]["Rank"] == 1
    assert out.iloc[0]["Player"] == "p1"
    assert 0 <= out["Matador Score"].min() <= out["Matador Score"].max() <= 100


def test_compute_matador_return_empty_when_required_columns_missing():
    out = compute_matador(pd.DataFrame([{"Player": "p1"}]))
    assert out.empty
