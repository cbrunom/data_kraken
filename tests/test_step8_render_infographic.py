import pandas as pd

from golgg.step012x_render_infographic import iter_safe_names_from_dataset_files, read_or_empty


def test_iter_safe_names_from_dataset_files_use_most_played_suffix():
    files = [
        "golgg/infographic_ready/CBLOL_2026_Split_1_champion_summary.csv",
        "golgg/infographic_ready/CBLOL_2026_Split_1_most_played_champions.csv",
        "golgg/infographic_ready/CBLOL_Cup_2026_most_played_champions.csv",
    ]

    out = list(iter_safe_names_from_dataset_files(files))
    assert out == ["CBLOL_2026_Split_1", "CBLOL_Cup_2026"]


def test_read_or_empty_return_empty_dataframe_for_missing_path(tmp_path):
    missing = tmp_path / "missing.csv"
    out = read_or_empty(str(missing))
    assert isinstance(out, pd.DataFrame)
    assert out.empty

