import glob
import os

import pandas as pd
import pytest


ROOT = "golgg"


def _safe_name_from_path(path, prefix, suffix):
    name = os.path.basename(path)
    if not (name.startswith(prefix) and name.endswith(suffix)):
        return None
    return name[len(prefix):-len(suffix)]


def _safe_names(pattern, prefix, suffix):
    names = []
    for path in glob.glob(pattern):
        safe_name = _safe_name_from_path(path, prefix, suffix)
        if safe_name:
            names.append(safe_name)
    return set(names)


def _assert_file_exists(path):
    assert os.path.exists(path), f"Expected file not found: {path}"


@pytest.mark.smoke
@pytest.mark.e2e
def test_smoke_e2e_pipeline_artifacts_chain():
    if os.getenv("RUN_SMOKE_E2E", "0") != "1":
        pytest.skip("Set RUN_SMOKE_E2E=1 to run smoke E2E pipeline checks")

    # Step 1
    _assert_file_exists(os.path.join(ROOT, "source", "info_teams.csv"))

    # Step 2
    all_torneios_path = os.path.join(ROOT, "torneios", "all_torneios.csv")
    _assert_file_exists(all_torneios_path)
    all_torneios_df = pd.read_csv(all_torneios_path)
    assert not all_torneios_df.empty

    # No true duplicate game rows in step2 output.
    strong_dupes = all_torneios_df.duplicated(subset=["Torneio", "Partida", "Stage", "Game"], keep=False)
    assert not strong_dupes.any(), "Found duplicate game rows in all_torneios.csv"

    # Step 3
    _assert_file_exists(os.path.join(ROOT, "source", "all_kills.csv"))
    _assert_file_exists(os.path.join(ROOT, "source", "sum_kills.csv"))

    # Step 4 -> Step 5 -> Step 7 -> Step 8 chain by tournament safe_name.
    safe_fullstats = _safe_names(
        os.path.join(ROOT, "fullstats", "fullstats_*.csv"),
        prefix="fullstats_",
        suffix=".csv",
    )
    safe_raw = _safe_names(
        os.path.join(ROOT, "player_raw_torneios", "raw_player_stats_*.csv"),
        prefix="raw_player_stats_",
        suffix=".csv",
    )
    safe_step7 = _safe_names(
        os.path.join(ROOT, "infographic_ready", "*_most_played_champions.csv"),
        prefix="",
        suffix="_most_played_champions.csv",
    )
    safe_step8 = _safe_names(
        os.path.join(ROOT, "infographic_ready", "images", "*_infographic.png"),
        prefix="",
        suffix="_infographic.png",
    )

    assert safe_step7, "No step7 outputs found"
    assert safe_step8, "No step8 rendered images found"

    # Every tournament rendered in step8 must exist through prior steps.
    for safe_name in sorted(safe_step8):
        assert safe_name in safe_step7, f"Missing step7 output for {safe_name}"
        assert safe_name in safe_raw, f"Missing step5 raw output for {safe_name}"
        assert safe_name in safe_fullstats, f"Missing step4 fullstats output for {safe_name}"

    # Step 0 assets (minimum smoke check for two currently required groups).
    player_images = glob.glob(os.path.join(ROOT, "images", "player_images", "*.png"))
    team_logos = glob.glob(os.path.join(ROOT, "images", "team_logos", "*.png"))
    assert player_images, "No player images found (step0 player images)"
    assert team_logos, "No team logos found (step0 team logos)"
