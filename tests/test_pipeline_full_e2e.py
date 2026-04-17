import os
import subprocess
import sys

import pytest


ROOT = "golgg"


def _assert_file_exists(path):
    assert os.path.exists(path), f"Expected file not found: {path}"


def _run_step(module_name, timeout_seconds=900, extra_env=None, module_args=None):
    env = os.environ.copy()
    if extra_env:
        env.update(extra_env)

    cmd = [sys.executable, "-m", module_name]
    if module_args:
        cmd.extend(module_args)
    completed = subprocess.run(
        cmd,
        capture_output=True,
        text=True,
        timeout=timeout_seconds,
        env=env,
    )

    if completed.returncode != 0:
        stdout_tail = "\n".join(completed.stdout.splitlines()[-40:])
        stderr_tail = "\n".join(completed.stderr.splitlines()[-40:])
        raise AssertionError(
            f"Step failed: {module_name}\n"
            f"Exit code: {completed.returncode}\n"
            f"STDOUT tail:\n{stdout_tail}\n"
            f"STDERR tail:\n{stderr_tail}"
        )


@pytest.mark.full_e2e
@pytest.mark.e2e
def test_full_e2e_pipeline_run_manual_or_nightly():
    if os.getenv("RUN_FULL_E2E", "0") != "1":
        pytest.skip("Set RUN_FULL_E2E=1 to run full E2E pipeline")

    include_step0 = os.getenv("RUN_FULL_E2E_INCLUDE_STEP0", "0") == "1"
    include_step3 = os.getenv("RUN_FULL_E2E_INCLUDE_STEP3", "0") == "1"
    force_refresh_step2 = os.getenv("RUN_FULL_E2E_FORCE_REFRESH_STEP2", "0") == "1"

    step2_env = {"GOLGG_FORCE_REFRESH_STEP2": "1"} if force_refresh_step2 else None

    main_args = ["--with-assets"] if include_step0 else None
    _run_step("golgg.main", timeout_seconds=3600, extra_env=step2_env, module_args=main_args)

    if include_step3:
        _run_step("golgg.step008x_timeline_kills_partidas_torneio", timeout_seconds=2400)

    _run_step("golgg.step012x_render_infographic", timeout_seconds=1200)

    # Final artifact checks across active flow (0,1,2,4,5,7,8 + optional 3).
    _assert_file_exists(os.path.join(ROOT, "source", "info_teams.csv"))
    _assert_file_exists(os.path.join(ROOT, "torneios", "all_torneios.csv"))

    _assert_file_exists(os.path.join(ROOT, "fullstats", "fullstats_CBLOL_2026_Split_1.csv"))
    _assert_file_exists(os.path.join(ROOT, "fullstats", "fullstats_CBLOL_Cup_2026.csv"))

    _assert_file_exists(os.path.join(ROOT, "player_raw_torneios", "raw_player_stats_CBLOL_2026_Split_1.csv"))
    _assert_file_exists(os.path.join(ROOT, "player_raw_torneios", "raw_player_stats_CBLOL_Cup_2026.csv"))

    _assert_file_exists(os.path.join(ROOT, "infographic_ready", "CBLOL_2026_Split_1_most_played_champions.csv"))
    _assert_file_exists(os.path.join(ROOT, "infographic_ready", "CBLOL_Cup_2026_most_played_champions.csv"))

    _assert_file_exists(os.path.join(ROOT, "infographic_ready", "images", "CBLOL_2026_Split_1_infographic.png"))
    _assert_file_exists(os.path.join(ROOT, "infographic_ready", "images", "CBLOL_Cup_2026_infographic.png"))

    if include_step3:
        _assert_file_exists(os.path.join(ROOT, "source", "all_kills.csv"))
        _assert_file_exists(os.path.join(ROOT, "source", "sum_kills.csv"))

    if include_step0:
        _assert_file_exists(os.path.join(ROOT, "images", "team_logos"))
        _assert_file_exists(os.path.join(ROOT, "images", "player_images"))

