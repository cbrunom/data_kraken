# -*- coding: utf-8 -*-
"""Single entrypoint to run the Gol.gg data pipeline.

This module keeps the current step-based architecture, but offers one command
for beginners: `python -m golgg.main`.
"""

from __future__ import annotations

import argparse
import subprocess
import sys
from typing import Iterable

MANDATORY_STEP_MODULES = [
    "golgg.pipeline.orquestracao.step001_generate_teams_all_xlsx",
    "golgg.pipeline.orquestracao.step002_info_teams",
    "golgg.pipeline.orquestracao.step003_generate_torneios_por_season_xlsx",
    "golgg.pipeline.orquestracao.step004_partidas_torneios",
    "golgg.pipeline.orquestracao.step005_fullstats_partidas_torneio",
    "golgg.pipeline.orquestracao.step006_ranking_players",
    "golgg.pipeline.orquestracao.step007_infographic_dataset",
]

OPTIONAL_ASSET_STEP_MODULES = [
    "golgg.step009x_download_champion_squares",
    "golgg.step010x_download_player_images",
    "golgg.step011x_download_team_logos",
]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run pipeline steps from a single command")
    parser.add_argument(
        "--with-assets",
        action="store_true",
        help="Also run optional image/logo download steps (009x, 010x, 011x)",
    )
    return parser.parse_args()


def run_modules(modules: Iterable[str]) -> None:
    for module in modules:
        print(f"[pipeline-main] running {module}", flush=True)
        subprocess.run([sys.executable, "-m", module], check=True)


def main() -> None:
    args = parse_args()

    run_modules(MANDATORY_STEP_MODULES)
    if args.with_assets:
        run_modules(OPTIONAL_ASSET_STEP_MODULES)

    print("[pipeline-main] done", flush=True)


if __name__ == "__main__":
    main()
