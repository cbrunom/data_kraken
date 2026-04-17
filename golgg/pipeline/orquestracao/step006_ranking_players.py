# -*- coding: utf-8 -*-

import os.path
import time
from pathlib import Path

import pandas as pd

from golgg.pipeline.common import log_step_end, log_step_start
from golgg.pipeline.publicacao import layered_output_path
from golgg.pipeline.publicacao import write_csv_with_compat
from golgg.pipeline.transformacao.ranking_players import (
    build_player_grades,
    build_raw_player_stats,
    prepare_fullstats_for_ranking,
)


BRONZE_TORNEIOS_DIR = Path("golgg/data/bronze/torneios")
SILVER_FULLSTATS_DIR = Path("golgg/data/silver/fullstats")


def main() -> None:
    start_time = log_step_start("step006_ranking_players")
    if not BRONZE_TORNEIOS_DIR.exists():
        print(f"Missing bronze tournament directory: {BRONZE_TORNEIOS_DIR}")
        log_step_end("step006_ranking_players", start_time)
        return

    tournament_dirs = [entry for entry in sorted(BRONZE_TORNEIOS_DIR.iterdir(), key=lambda path: path.name.lower()) if entry.is_dir()]
    torneios = len(tournament_dirs)
    print(torneios)

    torneio = 0
    for tournament_dir in tournament_dirs:
        torneio += 1
        print(f"---{torneio}/{torneios} - %s seconds ---" % (time.time() - start_time))
        nome_torneio = tournament_dir.name

        df = pd.read_csv(SILVER_FULLSTATS_DIR / f"fullstats_{nome_torneio.replace(' ', '_')}.csv")
        df = prepare_fullstats_for_ranking(df)

        raw_player_stats, player_avg, player_match_count = build_raw_player_stats(df)
        write_csv_with_compat(
            raw_player_stats,
            layered_output_path(f"golgg/player_raw_torneios/raw_player_stats_{nome_torneio.replace(' ', '_')}.csv", "silver"),
            "silver",
            index=False,
            header=True,
        )

        player_grade = build_player_grades(player_avg, player_match_count)
        write_csv_with_compat(
            player_grade,
            layered_output_path(f"golgg/player_grades_torneios/player_grades_{nome_torneio.replace(' ', '_')}.csv", "silver"),
            "silver",
            index=False,
            header=True,
        )

    print(f"--- %s seconds ---" % (time.time() - start_time))
    log_step_end("step006_ranking_players", start_time)


if __name__ == "__main__":
    main()
