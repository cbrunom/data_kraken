# -*- coding: utf-8 -*-

import os
import time
from pathlib import Path

import pandas as pd
import requests
from bs4 import BeautifulSoup

from golgg.pipeline.common import log_step_end, log_step_start, normalize_champion_name
from golgg.pipeline.publicacao import layered_output_path
from golgg.pipeline.publicacao import write_csv_with_compat
from golgg.pipeline.transformacao.fullstats import apply_fullstats_transformations


BRONZE_TORNEIOS_DIR = Path("golgg/data/bronze/torneios")
SILVER_INFO_TEAMS_CSV = Path("golgg/data/silver/info_teams.csv")


def main() -> None:
    start_time = log_step_start("step005_fullstats_partidas_torneio")
    headers = {
        "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/75.0.3770.80 Safari/537.36"
    }

    try:
        info_teams = pd.read_csv(SILVER_INFO_TEAMS_CSV)
    except Exception:
        info_teams = pd.DataFrame(columns=["Player", "Season", "Tournament", "Team"])

    silver_fullstats_dir = Path("golgg/data/silver/fullstats")
    silver_fullstats_dir.mkdir(parents=True, exist_ok=True)

    if not BRONZE_TORNEIOS_DIR.exists():
        print(f"Missing bronze tournament directory: {BRONZE_TORNEIOS_DIR}")
        log_step_end("step005_fullstats_partidas_torneio", start_time)
        return

    tournament_dirs = [entry for entry in sorted(BRONZE_TORNEIOS_DIR.iterdir(), key=lambda path: path.name.lower()) if entry.is_dir()]
    torneios = len(tournament_dirs)
    torneio_1 = 0

    for tournament_dir in tournament_dirs:
        torneio_1 += 1
        nome_torneio = tournament_dir.name
        out_path = layered_output_path(f"golgg/fullstats/fullstats_{nome_torneio.replace(' ', '_')}.csv", "silver")

        print(nome_torneio)
        if out_path.exists():
            print(f"{out_path} jÃ¡ existe")
            continue

        print(f"---{torneio_1}/{torneios} - {time.time() - start_time:.2f} seconds ---")
        df = pd.read_csv(tournament_dir / f"{nome_torneio.replace(' ', '_')}.csv")

        matches = len(df)
        match = 0
        all_matches_fullstats = []

        for _, partida in df.iterrows():
            if partida["Link"] == "-":
                continue

            url = partida["Link"].replace("page-summary/", "page-fullstats/")
            try:
                httpx = requests.get(url, headers=headers, timeout=30)
                soup = BeautifulSoup(httpx.text, "html.parser")
                table = soup.find("table", attrs={"class": "completestats tablesaw"})
                if table is None:
                    print(f"Warning: No fullstats table found for {partida['Partida']}. Skipping.")
                    continue
                li = table.find_all("tr", recursive=False)
            except Exception as exc:
                print(f"Error fetching {url}: {exc}. Skipping.")
                continue

            torneio = soup.find("div", attrs={"class": "col-12 col-sm-7"}).find("a").text

            if len(table.find_all("th", recursive=True)) == 1:
                match += 1
                continue

            fullstats = []
            n = 1
            while n <= 10:
                dfx = {
                    "Torneio": torneio,
                    "Partida": partida["Partida"],
                    "Season": partida["Season"],
                    "Game": partida.get("Game", n),
                    "GameLink": partida.get("Link", "N/A"),
                    "Stage": partida.get("Stage", "N/A"),
                    "GameDuration": partida.get("GameDuration", "N/A"),
                    "WinnerTeam": partida.get("WinnerTeam", "N/A"),
                    "LoserTeam": partida.get("LoserTeam", "N/A"),
                    "SeriesWinner": partida.get("SeriesWinner", "N/A"),
                    "SeriesScore": partida.get("SeriesScore", "N/A"),
                    "SeriesLoser": partida.get("SeriesLoser", "N/A"),
                    "Champ": normalize_champion_name(table.find_all("th", recursive=True)[n].find("img", recursive=False)["alt"]),
                }

                for row in li:
                    chi = row.find_all("td", recursive=False)
                    dfx[f"{chi[0].text}"] = chi[n].text

                fullstats.append(dfx)
                n += 1

            dffullstats = pd.DataFrame(fullstats)
            if dffullstats.empty:
                continue

            dffullstats = apply_fullstats_transformations(dffullstats, info_teams)
            all_matches_fullstats.append(dffullstats)

            match += 1
            print(f"---{match}/{matches} - {time.time() - start_time:.2f} seconds ---")

        if all_matches_fullstats:
            fullstats_out = pd.concat(all_matches_fullstats, ignore_index=True)
            write_csv_with_compat(fullstats_out, out_path, "silver", index=False, header=True)

    log_step_end("step005_fullstats_partidas_torneio", start_time)


if __name__ == "__main__":
    main()
