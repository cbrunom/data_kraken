import requests
from bs4 import BeautifulSoup
import pandas as pd
import os
import time
import re
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, as_completed

from golgg.pipeline.common import HEADERS, elapsed_label, log_step_end, log_step_start
from golgg.pipeline.publicacao import layered_output_path
from golgg.pipeline.ingestao.partidas_torneios import (
    build_game_url_from_href,
    build_summary_from_href,
    create_session,
    deduplicate_torneios_df,
    load_existing_torneio_records,
    stage_type_from_stage,
)

BASE_LINK = "https://gol.gg/"
GAME_RESULT_WORKERS = 8
BRONZE_TORNEIOS_XLSX = Path("golgg/data/bronze/torneios_por_season.xlsx")
BRONZE_TORNEIOS_DIR = Path("golgg/data/bronze/torneios")


def extract_game_result(session, game_url):
    try:
        response = session.get(game_url, headers=HEADERS, timeout=30)
        soup = BeautifulSoup(response.text, "html.parser")
        winner_tag = soup.find(string=re.compile(r"\s-\sWIN"))
        loser_tag = soup.find(string=re.compile(r"\s-\sLOSS"))
        if winner_tag is None or loser_tag is None:
            return {"WinnerTeam": "N/A", "LoserTeam": "N/A", "GameDuration": "N/A"}

        winner_anchor = winner_tag.parent.find("a") if winner_tag.parent is not None else None
        loser_anchor = loser_tag.parent.find("a") if loser_tag.parent is not None else None
        if winner_anchor is None or loser_anchor is None:
            return {"WinnerTeam": "N/A", "LoserTeam": "N/A", "GameDuration": "N/A"}

        game_time = "N/A"
        game_time_label = soup.find(string=re.compile(r"^\s*Game Time\s*$", re.IGNORECASE))
        if game_time_label is not None:
            game_time_header = game_time_label.find_next("h1")
            if game_time_header is not None:
                game_time = game_time_header.get_text(" ", strip=True)

        return {
            "WinnerTeam": winner_anchor.get_text(" ", strip=True),
            "LoserTeam": loser_anchor.get_text(" ", strip=True),
            "GameDuration": game_time,
        }
    except Exception:
        return {"WinnerTeam": "N/A", "LoserTeam": "N/A", "GameDuration": "N/A"}


def extract_series_data(session, series_summary_url):
    """Return (best_of, game_entries) for a series page.
    Works for BO1/BO3/BO5 and returns available GAME N links.
    """
    best_of = 'UNKNOWN'
    game_links = []
    try:
        series_response = session.get(series_summary_url, headers=HEADERS, timeout=30)
        series_soup = BeautifulSoup(series_response.text, 'html.parser')

        best_of_tag = series_soup.find(string=re.compile(r'^\s*BO\d+\s*$'))
        if best_of_tag is not None:
            best_of = best_of_tag.strip().upper()

        for a_tag in series_soup.find_all('a'):
            text = a_tag.get_text(strip=True).upper()
            href = a_tag.get('href')
            if not href or not href.startswith('../game/stats/') or '/page-game/' not in href:
                continue
            if not text.startswith('GAME '):
                continue
            match = re.search(r'GAME\s+(\d+)', text)
            if match is None:
                continue
            game_number = int(match.group(1))
            game_links.append(
                (
                    game_number,
                    build_summary_from_href(href),
                    build_game_url_from_href(href),
                )
            )
    except Exception:
        # Fallback: keep at least one entry so pipeline does not lose the series.
        return best_of, [(1, series_summary_url, series_summary_url.replace("page-summary/", "page-game/"), {"WinnerTeam": "N/A", "LoserTeam": "N/A", "GameDuration": "N/A"})]

    if len(game_links) == 0:
        return best_of, [(1, series_summary_url, series_summary_url.replace("page-summary/", "page-game/"), {"WinnerTeam": "N/A", "LoserTeam": "N/A", "GameDuration": "N/A"})]

    # Remove duplicates and keep deterministic order by game number.
    unique_links = {}
    for game_number, game_summary_url, game_url in game_links:
        unique_links[game_number] = (game_summary_url, game_url)

    result_by_game_number = {}
    with ThreadPoolExecutor(max_workers=GAME_RESULT_WORKERS) as executor:
        future_to_game_number = {
            executor.submit(extract_game_result, session, game_url): game_number
            for game_number, (_, game_url) in unique_links.items()
        }
        for future in as_completed(future_to_game_number):
            game_number = future_to_game_number[future]
            try:
                result_by_game_number[game_number] = future.result()
            except Exception:
                result_by_game_number[game_number] = {"WinnerTeam": "N/A", "LoserTeam": "N/A", "GameDuration": "N/A"}

    ordered_games = []
    for game_number, (game_summary_url, game_url) in sorted(unique_links.items(), key=lambda item: item[0]):
        ordered_games.append((game_number, game_summary_url, game_url, result_by_game_number.get(game_number, {"WinnerTeam": "N/A", "LoserTeam": "N/A", "GameDuration": "N/A"})))
    return best_of, ordered_games


def run_pipeline():
    start_time = log_step_start("step004_partidas_torneios")
    df = pd.read_excel(BRONZE_TORNEIOS_XLSX)
    force_refresh = os.getenv("GOLGG_FORCE_REFRESH_STEP2", "0") == "1"
    session = create_session()

    num_torneios = len(df)
    all_torneios = []
    torneio_1 = 0

    for _, partida in df.iterrows():
        torneio_csv_path = layered_output_path(
            f"golgg/torneios/{partida['NAME']}/{partida['NAME'].replace(' ', '_')}.csv",
            "bronze",
        )
        if torneio_csv_path.exists() and not force_refresh:
            print(f"{torneio_csv_path} jÃ¡ existe, reutilizando para compor all_torneios")
            all_torneios.extend(load_existing_torneio_records(torneio_csv_path))
            torneio_1 += 1
            continue

        torneios = []
        print(f"---{torneio_1}/{num_torneios} - {elapsed_label(start_time)} ---")
        url = BASE_LINK + "tournament/tournament-matchlist/" + partida['NAME'] + "/"
        print(str(torneio_csv_path))
        httpx = session.get(url, headers=HEADERS, timeout=30)
        string = httpx.text
        soup = BeautifulSoup(string, 'html.parser')
        table = soup.find_all("tbody")
        li = table[0].find_all("tr")

        for x in li:
            chi = x.find_all("td")
            if len(chi) < 5:
                continue

            partida_nome = ''
            stage = chi[4].get_text(strip=True)
            try:
                partida_nome = chi[0].find("a").text
            except Exception:
                partida_nome = chi[0].text

            series_summary_link = '-'
            try:
                series_summary_link = build_summary_from_href(chi[0].find("a").get("href"))
            except Exception:
                series_summary_link = '-'

            if series_summary_link == '-':
                best_of = 'UNKNOWN'
                game_entries = [(1, '-', '-', {'WinnerTeam': 'N/A', 'LoserTeam': 'N/A', 'GameDuration': 'N/A'})]
            else:
                best_of, game_entries = extract_series_data(session, series_summary_link)

            series_winner = chi[1].get_text(strip=True)
            series_score = chi[2].get_text(strip=True)
            series_loser = chi[3].get_text(strip=True)

            for game_number, game_link, game_url, game_result in game_entries:
                torneio = {
                    'Torneio': partida['NAME'],
                    'Season': partida['SEASON'],
                    'Partida': partida_nome,
                    'Game': game_number,
                    'Stage': stage,
                    'StageType': stage_type_from_stage(stage),
                    'BestOf': best_of,
                    'SeriesWinner': series_winner,
                    'SeriesScore': series_score,
                    'SeriesLoser': series_loser,
                    'WinnerTeam': game_result.get('WinnerTeam', 'N/A'),
                    'LoserTeam': game_result.get('LoserTeam', 'N/A'),
                    'GameDuration': game_result.get('GameDuration', 'N/A'),
                    'Link': game_link,
                }
                torneios.append(torneio)
                all_torneios.append(torneio)

        torneios = pd.DataFrame(torneios)
        torneio_csv_path.parent.mkdir(parents=True, exist_ok=True)
        torneios.to_csv(torneio_csv_path, index=False)
        torneio_1 += 1

    all_torneios = pd.DataFrame(all_torneios)
    before_dedup = len(all_torneios)
    all_torneios = deduplicate_torneios_df(all_torneios)
    removed_duplicates = before_dedup - len(all_torneios)
    if removed_duplicates > 0:
        print(f"[step2] removed {removed_duplicates} duplicated game rows from all_torneios")
    all_torneios_path = layered_output_path("golgg/torneios/all_torneios.csv", "bronze")
    all_torneios_path.parent.mkdir(parents=True, exist_ok=True)
    all_torneios.to_csv(all_torneios_path, index=False)
    session.close()
    log_step_end("step004_partidas_torneios", start_time)


if __name__ == "__main__":
    run_pipeline()

