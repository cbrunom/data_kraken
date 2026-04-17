import re
import time
from urllib.parse import unquote

import pandas as pd
import requests
from bs4 import BeautifulSoup


def extract_tournament_from_url(url):
    match = re.search(r"/tournament-([^/]+)/", str(url))
    if match is None:
        return "N/A"
    return unquote(match.group(1))


def extract_teams(worksheet, headers):
    teams = []
    num = 2
    start_time = time.time()

    while num <= worksheet.max_row:
        elapsed = time.time() - start_time
        print(f"---{num}/{worksheet.max_row} - {elapsed:.2f} seconds ---")

        hyperlink = worksheet.cell(row=num, column=1).hyperlink
        if hyperlink is None or not hyperlink.target:
            num += 1
            continue

        url = hyperlink.target
        response = requests.get(url, headers=headers, timeout=30)
        soup = BeautifulSoup(response.text, "html.parser")

        header_blocks = soup.find_all("div", {"class": "col-12 mt-4"})
        roster_tables = soup.find_all("table", {"class": "table_list footable toggle-square-filled"})
        if not header_blocks or not roster_tables:
            num += 1
            continue

        header_h1 = header_blocks[0].find("h1")
        roster_body = roster_tables[0].find("tbody")
        if header_h1 is None or roster_body is None:
            num += 1
            continue

        nome_time = header_h1.text
        roster_rows = roster_body.find_all("tr")

        for row in roster_rows:
            if row.find("em") is not None:
                continue

            row_cells = row.find_all("td")
            if len(row_cells) < 2:
                continue
            player_anchor = row_cells[1].find("a")
            if player_anchor is None:
                continue

            player_name = player_anchor.text
            season_cell = worksheet[f"B{num}"].value
            season_name = str(season_cell).replace("S", "") if season_cell is not None else "N/A"
            tournament_name = extract_tournament_from_url(url)
            teams_df = pd.DataFrame(teams, columns=["Team", "Season", "Tournament", "Role", "Player", "player_page"])
            already_exists = len(
                teams_df[
                    (teams_df["Player"] == player_name)
                    & (teams_df["Team"] == nome_time)
                    & (teams_df["Season"] == season_name)
                    & (teams_df["Tournament"] == tournament_name)
                ]
            ) > 0

            if already_exists:
                print(player_name)
                continue

            teams.append(
                {
                    "Team": nome_time,
                    "Season": season_name,
                    "Tournament": tournament_name,
                    "Role": row_cells[0].text.strip(),
                    "Player": player_name,
                    "player_page": "https://gol.gg" + player_anchor.get("href")[2:],
                }
            )

        num += 1

    return teams
