import re

from bs4 import BeautifulSoup


def normalize_text(value: str) -> str:
    return re.sub(r"\s+", " ", str(value)).strip().lower()


def read_tournament_rows(html: str) -> list[dict[str, str]]:
    soup = BeautifulSoup(html, "html.parser")

    table = soup.select_one("#result_tab table")
    if table is None:
        raise RuntimeError("Could not find tournament list table on gol.gg/tournament/list")

    body = table.find("tbody")
    if body is None:
        raise RuntimeError("Tournament table body is missing")

    rows: list[dict[str, str]] = []
    for tr in body.find_all("tr"):
        cells = tr.find_all("td")
        if len(cells) < 6:
            continue

        offset = 1 if len(cells) >= 7 and not cells[0].get_text(" ", strip=True) else 0

        name = cells[offset + 0].get_text(" ", strip=True)
        region = cells[offset + 1].get_text(" ", strip=True)
        number_of_games = cells[offset + 2].get_text(" ", strip=True)
        game_duration = cells[offset + 3].get_text(" ", strip=True)
        first_game = cells[offset + 4].get_text(" ", strip=True)
        last_game = cells[offset + 5].get_text(" ", strip=True)

        rows.append(
            {
                "NAME": name,
                "REGION": region,
                "NUMBER OF GAMES": number_of_games,
                "GAME DURATION": game_duration,
                "FIRST GAME": first_game,
                "LAST GAME": last_game,
            }
        )

    return rows
