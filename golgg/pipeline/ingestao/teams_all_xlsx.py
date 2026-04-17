import re
from urllib.parse import unquote, urljoin

from bs4 import BeautifulSoup


def normalize_link(url: str) -> str:
    return str(url).replace(" ", "%20")


def build_team_url(page_url: str, href: str) -> str:
    href = str(href).strip()
    if href.startswith("./team-stats/"):
        href = "/teams/" + href[2:]
    elif href.startswith("team-stats/"):
        href = "/teams/" + href
    return normalize_link(urljoin(page_url, href))


def extract_tournament_name(url: str) -> str:
    match = re.search(r"/tournament-([^/]+)/", str(url))
    if match is None:
        return "N/A"
    return unquote(match.group(1))


def find_teams_table(soup: BeautifulSoup):
    for table in soup.find_all("table"):
        headers = [th.get_text(" ", strip=True) for th in table.find_all("th")]
        lower_headers = {value.lower() for value in headers}
        if {"name", "season", "games"}.issubset(lower_headers):
            return table
    return None


def extract_headers(table) -> list[str]:
    return [th.get_text(" ", strip=True) for th in table.find_all("th")]


def extract_rows(table, page_url: str) -> list[dict[str, object]]:
    body = table.find("tbody")
    if body is None:
        return []

    rows: list[dict[str, object]] = []
    tournament_name = extract_tournament_name(page_url)
    for tr in body.find_all("tr"):
        cells = tr.find_all("td")
        if not cells:
            continue

        values = [cell.get_text(" ", strip=True) for cell in cells]
        anchor = cells[0].find("a")
        team_url = ""
        if anchor and anchor.get("href"):
            team_url = build_team_url(page_url, anchor.get("href"))

        rows.append(
            {
                "values": values,
                "team_url": team_url,
                "tournament": tournament_name,
            }
        )

    return rows
