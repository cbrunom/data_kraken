# -*- coding: utf-8 -*-

"""Download player images from Leaguepedia/Fandom using MediaWiki API.

This avoids direct page fetches that may return HTTP 403 in some environments.

Usage:
    python golgg/step010x_download_player_images.py
    python golgg/step010x_download_player_images.py --overwrite
"""

from __future__ import annotations

import argparse
import csv
import re
import unicodedata
from pathlib import Path
from typing import Dict, List, Optional, Tuple
from urllib.parse import unquote, urlparse

from bs4 import BeautifulSoup
import requests
from requests.exceptions import SSLError
import urllib3

from golgg.pipeline.common import log_step_end, log_step_start


PLAYERS_BY_TEAM: Dict[str, List[str]] = {
    "fluxo_w7m": ["curty", "Peach", "hauz", "Bao", "ProDelta", "Aoshi", "Samyy", "Guchi", "Momochi"],
    "furia": ["Guigo", "Tatu", "Tutsz", "Ayu", "JoJo", "furyz", "Luuukz"],
    "leviatan": ["Devost", "Booki", "Enga", "ceo", "TopLop", "Kaiba", "NewCosmo", "SrVenancio", "cody"],
    "loud": ["Xyno", "YoungJae", "Mago", "Envy", "Bull", "RedBert", "Raise", "Sephis"],
    "pain_gaming": ["Robo", "CarioK", "tinowns", "Marvin", "Trigger", "Kuri", "Sarkis", "Keine", "Samkz"],
    "red_canids": ["fNb", "Curse", "Kaze", "Rabelo", "Stepz", "frosty", "tockers", "BeellzY"],
    "vivo_keyd_stars": ["Boal", "Disamis", "Mireu", "Qats", "Morttheus", "Kaiwing", "SeeEl", "Smiley", "Wizer"],
    "los": ["Zest", "Drakehero", "Feisty", "Duduhh", "Ackerman", "Enatron", "Invokid"],
    "leviatan": ["Devost", "Booki", "Enga", "ceo", "TopLop", "Kaiba", "NewCosmo", "SrVenancio", "cody", "Snaker"],
}

# Disambiguation for known ambiguous page titles.
PLAYER_TITLE_OVERRIDES: Dict[str, str] = {
    "bao": "BAO_(Jeong_Hyeon-woo)",
    "jojo": "JoJo_(Gabriel_Dzelme)",
    "tatu": "Tatu_(Pedro_Seixas)",
    "kaze": "Kaze_(Lucas_Fe)",
    "frosty": "Frosty_(JosÃ©_Eduardo)",
    "curse": "Curse_(RaÃ­_Yamada)",
    "ackerman": "Ackerman_(Gabriel_Aparicio)",
    "zest": "Zest_(Kim_Dong-min)",
    "newcosmo": "NewCosmo",
    "envy": "Envy_(Bruno_Farias)",
    "trigger": "Trigger_(Kim_Eui-joo)",
    "smiley": "Smiley_(Ludvig_Granquist)",
    "guchi": "Guchi",
    "ayu": "Ayu_(Andrey_Saraiva)",
}

PLAYER_ALIASES: Dict[str, List[str]] = {
    "guchi": ["gankkkk", "gankk", "silenced"],
    "smiley": ["xdsmiley", "xdsm1ley", "xdsmiley"],
}

# Known cases where the page has no player portrait in Media > Images (e.g., coach profile).
EXPECTED_NO_IMAGE_PLAYERS = {"newcosmo"}

TEAM_HINTS: Dict[str, List[str]] = {
    "fluxo_w7m": ["fluxo", "w7m", "fxw7"],
    "furia": ["furia"],
    "leviatan": ["leviatan", "leviatan esports"],
    "loud": ["loud"],
    "pain_gaming": ["pain", "pain gaming"],
    "red_canids": ["red canids", "red"],
    "vivo_keyd_stars": ["vivo keyd", "keyd"],
    "los": ["los", "los grandes"],
}

MANUAL_PLAYER_IMAGE_URLS: Dict[Tuple[str, str], str] = {
    ("red_canids", "stepz"): "https://static.wikia.nocookie.net/lolesports_gamepedia_en/images/1/18/RED_STEPZ_2026_Split_1.png/revision/latest?cb=20260327042536",
    ("red_canids", "curse"): "https://static.wikia.nocookie.net/lolesports_gamepedia_en/images/a/af/RED.A_Curse_2023_Split_2.png/revision/latest",
    ("red_canids", "frosty"): "https://static.wikia.nocookie.net/lolesports_gamepedia_en/images/8/85/RED_Frosty_2026_Split_1.png/revision/latest",
    ("leviatan", "cody"): "https://static.wikia.nocookie.net/lolesports_gamepedia_en/images/3/3b/FX_Cody_2026_Split_1.png/revision/latest",
}


def normalize_text(value: str) -> str:
    value = (
        value.replace("Ã˜", "O")
        .replace("Ã¸", "o")
        .replace("Ä", "D")
        .replace("Ä‘", "d")
    )
    value = unicodedata.normalize("NFKD", value)
    value = value.encode("ascii", "ignore").decode("ascii")
    value = value.lower()
    value = re.sub(r"[^a-z0-9]+", " ", value)
    return f" {value.strip()} "


def slugify(value: str) -> str:
    value = normalize_text(value).strip().replace(" ", "_")
    return value or "player"


def canonicalize_wikia_image_url(url: str) -> str:
    parsed = urlparse(url)
    scheme = parsed.scheme or "https"
    netloc = parsed.netloc
    path = parsed.path

    if "/images/thumb/" in path:
        path = path.replace("/images/thumb/", "/images/", 1)
        parts = path.split("/")
        if len(parts) >= 2 and re.match(r"^\d+px-", parts[-1]):
            path = "/".join(parts[:-1])

    # Keep best available version by trimming resize segment.
    scale_marker = "/revision/latest/scale-to-width-down/"
    if scale_marker in path:
        path = path.split(scale_marker)[0] + "/revision/latest"

    return f"{scheme}://{netloc}{path}"


def build_player_title(player_name: str) -> str:
    override = PLAYER_TITLE_OVERRIDES.get(slugify(player_name))
    if override:
        return override
    return player_name.replace(" ", "_")


def fetch_player_page_html_from_api(player_title: str) -> str:
    endpoint = "https://lol.fandom.com/api.php"
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.0.0 Safari/537.36",
        "Referer": "https://lol.fandom.com/",
    }
    params = {
        "action": "parse",
        "page": player_title,
        "prop": "text",
        "format": "json",
        "formatversion": "2",
    }
    try:
        response = requests.get(endpoint, params=params, headers=headers, timeout=30)
    except SSLError:
        # Fallback for machines with invalid clock/cert chain issues.
        urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
        response = requests.get(endpoint, params=params, headers=headers, timeout=30, verify=False)
    response.raise_for_status()
    payload = response.json()

    if "error" in payload:
        raise RuntimeError(f"API parse error for {player_title}: {payload['error'].get('info', 'unknown')}")

    html = payload.get("parse", {}).get("text")
    if not isinstance(html, str) or not html.strip():
        raise RuntimeError(f"Empty parse output for {player_title}")
    return html


def choose_player_image_from_soup(soup: BeautifulSoup, player_name: str, team_key: str) -> Optional[str]:
    player_norm = normalize_text(player_name)
    player_key_norm = normalize_text(slugify(player_name).replace("_", " "))
    alias_norms = [normalize_text(alias) for alias in PLAYER_ALIASES.get(slugify(player_name), [])]
    team_norms = [normalize_text(v) for v in TEAM_HINTS.get(team_key, [])]

    best_url: Optional[str] = None
    best_score = -1

    for img in soup.find_all("img"):
        url = img.get("data-src") or img.get("src") or ""
        if not isinstance(url, str) or not url.startswith("http"):
            continue

        filename = unquote(urlparse(url).path.split("/")[-1])

        bits = [
            img.get("alt", ""),
            img.get("title", ""),
            img.get("data-image-name", ""),
            img.get("data-image-key", ""),
            " ".join(img.get("class") or []),
            filename,
        ]

        parent_a = img.find_parent("a")
        if parent_a is not None:
            bits.extend([
                parent_a.get("title", ""),
                parent_a.get("href", ""),
                parent_a.get_text(" ", strip=True),
            ])

        context = normalize_text(" ".join([b for b in bits if isinstance(b, str)]))

        score = 0
        if player_norm.strip() and player_norm in context:
            score += 100
        for team_norm in team_norms:
            if team_norm.strip() and team_norm in context:
                score += 30

        if " logo " in context or " team icon " in context or " logostd " in context:
            score -= 120
        if " infobox lolproslogo " in context:
            score -= 220
        if " player " in context or " split " in context:
            score += 20
        if " crop " in context or " icon " in context:
            score -= 30

        # Strong preference for tournament-era player images.
        if " 2026 " in context:
            score += 60
        if " split 1 " in context:
            score += 35
        if " cblol cup " in context:
            score += 35

        filename_norm = normalize_text(filename)
        has_player_match = (
            (player_norm.strip() and player_norm in context)
            or (player_key_norm.strip() and player_key_norm in context)
            or (player_norm.strip() and player_norm in filename_norm)
            or (player_key_norm.strip() and player_key_norm in filename_norm)
        )
        if not has_player_match:
            for alias_norm in alias_norms:
                if (
                    (alias_norm.strip() and alias_norm in context)
                    or (alias_norm.strip() and alias_norm in filename_norm)
                ):
                    has_player_match = True
                    break

        # Reject entries that do not mention the player directly.
        if not has_player_match:
            continue

        # Reject obvious logo assets even if score is high.
        if "logo" in filename.lower():
            continue

        if score < 0:
            continue

        if score > best_score:
            best_score = score
            best_url = canonicalize_wikia_image_url(url)

    if best_url:
        return best_url

    # Fallback: use Media > Images links if structured img tags are missing.
    for link in soup.find_all("a"):
        href = link.get("href") or ""
        if not href.startswith("https://static.wikia.nocookie.net/"):
            continue
        label = normalize_text(link.get_text(" ", strip=True) + " " + unquote(urlparse(href).path))
        if (player_norm in label or player_key_norm in label) and " logo " not in label:
            return canonicalize_wikia_image_url(href)

    return None


def download_image(url: str, target_path: Path) -> None:
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.0.0 Safari/537.36",
        "Referer": "https://lol.fandom.com/",
    }
    try:
        response = requests.get(url, headers=headers, timeout=30)
    except SSLError:
        # Fallback for machines with invalid clock/cert chain issues.
        urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
        response = requests.get(url, headers=headers, timeout=30, verify=False)
    response.raise_for_status()
    target_path.parent.mkdir(parents=True, exist_ok=True)
    target_path.write_bytes(response.content)


def image_extension(url: str) -> str:
    path = urlparse(url).path.lower()
    if path.endswith(".jpg") or path.endswith(".jpeg"):
        return ".jpg"
    if path.endswith(".webp"):
        return ".webp"
    return ".png"


def iter_players() -> List[Tuple[str, str]]:
    rows: List[Tuple[str, str]] = []
    for team_key, players in PLAYERS_BY_TEAM.items():
        for player in players:
            rows.append((team_key, player))
    return rows


def main() -> None:
    parser = argparse.ArgumentParser(description="Download roster player images from Leaguepedia")
    parser.add_argument("--output", default="golgg/images/player_images", help="Output image folder")
    parser.add_argument(
        "--csv",
        default="golgg/images/player_images/player_images_mapping.csv",
        help="CSV output path",
    )
    parser.add_argument("--overwrite", action="store_true", help="Overwrite existing files")
    args = parser.parse_args()

    output_dir = Path(args.output)
    csv_path = Path(args.csv)
    start_time = log_step_start("step010x_download_player_images")

    records: List[Dict[str, str]] = []

    for team_key, player_name in iter_players():
        page_title = build_player_title(player_name)
        page_url = f"https://lol.fandom.com/wiki/{page_title}"
        player_key = slugify(player_name)
        manual_image_url = MANUAL_PLAYER_IMAGE_URLS.get((team_key, player_key))

        row: Dict[str, str] = {
            "team_key": team_key,
            "player_name": player_name,
            "player_key": player_key,
            "page_url": page_url,
            "image_url": "",
            "target_file": "",
            "status": "",
            "error": "",
        }

        if player_key in EXPECTED_NO_IMAGE_PLAYERS:
            row["status"] = "skipped_expected_no_image"
            row["error"] = "Known profile without player portrait"
            records.append(row)
            print(f"SKIP (expected no image): {team_key} | {player_name}")
            continue

        try:
            image_url: Optional[str] = manual_image_url
            if not image_url:
                html = fetch_player_page_html_from_api(page_title)
                soup = BeautifulSoup(html, "html.parser")
                image_url = choose_player_image_from_soup(soup, player_name, team_key)
            if not image_url:
                row["status"] = "image_not_found"
                records.append(row)
                print(f"NOT FOUND: {team_key} | {player_name}")
                continue

            ext = image_extension(image_url)
            target = output_dir / f"{team_key}__{player_key}{ext}"
            row["image_url"] = image_url
            row["target_file"] = str(target)

            if target.exists() and not args.overwrite:
                row["status"] = "skipped_exists"
                records.append(row)
                print(f"SKIP: {target}")
                continue

            download_image(image_url, target)
            row["status"] = "downloaded"
            records.append(row)
            print(f"OK: {target}")
        except Exception as exc:  # pragma: no cover - network and remote errors
            row["status"] = "failed"
            error_text = str(exc)
            if isinstance(exc, SSLError) or "certificate verify failed" in error_text.lower():
                error_text = (
                    f"{error_text} | hint: check system date/time/timezone and sync clock, "
                    "then retry step010x_download_player_images"
                )
            row["error"] = error_text
            records.append(row)
            print(f"FAIL: {team_key} | {player_name} | {error_text}")

    csv_path.parent.mkdir(parents=True, exist_ok=True)
    with csv_path.open("w", encoding="utf-8", newline="") as fh:
        writer = csv.DictWriter(
            fh,
            fieldnames=[
                "team_key",
                "player_name",
                "player_key",
                "page_url",
                "image_url",
                "target_file",
                "status",
                "error",
            ],
        )
        writer.writeheader()
        writer.writerows(records)

    print(f"\nCSV: {csv_path}")
    log_step_end("step010x_download_player_images", start_time)


if __name__ == "__main__":
    main()

