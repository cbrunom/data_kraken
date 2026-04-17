# -*- coding: utf-8 -*-

"""Download target team logos from a saved Leaguepedia HTML page.

Usage:
    python golgg/step011x_download_team_logos.py
    python golgg/step011x_download_team_logos.py --html legacy/root_scratch/teams.html --overwrite
"""

from __future__ import annotations

import argparse
import csv
import re
import unicodedata
from pathlib import Path
from typing import Dict, List, Optional
from urllib.parse import unquote, urlparse

import requests
from bs4 import BeautifulSoup

from golgg.pipeline.common import log_step_end, log_step_start


TEAM_PATTERNS: Dict[str, Dict[str, List[str]]] = {
    "fluxo_w7m": {
        "wiki": ["/wiki/fluxo", "/wiki/w7m"],
        "text": ["fluxo w7m", "fluxo", "w7m"],
    },
    "furia": {
        "wiki": ["/wiki/furia"],
        "text": ["furia"],
    },
    "leviatan": {
        "wiki": ["/wiki/leviatan"],
        "text": ["leviatan", "leviatan esports"],
    },
    "loud": {
        "wiki": ["/wiki/loud"],
        "text": ["loud"],
    },
    "pain_gaming": {
        "wiki": ["/wiki/pain_gaming", "/wiki/pain"],
        "text": ["pain gaming", "pain"],
    },
    "red_canids": {
        "wiki": ["/wiki/red_canids"],
        "text": ["red canids", "red kalunga"],
    },
    "vivo_keyd_stars": {
        "wiki": ["/wiki/vivo_keyd_stars", "/wiki/vivo_keyd"],
        "text": ["vivo keyd stars", "vivo keyd", "keyd stars"],
    },
    "los": {
        "wiki": ["/wiki/los", "/wiki/los_grandes"],
        "text": ["los grandes", "los"],
    },
}


TEAM_URL_OVERRIDES: Dict[str, str] = {
    "fluxo_w7m": "https://static.wikia.nocookie.net/lolesports_gamepedia_en/images/1/14/Fluxo_W7Mlogo_square.png/revision/latest/scale-to-width-down/123?cb=20260105211238",
    "furia": "https://static.wikia.nocookie.net/lolesports_gamepedia_en/images/f/f2/FURIAlogo_square.png/revision/latest/scale-to-width-down/123?cb=20211007041545",
    "leviatan": "https://static.wikia.nocookie.net/lolesports_gamepedia_en/images/2/2f/Leviatanlogo_square.png/revision/latest/scale-to-width-down/123?cb=20230406020015",
    "pain_gaming": "https://static.wikia.nocookie.net/lolesports_gamepedia_en/images/7/7c/PaiN_Gaminglogo_square.png/revision/latest/scale-to-width-down/123?cb=20221211061225",
    "red_canids": "https://static.wikia.nocookie.net/lolesports_gamepedia_en/images/2/27/RED_Canidslogo_square.png/revision/latest/scale-to-width-down/123?cb=20240305110229",
    "vivo_keyd_stars": "https://static.wikia.nocookie.net/lolesports_gamepedia_en/images/1/15/Vivo_Keyd_Starslogo_square.png/revision/latest/scale-to-width-down/123?cb=20221130053621",
    "los": "https://static.wikia.nocookie.net/lolesports_gamepedia_en/images/b/bc/L%C3%98Slogo_square.png/revision/latest/scale-to-width-down/123?cb=20230608222207",
    "loud": "https://static.wikia.nocookie.net/lolesports_gamepedia_en/images/7/76/LOUDlogo_square.png/revision/latest/scale-to-width-down/123?cb=20260105230305",
}


def normalize_text(value: str) -> str:
    # Normalize common special chars used in team names before ASCII cleanup.
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


def canonicalize_wikia_url(url: str) -> str:
    parsed = urlparse(url)
    path = parsed.path

    # Leaguepedia/Fandom thumbnails usually look like:
    # /images/thumb/a/ab/File.png/120px-File.png
    # We convert them to original image path:
    # /images/a/ab/File.png
    if "/images/thumb/" in path:
        path = path.replace("/images/thumb/", "/images/", 1)
        parts = path.split("/")
        if len(parts) >= 2 and re.match(r"^\d+px-", parts[-1]):
            path = "/".join(parts[:-1])

    scheme = parsed.scheme or "https"
    netloc = parsed.netloc
    if not netloc and url.startswith("//"):
        netloc = urlparse(f"https:{url}").netloc

    return f"{scheme}://{netloc}{path}"


def choose_image_url(img_tag) -> Optional[str]:
    for attr in ("data-src", "src", "data-original"):
        value = img_tag.get(attr)
        if value:
            url = value.strip()
            if url.startswith("//"):
                url = f"https:{url}"
            if url.startswith("http://") or url.startswith("https://"):
                return canonicalize_wikia_url(url)
    return None


def collect_context(img_tag) -> str:
    pieces: List[str] = []

    for attr in ("alt", "title", "data-image-name", "data-image-key"):
        value = img_tag.get(attr)
        if isinstance(value, str) and value.strip():
            pieces.append(value)

    class_attr = img_tag.get("class")
    if isinstance(class_attr, list):
        pieces.append(" ".join(class_attr))

    anchor = img_tag.find_parent("a")
    if anchor is not None:
        pieces.append(anchor.get("title", ""))
        pieces.append(anchor.get("href", ""))
        pieces.append(anchor.get_text(" ", strip=True))

    filename = unquote(urlparse(choose_image_url(img_tag) or "").path.split("/")[-1])
    if filename:
        pieces.append(filename)

    return normalize_text(" ".join(pieces))


def score_candidate(team_key: str, context: str) -> int:
    spec = TEAM_PATTERNS[team_key]
    score = 0

    for wiki_alias in spec["wiki"]:
        alias_norm = normalize_text(wiki_alias)
        if alias_norm.strip() and alias_norm in context:
            score += 60

    for text_alias in spec["text"]:
        alias_norm = normalize_text(text_alias)
        if alias_norm.strip() and alias_norm in context:
            score += 20 + len(text_alias)

    # Prefer square logos over standard ones when both appear in page content.
    if " logo square " in context:
        score += 90
    if " logo std " in context:
        score -= 35

    # Penalize very weak LOS-only matches to avoid accidental Portuguese/Spanish noise.
    if team_key == "los" and " los " in context and " los grandes " not in context and "/wiki/los" not in context:
        score -= 20

    return score


def extract_best_team_logo_candidates(html_path: Path) -> Dict[str, Dict[str, object]]:
    html = html_path.read_text(encoding="utf-8", errors="ignore")
    soup = BeautifulSoup(html, "html.parser")

    best: Dict[str, Dict[str, object]] = {}

    for img in soup.find_all("img"):
        url = choose_image_url(img)
        if not url:
            continue

        context = collect_context(img)
        if not context.strip():
            continue

        for team_key in TEAM_PATTERNS:
            score = score_candidate(team_key, context)
            if score <= 0:
                continue

            current = best.get(team_key)
            if current is None or score > int(current["score"]):
                best[team_key] = {
                    "score": score,
                    "url": url,
                }

    return best


def write_mapping_csv(csv_path: Path, rows: List[Dict[str, str]]) -> None:
    csv_path.parent.mkdir(parents=True, exist_ok=True)
    fieldnames = ["team_key", "found", "url", "target_file", "download_status", "error"]
    with csv_path.open("w", encoding="utf-8", newline="") as fh:
        writer = csv.DictWriter(fh, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def download_file(url: str, target_path: Path) -> None:
    target_path.parent.mkdir(parents=True, exist_ok=True)
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.0.0 Safari/537.36",
        "Referer": "https://lol.fandom.com/",
    }

    response = requests.get(url, headers=headers, timeout=30)
    response.raise_for_status()
    target_path.write_bytes(response.content)


def extension_from_url(url: str) -> str:
    path = urlparse(url).path.lower()
    if path.endswith(".jpg") or path.endswith(".jpeg"):
        return ".jpg"
    if path.endswith(".webp"):
        return ".webp"
    return ".png"


def main() -> None:
    parser = argparse.ArgumentParser(description="Download selected CBLOL team logos from local HTML")
    parser.add_argument(
        "--html",
        default="legacy/root_scratch/teams.html",
        help="Path to local HTML file",
    )
    parser.add_argument(
        "--output",
        default="golgg/images/team_logos",
        help="Output directory for downloaded logos",
    )
    parser.add_argument(
        "--overwrite",
        action="store_true",
        help="Overwrite existing files",
    )
    parser.add_argument(
        "--csv",
        default="golgg/images/team_logos/team_logos_mapping.csv",
        help="CSV output path for mapping and download results",
    )
    args = parser.parse_args()
    start_time = log_step_start("step011x_download_team_logos")

    html_path = Path(args.html)
    output_dir = Path(args.output)
    csv_path = Path(args.csv)

    team_logo_urls: Dict[str, str] = {}
    if html_path.exists() and html_path.stat().st_size > 0:
        team_logo_candidates = extract_best_team_logo_candidates(html_path)
        team_logo_urls = {team: str(data["url"]) for team, data in team_logo_candidates.items()}
    else:
        print(f"WARN: HTML file not found/empty ({html_path}). Using URL overrides only.")

    for team_key, override_url in TEAM_URL_OVERRIDES.items():
        team_logo_urls[team_key] = canonicalize_wikia_url(override_url)
    csv_rows: List[Dict[str, str]] = []

    print("\n=== Team Logo Mapping ===")
    for team_key in TEAM_PATTERNS:
        print(f"- {team_key}: {team_logo_urls.get(team_key, 'NOT FOUND')}")

    print("\n=== Download ===")
    downloaded_by_url: Dict[str, Path] = {}
    for team_key in TEAM_PATTERNS:
        url = team_logo_urls.get(team_key)
        if not url:
            csv_rows.append(
                {
                    "team_key": team_key,
                    "found": "no",
                    "url": "",
                    "target_file": "",
                    "download_status": "not_found",
                    "error": "",
                }
            )
            continue

        ext = extension_from_url(url)
        target = output_dir / f"{team_key}{ext}"

        existing_for_same_url = downloaded_by_url.get(url)
        if existing_for_same_url is not None and existing_for_same_url.exists():
            target.write_bytes(existing_for_same_url.read_bytes())
            print(f"OK (copied same source): {target}")
            csv_rows.append(
                {
                    "team_key": team_key,
                    "found": "yes",
                    "url": url,
                    "target_file": str(target),
                    "download_status": "copied_from_same_url",
                    "error": "",
                }
            )
            continue

        if target.exists() and not args.overwrite:
            print(f"SKIP (exists): {target}")
            csv_rows.append(
                {
                    "team_key": team_key,
                    "found": "yes",
                    "url": url,
                    "target_file": str(target),
                    "download_status": "skipped_exists",
                    "error": "",
                }
            )
            continue

        try:
            download_file(url, target)
            print(f"OK: {target}")
            downloaded_by_url[url] = target
            csv_rows.append(
                {
                    "team_key": team_key,
                    "found": "yes",
                    "url": url,
                    "target_file": str(target),
                    "download_status": "downloaded",
                    "error": "",
                }
            )
        except Exception as exc:  # pragma: no cover - network and remote errors
            print(f"FAIL: {team_key} -> {url} | {exc}")
            csv_rows.append(
                {
                    "team_key": team_key,
                    "found": "yes",
                    "url": url,
                    "target_file": str(target),
                    "download_status": "failed",
                    "error": str(exc),
                }
            )

    write_mapping_csv(csv_path, csv_rows)
    print(f"\nCSV: {csv_path}")
    log_step_end("step011x_download_team_logos", start_time)


if __name__ == "__main__":
    main()

