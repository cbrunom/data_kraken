"""Download champion square images from the League of Legends Wiki.

The script starts from the Champion squares category, walks through every
subcategory recursively, collects file links from each category page, resolves
the original image URL through file pages, and stores the images locally.

By default the files are saved under golgg/images/champion_squares.
"""

from __future__ import annotations

import argparse
import html.parser
import re
import time
from pathlib import Path
from urllib.parse import unquote, urljoin

from playwright.sync_api import sync_playwright

from golgg.pipeline.common import (
    log_step_end,
    log_step_start,
    normalize_champion_square_filename,
)


BASE_URL = "https://wiki.leagueoflegends.com"
START_URL = f"{BASE_URL}/en-us/Category:Champion_squares"
OUTPUT_DIR = Path(__file__).resolve().parent / "images" / "champion_squares"
FILE_URL_TEMPLATE = f"{BASE_URL}/en-us/File:{{file_title}}"
MANUAL_CHAMPION_FILE_TITLES = [
    "Kai'Sa_OriginalSquare.png",
    "Rek'Sai_OriginalSquare.png",
]
HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/124.0 Safari/537.36",
    "Accept-Language": "en-US,en;q=0.9",
}
INVALID_FILENAME_CHARS = r'<>:"/\\|?*'


class CategoryPageParser(html.parser.HTMLParser):
    def __init__(self) -> None:
        super().__init__()
        self._div_stack: list[str | None] = []
        self.subcategory_urls: list[str] = []
        self.file_titles: list[str] = []

    @property
    def current_section(self) -> str | None:
        for value in reversed(self._div_stack):
            if value in {"mw-subcategories", "mw-category-media"}:
                return value
        return None

    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        attr_map = dict(attrs)

        if tag == "div":
            self._div_stack.append(attr_map.get("id"))
            return

        if tag != "a":
            return

        href = attr_map.get("href") or ""
        section = self.current_section

        if section == "mw-subcategories" and href.startswith("/en-us/Category:"):
            absolute_url = urljoin(BASE_URL, href)
            if absolute_url not in self.subcategory_urls:
                self.subcategory_urls.append(absolute_url)

        if section == "mw-category-media" and href.startswith("/en-us/File:"):
            file_title = unquote(href.split("/en-us/File:", 1)[1])
            if file_title not in self.file_titles:
                self.file_titles.append(file_title)

    def handle_endtag(self, tag: str) -> None:
        if tag == "div" and self._div_stack:
            self._div_stack.pop()


class FilePageParser(html.parser.HTMLParser):
    def __init__(self) -> None:
        super().__init__()
        self.direct_image_urls: list[str] = []
        self.image_urls: list[str] = []

    def feed(self, data: str) -> None:
        self.direct_image_urls = extract_file_page_direct_urls(data)
        self.image_urls = extract_file_page_img_urls(data)
        super().feed(data)


def safe_filename(name: str) -> str:
    cleaned = re.sub(f"[{re.escape(INVALID_FILENAME_CHARS)}]", "_", name)
    cleaned = cleaned.replace("\n", " ").replace("\r", " ").strip()
    return cleaned or "image"


def normalize_existing_champion_square_files(output_dir: Path) -> None:
    if not output_dir.exists():
        return

    for image_path in sorted(output_dir.glob("*_OriginalSquare.png")):
        normalized_name = normalize_champion_square_filename(image_path.name)
        normalized_path = output_dir / normalized_name
        if normalized_path.name == image_path.name:
            continue

        if image_path.name.lower() == normalized_path.name.lower() and image_path.name != normalized_path.name:
            file_bytes = image_path.read_bytes()
            image_path.unlink(missing_ok=True)
            normalized_path.write_bytes(file_bytes)
            continue

        if normalized_path.exists() and normalized_path.stat().st_size > 0:
            image_path.unlink(missing_ok=True)
            continue

        image_path.rename(normalized_path)


class WikiBrowser:
    def __init__(self) -> None:
        self._playwright = sync_playwright().start()
        self._browser = self._playwright.chromium.launch(headless=True)
        self._page = self._browser.new_page(viewport={"width": 1400, "height": 900})

    def _goto_with_retry(self, url: str):
        # Handle source throttling (HTTP 429) and transient network issues without failing the full run.
        max_attempts = 6
        for attempt in range(1, max_attempts + 1):
            try:
                response = self._page.goto(url, wait_until="domcontentloaded", timeout=60000)
                if response is None:
                    raise RuntimeError("empty response")

                status = response.status
                if status == 429:
                    wait_seconds = min(60, 2 ** attempt)
                    print(
                        f"[retry] HTTP 429 for {url} | attempt={attempt}/{max_attempts} "
                        f"waiting={wait_seconds}s",
                        flush=True,
                    )
                    time.sleep(wait_seconds)
                    continue

                if status >= 500:
                    wait_seconds = min(30, attempt * 2)
                    print(
                        f"[retry] HTTP {status} for {url} | attempt={attempt}/{max_attempts} "
                        f"waiting={wait_seconds}s",
                        flush=True,
                    )
                    time.sleep(wait_seconds)
                    continue

                if status >= 400:
                    raise RuntimeError(f"Could not load {url} (HTTP {status})")

                return response
            except Exception as exc:
                if attempt == max_attempts:
                    raise RuntimeError(f"Could not load {url} after {max_attempts} attempts: {exc}") from exc
                wait_seconds = min(20, attempt * 2)
                print(
                    f"[retry] fetch error for {url} | attempt={attempt}/{max_attempts} "
                    f"waiting={wait_seconds}s error={exc}",
                    flush=True,
                )
                time.sleep(wait_seconds)

        raise RuntimeError(f"Could not load {url}")

    def fetch_html(self, url: str) -> str:
        self._goto_with_retry(url)
        return self._page.content()

    def fetch_bytes(self, url: str) -> bytes:
        response = self._goto_with_retry(url)
        return response.body()

    def close(self) -> None:
        self._browser.close()
        self._playwright.stop()


def parse_category_page(html: str) -> CategoryPageParser:
    parser = CategoryPageParser()
    parser.feed(html)
    return parser


def extract_subcategory_urls(html: str) -> list[str]:
    return parse_category_page(html).subcategory_urls


def extract_file_titles(html: str) -> list[str]:
    return parse_category_page(html).file_titles


def is_original_square_file(file_title: str) -> bool:
    normalized = file_title.lower()
    return "_originalsquare." in normalized


def is_direct_champion_category_url(url: str) -> bool:
    title = url.rsplit("/", 1)[-1]
    if not title.startswith("Category:") or title == "Category:Champion_squares":
        return False

    category_name = title.removeprefix("Category:")
    excluded_prefixes = (
        "Old_",
        "Unused_",
        "Special_",
        "TFT_",
        "WR_",
    )
    return not category_name.startswith(excluded_prefixes)


def extract_best_image_url(srcset: str, src: str) -> str:
    candidates: list[tuple[int, str]] = []

    if srcset:
        for part in srcset.split(","):
            item = part.strip()
            if not item:
                continue
            pieces = item.split()
            if not pieces:
                continue
            candidate_url = pieces[0]
            multiplier = 1
            if len(pieces) > 1 and pieces[1].endswith("x"):
                try:
                    multiplier = int(float(pieces[1][:-1]) * 1000)
                except ValueError:
                    multiplier = 1
            candidates.append((multiplier, candidate_url))

    if candidates:
        candidates.sort(key=lambda item: item[0], reverse=True)
        return urljoin(BASE_URL, candidates[0][1])

    if src:
        return urljoin(BASE_URL, src)

    return ""


def extract_file_page_direct_urls(html: str) -> list[str]:
    match = re.search(r'<meta property="og:image" content="([^"]+)"', html)
    if match and "/thumb/" not in match.group(1):
        return [match.group(1)]

    match = re.search(r'href="(/en-us/images/(?!thumb/)[^"]+)"', html)
    if match:
        return [urljoin(BASE_URL, match.group(1))]

    return []


def extract_file_page_img_urls(html: str) -> list[str]:
    image_urls: list[str] = []
    for match in re.finditer(r'<img[^>]+srcset="([^"]+)"[^>]+src="([^"]+)"', html):
        image_url = extract_best_image_url(match.group(1), match.group(2))
        if image_url and image_url not in image_urls:
            image_urls.append(image_url)
    return image_urls


def resolve_original_image_url(browser: WikiBrowser, file_title: str) -> str:
    file_url = FILE_URL_TEMPLATE.format(file_title=file_title)
    html = browser.fetch_html(file_url)
    direct_urls = extract_file_page_direct_urls(html)
    if direct_urls:
        return direct_urls[0]

    image_urls = extract_file_page_img_urls(html)
    for image_url in image_urls:
        if "/images/" in image_url and "/thumb/" not in image_url:
            return image_url

    if image_urls:
        return image_urls[0]

    raise RuntimeError(f"Could not resolve original URL for {file_title}")


def download_file(browser: WikiBrowser, file_title: str, output_dir: Path) -> Path:
    source_url = resolve_original_image_url(browser, file_title)
    filename = normalize_champion_square_filename(Path(unquote(file_title)).name)
    target_path = output_dir / filename

    if target_path.exists() and target_path.stat().st_size > 0:
        return target_path

    target_path.write_bytes(browser.fetch_bytes(source_url))
    return target_path


def format_duration(total_seconds: float) -> str:
    seconds = int(total_seconds)
    hours, remainder = divmod(seconds, 3600)
    minutes, secs = divmod(remainder, 60)
    if hours > 0:
        return f"{hours:02d}:{minutes:02d}:{secs:02d}"
    return f"{minutes:02d}:{secs:02d}"


def crawl_category(browser: WikiBrowser, start_url: str, recursive: bool = False) -> list[str]:
    visited_categories: set[str] = set()
    queued_categories = [start_url]
    collected_files: list[str] = []
    seen_files: set[str] = set()
    crawled_categories = 0
    crawl_started_at = time.time()

    while queued_categories:
        current_url = queued_categories.pop()
        if current_url in visited_categories:
            continue
        visited_categories.add(current_url)
        crawled_categories += 1
        elapsed = format_duration(time.time() - crawl_started_at)
        print(
            f"[crawl] category={crawled_categories} queued={len(queued_categories)} "
            f"visited={len(visited_categories)} files={len(collected_files)} elapsed={elapsed}\n"
            f"        url={current_url}",
            flush=True,
        )

        html = browser.fetch_html(current_url)

        for file_title in extract_file_titles(html):
            if not is_original_square_file(file_title):
                continue
            if file_title not in seen_files:
                seen_files.add(file_title)
                collected_files.append(file_title)

        if not recursive and current_url != start_url:
            continue

        for subcategory_url in extract_subcategory_urls(html):
            if current_url == start_url and not recursive and not is_direct_champion_category_url(subcategory_url):
                continue
            if subcategory_url not in visited_categories:
                queued_categories.append(subcategory_url)

    print(
        f"[crawl-summary] categories={crawled_categories} unique_files={len(collected_files)} "
        f"elapsed={format_duration(time.time() - crawl_started_at)}",
        flush=True,
    )

    return collected_files


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Download champion square images from the League of Legends Wiki."
    )
    parser.add_argument(
        "--url",
        default=START_URL,
        help="Category URL to start from. Defaults to the Champion squares category.",
    )
    parser.add_argument(
        "--output",
        default=str(OUTPUT_DIR),
        help="Directory where downloaded images will be stored.",
    )
    parser.add_argument(
        "--recursive",
        action="store_true",
        help="Traverse nested subcategories as well as the direct champion categories.",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    output_dir = Path(args.output)
    output_dir.mkdir(parents=True, exist_ok=True)
    normalize_existing_champion_square_files(output_dir)
    start_time = log_step_start("step009x_download_champion_squares")

    browser = WikiBrowser()
    try:
        mode_label = "recursive" if args.recursive else "direct-only"
        print(f"[info] crawl mode={mode_label} start_url={args.url}", flush=True)
        file_titles = crawl_category(browser, args.url, recursive=args.recursive)
        for manual_file_title in MANUAL_CHAMPION_FILE_TITLES:
            if manual_file_title not in file_titles:
                file_titles.append(manual_file_title)
        print(f"Found {len(file_titles)} champion square files", flush=True)

        downloaded = 0
        skipped_existing = 0
        download_started_at = time.time()
        total_files = len(file_titles)
        for file_title in file_titles:
            expected_target_path = output_dir / normalize_champion_square_filename(Path(unquote(file_title)).name)
            was_existing = expected_target_path.exists() and expected_target_path.stat().st_size > 0
            target_path = download_file(browser, file_title, output_dir)
            if was_existing:
                skipped_existing += 1
                status = "skip"
            else:
                downloaded += 1
                status = "download"

            processed = downloaded + skipped_existing
            elapsed = format_duration(time.time() - download_started_at)
            print(
                f"[files] {processed}/{total_files} status={status} "
                f"new={downloaded} skipped={skipped_existing} elapsed={elapsed}\n"
                f"        {file_title} -> {target_path}",
                flush=True,
            )

        print(
            f"Finished. total={total_files} new_downloads={downloaded} "
            f"skipped_existing={skipped_existing} output={output_dir}",
            flush=True,
        )
    finally:
        browser.close()
        log_step_end("step009x_download_champion_squares", start_time)


if __name__ == "__main__":
    main()
