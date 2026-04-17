import time

from playwright.sync_api import sync_playwright


def fetch_rendered_html(url: str) -> str:
    attempts = 4
    last_error: Exception | None = None

    with sync_playwright() as playwright:
        browser = playwright.chromium.launch(headless=True)
        page = browser.new_page(viewport={"width": 1400, "height": 900})
        try:
            for attempt in range(1, attempts + 1):
                try:
                    page.goto(url, wait_until="networkidle", timeout=90000)
                    page.wait_for_timeout(2500)
                    return page.content()
                except Exception as exc:  # pragma: no cover - network variability
                    last_error = exc
                    if attempt == attempts:
                        break
                    time.sleep(min(6, attempt * 2))
        finally:
            browser.close()

    raise RuntimeError(f"Could not fetch rendered HTML for {url}: {last_error}")
