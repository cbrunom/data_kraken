import os

import pandas as pd
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry


BASE_LINK = "https://gol.gg/"


def create_session():
    session = requests.Session()
    retry = Retry(
        total=3,
        backoff_factor=0.5,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["GET"],
    )
    adapter = HTTPAdapter(max_retries=retry)
    session.mount("https://", adapter)
    session.mount("http://", adapter)
    return session


def stage_type_from_stage(stage_name):
    stage_upper = str(stage_name).upper()
    if stage_upper.startswith("WEEK"):
        return "WEEK"
    return "PLAYOFFS"


def build_summary_from_href(href):
    return BASE_LINK + href[3:].rsplit("/", 2)[0] + "/page-summary/"


def build_game_url_from_href(href):
    return BASE_LINK + href[3:]


def load_existing_torneio_records(torneio_csv_path):
    if not os.path.exists(torneio_csv_path):
        return []
    try:
        existing_df = pd.read_csv(torneio_csv_path)
    except Exception:
        return []
    if existing_df.empty:
        return []
    return existing_df.to_dict("records")


def deduplicate_torneios_df(df):
    if df.empty:
        return df

    if "Link" in df.columns:
        linked = df[df["Link"].astype(str).str.strip().ne("-")]
        if not linked.empty:
            return df.drop_duplicates(subset=["Link"], keep="first")

    fallback_subset = [c for c in ["Torneio", "Partida", "Stage", "Game"] if c in df.columns]
    if fallback_subset:
        return df.drop_duplicates(subset=fallback_subset, keep="first")
    return df.drop_duplicates(keep="first")
