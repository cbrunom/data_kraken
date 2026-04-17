from concurrent.futures import Future
from unittest.mock import patch

from golgg.pipeline.orquestracao.step004_partidas_torneios import (
    build_summary_from_href,
    deduplicate_torneios_df,
    extract_series_data,
    load_existing_torneio_records,
    stage_type_from_stage,
)


class FakeExecutor:
    def __init__(self, max_workers=None):
        self.max_workers = max_workers

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return False

    def submit(self, fn, *args, **kwargs):
        future = Future()
        try:
            future.set_result(fn(*args, **kwargs))
        except Exception as error:
            future.set_exception(error)
        return future


def test_stage_type_from_stage_week_prefix_case_insensitive():
    assert stage_type_from_stage("Week 3") == "WEEK"
    assert stage_type_from_stage("week 1") == "WEEK"


def test_stage_type_from_stage_default_playoffs():
    assert stage_type_from_stage("Final") == "PLAYOFFS"


def test_build_summary_from_href_transform_game_to_summary_url():
    href = "../game/stats/74020/page-game/"
    assert build_summary_from_href(href) == "https://gol.gg/game/stats/74020/page-summary/"


def test_extract_series_data_parse_best_of_and_game_links():
    html = """
    <html>
      <body>
        <div>BO5</div>
        <a href="../game/stats/111/page-game/">GAME 1</a>
        <a href="../game/stats/333/page-game/">GAME 3</a>
        <a href="../game/stats/222/page-game/">GAME 2</a>
      </body>
    </html>
    """

    class FakeSession:
        def get(self, *_args, **_kwargs):
            class Response:
                text = html

            return Response()

    with patch("golgg.pipeline.orquestracao.step004_partidas_torneios.ThreadPoolExecutor", FakeExecutor), patch(
        "golgg.pipeline.orquestracao.step004_partidas_torneios.extract_game_result",
        return_value={"WinnerTeam": "N/A", "LoserTeam": "N/A", "GameDuration": "N/A"},
    ):
        best_of, games = extract_series_data(FakeSession(), "https://gol.gg/fake-summary")

    assert best_of == "BO5"
    assert games == [
        (1, "https://gol.gg/game/stats/111/page-summary/", "https://gol.gg/game/stats/111/page-game/", {"WinnerTeam": "N/A", "LoserTeam": "N/A", "GameDuration": "N/A"}),
        (2, "https://gol.gg/game/stats/222/page-summary/", "https://gol.gg/game/stats/222/page-game/", {"WinnerTeam": "N/A", "LoserTeam": "N/A", "GameDuration": "N/A"}),
        (3, "https://gol.gg/game/stats/333/page-summary/", "https://gol.gg/game/stats/333/page-game/", {"WinnerTeam": "N/A", "LoserTeam": "N/A", "GameDuration": "N/A"}),
    ]


def test_extract_series_data_fallback_when_request_fails():
    class BrokenSession:
        def get(self, *_args, **_kwargs):
            raise Exception("boom")

    best_of, games = extract_series_data(BrokenSession(), "https://gol.gg/fake-summary")

    assert best_of == "UNKNOWN"
    assert games == [
        (
            1,
            "https://gol.gg/fake-summary",
            "https://gol.gg/fake-summary",
            {"WinnerTeam": "N/A", "LoserTeam": "N/A", "GameDuration": "N/A"},
        )
    ]


def test_load_existing_torneio_records_return_records(tmp_path):
    csv_path = tmp_path / "torneio.csv"
    csv_path.write_text("Torneio,Partida,Game\nCBLOL,Serie A,1\n", encoding="utf-8")

    records = load_existing_torneio_records(str(csv_path))

    assert len(records) == 1
    assert records[0]["Torneio"] == "CBLOL"


def test_deduplicate_torneios_df_remove_exact_duplicate_link_rows():
    import pandas as pd

    df = pd.DataFrame(
        [
            {"Torneio": "Cup", "Partida": "A vs B", "Stage": "Week 1", "Game": 1, "Link": "https://gol.gg/game/stats/1/page-summary/"},
            {"Torneio": "Cup", "Partida": "A vs B", "Stage": "Week 1", "Game": 1, "Link": "https://gol.gg/game/stats/1/page-summary/"},
            {"Torneio": "Cup", "Partida": "A vs B", "Stage": "Week 2", "Game": 1, "Link": "https://gol.gg/game/stats/2/page-summary/"},
        ]
    )

    out = deduplicate_torneios_df(df)

    assert len(out) == 2

