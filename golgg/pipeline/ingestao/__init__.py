from .partidas_torneios import (
    build_game_url_from_href,
    build_summary_from_href,
    create_session,
    deduplicate_torneios_df,
    load_existing_torneio_records,
    stage_type_from_stage,
)
from .info_teams import extract_teams, extract_tournament_from_url
from .rendered_page import fetch_rendered_html
from .teams_all_xlsx import (
    build_team_url,
    extract_headers,
    extract_rows,
    extract_tournament_name,
    find_teams_table,
    normalize_link,
)
from .torneios_por_season import normalize_text, read_tournament_rows
