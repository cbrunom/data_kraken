# -*- coding: utf-8 -*-

from pathlib import Path

import openpyxl
import pandas as pd

from golgg.pipeline.common import HEADERS, log_step_end, log_step_start
from golgg.pipeline.ingestao.info_teams import extract_teams


BRONZE_TEAMS_XLSX = Path("golgg/data/bronze/teams-All.xlsx")
SILVER_INFO_TEAMS_CSV = Path("golgg/data/silver/info_teams.csv")


def main():
  start_time = log_step_start("step002_info_teams")
  workbook = openpyxl.load_workbook(BRONZE_TEAMS_XLSX, data_only=True)
  worksheet = workbook['teams-All']
  teams = extract_teams(worksheet, HEADERS)
  info_teams = pd.DataFrame(teams)
  SILVER_INFO_TEAMS_CSV.parent.mkdir(parents=True, exist_ok=True)
  info_teams.to_csv(SILVER_INFO_TEAMS_CSV, index=False)
  log_step_end("step002_info_teams", start_time)


if __name__ == "__main__":
  main()

# info_teams.to_csv('golgg/source/info_teams.csv',index=False)
# info_teams = pd.read_csv('golgg/source/info_teams.csv')
# info_teams[info_teams['player'] == 'Guigo']
