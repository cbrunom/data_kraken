# -*- coding: utf-8 -*-
"""
Render infographic images from step7 outputs.
"""

import glob
import os

from pathlib import Path

import matplotlib.pyplot as plt
import pandas as pd

from golgg.pipeline.common import log_step_end, log_step_start


LEGACY_IN_DIR = Path("golgg/infographic_ready")
GOLD_IN_DIR = Path("golgg/data/gold/infographic_ready")
IN_DIR = str(GOLD_IN_DIR if GOLD_IN_DIR.exists() and any(GOLD_IN_DIR.glob("*_summary.csv")) else LEGACY_IN_DIR)
OUT_IMG_DIR = os.path.join(IN_DIR, "images")


def read_or_empty(path):
    if os.path.exists(path):
        return pd.read_csv(path)
    return pd.DataFrame()


def draw_header(ax, title):
    ax.set_facecolor("#0C1A2B")
    ax.set_xticks([])
    ax.set_yticks([])
    for spine in ax.spines.values():
        spine.set_visible(False)
    ax.text(0.02, 0.62, title, fontsize=24, color="#F4C76A", fontweight="bold", va="center")
    ax.text(0.02, 0.20, "MVP Infographic", fontsize=12, color="#D6E0F0", va="center")


def draw_kv_box(ax, title, df):
    ax.set_facecolor("#12263A")
    ax.set_xticks([])
    ax.set_yticks([])
    for spine in ax.spines.values():
        spine.set_visible(False)

    ax.text(0.03, 0.95, title, fontsize=13, color="#F4C76A", fontweight="bold", va="top")
    if df.empty:
        ax.text(0.03, 0.5, "No data", color="white", fontsize=11)
        return

    row = df.iloc[0]
    y = 0.83
    for col in df.columns:
        ax.text(0.04, y, str(col), fontsize=10, color="#9DB5D2", va="center")
        ax.text(0.96, y, str(row[col]), fontsize=10, color="white", va="center", ha="right", fontweight="bold")
        y -= 0.09
        if y < 0.08:
            break


def draw_table(ax, df, title, columns=None, max_rows=8, font_size=8):
    ax.set_facecolor("#12263A")
    ax.set_xticks([])
    ax.set_yticks([])
    for spine in ax.spines.values():
        spine.set_visible(False)

    ax.text(0.03, 0.96, title, fontsize=12, color="#F4C76A", fontweight="bold", va="top")

    if df.empty:
        ax.text(0.03, 0.5, "No data", color="white", fontsize=11)
        return

    use_df = df.copy().head(max_rows)
    if columns:
        available = [c for c in columns if c in use_df.columns]
        if available:
            use_df = use_df[available]

    table = ax.table(
        cellText=use_df.values,
        colLabels=use_df.columns,
        loc="center",
        cellLoc="center",
        colLoc="center",
        bbox=[0.02, 0.03, 0.96, 0.88],
    )
    table.auto_set_font_size(False)
    table.set_fontsize(font_size)

    for (r, c), cell in table.get_celld().items():
        if r == 0:
            cell.set_text_props(weight="bold", color="#0C1A2B")
            cell.set_facecolor("#F4C76A")
        else:
            cell.set_text_props(color="white")
            cell.set_facecolor("#16324C")
        cell.set_edgecolor("#0C1A2B")


def render_tournament(safe_name):
    title = safe_name.replace("_", " ")

    player_df = read_or_empty(os.path.join(IN_DIR, f"{safe_name}_player_match_highlights.csv"))
    champs_df = read_or_empty(os.path.join(IN_DIR, f"{safe_name}_most_played_champions.csv"))
    team_df = read_or_empty(os.path.join(IN_DIR, f"{safe_name}_team_match_highlights.csv"))
    best_df = read_or_empty(os.path.join(IN_DIR, f"{safe_name}_best_players_performance.csv"))
    top_kda_df = read_or_empty(os.path.join(IN_DIR, f"{safe_name}_top_kda.csv"))
    most_kills_df = read_or_empty(os.path.join(IN_DIR, f"{safe_name}_most_kills_single_game.csv"))
    missing_df = read_or_empty(os.path.join(IN_DIR, f"{safe_name}_missing_metrics.csv"))

    fig = plt.figure(figsize=(20, 14), dpi=180)
    fig.patch.set_facecolor("#081421")
    gs = fig.add_gridspec(4, 2, height_ratios=[0.14, 0.30, 0.30, 0.30], wspace=0.08, hspace=0.12)

    ax_header = fig.add_subplot(gs[0, :])
    draw_header(ax_header, title)

    ax_player = fig.add_subplot(gs[1, 0])
    draw_table(
        ax_player,
        player_df,
        "Matches Highlights - Player",
        columns=["Highlight", "Player", "Team", "Champ", "Value"],
        max_rows=8,
        font_size=8,
    )

    ax_team = fig.add_subplot(gs[1, 1])
    draw_table(
        ax_team,
        team_df,
        "Matches Highlights - Team",
        columns=["Highlight", "Team", "Partida", "Stage", "Duration", "Value"],
        max_rows=6,
        font_size=8,
    )

    ax_champs = fig.add_subplot(gs[2, 0])
    draw_table(
        ax_champs,
        champs_df,
        "Top 10 Most Played Champions",
        columns=["Rank", "Champ", "Games", "WinRate%", "KDA_Open"],
        max_rows=10,
        font_size=7,
    )

    ax_best = fig.add_subplot(gs[2, 1])
    draw_table(
        ax_best,
        best_df,
        "Best Players Performance",
        columns=["Metric", "Player", "Team", "Value"],
        max_rows=7,
        font_size=8,
    )

    ax_kda = fig.add_subplot(gs[3, 0])
    draw_table(
        ax_kda,
        top_kda_df,
        "Top KDA",
        columns=["Rank", "Player", "Team", "Role", "KDA_Open", "KDA"],
        max_rows=5,
        font_size=8,
    )

    ax_most_kills = fig.add_subplot(gs[3, 1])
    draw_table(
        ax_most_kills,
        most_kills_df,
        "Most Kills in a Single Game",
        columns=["Rank", "Player", "Team", "Champ", "Partida", "Kills"],
        max_rows=5,
        font_size=8,
    )

    if not missing_df.empty:
        fig.text(
            0.01,
            0.01,
            "Missing data mapped in file: " + f"{safe_name}_missing_metrics.csv",
            fontsize=9,
            color="#9DB5D2",
        )

    os.makedirs(OUT_IMG_DIR, exist_ok=True)
    out_path = os.path.join(OUT_IMG_DIR, f"{safe_name}_infographic.png")
    plt.savefig(out_path, bbox_inches="tight")
    plt.close(fig)
    print(f"Saved {out_path}")


def iter_safe_names_from_dataset_files(dataset_files):
    """Yield safe tournament names from the current section file naming pattern."""
    suffix = "_most_played_champions.csv"
    for dataset_path in dataset_files:
        base_name = os.path.basename(dataset_path)
        if base_name.endswith(suffix):
            yield base_name.replace(suffix, "")


def main():
    start_time = log_step_start("step012x_render_infographic")
    dataset_files = sorted(glob.glob(os.path.join(IN_DIR, "*_most_played_champions.csv")))
    if not dataset_files:
        print("No infographic CSV files found. Run step7 first.")
        log_step_end("step012x_render_infographic", start_time)
        return

    for safe_name in iter_safe_names_from_dataset_files(dataset_files):
        render_tournament(safe_name)

    log_step_end("step012x_render_infographic", start_time)


if __name__ == "__main__":
    print("[DISABLED] Step 8 - PNG infographic generation has been disabled.")
    print("The Streamlit app now displays data directly without generating PNG exports.")
    start_time = log_step_start("step012x_render_infographic")
    log_step_end("step012x_render_infographic", start_time)

