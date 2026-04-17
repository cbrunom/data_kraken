"""step0_clean_downloads

Small utility to remove pipeline artifacts and image caches so the
pipeline can be run from scratch. Supports dry-run and an explicit
`--yes` flag to avoid accidental deletions.

Usage:
  python -m golgg.step0_clean_downloads --dry-run
  python -m golgg.step0_clean_downloads --yes
  python -m golgg.step0_clean_downloads --targets data/bronze,images/player_images --yes
"""
from __future__ import annotations

import argparse
import shutil
from pathlib import Path
import sys

DEFAULT_RELATIVE_TARGETS = [
    "data/bronze",
    "data/silver",
    "data/gold",
    "fullstats",
    "torneios",
    "player_raw_torneios",
    "player_grades_torneios",
    "infographic_ready",
    "images/player_images",
    "images/team_logos",
]


def confirm(prompt: str) -> bool:
    try:
        return input(prompt).strip().lower() in ("y", "yes")
    except EOFError:
        return False


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="Clean golgg download/artifacts to force re-download"
    )
    parser.add_argument(
        "--yes",
        "-y",
        action="store_true",
        help="Actually delete targets (otherwise asks for confirmation)",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Show what would be deleted and exit",
    )
    parser.add_argument(
        "--targets",
        type=str,
        help=(
            "Comma-separated list of relative paths to delete (relative to golgg/)."
        ),
    )
    args = parser.parse_args(argv)

    base_dir = Path(__file__).resolve().parent

    if args.targets:
        targets = [Path(p.strip()) for p in args.targets.split(",") if p.strip()]
    else:
        targets = [Path(p) for p in DEFAULT_RELATIVE_TARGETS]

    resolved = []
    for t in targets:
        candidate = (base_dir / t).resolve()
        try:
            # Safety: only allow targets inside the golgg package dir
            candidate.relative_to(base_dir.resolve())
            resolved.append(candidate)
        except Exception:
            print(f"Skipping unsafe target (outside package): {candidate}")

    if not resolved:
        print("No safe targets to remove. Exiting.")
        return 1

    print("Targets to remove:")
    for p in resolved:
        print(" -", p)

    if args.dry_run:
        print("Dry run: nothing deleted.")
        return 0

    if not args.yes:
        if not confirm("Confirm delete of the above targets? Type 'yes' to proceed: "):
            print("Aborted by user.")
            return 2

    for p in resolved:
        if p.exists():
            print(f"Removing {p} ...")
            try:
                shutil.rmtree(p)
                print("Removed.")
            except Exception as exc:  # pragma: no cover - best-effort cleanup
                print(f"Failed to remove {p}: {exc}")
        else:
            print(f"Not found: {p}")

    print("Cleanup finished.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
