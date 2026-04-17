from __future__ import annotations

from pathlib import Path

import pandas as pd


ROOT_DIR = Path("golgg")
DATA_DIR = ROOT_DIR / "data"


def layered_output_path(legacy_path: str | Path, layer: str) -> Path:
    legacy = Path(legacy_path)
    if len(legacy.parts) >= 3 and legacy.parts[0] == "golgg" and legacy.parts[1] == "data":
        return legacy
    if legacy.parts and legacy.parts[0] == "golgg":
        relative = Path(*legacy.parts[1:])
    else:
        return legacy
    return DATA_DIR / layer / relative


def write_csv_with_compat(df: pd.DataFrame, legacy_path: str | Path, layer: str, *, index: bool = False, header: bool = True) -> tuple[Path, Path]:
    legacy = Path(legacy_path)
    layer_path = layered_output_path(legacy, layer)

    layer_path.parent.mkdir(parents=True, exist_ok=True)

    df.to_csv(layer_path, index=index, header=header)
    return legacy, layer_path
