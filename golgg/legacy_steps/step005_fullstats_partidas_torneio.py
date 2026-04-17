"""Legacy compatibility wrapper for step005."""

from golgg.legacy_steps import DEPRECATION_REMOVAL_DATE
from golgg.pipeline.orquestracao.step005_fullstats_partidas_torneio import main as _main


def main() -> None:
    print(
        "[deprecation] step005 is legacy compatibility and will be removed after "
        f"{DEPRECATION_REMOVAL_DATE}; prefer `python -m golgg.main`."
    )
    _main()
