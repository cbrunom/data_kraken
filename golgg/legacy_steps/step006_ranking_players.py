"""Legacy compatibility wrapper for step006."""

from golgg.legacy_steps import DEPRECATION_REMOVAL_DATE
from golgg.pipeline.orquestracao.step006_ranking_players import main as _main


def main() -> None:
    print(
        "[deprecation] step006 is legacy compatibility and will be removed after "
        f"{DEPRECATION_REMOVAL_DATE}; prefer `python -m golgg.main`."
    )
    _main()
