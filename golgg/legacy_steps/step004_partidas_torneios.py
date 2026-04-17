"""Legacy compatibility wrapper for step004."""

from golgg.legacy_steps import DEPRECATION_REMOVAL_DATE
from golgg.pipeline.orquestracao.step004_partidas_torneios import main as _main


def main() -> None:
    print(
        "[deprecation] step004 is legacy compatibility and will be removed after "
        f"{DEPRECATION_REMOVAL_DATE}; prefer `python -m golgg.main`."
    )
    _main()
