"""Legacy compatibility wrapper for step003."""

from golgg.legacy_steps import DEPRECATION_REMOVAL_DATE
from golgg.pipeline.orquestracao.step003_generate_torneios_por_season_xlsx import main as _main


def main() -> None:
    print(
        "[deprecation] step003 is legacy compatibility and will be removed after "
        f"{DEPRECATION_REMOVAL_DATE}; prefer `python -m golgg.main`."
    )
    _main()
