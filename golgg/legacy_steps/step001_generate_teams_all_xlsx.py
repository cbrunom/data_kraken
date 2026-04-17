"""Legacy compatibility wrapper for step001."""

from golgg.legacy_steps import DEPRECATION_REMOVAL_DATE
from golgg.pipeline.orquestracao.step001_generate_teams_all_xlsx import main as _main


def main() -> None:
    print(
        "[deprecation] step001 is legacy compatibility and will be removed after "
        f"{DEPRECATION_REMOVAL_DATE}; prefer `python -m golgg.main`."
    )
    _main()
