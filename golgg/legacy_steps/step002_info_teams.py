"""Legacy compatibility wrapper for step002."""

from golgg.legacy_steps import DEPRECATION_REMOVAL_DATE
from golgg.pipeline.orquestracao.step002_info_teams import main as _main


def main() -> None:
    print(
        "[deprecation] step002 is legacy compatibility and will be removed after "
        f"{DEPRECATION_REMOVAL_DATE}; prefer `python -m golgg.main`."
    )
    _main()
