"""Legacy compatibility wrapper for step007."""

from golgg.legacy_steps import DEPRECATION_REMOVAL_DATE
from golgg.pipeline.orquestracao.step007_infographic_dataset import *  # noqa: F401,F403
from golgg.pipeline.orquestracao.step007_infographic_dataset import main as _main


def main() -> None:
    print(
        "[deprecation] step007 is legacy compatibility and will be removed after "
        f"{DEPRECATION_REMOVAL_DATE}; prefer `python -m golgg.main`."
    )
    _main()
