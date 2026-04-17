"""Thin shim that points legacy step007 entrypoint to legacy_steps."""

from golgg.legacy_steps.step007_infographic_dataset import *  # noqa: F401,F403
from golgg.legacy_steps.step007_infographic_dataset import main


if __name__ == "__main__":
    main()
