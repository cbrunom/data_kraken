"""Thin shim that points legacy step002 entrypoint to legacy_steps."""

from golgg.legacy_steps.step002_info_teams import main


if __name__ == "__main__":
    main()
