"""Thin shim that points legacy step001 entrypoint to legacy_steps."""

from golgg.legacy_steps.step001_generate_teams_all_xlsx import main


if __name__ == "__main__":
    main()

