"""Thin shim that points legacy step003 entrypoint to legacy_steps."""

from golgg.legacy_steps.step003_generate_torneios_por_season_xlsx import main


if __name__ == "__main__":
    main()

