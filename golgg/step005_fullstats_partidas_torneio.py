"""Thin shim that points legacy step005 entrypoint to legacy_steps."""

from golgg.legacy_steps.step005_fullstats_partidas_torneio import main


if __name__ == "__main__":
    main()

