"""Thin shim that points legacy step004 entrypoint to legacy_steps."""

from golgg.legacy_steps.step004_partidas_torneios import main as run_pipeline


if __name__ == "__main__":
    run_pipeline()

