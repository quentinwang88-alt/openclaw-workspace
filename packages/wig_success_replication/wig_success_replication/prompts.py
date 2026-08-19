"""Versioned prompt loading."""

from pathlib import Path


PROMPT_ROOT = Path(__file__).resolve().parents[1] / "prompts" / "v1"


def load_prompt(name: str) -> str:
    return (PROMPT_ROOT / f"{name}.txt").read_text(encoding="utf-8").strip()
