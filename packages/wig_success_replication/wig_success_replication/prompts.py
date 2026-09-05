"""Versioned prompt loading."""

from pathlib import Path
from .hashing import stable_hash


PROMPT_ROOT = Path(__file__).resolve().parents[1] / "prompts" / "v1"


def load_prompt(name: str) -> str:
    return (PROMPT_ROOT / f"{name}.txt").read_text(encoding="utf-8").strip()


def prompt_fingerprint(*names: str) -> str:
    return stable_hash({name: load_prompt(name) for name in names})
