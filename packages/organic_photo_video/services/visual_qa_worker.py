"""Isolated provider request, killed by the parent at its hard deadline."""
from __future__ import annotations

import contextlib
import json
from pathlib import Path
import sys

PACKAGE_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PACKAGE_ROOT.parents[1]))
sys.path.insert(0, str(PACKAGE_ROOT))


def main():
    payload = json.load(sys.stdin)
    with contextlib.redirect_stdout(sys.stderr):
        from workspace_support import load_repo_env
        load_repo_env()
        from services.visual_qa import CreatorCrmVisualQaAdapter
        result = CreatorCrmVisualQaAdapter()._review_direct(payload)
    print(json.dumps(result, ensure_ascii=False), flush=True)


if __name__ == "__main__":
    main()
