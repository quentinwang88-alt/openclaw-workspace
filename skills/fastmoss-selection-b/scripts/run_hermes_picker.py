#!/usr/bin/env python3
"""调用本地 Hermes CLI 执行结构化 picker 判断。"""

from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys
from pathlib import Path
from json import JSONDecoder


DEFAULT_HERMES_BIN = Path.home() / ".hermes" / "hermes-agent" / "venv" / "bin" / "hermes"


def _read_text(path: str) -> str:
    return Path(path).read_text(encoding="utf-8").strip()


def _extract_response_text(stdout: str) -> str:
    text = (stdout or "").strip()
    if not text:
        return ""
    if "\nsession_id:" in text:
        text = text.rsplit("\nsession_id:", 1)[0].strip()
    decoder = JSONDecoder()
    for index, char in enumerate(text):
        if char not in "{[":
            continue
        try:
            _, end = decoder.raw_decode(text[index:])
        except ValueError:
            continue
        return text[index : index + end].strip()
    return text


def build_query(system_prompt: str, user_prompt: str) -> str:
    return (
        "下面有两段提示词。\n\n"
        "[System Prompt]\n"
        f"{system_prompt}\n\n"
        "[User Prompt]\n"
        f"{user_prompt}\n"
    )


def main() -> int:
    parser = argparse.ArgumentParser(description="Run Hermes picker for FastMoss shortlist.")
    parser.add_argument("--profile", default="picker")
    parser.add_argument("--system", required=True)
    parser.add_argument("--prompt", required=True)
    parser.add_argument("--input", required=True)
    parser.add_argument("--output", required=True)
    parser.add_argument("--batch-id", required=True)
    parser.add_argument("--hermes-bin", default=os.environ.get("FASTMOSS_B_HERMES_BIN", str(DEFAULT_HERMES_BIN)))
    parser.add_argument("--model", default=os.environ.get("FASTMOSS_B_HERMES_MODEL", "gpt-5.5"))
    parser.add_argument("--provider", default=os.environ.get("FASTMOSS_B_HERMES_PROVIDER", "openai-codex"))
    args = parser.parse_args()

    hermes_bin = Path(args.hermes_bin).expanduser()
    if not hermes_bin.exists():
        print("Hermes binary not found: {path}".format(path=hermes_bin), file=sys.stderr)
        return 2

    system_prompt = _read_text(args.system)
    user_prompt = _read_text(args.prompt)
    _ = json.loads(_read_text(args.input))

    command = [
        str(hermes_bin),
        "chat",
        "-Q",
        "--source",
        "tool",
        "-m",
        args.model,
        "--provider",
        args.provider,
        "-q",
        build_query(system_prompt, user_prompt),
    ]
    completed = subprocess.run(
        command,
        capture_output=True,
        text=True,
        check=False,
        env=os.environ.copy(),
    )
    if completed.returncode != 0:
        sys.stderr.write(completed.stderr or completed.stdout or "")
        return completed.returncode

    response_text = _extract_response_text(completed.stdout)
    if not response_text:
        sys.stderr.write("Hermes returned empty output\n")
        return 3

    output_path = Path(args.output)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(response_text, encoding="utf-8")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
