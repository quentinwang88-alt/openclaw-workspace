"""Export checked-in JSON Schemas from the Pydantic source of truth."""

from __future__ import annotations

import json
from pathlib import Path

from .models import SCHEMA_MODELS


def export_schemas(output_dir: Path) -> list[Path]:
    output_dir.mkdir(parents=True, exist_ok=True)
    paths: list[Path] = []
    for name, model in SCHEMA_MODELS.items():
        path = output_dir / f"{name}.schema.json"
        path.write_text(
            json.dumps(model.model_json_schema(), ensure_ascii=False, indent=2) + "\n",
            encoding="utf-8",
        )
        paths.append(path)
    return paths


if __name__ == "__main__":
    export_schemas(Path(__file__).resolve().parents[1] / "schemas")
