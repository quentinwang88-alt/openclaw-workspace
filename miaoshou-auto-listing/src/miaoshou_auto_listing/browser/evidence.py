from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict


class EvidenceRecorder:
    def __init__(self, artifact_root: Path) -> None:
        self.artifact_root = artifact_root.resolve()

    async def capture(self, page: Any, task_id: str, payload: Dict[str, Any]) -> str:
        stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S_%fZ")
        task_dir = self.artifact_root / task_id
        task_dir.mkdir(parents=True, exist_ok=True)
        screenshot = task_dir / f"{stamp}.png"
        metadata = task_dir / f"{stamp}.json"
        try:
            await page.screenshot(path=str(screenshot), full_page=True)
        except Exception as exc:
            payload["screenshot_error"] = str(exc)
            screenshot = Path("")
        metadata.write_text(
            json.dumps(payload, ensure_ascii=False, indent=2, default=str),
            encoding="utf-8",
        )
        return str(screenshot) if screenshot else ""
