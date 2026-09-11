from __future__ import annotations

import json
import os
import subprocess
from pathlib import Path
from typing import Any, Dict, Mapping


DEFAULT_RUNNER = Path(
    "/Users/likeu3/.openclaw/workspace/skills/jimeng-video-generator/"
    "platforms/minimax-h3/local-job-runner.js"
)
DEFAULT_CONFIG = Path(
    "/Users/likeu3/.openclaw/workspace/skills/jimeng-video-generator/feishu-direct.json"
)


class H3Gateway:
    def __init__(self, runner: str | Path = DEFAULT_RUNNER, config: str | Path = DEFAULT_CONFIG,
                 state_root: str | Path = ""):
        self.runner = Path(runner)
        self.config = Path(config)
        self.state_root = Path(state_root) if state_root else None

    def credential_env_name(self) -> str:
        if not self.config.is_file():
            return "METASO_MINIMAX_API_KEY"
        value = json.loads(self.config.read_text(encoding="utf-8"))
        return str(
            ((value.get("channels") or {}).get("metasoH3") or {}).get("apiKeyEnv")
            or "METASO_MINIMAX_API_KEY"
        )

    def _runtime_env(self) -> Dict[str, str]:
        """Use the process environment, then the normal macOS launchd environment.

        Credentials are never read from task history, source files or logs.
        """

        env = dict(os.environ)
        name = self.credential_env_name()
        if not env.get(name):
            try:
                completed = subprocess.run(
                    ["launchctl", "getenv", name], capture_output=True, text=True,
                    timeout=5, check=False,
                )
                value = completed.stdout.strip() if completed.returncode == 0 else ""
                if value:
                    env[name] = value
            except (OSError, subprocess.SubprocessError):
                pass
        return env

    def preflight(self, *, require_api_key: bool = False) -> Dict[str, Any]:
        problems = []
        if not self.runner.is_file():
            problems.append(f"H3 runner 不存在: {self.runner}")
        if not self.config.is_file():
            problems.append(f"H3 config 不存在: {self.config}")
        name = self.credential_env_name()
        if require_api_key and not self._runtime_env().get(name):
            problems.append(
                f"缺少 {name}；请通过当前进程环境或 launchctl setenv 正常注入"
            )
        return {
            "ready": not problems,
            "credential_env": name,
            "credential_present": bool(self._runtime_env().get(name)),
            "problems": problems,
        }

    def _run(self, args: list[str], timeout: int = 900) -> Dict[str, Any]:
        command = ["node", str(self.runner), *args, "--config", str(self.config)]
        if self.state_root:
            command.extend(["--state-root", str(self.state_root)])
        completed = subprocess.run(
            command, capture_output=True, text=True, timeout=timeout, check=False,
            env=self._runtime_env(),
        )
        if completed.returncode != 0:
            detail = completed.stderr or completed.stdout
            try:
                parsed = json.loads(detail)
                detail = parsed.get("error") or detail
            except Exception:
                pass
            raise RuntimeError(f"MiniMax H3 本地网关失败: {str(detail)[-1200:]}")
        return json.loads(completed.stdout)

    @staticmethod
    def build_segment_request(segment: Mapping[str, Any], *, start_frame: str,
                              end_frame: str = "",
                              reference_images: list[str] | None = None) -> Dict[str, Any]:
        mode = str(segment.get("generation_mode") or "")
        images = [start_frame]
        if mode == "first_last":
            if not end_frame:
                raise ValueError("片段A的 first_last 模式必须提供计划桥接帧")
            images.append(end_frame)
        elif mode not in {"first_frame", "reference"}:
            raise ValueError(f"长视频旁路不支持的 H3 模式: {mode}")
        if mode == "reference":
            images.extend(item for item in (reference_images or []) if item not in images)
        if any(not Path(item).is_file() for item in images):
            raise ValueError("H3 首帧/尾帧文件不存在")
        return {
            "prompt": str(segment.get("video_prompt") or ""),
            "mode": mode,
            "duration": int(segment.get("duration_seconds") or 0),
            "ratio": "9:16",
            "resolution": "768P",
            "imagePaths": images,
        }

    def prepare(self, request_path: str | Path) -> Dict[str, Any]:
        return self._run(["prepare", "--request", str(request_path)])

    def submit(self, request_path: str | Path, *, allow_real_submit: bool) -> Dict[str, Any]:
        if not allow_real_submit:
            raise ValueError("真实 H3 提交必须显式授权")
        return self._run(["submit", "--request", str(request_path), "--allow-real-submit"])

    def query(self, task_id: str) -> Dict[str, Any]:
        return self._run(["query", "--task-id", task_id], timeout=180)

    def download(self, task_id: str, output: str | Path) -> Dict[str, Any]:
        return self._run(["download", "--task-id", task_id, "--output", str(output)], timeout=900)


def write_segment_request(path: str | Path, request: Mapping[str, Any]) -> str:
    target = Path(path)
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_text(json.dumps(dict(request), ensure_ascii=False, indent=2), encoding="utf-8")
    return str(target)
