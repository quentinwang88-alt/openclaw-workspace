#!/usr/bin/env python3
"""Run the scheduled original-script pipeline behind a Codex auth guard."""

from __future__ import annotations

import json
import os
import subprocess
import sys
import time
import urllib.error
import urllib.request
from pathlib import Path
from typing import Any, Dict, List, Tuple


SKILL_DIR = Path(__file__).resolve().parents[1]
OPENCLAW_CONFIG_PATH = Path.home() / ".openclaw" / "openclaw.json"
DEFAULT_ALERT_OPEN_ID = "ou_49b2e9a1ccc3820264b5f4b7b9d1301d"

sys.path.insert(0, str(SKILL_DIR))

from core.llm_client import OriginalScriptLLMClient  # noqa: E402
from core.model_circuit_breaker import ModelCircuitBreaker  # noqa: E402


def _read_json(path: Path) -> Dict[str, Any]:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
        return payload if isinstance(payload, dict) else {}
    except Exception:
        return {}


def _circuit_auth_open(breaker: ModelCircuitBreaker) -> bool:
    state = _read_json(breaker.state_path)
    if state.get("status") != "open":
        return False
    if str(state.get("last_failure_kind") or "") == ModelCircuitBreaker.AUTH_ERROR:
        return True
    reason = str(state.get("reason") or "")
    return "认证" in reason or "auth" in reason.lower()


def _is_auth_error(exc: BaseException) -> bool:
    text = str(exc).lower()
    return any(
        token in text
        for token in (
            "llm 认证失败",
            "token_expired",
            "authentication token is expired",
            "provided authentication token is expired",
            "invalid_api_key",
            "unauthorized",
            "401",
            "refresh_token",
        )
    )


def _smoke_primary_llm() -> Tuple[bool, str]:
    try:
        client = OriginalScriptLLMClient(timeout=60, max_retries=0)
        if not client.primary_api_key:
            return False, "主线路未找到 Codex access token"
        response = client._call_primary_text("Reply exactly: OK", max_tokens=20)
        text = str(response["choices"][0]["message"]["content"] or "").strip()
        if text != "OK":
            return False, f"Codex 主线路 smoke 返回异常: {text[:120] or '<empty>'}"
        return True, f"Codex 主线路可用: model={client.primary_model}"
    except Exception as exc:  # noqa: BLE001 - guard must convert all model errors into circuit state.
        return False, str(exc)


def _post_json(url: str, payload: Dict[str, Any], headers: Dict[str, str] | None = None) -> Dict[str, Any]:
    body = json.dumps(payload, ensure_ascii=False).encode("utf-8")
    request = urllib.request.Request(
        url,
        data=body,
        headers={"Content-Type": "application/json", **(headers or {})},
        method="POST",
    )
    with urllib.request.urlopen(request, timeout=20) as response:  # noqa: S310 - fixed Feishu API endpoints.
        data = response.read().decode("utf-8")
    return json.loads(data) if data else {}


def _send_feishu_alert(message: str) -> None:
    """Best-effort alert. Failure must not block the scheduler guard."""

    open_id = str(os.environ.get("ORIGINAL_SCRIPT_AUTH_ALERT_OPEN_ID", DEFAULT_ALERT_OPEN_ID) or "").strip()
    if not open_id:
        print("WARN: 未配置 ORIGINAL_SCRIPT_AUTH_ALERT_OPEN_ID，跳过飞书提醒")
        return

    config = _read_json(OPENCLAW_CONFIG_PATH)
    feishu = ((config.get("channels") or {}).get("feishu") or {}) if isinstance(config, dict) else {}
    account = (feishu.get("accounts") or {}).get("default") or {}
    app_id = str(account.get("appId") or feishu.get("appId") or "").strip()
    app_secret = str(account.get("appSecret") or feishu.get("appSecret") or "").strip()
    if not app_id or not app_secret:
        print("WARN: 未找到 Feishu default appId/appSecret，跳过飞书提醒")
        return

    try:
        token_resp = _post_json(
            "https://open.feishu.cn/open-apis/auth/v3/tenant_access_token/internal",
            {"app_id": app_id, "app_secret": app_secret},
        )
        tenant_token = str(token_resp.get("tenant_access_token") or "").strip()
        if not tenant_token:
            print(f"WARN: 获取 Feishu tenant token 失败: {token_resp.get('msg') or token_resp}")
            return
        send_resp = _post_json(
            "https://open.feishu.cn/open-apis/im/v1/messages?receive_id_type=open_id",
            {
                "receive_id": open_id,
                "msg_type": "text",
                "content": json.dumps({"text": message}, ensure_ascii=False),
            },
            {"Authorization": f"Bearer {tenant_token}"},
        )
        if int(send_resp.get("code") or 0) != 0:
            print(f"WARN: 飞书提醒发送失败: {send_resp.get('msg') or send_resp}")
    except (urllib.error.URLError, TimeoutError, OSError, ValueError) as exc:
        print(f"WARN: 飞书提醒发送异常: {exc}")


def _auth_alert_message(detail: str) -> str:
    now = time.strftime("%Y-%m-%d %H:%M:%S")
    return (
        "原创脚本小时巡检已暂停：Codex 主线路授权检测未通过。\n"
        f"时间：{now}\n"
        f"原因：{detail[:500]}\n"
        "处理：请重新执行 openclaw models auth login --provider openai-codex；登录后下一轮巡检会自动重测，通过后恢复。"
    )


def run() -> int:
    breaker = ModelCircuitBreaker()

    can_start, circuit_reason = breaker.can_start()
    if not can_start and not _circuit_auth_open(breaker):
        print(f"SKIP: 模型熔断中，本轮不拉起原创脚本任务: {circuit_reason}")
        return 0

    ok, detail = _smoke_primary_llm()
    if not ok:
        kind = ModelCircuitBreaker.AUTH_ERROR if _is_auth_error(Exception(detail)) else ModelCircuitBreaker.MODEL_ERROR
        _, reason = breaker.record_failure(kind, detail, "scheduled_auth_guard")
        print("SKIP: Codex 主线路预检未通过，本轮不拉起原创脚本任务")
        print(f"reason={reason or detail}")
        _send_feishu_alert(_auth_alert_message(reason or detail))
        return 0

    breaker.record_success()
    print(f"OK: {detail}")

    if str(os.environ.get("ORIGINAL_SCRIPT_AUTH_GUARD_ONLY", "") or "").strip() == "1":
        return 0

    command: List[str] = [sys.executable, str(SKILL_DIR / "run_pipeline.py"), *sys.argv[1:]]
    print("RUN: " + " ".join(command))
    return subprocess.run(command, cwd=str(SKILL_DIR), check=False).returncode


if __name__ == "__main__":
    raise SystemExit(run())
