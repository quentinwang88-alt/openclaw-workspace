"""Scoped, read-only NeoBund browser credentials and atomic local persistence.

This module never logs credentials, logs in, navigates tabs or submits content.
The caller must validate extracted credentials before persisting them.
"""
from __future__ import annotations

import json
import os
from pathlib import Path
import tempfile
import time
from typing import Any, Dict
from urllib.parse import unquote, urlparse

import requests


TRUSTED_HOSTS = frozenset({"neobund.ai", "www.neobund.ai", "cn.neobund.ai"})
TOKEN_KEYS = ("access_token", "accessToken", "token")


class NeoBundAuthError(RuntimeError):
    """Explicit auth rejection; safe for the scheduler to pause this channel."""

    auth_failed = True


def _token(value: Any) -> str:
    if isinstance(value, dict):
        for key in TOKEN_KEYS:
            candidate = value.get(key)
            if isinstance(candidate, str) and candidate.strip():
                return candidate.strip().removeprefix("Bearer ")
        for key in ("auth", "data", "userInfo"):
            nested = _token(value.get(key))
            if nested:
                return nested
    return ""


def _decoded(value: str) -> Any:
    try:
        return json.loads(unquote(value))
    except (ValueError, TypeError):
        return None


def _credentials_from_values(storage: Dict[str, Any], cookies: list) -> Dict[str, str]:
    token = _token(storage)
    scoped = [c for c in cookies if str(c.get("domain", "")).lstrip(".") in TRUSTED_HOSTS]
    for cookie in scoped:
        name = str(cookie.get("name", ""))
        value = str(cookie.get("value", ""))
        if not token and name in TOKEN_KEYS:
            token = value.strip().removeprefix("Bearer ")
        if not token and name == "auth":
            token = _token(_decoded(value))
    # Auth cookies only. Analytics and unrelated site cookies are never copied.
    auth_names = set(TOKEN_KEYS) | {"auth", "user", "JSESSIONID"}
    cookie_header = "; ".join(f"{c['name']}={c['value']}" for c in scoped
                              if c.get("name") in auth_names)
    if not token and not cookie_header:
        raise NeoBundAuthError("NeoBund 专用窗口没有可用登录凭证，请重新登录")
    return {"access_token": token, "cookie": cookie_header}


def read_browser_credentials() -> Dict[str, str]:
    """Only inspect NeoBund tabs on the dedicated loopback Chrome port 9222."""
    import websocket

    with requests.Session() as session:
        session.trust_env = False
        response = session.get("http://127.0.0.1:9222/json", timeout=5, allow_redirects=False)
        response.raise_for_status()
        tabs = response.json()
    for tab in tabs:
        page_url = urlparse(str(tab.get("url", "")))
        if tab.get("type") != "page" or page_url.scheme != "https" or page_url.hostname not in TRUSTED_HOSTS:
            continue
        ws_url = str(tab.get("webSocketDebuggerUrl", ""))
        parsed_ws = urlparse(ws_url)
        if parsed_ws.scheme != "ws" or parsed_ws.hostname not in {"127.0.0.1", "localhost"} or parsed_ws.port != 9222:
            continue
        ws = websocket.create_connection(ws_url, timeout=5, suppress_origin=True)
        try:
            call_id = 0

            def cdp(method: str, params: dict) -> dict:
                nonlocal call_id
                call_id += 1
                ws.send(json.dumps({"id": call_id, "method": method, "params": params}))
                deadline = time.monotonic() + 8
                for _ in range(100):
                    remaining = deadline - time.monotonic()
                    if remaining <= 0:
                        break
                    ws.settimeout(min(5, remaining))
                    message = json.loads(ws.recv())
                    if message.get("id") == call_id:
                        if message.get("error"):
                            raise NeoBundAuthError("NeoBund 专用窗口凭证读取失败")
                        return message.get("result", {})
                raise NeoBundAuthError("NeoBund 专用窗口响应超时")

            # Recheck origin inside the page to protect against tab navigation races.
            expression = """(() => {
              if (location.protocol !== 'https:' ||
                  !['neobund.ai','www.neobund.ai','cn.neobund.ai'].includes(location.hostname)) return null;
              const out = {};
              for (const store of [localStorage, sessionStorage]) {
                for (const key of ['access_token','accessToken','token','auth']) {
                  const value = store.getItem(key);
                  if (value) { try { out[key] = JSON.parse(value); } catch (_) { out[key] = value; } }
                }
              }
              return out;
            })()"""
            result = cdp("Runtime.evaluate", {"expression": expression, "returnByValue": True})
            storage = result.get("result", {}).get("value")
            if not isinstance(storage, dict):
                continue
            cookies = cdp("Network.getCookies", {"urls": ["https://www.neobund.ai/", "https://cn.neobund.ai/"]})
            return _credentials_from_values(storage, cookies.get("cookies", []))
        finally:
            ws.close()
    raise NeoBundAuthError("NeoBund 专用窗口未打开，请打开并登录")


def persist_validated_credentials(config_path: str, credentials: Dict[str, str]) -> None:
    """Atomically replace machine-local config, preserving unrelated settings."""
    path = Path(config_path).expanduser()
    if path.is_symlink():
        raise ValueError("NeoBund 配置不能是符号链接")
    config = json.loads(path.read_text(encoding="utf-8")) if path.exists() else {}
    if not isinstance(config, dict):
        raise ValueError("NeoBund 配置必须是 JSON 对象")
    config["neobund_access_token"] = str(credentials.get("access_token") or "")
    config["neobund_cookie"] = str(credentials.get("cookie") or "")
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, temporary = tempfile.mkstemp(prefix=f".{path.name}.", dir=str(path.parent))
    try:
        os.fchmod(fd, 0o600)
        with os.fdopen(fd, "w", encoding="utf-8") as handle:
            json.dump(config, handle, ensure_ascii=False, indent=2)
            handle.write("\n")
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temporary, path)
    finally:
        if os.path.exists(temporary):
            os.unlink(temporary)
