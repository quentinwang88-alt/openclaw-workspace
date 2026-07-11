#!/usr/bin/env python3
"""Update Kalodata Cookie in creator-crm config without pasting it into chat."""

from __future__ import annotations

import argparse
import getpass
import json
import re
import sys
from pathlib import Path


SKILL_DIR = Path(__file__).resolve().parents[1]
CONFIG_PATH = SKILL_DIR / "config" / "api_config.json"


def main() -> int:
    parser = argparse.ArgumentParser(description="更新 Creator CRM 的 Kalodata Cookie")
    parser.add_argument("--cookie", default="", help="不推荐：直接通过参数传 Cookie；建议留空后按提示粘贴")
    parser.add_argument("--headers-file", default="", help="包含浏览器 Request Headers 的文本文件，可自动提取 cookie/user-agent")
    parser.add_argument("--user-agent", default="", help="浏览器 User-Agent；建议和 Cookie 同步保存")
    parser.add_argument("--config", default=str(CONFIG_PATH), help="配置文件路径")
    args = parser.parse_args()

    raw_headers = ""
    if args.headers_file:
        raw_headers = Path(args.headers_file).expanduser().read_text(encoding="utf-8")

    cookie = args.cookie.strip() or extract_header(raw_headers, "cookie")
    user_agent = args.user_agent.strip() or extract_header(raw_headers, "user-agent")
    sec_ch_ua = extract_header(raw_headers, "sec-ch-ua")
    sec_ch_ua_platform = extract_header(raw_headers, "sec-ch-ua-platform")

    if not cookie:
        print("请在本机终端粘贴从浏览器复制的完整 Cookie 字符串。输入时不会回显。")
        print("更推荐：把整段 Request Headers 保存成 txt，然后用 --headers-file 自动提取 Cookie 和 User-Agent。")
        cookie = getpass.getpass("Kalodata Cookie: ").strip()

    if not cookie:
        print("❌ Cookie 为空，未更新")
        return 1
    if "SESSION=" not in cookie and "session" not in cookie.lower():
        print("⚠️ 这个 Cookie 看起来不包含 SESSION/session 字段，但仍会写入配置")

    config_path = Path(args.config).expanduser()
    config_path.parent.mkdir(parents=True, exist_ok=True)

    config = {}
    if config_path.exists():
        try:
            config = json.loads(config_path.read_text(encoding="utf-8"))
        except json.JSONDecodeError:
            backup_path = config_path.with_suffix(config_path.suffix + ".bak")
            backup_path.write_text(config_path.read_text(encoding="utf-8"), encoding="utf-8")
            print(f"⚠️ 原配置不是合法 JSON，已备份到: {backup_path}")
            config = {}

    kalodata = config.setdefault("kalodata", {})
    kalodata["cookie"] = cookie
    if user_agent:
        kalodata["user_agent"] = user_agent
    if sec_ch_ua:
        kalodata["sec_ch_ua"] = sec_ch_ua
    if sec_ch_ua_platform:
        kalodata["sec_ch_ua_platform"] = sec_ch_ua_platform
    config_path.write_text(json.dumps(config, ensure_ascii=False, indent=2), encoding="utf-8")
    print(f"✅ Kalodata Cookie 已更新: {config_path}")
    print(f"   User-Agent: {'已同步' if user_agent else '未同步'}")
    print("   下一步可以运行小批测试，确认 403 是否解除。")
    return 0


def extract_header(raw_headers: str, header_name: str) -> str:
    if not raw_headers:
        return ""
    pattern = re.compile(rf"(?im)^{re.escape(header_name)}\\s*[:：]?\\s*(.+)$")
    match = pattern.search(raw_headers)
    return match.group(1).strip() if match else ""


if __name__ == "__main__":
    raise SystemExit(main())
