#!/usr/bin/env python3
"""
飞书消息发送器

通过老王助理飞书机器人发送巡检卡片给老板。
支持两种发送方式：
1. 直接调用飞书 IM 消息 API（需知道 receive_id）
2. 通过 webhook URL（如果配置了）
"""

import json
import time
from pathlib import Path
from datetime import datetime
from typing import Dict, Optional

import requests

OPENCLAW_CONFIG_PATH = Path.home() / ".openclaw" / "openclaw.json"
SKILL_DIR = Path(__file__).resolve().parent
CONFIG_PATH = SKILL_DIR / "config.json"


class FeishuSender:
    """飞书消息发送器"""

    def __init__(self, config_path: Path = CONFIG_PATH):
        with open(config_path, "r", encoding="utf-8") as f:
            self.config = json.load(f)
        self._app_id = None
        self._app_secret = None
        self._access_token = None
        self._token_expires_at = 0

    def _load_bot_credentials(self) -> tuple:
        """加载消息发送 bot 的应用凭证"""
        feishu_cfg = self.config["feishu"]
        sender_app_id = feishu_cfg.get("sender_app_id", feishu_cfg["laowang_app_id"])

        if not OPENCLAW_CONFIG_PATH.exists():
            raise FileNotFoundError(f"openclaw.json not found: {OPENCLAW_CONFIG_PATH}")

        with open(OPENCLAW_CONFIG_PATH, "r", encoding="utf-8") as f:
            oc = json.load(f)

        channels = oc.get("channels", {}).get("feishu", {})
        top_app_id = channels.get("appId", "")
        top_app_secret = channels.get("appSecret", "")
        accounts = channels.get("accounts", {})

        # 先查顶层 app
        if sender_app_id == top_app_id:
            return top_app_id, top_app_secret

        # 再查 accounts
        for key, acc in accounts.items():
            if acc.get("appId") == sender_app_id:
                return acc["appId"], acc["appSecret"]

        raise ValueError(
            f"Sender app (appId={sender_app_id}) not found in openclaw.json"
        )

    def _get_access_token(self) -> str:
        """获取 bot 的 tenant_access_token"""
        if self._access_token and time.time() < self._token_expires_at:
            return self._access_token

        if not self._app_id:
            self._app_id, self._app_secret = self._load_bot_credentials()

        resp = requests.post(
            "https://open.feishu.cn/open-apis/auth/v3/tenant_access_token/internal",
            json={"app_id": self._app_id, "app_secret": self._app_secret},
            timeout=15,
        )
        data = resp.json()
        if data.get("code") != 0:
            raise RuntimeError(
                f"Failed to get laowang bot access_token: {data}"
            )

        self._access_token = data["tenant_access_token"]
        self._token_expires_at = time.time() + data.get("expire", 7200) - 300
        return self._access_token

    def send_text_message(
        self, content: str, receive_id: str = None, receive_id_type: str = None
    ) -> Dict:
        """
        发送文本消息

        Args:
            content: 消息内容（Markdown 文本）
            receive_id: 接收者 ID（open_id / user_id / chat_id / email）
            receive_id_type: ID 类型（open_id / user_id / chat_id / email）

        Returns:
            API 响应
        """
        receive_id = receive_id or self.config["feishu"]["receive_id"]
        receive_id_type = receive_id_type or self.config["feishu"]["receive_id_type"]

        if not receive_id:
            return {
                "code": -1,
                "msg": "未配置 receive_id。请在 config.json 中设置 feishu.receive_id。"
                       "可以从飞书开发者后台获取用户的 open_id，"
                       "或使用 chat_id 发送到指定群聊。",
            }

        token = self._get_access_token()
        headers = {
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json",
        }

        # 构建 Markdown 格式的消息内容
        msg_content = json.dumps({"text": content})

        payload = {
            "receive_id": receive_id,
            "msg_type": "text",
            "content": msg_content,
        }

        resp = requests.post(
            f"https://open.feishu.cn/open-apis/im/v1/messages?receive_id_type={receive_id_type}",
            headers=headers,
            json=payload,
            timeout=15,
        )

        return resp.json()

    def send_interactive_card(
        self, title: str, content: str, receive_id: str = None, receive_id_type: str = None
    ) -> Dict:
        """
        发送卡片消息（更丰富的展示样式）

        Args:
            title: 卡片标题
            content: Markdown 内容
            receive_id: 接收者 ID
            receive_id_type: ID 类型

        Returns:
            API 响应
        """
        receive_id = receive_id or self.config["feishu"]["receive_id"]
        receive_id_type = receive_id_type or self.config["feishu"]["receive_id_type"]

        if not receive_id:
            return {"code": -1, "msg": "未配置 receive_id"}

        token = self._get_access_token()
        headers = {
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json",
        }

        # 构建交互式卡片
        card = {
            "config": {"wide_screen_mode": True},
            "header": {
                "title": {"tag": "plain_text", "content": title},
                "template": "indigo",
            },
            "elements": [
                {
                    "tag": "markdown",
                    "content": content,
                },
                {
                    "tag": "hr",
                },
                {
                    "tag": "note",
                    "elements": [
                        {
                            "tag": "plain_text",
                            "content": f"🦞 OpenClaw 日报自动巡检 | {datetime.now().strftime('%Y-%m-%d %H:%M')}",
                        }
                    ],
                },
            ],
        }

        msg_content = json.dumps(card, ensure_ascii=False)

        payload = {
            "receive_id": receive_id,
            "msg_type": "interactive",
            "content": msg_content,
        }

        resp = requests.post(
            f"https://open.feishu.cn/open-apis/im/v1/messages?receive_id_type={receive_id_type}",
            headers=headers,
            json=payload,
            timeout=15,
        )

        return resp.json()

    def send_webhook(self, content: str, webhook_url: str) -> bool:
        """
        通过 webhook 发送消息（备用方案）

        Args:
            content: 消息文本
            webhook_url: webhook URL

        Returns:
            是否发送成功
        """
        if not webhook_url:
            print("未配置 webhook URL")
            return False

        payload = {
            "msg_type": "text",
            "content": {"text": content},
        }

        try:
            resp = requests.post(webhook_url, json=payload, timeout=10)
            resp.raise_for_status()
            result = resp.json()
            return result.get("code") == 0
        except Exception as e:
            print(f"Webhook 发送失败: {e}")
            return False


if __name__ == "__main__":
    sender = FeishuSender()
    # Test: print credentials (no actual send)
    try:
        app_id, _ = sender._load_bot_credentials()
        print(f"✅ 老王助理 bot 配置加载成功: appId={app_id[:12]}...")
    except Exception as e:
        print(f"❌ 配置加载失败: {e}")
