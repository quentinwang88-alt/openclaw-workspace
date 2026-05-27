#!/usr/bin/env python3
"""
飞书知识库日报读取器

自动定位当天日报（配饰 + 女装），读取正文内容。
支持四级匹配：精准匹配 → 模糊匹配 → 最近修改匹配 → 缺失提醒

Feishu APIs used:
- Wiki Node Info: GET /wiki/v2/spaces/get_node?token={token}
- Wiki Children:  GET /wiki/v2/spaces/{space_id}/nodes/{token}/children
- Doc Raw Content: GET /docx/v1/documents/{doc_id}/raw_content
"""

import json
import re
import time
from pathlib import Path
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple

import requests

OPENCLAW_CONFIG_PATH = Path.home() / ".openclaw" / "openclaw.json"
SKILL_DIR = Path(__file__).resolve().parent
CONFIG_PATH = SKILL_DIR / "config.json"


class FeishuWikiReader:
    """飞书知识库日报读取器"""

    def __init__(self, config_path: Path = CONFIG_PATH):
        with open(config_path, "r", encoding="utf-8") as f:
            self.config = json.load(f)
        wiki_cfg = self.config["wiki"]
        self.wiki_url = wiki_cfg["url"]
        self.wiki_token = wiki_cfg["wiki_token"]
        self.space_id = wiki_cfg.get("space_id")
        self._has_child = wiki_cfg.get("has_child")
        self._doc_token = wiki_cfg.get("doc_token")
        self._app_id = None
        self._app_secret = None
        self._access_token = None
        self._token_expires_at = 0

    def _load_app_credentials(self, app_id: str = None) -> Tuple[str, str]:
        """从 openclaw.json 加载飞书应用凭证"""
        if not OPENCLAW_CONFIG_PATH.exists():
            raise FileNotFoundError(f"openclaw.json not found: {OPENCLAW_CONFIG_PATH}")

        with open(OPENCLAW_CONFIG_PATH, "r", encoding="utf-8") as f:
            oc = json.load(f)

        channels = oc.get("channels", {}).get("feishu", {})
        top_app_id = channels.get("appId", "")
        top_app_secret = channels.get("appSecret", "")

        if app_id:
            # 先在 accounts 中查找
            accounts = channels.get("accounts", {})
            for key, acc in accounts.items():
                if acc.get("appId") == app_id:
                    return acc["appId"], acc["appSecret"]
            # 如果 app_id 就是顶层的 appId，用顶层凭证
            if app_id == top_app_id:
                return top_app_id, top_app_secret
            raise ValueError(f"App {app_id} not found in openclaw.json")

        # 没有指定 app_id 时，优先用顶层凭证
        if top_app_id and top_app_secret:
            return top_app_id, top_app_secret

        raise ValueError("No Feishu app credentials found in openclaw.json")

    def _get_access_token(self) -> str:
        """获取或刷新 tenant_access_token"""
        if self._access_token and time.time() < self._token_expires_at:
            return self._access_token

        if not self._app_id or not self._app_secret:
            default_id = self.config["feishu"]["default_app_id"]
            self._app_id, self._app_secret = self._load_app_credentials(default_id)

        resp = requests.post(
            "https://open.feishu.cn/open-apis/auth/v3/tenant_access_token/internal",
            json={"app_id": self._app_id, "app_secret": self._app_secret},
            timeout=15,
        )
        data = resp.json()
        if data.get("code") != 0:
            raise RuntimeError(f"Failed to get tenant_access_token: {data}")

        self._access_token = data["tenant_access_token"]
        self._token_expires_at = time.time() + data.get("expire", 7200) - 300
        return self._access_token

    def _api_get(self, path: str, params: dict = None) -> dict:
        """调用飞书 OpenAPI GET"""
        token = self._get_access_token()
        headers = {
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json",
        }
        resp = requests.get(
            f"https://open.feishu.cn{path}",
            headers=headers,
            params=params,
            timeout=30,
        )
        data = resp.json()
        if data.get("code") != 0:
            print(f"  ⚠️ API error [{path}]: {data.get('msg', data)}")
        return data

    def _resolve_space_id(self, force: bool = False) -> str:
        """通过 wiki_token 解析 space_id 和文档信息"""
        if self.space_id and self._doc_token and not force:
            return self.space_id

        print(f"  获取 node info, wiki_token={self.wiki_token}")
        data = self._api_get("/open-apis/wiki/v2/spaces/get_node", {"token": self.wiki_token})
        node = data.get("data", {}).get("node", {})

        self.space_id = node.get("space_id", "")
        self._doc_token = node.get("obj_token", "")
        self._has_child = node.get("has_child", False)
        self._obj_type = node.get("obj_type", "")

        if not self.space_id:
            raise RuntimeError(
                f"Cannot resolve space_id for wiki {self.wiki_token}. "
                f"Check wiki permissions and app scopes."
            )

        self.config["wiki"]["space_id"] = self.space_id
        self.config["wiki"]["doc_token"] = self._doc_token
        self.config["wiki"]["has_child"] = self._has_child
        with open(CONFIG_PATH, "w", encoding="utf-8") as f:
            json.dump(self.config, f, indent=2, ensure_ascii=False)

        return self.space_id

    def read_document_content(self, doc_id: str = None) -> str:
        """读取指定文档的纯文本内容"""
        if doc_id is None:
            if not self._doc_token:
                self._resolve_space_id(force=True)
            doc_id = self._doc_token
        if not doc_id:
            raise ValueError("No doc_token available")

        print(f"  读取文档: {doc_id}")
        return self._read_document_content(doc_id)

    def _extract_date_section(self, text: str, target_date: datetime) -> str:
        """已废弃 - 改用 _find_and_extract_reports 直接匹配"""
        return text

    def find_reports_in_document(
        self, target_date: datetime
    ) -> List[dict]:
        """
        在单文档中按日期+业务线查找日报内容

        文档格式: MMDD配饰 / MMDD女装 (如 0519配饰)
        """
        content = self.read_document_content()
        if not content:
            return []

        print(f"  文档长度: {len(content)} 字符")
        lines = content.split("\n")

        date_mmdd = target_date.strftime("%m%d")
        reports = []
        biz_lines = self.config["business_lines"]

        for biz in biz_lines:
            biz_name = biz["name"]
            # 匹配模式: MMDD配饰 或 MMDD女装
            marker = f"{date_mmdd}{biz_name}"

            start_idx = None
            for i, line in enumerate(lines):
                if line.strip() == marker:
                    start_idx = i
                    break

            if start_idx is None:
                print(f"  ❌ 未找到: {marker}")
                reports.append({
                    "business_line": biz_name,
                    "found": False,
                    "match_level": 0,
                    "match_desc": f"未找到 '{marker}'",
                    "title": "",
                    "doc_token": self._doc_token or "",
                    "content": "",
                })
                continue

            # 提取到下一个日期标记的内容
            end_idx = len(lines)
            for j in range(start_idx + 1, len(lines)):
                stripped = lines[j].strip()
                # 检测下一个日期标记 (MMDD配饰 / MMDD女装 / MMDD)
                if re.match(r"^\d{4}(?:配饰|女装)?$", stripped) or re.match(
                    r"^\d{1,2}[-/]\d{1,2}", stripped
                ):
                    end_idx = j
                    break

            biz_content = "\n".join(lines[start_idx:end_idx])
            print(f"  ✅ 找到: {marker} (行{start_idx}-{end_idx}, {len(biz_content)}字)")

            reports.append({
                "business_line": biz_name,
                "found": True,
                "match_level": 1,
                "match_desc": f"文档内精准定位: '{marker}'",
                "title": marker,
                "doc_token": self._doc_token or "",
                "content": biz_content,
            })

        return reports

    def find_daily_reports(
        self, target_date: datetime
    ) -> List[dict]:
        """
        查找指定日期的所有日报

        策略：
        - 如果 wiki 有子节点 → 遍历子节点查找
        - 如果 wiki 是单文档 → 在文档内按日期分段提取
        """
        print(f"\n📂 查找 {target_date.strftime('%Y-%m-%d')} 日报...")

        date_keywords = self._build_date_keywords(target_date)
        print(f"  日期关键词: {date_keywords}")

        space_id = self._resolve_space_id()
        print(f"  space_id: {space_id}")

        # 检查 wiki 类型
        if self._has_child is None:
            self._resolve_space_id(force=True)
        has_child = self._has_child

        if has_child:
            # 有子节点：原来的遍历逻辑
            children = self.list_child_nodes()
            print(f"  子节点数量: {len(children)}")
            return self._match_reports_from_children(children, date_keywords)
        else:
            # 单文档：在文档内按日期查找
            return self.find_reports_in_document(target_date)

    def _match_reports_from_children(
        self, children: List[dict], date_keywords: List[str]
    ) -> List[dict]:
        """从子节点列表中匹配日报"""
        reports = []
        biz_lines = self.config["business_lines"]

        for biz in biz_lines:
            biz_name = biz["name"]
            biz_keywords = biz["keywords"]

            node, level, desc = self._match_report_node(
                children, date_keywords, biz_keywords, match_level=3
            )

            result = {
                "business_line": biz_name,
                "found": node is not None,
                "match_level": level,
                "match_desc": desc,
                "title": node.get("title", "") if node else "",
                "doc_token": "",
                "content": "",
            }

            if node:
                doc_token = node.get("obj_token", "") or node.get("node_token", "")
                result["doc_token"] = doc_token
                print(f"  ✅ {desc}")
                try:
                    content = self._read_document_content(doc_token)
                    result["content"] = content
                    print(f"     内容长度: {len(content)} 字符")
                except Exception as e:
                    print(f"  ❌ 读取内容失败: {e}")
                    result["match_desc"] += "（内容读取失败）"
            else:
                print(f"  ❌ {desc}")

            reports.append(result)

        return reports

    def _read_document_content(self, doc_id: str) -> str:
        """读取飞书文档的纯文本内容"""
        data = self._api_get(
            f"/open-apis/docx/v1/documents/{doc_id}/raw_content"
        )
        raw = data.get("data", {}).get("content", "")
        if not raw:
            # Fallback: try blocks API
            print(f"  ⚠️ raw_content empty, trying blocks API...")
            raw = self._read_document_blocks(doc_id)
        return raw

    def _read_document_blocks(self, doc_id: str) -> str:
        """通过 blocks API 读取文档内容"""
        blocks_data = self._api_get(
            f"/open-apis/docx/v1/documents/{doc_id}/blocks"
        )
        blocks = blocks_data.get("data", {}).get("items", []) or []

        lines = []
        for block in blocks:
            block_type = block.get("block_type", 0)
            text_elems = []
            if block_type == 2:  # text block
                for elem in block.get("text", {}).get("elements", []):
                    text_run = elem.get("text_run", {})
                    text_elems.append(text_run.get("content", ""))
            elif block_type == 3:  # heading1
                for elem in block.get("heading1", {}).get("elements", []):
                    text_run = elem.get("text_run", {})
                    text_elems.append("# " + text_run.get("content", ""))
            elif block_type == 4:  # heading2
                for elem in block.get("heading2", {}).get("elements", []):
                    text_run = elem.get("text_run", {})
                    text_elems.append("## " + text_run.get("content", ""))
            elif block_type == 9:  # bullet
                for elem in block.get("bullet", {}).get("elements", []):
                    text_run = elem.get("text_run", {})
                    text_elems.append("- " + text_run.get("content", ""))
            elif block_type == 22:  # table
                lines.append("[表格]")
                continue

            line = "".join(text_elems).strip()
            if line:
                lines.append(line)

        return "\n".join(lines)

    def _build_date_keywords(self, target_date: datetime) -> List[str]:
        """构建当天日期的匹配关键词列表"""
        keywords = [
            target_date.strftime("%m%d"),
            target_date.strftime("%Y-%m-%d"),
            f"{target_date.month}月{target_date.day}日",
            target_date.strftime("%Y%m%d"),
        ]
        return keywords

    def _match_report_node(
        self,
        children: List[dict],
        date_keywords: List[str],
        biz_keywords: List[str],
        match_level: int = 1,
    ) -> Tuple[Optional[dict], int, str]:
        """
        匹配日报节点

        level 1: 精准匹配 {MMDD} 配饰 / {MMDD} 女装
        level 2: 模糊匹配（标题同时包含日期关键词和业务线关键词）
        level 3: 最近修改匹配（匹配业务线关键词 + 日报特征关键词）

        Returns: (node, level, match_desc)
        """
        biz_label = "/".join(biz_keywords)

        # Level 1: 精准匹配
        for child in children:
            title = child.get("title", "")
            for dk in date_keywords[:2]:  # 只用 MMDD 和 YYYY-MM-DD
                for bk in biz_keywords:
                    if dk in title and bk in title:
                        return child, 1, f"精准匹配: '{title}'"

        # Level 2: 模糊匹配
        for child in children:
            title = child.get("title", "")
            date_matched = any(dk in title for dk in date_keywords)
            biz_matched = any(bk in title for bk in biz_keywords)
            if date_matched and biz_matched:
                return child, 2, f"模糊匹配: '{title}'"

        # Level 3: 最近修改匹配
        if match_level >= 3:
            report_keywords = [
                "日报", "日常", "工作情况", "短视频生成",
                "视频发布", "选品", "达人建联", "样品审批",
            ]
            for child in children:
                title = child.get("title", "")
                biz_matched = any(bk in title for bk in biz_keywords)
                report_matched = any(rk in title for rk in report_keywords)
                if biz_matched and report_matched:
                    return child, 3, f"疑似日报: '{title}'（标题不规范）"

        return None, 0, f"未找到 {biz_label} 日报"

    def find_and_read_reports(
        self, target_date: Optional[datetime] = None
    ) -> List[dict]:
        """
        一站式：查找并读取当天所有日报

        Args:
            target_date: 目标日期，默认今天

        Returns:
            日报列表，每个日报包含 business_line, title, content 等字段
        """
        if target_date is None:
            target_date = datetime.now()

        return self.find_daily_reports(target_date)


if __name__ == "__main__":
    reader = FeishuWikiReader()
    reports = reader.find_and_read_reports()
    for r in reports:
        print(f"\n{'='*60}")
        print(f"业务线: {r['business_line']}")
        print(f"找到: {r['found']}, 匹配: {r['match_desc']}")
        if r["content"]:
            print(f"内容预览 (前200字):\n{r['content'][:200]}...")
