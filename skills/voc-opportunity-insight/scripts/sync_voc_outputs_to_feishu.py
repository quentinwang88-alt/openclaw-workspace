#!/usr/bin/env python3
"""Publish standalone VOC opportunity artifacts to Feishu wiki + bitable."""
from __future__ import annotations

import argparse
import hashlib
import json
import re
import time
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Sequence, Tuple
from urllib.parse import parse_qs, urlparse

import requests


DEFAULT_BITABLE_URL = "https://gcngopvfvo0q.feishu.cn/wiki/DMIYwxge7iV8sAktvjDcZx84nzQ?table=tbl3wdy3DTHdjHlH&view=vewFYyTCLh"
DEFAULT_REPORT_PARENT_URL = "https://gcngopvfvo0q.feishu.cn/wiki/Qe8rwXS8uiiHsukBGVMcInHBnO2"
TENANT_BASE = "https://gcngopvfvo0q.feishu.cn"
OPEN_API = "https://open.feishu.cn/open-apis"
CONFIG_PATH = Path.home() / ".openclaw" / "openclaw.json"
VOC_INSIGHT_DIR = Path(__file__).resolve().parents[2] / "voc-insight"


class FeishuError(RuntimeError):
    pass


def load_json(path: Path) -> Dict[str, Any]:
    with path.open(encoding="utf-8") as f:
        return json.load(f)


def get_credentials() -> Tuple[str, str]:
    config = load_json(CONFIG_PATH)
    channel = (config.get("channels") or {}).get("feishu") or {}
    app_id = str(channel.get("appId") or "").strip()
    app_secret = str(channel.get("appSecret") or "").strip()
    if not app_id or not app_secret:
        raise FeishuError("OpenClaw未配置飞书appId/appSecret")
    return app_id, app_secret


class FeishuClient:
    def __init__(self):
        self.token = ""
        self.expires_at = 0.0

    def access_token(self) -> str:
        if self.token and time.time() < self.expires_at:
            return self.token
        app_id, app_secret = get_credentials()
        result = self.request_raw(
            "POST", "https://open.feishu.cn/open-apis/auth/v3/tenant_access_token/internal",
            json={"app_id": app_id, "app_secret": app_secret}, auth=False,
        )
        self.token = str(result.get("tenant_access_token") or "")
        if not self.token:
            raise FeishuError("飞书未返回tenant_access_token")
        self.expires_at = time.time() + int(result.get("expire") or 7200) - 300
        return self.token

    def request_raw(self, method: str, url: str, auth: bool = True, **kwargs) -> Dict[str, Any]:
        headers = dict(kwargs.pop("headers", {}) or {})
        if auth:
            headers["Authorization"] = "Bearer {}".format(self.access_token())
        headers.setdefault("Content-Type", "application/json; charset=utf-8")
        last_error: Optional[Exception] = None
        for attempt in range(4):
            try:
                response = requests.request(method, url, headers=headers, timeout=60, **kwargs)
                payload = response.json()
                if response.status_code >= 400 or int(payload.get("code") or 0) != 0:
                    raise FeishuError("{} {} failed: HTTP {} code={} msg={}".format(
                        method, url, response.status_code, payload.get("code"), payload.get("msg")
                    ))
                return payload
            except (requests.RequestException, ValueError, FeishuError) as exc:
                last_error = exc
                if attempt < 3:
                    time.sleep(2 ** attempt)
        raise FeishuError(str(last_error))

    def get_wiki_node(self, wiki_token: str) -> Dict[str, Any]:
        result = self.request_raw(
            "GET", "{}/wiki/v2/spaces/get_node".format(OPEN_API), params={"token": wiki_token}
        )
        return (result.get("data") or {}).get("node") or {}

    def create_wiki_doc(self, parent_wiki_token: str, title: str) -> Dict[str, str]:
        parent = self.get_wiki_node(parent_wiki_token)
        space_id = str(parent.get("space_id") or "")
        parent_token = str(parent.get("node_token") or parent_wiki_token)
        if not space_id:
            raise FeishuError("无法解析报告父节点space_id")
        result = self.request_raw(
            "POST", "{}/wiki/v2/spaces/{}/nodes".format(OPEN_API, space_id),
            json={
                "obj_type": "docx", "parent_node_token": parent_token,
                "node_type": "origin", "title": title,
            },
        )
        node = (result.get("data") or {}).get("node") or {}
        node_token = str(node.get("node_token") or "")
        document_id = str(node.get("obj_token") or "")
        if not node_token or not document_id:
            raise FeishuError("知识库创建成功但未返回node_token/obj_token")
        return {
            "node_token": node_token,
            "document_id": document_id,
            "url": "{}/wiki/{}".format(TENANT_BASE, node_token),
            "title": title,
            "location": "wiki_child",
        }

    def create_cloud_doc(self, title: str) -> Dict[str, str]:
        result = self.request_raw(
            "POST", "{}/docx/v1/documents".format(OPEN_API), json={"title": title}
        )
        document = (result.get("data") or {}).get("document") or {}
        document_id = str(document.get("document_id") or "")
        if not document_id:
            raise FeishuError("云文档创建成功但未返回document_id")
        return {
            "document_id": document_id,
            "url": "{}/docx/{}".format(TENANT_BASE, document_id),
            "title": title,
            "location": "standalone_docx",
            "warning": "知识库父节点未授予应用创建子节点权限，已降级为独立飞书文档",
        }

    def append_markdown(self, document_id: str, content: str, batch_size: int = 40) -> Dict[str, int]:
        children = markdown_children(content)
        url = "{}/docx/v1/documents/{}/blocks/{}/children".format(OPEN_API, document_id, document_id)
        batches = 0
        for start in range(0, len(children), batch_size):
            self.request_raw("POST", url, json={"children": children[start:start + batch_size]})
            batches += 1
            time.sleep(0.25)
        return {"block_count": len(children), "batch_count": batches}

    def resolve_bitable(self, wiki_token: str) -> str:
        node = self.get_wiki_node(wiki_token)
        if node.get("obj_type") != "bitable":
            raise FeishuError("目标wiki节点不是bitable: {}".format(node.get("obj_type")))
        app_token = str(node.get("obj_token") or "")
        if not app_token:
            raise FeishuError("bitable节点未返回obj_token")
        return app_token

    def list_fields(self, app_token: str, table_id: str) -> List[Dict[str, Any]]:
        result = self.request_raw(
            "GET", "{}/bitable/v1/apps/{}/tables/{}/fields".format(OPEN_API, app_token, table_id),
            params={"page_size": 500},
        )
        return (result.get("data") or {}).get("items") or []

    def create_field(self, app_token: str, table_id: str, name: str, field_type: int, ui_type: str) -> None:
        self.request_raw(
            "POST", "{}/bitable/v1/apps/{}/tables/{}/fields".format(OPEN_API, app_token, table_id),
            json={"field_name": name, "type": field_type, "ui_type": ui_type},
        )

    def list_tables(self, app_token: str) -> List[Dict[str, Any]]:
        result = self.request_raw(
            "GET", "{}/bitable/v1/apps/{}/tables".format(OPEN_API, app_token), params={"page_size": 100}
        )
        return (result.get("data") or {}).get("items") or []

    def create_table(
        self, app_token: str, name: str, fields: Sequence[Tuple[str, int, str]],
    ) -> Tuple[str, str]:
        result = self.request_raw(
            "POST", "{}/bitable/v1/apps/{}/tables".format(OPEN_API, app_token),
            json={"table": {
                "name": name,
                "default_view_name": name,
                "fields": [
                    {"field_name": field_name, "type": field_type, "ui_type": ui_type}
                    for field_name, field_type, ui_type in fields
                ],
            }},
        )
        data = result.get("data") or {}
        table_id = str(data.get("table_id") or "")
        view_id = str(data.get("default_view_id") or "")
        if not table_id:
            raise FeishuError("创建数据表成功但未返回table_id: {}".format(name))
        return table_id, view_id

    def patch_table_name(self, app_token: str, table_id: str, name: str) -> None:
        self.request_raw(
            "PATCH", "{}/bitable/v1/apps/{}/tables/{}".format(OPEN_API, app_token, table_id),
            json={"name": name},
        )

    def patch_view(
        self, app_token: str, table_id: str, view_id: str, name: str, hidden_fields: Sequence[str],
    ) -> None:
        url = "{}/bitable/v1/apps/{}/tables/{}/views/{}".format(OPEN_API, app_token, table_id, view_id)
        self.request_raw("PATCH", url, json={"view_name": name})
        self.request_raw("PATCH", url, json={"property": {"hidden_fields": list(hidden_fields)}})

    def list_records(self, app_token: str, table_id: str) -> List[Dict[str, Any]]:
        url = "{}/bitable/v1/apps/{}/tables/{}/records".format(OPEN_API, app_token, table_id)
        items: List[Dict[str, Any]] = []
        page_token = ""
        while True:
            params: Dict[str, Any] = {"page_size": 500}
            if page_token:
                params["page_token"] = page_token
            result = self.request_raw("GET", url, params=params)
            data = result.get("data") or {}
            items.extend(data.get("items") or [])
            if not data.get("has_more"):
                break
            page_token = str(data.get("page_token") or "")
        return items

    def batch_create_records(self, app_token: str, table_id: str, records: Sequence[Dict[str, Any]]) -> None:
        url = "{}/bitable/v1/apps/{}/tables/{}/records/batch_create".format(OPEN_API, app_token, table_id)
        for start in range(0, len(records), 100):
            self.request_raw("POST", url, json={"records": list(records[start:start + 100])})
            time.sleep(0.25)

    def batch_update_records(self, app_token: str, table_id: str, records: Sequence[Dict[str, Any]]) -> None:
        url = "{}/bitable/v1/apps/{}/tables/{}/records/batch_update".format(OPEN_API, app_token, table_id)
        for start in range(0, len(records), 100):
            self.request_raw("POST", url, json={"records": list(records[start:start + 100])})
            time.sleep(0.25)

    def batch_delete_records(self, app_token: str, table_id: str, record_ids: Sequence[str]) -> None:
        url = "{}/bitable/v1/apps/{}/tables/{}/records/batch_delete".format(OPEN_API, app_token, table_id)
        for start in range(0, len(record_ids), 100):
            self.request_raw("POST", url, json={"records": list(record_ids[start:start + 100])})
            time.sleep(0.25)


def parse_wiki_url(url: str) -> str:
    match = re.search(r"/wiki/([A-Za-z0-9]+)", str(url or ""))
    if not match:
        raise FeishuError("无法解析wiki URL: {}".format(url))
    return match.group(1)


def parse_bitable_url(url: str) -> Tuple[str, str]:
    wiki_token = parse_wiki_url(url)
    table_id = parse_qs(urlparse(url).query).get("table", [""])[0]
    if not table_id:
        raise FeishuError("多维表格URL缺少table参数")
    return wiki_token, table_id


def parse_view_id(url: str) -> str:
    return parse_qs(urlparse(url).query).get("view", [""])[0]


def text_block(text: str, block_type: int = 2, key: str = "text") -> Dict[str, Any]:
    return {
        "block_type": block_type,
        key: {"elements": [{"text_run": {"content": str(text or "")}}], "style": {}},
    }


def markdown_children(content: str) -> List[Dict[str, Any]]:
    lines = str(content or "").replace("\r\n", "\n").split("\n")
    children: List[Dict[str, Any]] = []
    for raw in lines:
        line = raw.strip()
        if not line:
            continue
        if line.startswith("|"):
            # Feishu markdown tables are not accepted directly; keep each row readable as text.
            if all(set(cell.strip()) <= {"-", ":"} for cell in line.strip("|").split("|")):
                continue
            children.append(text_block(" | ".join(cell.strip() for cell in line.strip("|").split("|"))))
            continue
        if line.startswith("> "):
            children.append(text_block(line[2:], 15, "quote"))
            continue
        if line in {"---", "***", "___"}:
            children.append({"block_type": 22, "divider": {}})
            continue
        heading = len(line) - len(line.lstrip("#"))
        if 1 <= heading <= 6 and len(line) > heading and line[heading] == " ":
            children.append(text_block(line[heading + 1:], 2 + heading, "heading{}".format(heading)))
            continue
        ordered = re.match(r"^\d+\.\s+(.+)$", line)
        if ordered:
            children.append(text_block(ordered.group(1), 13, "ordered"))
            continue
        if line.startswith(("- ", "* ")):
            children.append(text_block(line[2:], 12, "bullet"))
            continue
        children.append(text_block(line))
    return children


FIELD_SPECS: Sequence[Tuple[str, int, str]] = (
    ("唯一键", 1, "Text"), ("数据类型", 1, "Text"), ("分析批次", 1, "Text"),
    ("市场", 1, "Text"), ("类目", 1, "Text"), ("产品形态", 1, "Text"),
    ("机会类型", 1, "Text"), ("置信度", 1, "Text"), ("用户任务", 1, "Text"),
    ("未满足需求", 1, "Text"), ("机会假设", 1, "Text"), ("必备规格", 1, "Text"),
    ("可选升级", 1, "Text"), ("规避项", 1, "Text"), ("检查项", 1, "Text"),
    ("支持信号", 1, "Text"), ("风险信号", 1, "Text"), ("数据限制", 1, "Text"),
    ("规格类型", 1, "Text"), ("规格/风险项", 1, "Text"), ("消费者原因", 1, "Text"),
    ("证据来源商品ID", 1, "Text"), ("原始VOC", 1, "Text"), ("原子证据ID", 1, "Text"),
    ("信号标签", 1, "Text"), ("正负向", 1, "Text"), ("属性组", 1, "Text"),
    ("报告链接", 1, "Text"), ("内容指纹", 1, "Text"), ("更新时间", 1, "Text"),
    ("证据商品数", 2, "Number"), ("证据数", 2, "Number"),
    ("来源批次数", 2, "Number"), ("最大单商品贡献", 2, "Number"),
)

CONFIDENCE_LABELS = {
    "direction_candidate": "P1｜方向候选",
    "emerging_opportunity": "P2｜新兴机会",
    "signal_only": "P3｜仅作信号",
}
OPPORTUNITY_TYPE_LABELS = {
    "pain_gap": "明确痛点缺口",
    "feature_upgrade": "功能/体验升级",
    "established_preference": "稳定消费者偏好",
}
SPEC_TYPE_LABELS = {
    "must_have": "必备规格", "optional": "可选升级", "avoid": "规避项",
    "inspection": "验收检查",
}
POLARITY_LABELS = {"positive": "正向", "negative": "负向", "mixed": "正负混合", "neutral": "中性"}
ASPECT_LABELS = {
    "appearance": "外观自然度", "color": "颜色色号", "material": "材质",
    "install": "佩戴安装", "hold": "固定稳定", "fit": "尺寸适配",
    "comfort": "舒适度", "usage": "使用体验", "durability": "耐用性",
    "value": "性价比", "trust": "复购信任", "fulfillment": "履约",
}

VIEW_VISIBLE_FIELDS: Dict[str, Sequence[str]] = {
    "01_机会总览": (
        "文本", "产品形态", "机会类型", "置信度", "用户任务", "未满足需求", "机会假设",
        "必备规格", "可选升级", "规避项", "检查项", "证据商品数", "证据数", "来源批次数",
        "最大单商品贡献", "数据限制", "报告链接",
    ),
    "02_规格与风险": (
        "文本", "规格类型", "产品形态", "置信度", "消费者原因", "证据数", "报告链接",
    ),
    "03_信号分析": (
        "文本", "产品形态", "信号标签", "置信度", "证据商品数", "证据数", "来源批次数",
        "最大单商品贡献", "报告链接",
    ),
    "04_原始证据": (
        "文本", "产品形态", "信号标签", "正负向", "属性组", "原始VOC", "证据来源商品ID", "报告链接",
    ),
}
LAYER_TABLE_SCHEMAS: Dict[str, Sequence[str]] = {
    "02_规格与风险": (
        "规格/风险项", "规格类型", "产品形态", "置信度", "消费者原因", "证据数",
        "报告链接", "分析批次", "唯一键", "更新时间",
    ),
    "03_信号分析": (
        "信号主题", "产品形态", "信号标签", "置信度", "证据商品数", "证据数",
        "来源批次数", "最大单商品贡献", "报告链接", "分析批次", "唯一键", "更新时间",
    ),
    "04_原始证据": (
        "证据主题", "产品形态", "信号标签", "正负向", "属性组", "原始VOC",
        "证据来源商品ID", "报告链接", "分析批次", "唯一键", "更新时间",
    ),
    "99_后台全量": ("文本",) + tuple(spec[0] for spec in FIELD_SPECS),
}
LAYER_DATA_TYPES = {
    "02_规格与风险": "规格风险", "03_信号分析": "信号聚合", "04_原始证据": "原子证据",
}
LAYER_PRIMARY_SOURCE = {"03_信号分析": "文本", "04_原始证据": "文本"}
NUMBER_FIELDS = {"证据商品数", "证据数", "来源批次数", "最大单商品贡献"}


def joined(value: Any) -> str:
    if isinstance(value, list):
        return "；".join(str(item) for item in value if str(item).strip())
    return str(value or "")


def display_maps(category_key: str) -> Tuple[Dict[str, str], Dict[str, str]]:
    path = VOC_INSIGHT_DIR / "references" / "{}_voc_taxonomy_v1.json".format(category_key)
    if not path.exists():
        return {}, {}
    payload = load_json(path)
    product_forms = {str(key): str(value) for key, value in (payload.get("product_forms") or {}).items()}
    signals = {
        str(key): str(value.get("label_zh") or key)
        for key, value in (payload.get("signals") or {}).items()
    }
    return product_forms, signals


def translated_joined(value: Any, labels: Dict[str, str]) -> str:
    values = value if isinstance(value, list) else [value]
    return "；".join(labels.get(str(item), str(item)) for item in values if str(item or "").strip())


def fingerprint(fields: Dict[str, Any]) -> str:
    clean = {key: value for key, value in fields.items() if key not in {"内容指纹", "更新时间"}}
    raw = json.dumps(clean, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()


def finalize_fields(fields: Dict[str, Any], generated_at: str, report_url: str) -> Dict[str, Any]:
    clean = {key: value for key, value in fields.items() if value not in (None, "", [], {})}
    clean["报告链接"] = report_url
    clean["更新时间"] = generated_at
    clean["内容指纹"] = fingerprint(clean)
    return clean


def build_table_rows(payload: Dict[str, Any], report_url: str) -> List[Dict[str, Any]]:
    batch_id = str(payload.get("batch_id") or "")
    market = str(payload.get("market") or "")
    category = str(payload.get("category_key") or "")
    generated_at = str(payload.get("generated_at") or "")
    product_form_labels, signal_labels = display_maps(category)
    rows: List[Dict[str, Any]] = []

    for card in payload.get("opportunity_cards") or []:
        metrics = card.get("metrics") or {}
        rows.append(finalize_fields({
            "文本": card.get("title_zh"), "唯一键": "opportunity:{}".format(card.get("opportunity_id")),
            "数据类型": "机会卡", "分析批次": batch_id, "市场": market, "类目": category,
            "产品形态": translated_joined(card.get("product_forms"), product_form_labels),
            "机会类型": OPPORTUNITY_TYPE_LABELS.get(str(card.get("opportunity_type")), card.get("opportunity_type")),
            "置信度": CONFIDENCE_LABELS.get(str(card.get("confidence")), card.get("confidence")), "用户任务": card.get("user_job"),
            "未满足需求": card.get("unmet_need"), "机会假设": card.get("opportunity_hypothesis"),
            "必备规格": joined(card.get("must_have_specs")), "可选升级": joined(card.get("optional_specs")),
            "规避项": joined(card.get("avoid_specs")), "检查项": joined(card.get("inspection_checks")),
            "支持信号": joined(card.get("supporting_signal_ids")), "风险信号": joined(card.get("contradicting_signal_ids")),
            "数据限制": joined(card.get("limitations")), "证据商品数": metrics.get("direct_product_count"),
            "证据数": metrics.get("direct_evidence_count"), "来源批次数": metrics.get("direct_batch_count"),
            "最大单商品贡献": metrics.get("max_product_contribution"),
        }, generated_at, report_url))

    for summary in payload.get("signal_summaries") or []:
        metrics = summary.get("metrics") or {}
        form_key = str(summary.get("product_form") or "")
        form = product_form_labels.get(form_key, form_key) if form_key else "跨形态"
        signal_key = str(summary.get("signal_tag") or "")
        signal_label = signal_labels.get(signal_key, signal_key)
        rows.append(finalize_fields({
            "文本": "信号｜{}｜{}".format(form, signal_label),
            "唯一键": "signal:{}:{}".format(batch_id, summary.get("signal_summary_id")),
            "数据类型": "信号聚合", "分析批次": batch_id, "市场": market, "类目": category,
            "产品形态": form, "置信度": CONFIDENCE_LABELS.get(str(summary.get("confidence")), summary.get("confidence")),
            "信号标签": signal_label,
            "证据商品数": metrics.get("direct_product_count"), "证据数": metrics.get("direct_evidence_count"),
            "来源批次数": metrics.get("direct_batch_count"), "最大单商品贡献": metrics.get("max_product_contribution"),
        }, generated_at, report_url))

    for item in payload.get("spec_risk_library") or []:
        rows.append(finalize_fields({
            "文本": "{}｜{}".format(item.get("item_type"), item.get("requirement_zh")),
            "唯一键": "spec:{}".format(item.get("item_id")), "数据类型": "规格风险",
            "分析批次": batch_id, "市场": market, "类目": category,
            "产品形态": translated_joined(item.get("product_forms"), product_form_labels) or "跨形态",
            "置信度": CONFIDENCE_LABELS.get(str(item.get("confidence")), item.get("confidence")),
            "规格类型": SPEC_TYPE_LABELS.get(str(item.get("item_type")), item.get("item_type")),
            "规格/风险项": item.get("requirement_zh"),
            "消费者原因": item.get("consumer_reason"), "证据数": len(item.get("evidence_refs") or []),
        }, generated_at, report_url))

    for evidence in payload.get("atomic_evidence") or []:
        signal_key = str(evidence.get("signal_tag") or "")
        signal_label = signal_labels.get(signal_key, signal_key)
        rows.append(finalize_fields({
            "文本": "证据｜{}｜{}".format(signal_label, evidence.get("product_id")),
            "唯一键": "atomic:{}".format(evidence.get("atomic_evidence_id")), "数据类型": "原子证据",
            "分析批次": batch_id, "市场": market, "类目": category,
            "产品形态": evidence.get("product_form_label") or evidence.get("product_form"),
            "证据来源商品ID": evidence.get("product_id"), "原始VOC": evidence.get("source_text"),
            "原子证据ID": evidence.get("atomic_evidence_id"), "信号标签": signal_label,
            "正负向": POLARITY_LABELS.get(str(evidence.get("polarity")), evidence.get("polarity")),
            "属性组": ASPECT_LABELS.get(str(evidence.get("aspect_group")), evidence.get("aspect_group")),
        }, generated_at, report_url))
    return rows


def configure_main_view(
    client: FeishuClient, app_token: str, table_id: str, default_view_id: str, dry_run: bool,
) -> Dict[str, Any]:
    fields = client.list_fields(app_token, table_id)
    field_ids = {str(item.get("field_name") or ""): str(item.get("field_id") or "") for item in fields}
    visible = set(VIEW_VISIBLE_FIELDS["01_机会总览"])
    hidden = [field_id for field_name, field_id in field_ids.items() if field_name not in visible and field_id]
    if not dry_run:
        client.patch_table_name(app_token, table_id, "01_VOC机会看板")
        if default_view_id:
            client.patch_view(app_token, table_id, default_view_id, "01_机会总览", hidden)
    return {"view_id": default_view_id, "visible_field_count": len(fields) - len(hidden)}


def field_spec(name: str) -> Tuple[str, int, str]:
    return (name, 2, "Number") if name in NUMBER_FIELDS else (name, 1, "Text")


def build_layer_rows(rows: Sequence[Dict[str, Any]], table_name: str) -> List[Dict[str, Any]]:
    names = LAYER_TABLE_SCHEMAS[table_name]
    data_type = LAYER_DATA_TYPES.get(table_name)
    selected = rows if not data_type else [row for row in rows if row.get("数据类型") == data_type]
    result: List[Dict[str, Any]] = []
    for row in selected:
        target: Dict[str, Any] = {}
        for name in names:
            source = LAYER_PRIMARY_SOURCE.get(table_name) if name == names[0] else name
            value = row.get(source or name)
            if value not in (None, "", [], {}):
                target[name] = value
        result.append(target)
    return result


def ensure_table_fields(
    client: FeishuClient, app_token: str, table_id: str, names: Sequence[str], dry_run: bool,
) -> List[str]:
    existing = {str(item.get("field_name") or "") for item in client.list_fields(app_token, table_id)}
    missing = [name for name in names if name not in existing]
    if not dry_run:
        for name in missing:
            client.create_field(app_token, table_id, *field_spec(name))
            time.sleep(0.2)
    return missing


def ensure_layered_tables(
    client: FeishuClient, app_token: str, wiki_token: str, rows: Sequence[Dict[str, Any]], dry_run: bool,
) -> Dict[str, Any]:
    existing = {str(item.get("name") or ""): str(item.get("table_id") or "") for item in client.list_tables(app_token)}
    result: Dict[str, Any] = {}
    for table_name, names in LAYER_TABLE_SCHEMAS.items():
        table_id = existing.get(table_name, "")
        view_id = ""
        action = "update" if table_id else "create"
        layer_rows = build_layer_rows(rows, table_name)
        if not dry_run:
            if not table_id:
                table_id, view_id = client.create_table(app_token, table_name, [field_spec(name) for name in names])
                time.sleep(0.8)
            missing = ensure_table_fields(client, app_token, table_id, names, False)
            sync = sync_rows(client, app_token, table_id, layer_rows, False)
            prune = prune_records_not_in_keys(client, app_token, table_id, layer_rows, False)
            count = len([record for record in client.list_records(app_token, table_id) if not is_blank_record(record)])
            if count != len(layer_rows):
                raise FeishuError("{} 写入后数量异常: expected={} actual={}".format(table_name, len(layer_rows), count))
        else:
            missing = [] if not table_id else ensure_table_fields(client, app_token, table_id, names, True)
            sync = {"planned": len(layer_rows)}
            prune = {"planned": 0}
            count = len(layer_rows)
        query = "table={}".format(table_id) + ("&view={}".format(view_id) if view_id else "")
        result[table_name] = {
            "action": action, "table_id": table_id, "view_id": view_id, "rows": count,
            "missing_fields": missing, "sync": sync, "prune": prune,
            "url": "{}/wiki/{}?{}".format(TENANT_BASE, wiki_token, query) if table_id else "DRY_RUN",
        }
    return result


def cleanup_main_table(
    client: FeishuClient, app_token: str, table_id: str,
    keep_rows: Sequence[Dict[str, Any]], dry_run: bool,
) -> Dict[str, int]:
    return prune_records_not_in_keys(client, app_token, table_id, keep_rows, dry_run)


def ensure_fields(client: FeishuClient, app_token: str, table_id: str, dry_run: bool) -> List[str]:
    existing = {str(item.get("field_name") or "") for item in client.list_fields(app_token, table_id)}
    missing = [spec for spec in FIELD_SPECS if spec[0] not in existing]
    if not dry_run:
        for name, field_type, ui_type in missing:
            client.create_field(app_token, table_id, name, field_type, ui_type)
            time.sleep(0.15)
    return [item[0] for item in missing]


def is_blank_record(record: Dict[str, Any]) -> bool:
    fields = record.get("fields") or {}
    return not any(value not in (None, "", [], {}) for value in fields.values())


def sync_rows(client: FeishuClient, app_token: str, table_id: str, rows: Sequence[Dict[str, Any]], dry_run: bool) -> Dict[str, int]:
    existing = client.list_records(app_token, table_id)
    by_key = {
        str((record.get("fields") or {}).get("唯一键") or ""): record
        for record in existing if (record.get("fields") or {}).get("唯一键")
    }
    blank = [record for record in existing if is_blank_record(record)]
    updates: List[Dict[str, Any]] = []
    creates: List[Dict[str, Any]] = []
    skipped = 0
    for fields in rows:
        key = str(fields.get("唯一键") or "")
        current = by_key.get(key)
        if current:
            if str((current.get("fields") or {}).get("内容指纹") or "") == fields.get("内容指纹"):
                skipped += 1
            else:
                updates.append({"record_id": current["record_id"], "fields": fields})
        elif blank:
            target = blank.pop(0)
            updates.append({"record_id": target["record_id"], "fields": fields})
        else:
            creates.append({"fields": fields})
    if not dry_run:
        client.batch_update_records(app_token, table_id, updates)
        client.batch_create_records(app_token, table_id, creates)
    return {"existing": len(existing), "updated": len(updates), "created": len(creates), "skipped": skipped}


def prune_records_not_in_keys(
    client: FeishuClient, app_token: str, table_id: str,
    keep_rows: Sequence[Dict[str, Any]], dry_run: bool,
) -> Dict[str, int]:
    """Keep only the current published snapshot so decision views do not mix batches."""
    keep_keys = {str(row.get("唯一键") or "") for row in keep_rows if row.get("唯一键")}
    records = client.list_records(app_token, table_id)
    delete_ids: List[str] = []
    for record in records:
        fields = record.get("fields") or {}
        key = str(fields.get("唯一键") or "")
        if is_blank_record(record) or not key or key not in keep_keys:
            record_id = str(record.get("record_id") or "")
            if record_id:
                delete_ids.append(record_id)
    if not dry_run:
        client.batch_delete_records(app_token, table_id, delete_ids)
    return {
        "before": len(records), "deleted": len(delete_ids),
        "remaining": len(records) - len(delete_ids), "kept_keys": len(keep_keys),
    }


def default_report_title(payload: Dict[str, Any]) -> str:
    market = str(payload.get("market") or "")
    category = "假发" if payload.get("category_key") == "wigs" else str(payload.get("category_key") or "")
    date = str(payload.get("generated_at") or "")[:10]
    return "VOC产品机会报告｜{} {}｜{}".format(market, category, date)


def verify_counts(client: FeishuClient, app_token: str, table_id: str, batch_id: str) -> Dict[str, int]:
    records = client.list_records(app_token, table_id)
    counts: Dict[str, int] = {}
    keys = set()
    duplicate_keys = 0
    for record in records:
        fields = record.get("fields") or {}
        if str(fields.get("分析批次") or "") != batch_id:
            continue
        data_type = str(fields.get("数据类型") or "")
        if data_type:
            counts[data_type] = counts.get(data_type, 0) + 1
        key = str(fields.get("唯一键") or "")
        if key in keys:
            duplicate_keys += 1
        elif key:
            keys.add(key)
    counts["duplicate_keys"] = duplicate_keys
    counts["unique_keys"] = len(keys)
    return counts


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Sync VOC opportunity artifacts to Feishu")
    parser.add_argument("--result-json", required=True)
    parser.add_argument("--report-markdown", required=True)
    parser.add_argument("--bitable-url", default=DEFAULT_BITABLE_URL)
    parser.add_argument("--report-parent-wiki-url", default=DEFAULT_REPORT_PARENT_URL)
    parser.add_argument("--report-title", default="")
    parser.add_argument("--reuse-report-url", default="")
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--pretty", action="store_true")
    return parser


def main() -> int:
    args = build_parser().parse_args()
    payload = load_json(Path(args.result_json))
    if not ((payload.get("quality_gate") or {}).get("passed")):
        raise SystemExit("VOC opportunity quality gate did not pass")
    markdown = Path(args.report_markdown).read_text(encoding="utf-8")
    title = args.report_title or default_report_title(payload)
    wiki_token, table_id = parse_bitable_url(args.bitable_url)
    default_view_id = parse_view_id(args.bitable_url)
    report_parent = parse_wiki_url(args.report_parent_wiki_url)
    client = FeishuClient()
    app_token = client.resolve_bitable(wiki_token)
    missing_fields = ensure_fields(client, app_token, table_id, args.dry_run)

    if args.reuse_report_url:
        report = {"title": title, "url": args.reuse_report_url, "location": "reused"}
        doc_write = {"block_count": 0, "batch_count": 0, "reused": True}
    elif args.dry_run:
        report = {"title": title, "url": "DRY_RUN"}
        doc_write = {"block_count": len(markdown_children(markdown)), "batch_count": 0}
    else:
        try:
            report = client.create_wiki_doc(report_parent, title)
        except FeishuError as exc:
            if "code=131006" not in str(exc):
                raise
            report = client.create_cloud_doc(title)
        doc_write = client.append_markdown(report["document_id"], markdown)

    rows = build_table_rows(payload, report["url"])
    opportunity_rows = [row for row in rows if row.get("数据类型") == "机会卡"]
    table_sync = sync_rows(client, app_token, table_id, opportunity_rows, args.dry_run)
    main_view = configure_main_view(client, app_token, table_id, default_view_id, args.dry_run)
    layered_tables = ensure_layered_tables(client, app_token, wiki_token, rows, args.dry_run)
    cleanup = cleanup_main_table(client, app_token, table_id, opportunity_rows, args.dry_run)
    verification = {} if args.dry_run else verify_counts(client, app_token, table_id, str(payload.get("batch_id") or ""))
    result = {
        "success": True, "dry_run": args.dry_run, "batch_id": payload.get("batch_id"),
        "report": report, "document_write": doc_write, "missing_fields_created": missing_fields,
        "planned_rows": len(rows), "row_type_counts": {
            "机会卡": len(payload.get("opportunity_cards") or []),
            "信号聚合": len(payload.get("signal_summaries") or []),
            "规格风险": len(payload.get("spec_risk_library") or []),
            "原子证据": len(payload.get("atomic_evidence") or []),
        },
        "table_sync": table_sync, "verification": verification,
        "main_view": main_view, "layered_tables": layered_tables, "main_cleanup": cleanup,
        "bitable_url": args.bitable_url,
    }
    print(json.dumps(result, ensure_ascii=False, indent=2 if args.pretty else None))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
