#!/usr/bin/env python3
"""Standalone hair accessory style review job."""

from __future__ import annotations

import argparse
import importlib.util
import json
import sys
import tempfile
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Sequence
from zoneinfo import ZoneInfo

import requests

SKILL_DIR = Path(__file__).resolve().parent
if str(SKILL_DIR) not in sys.path:
    sys.path.insert(0, str(SKILL_DIR))

REPO_ROOT = Path(__file__).resolve().parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.append(str(REPO_ROOT))

from workspace_support import load_repo_env

load_repo_env()

from config import (
    BASIC_INFO_FIELD_ALIASES,
    CANDIDATE_FIELD_ALIASES,
    DEFAULT_FEISHU_URL,
    DEFAULT_MAX_TOKENS,
    DEFAULT_MAX_WORKERS,
    DEFAULT_TIMEZONE,
    IMAGE_FIELD_ALIASES,
    OUTPUT_REASON_FIELD,
    OUTPUT_RECOMMEND_FIELD,
    OUTPUT_STYLE_FIELD,
    RECOMMENDED_STYLE_OPTIONS,
    STATUS_ERROR_FIELD,
    STATUS_FIELD,
    STATUS_TIME_FIELD,
    TITLE_FIELD_ALIASES,
)
from core.feishu import (
    FeishuAPIError,
    FeishuBitableClient,
    TableRecord,
    parse_feishu_bitable_url,
    resolve_bitable_app_token,
)

OUTPUT_DIR = SKILL_DIR / "output"
CREATOR_CRM_DIR = REPO_ROOT / "skills" / "creator-crm"


@dataclass
class ProcessorOptions:
    feishu_url: str = DEFAULT_FEISHU_URL
    dry_run: bool = False
    force: bool = False
    limit: Optional[int] = None
    record_ids: Optional[List[str]] = None
    timezone_name: str = DEFAULT_TIMEZONE
    max_workers: int = DEFAULT_MAX_WORKERS
    max_tokens: int = DEFAULT_MAX_TOKENS


def normalize_cell_value(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, str):
        return value.strip()
    if isinstance(value, (int, float)):
        if isinstance(value, float) and value.is_integer():
            return str(int(value))
        return str(value)
    if isinstance(value, dict):
        for key in ("text", "name", "url", "link"):
            cell = value.get(key)
            if isinstance(cell, str) and cell.strip():
                return cell.strip()
        return json.dumps(value, ensure_ascii=False)
    if isinstance(value, list):
        parts = [normalize_cell_value(item) for item in value]
        return " / ".join(part for part in parts if part)
    return str(value).strip()


def is_yes_value(value: Any) -> bool:
    normalized = normalize_cell_value(value).lower()
    return normalized in {"是", "yes", "true", "1", "y"}


def is_empty(value: Any) -> bool:
    return normalize_cell_value(value) == ""


def extract_json_object(text: str) -> Dict[str, Any]:
    content = text.strip()
    if content.startswith("```"):
        lines = content.splitlines()
        if lines and lines[0].startswith("```"):
            lines = lines[1:]
        if lines and lines[-1].startswith("```"):
            lines = lines[:-1]
        content = "\n".join(lines).strip()
    start = content.find("{")
    end = content.rfind("}")
    if start >= 0 and end >= start:
        content = content[start : end + 1]
    return json.loads(content)


def load_creator_crm_llm_objects():
    creator_crm_path = str(CREATOR_CRM_DIR)
    if creator_crm_path not in sys.path:
        sys.path.insert(0, creator_crm_path)
    spec = importlib.util.spec_from_file_location(
        "creator_crm_model_runtime_config",
        CREATOR_CRM_DIR / "core" / "model_runtime_config.py",
    )
    if spec is None or spec.loader is None:
        raise RuntimeError("无法加载 creator_crm/core/model_runtime_config.py")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return {
        "LLM_API_URL": module.LLM_API_URL,
        "LLM_API_KEY": module.LLM_API_KEY,
        "LLM_MODEL": module.LLM_MODEL,
        "DEFAULT_VISION_MODELS": module.DEFAULT_VISION_MODELS,
    }


CREATOR_CRM_LLM = load_creator_crm_llm_objects()


class StyleAnalyzer:
    def __init__(self, max_tokens: int):
        self.max_tokens = max_tokens
        self.api_url = CREATOR_CRM_LLM["LLM_API_URL"]
        self.api_key = CREATOR_CRM_LLM["LLM_API_KEY"]
        self.model = CREATOR_CRM_LLM["LLM_MODEL"]
        self.fallback_models = CREATOR_CRM_LLM["DEFAULT_VISION_MODELS"]

    def analyze(
        self,
        title: str,
        image_url: Optional[str],
        basic_info: str,
    ) -> Dict[str, str]:
        prompt = build_style_analysis_prompt(
            product_title=title,
            image_url=image_url,
            basic_info=basic_info,
        )
        if image_url:
            response_text = self._analyze_with_image(prompt, image_url)
        else:
            response_text = self._analyze_text_only(prompt)

        return parse_style_analysis_result(response_text, text_only=not bool(image_url))

    def _analyze_with_image(self, prompt: str, image_url: str) -> str:
        with tempfile.TemporaryDirectory(prefix="hair-style-review-") as temp_dir:
            temp_dir_path = Path(temp_dir)
            raw_destination = temp_dir_path / "input_image"
            response = requests.get(image_url, timeout=60)
            response.raise_for_status()
            content_type = response.headers.get("Content-Type", "image/jpeg").split(";")[0].strip()
            extension = {
                "image/jpeg": ".jpg",
                "image/jpg": ".jpg",
                "image/png": ".png",
                "image/webp": ".webp",
            }.get(content_type, ".jpg")
            local_path = raw_destination.with_suffix(extension)
            local_path.write_bytes(response.content)

            image_bytes = local_path.read_bytes()
        return self._call_visual_chat(
            prompt=prompt,
            image_bytes=image_bytes,
            content_type=content_type,
        )

    def _analyze_text_only(self, prompt: str) -> str:
        url = f"{self.api_url.rstrip('/')}/chat/completions"
        headers = {
            "Authorization": f"Bearer {self.api_key}",
            "Content-Type": "application/json",
        }
        payload = {
            "model": self.model,
            "messages": [
                {"role": "user", "content": prompt},
            ],
            "max_tokens": self.max_tokens,
        }
        response = requests.post(url, headers=headers, json=payload, timeout=120)
        if response.status_code != 200:
            raise RuntimeError(f"文本分析调用失败: HTTP {response.status_code} - {response.text[:400]}")
        return self._extract_response_text(response.json())

    def _call_visual_chat(self, prompt: str, image_bytes: bytes, content_type: str) -> str:
        import base64

        image_base64 = base64.b64encode(image_bytes).decode("utf-8")
        models_to_try: List[str] = []
        if self.model:
            models_to_try.append(self.model)
        for model in self.fallback_models:
            if model not in models_to_try:
                models_to_try.append(model)

        last_error: Optional[Exception] = None
        for model in models_to_try:
            try:
                response_json = self._call_openai_compatible_image_api(
                    prompt=prompt,
                    model=model,
                    image_base64=image_base64,
                    content_type=content_type,
                )
                return self._extract_response_text(response_json)
            except Exception as exc:
                last_error = exc
                error_text = str(exc).lower()
                if (
                    "no available channels" in error_text
                    or "not found" in error_text
                    or "502" in error_text
                    or "503" in error_text
                    or "timeout" in error_text
                    or "超时" in error_text
                ):
                    continue
                raise
        raise RuntimeError(f"所有模型都不可用，最后错误: {last_error}")

    def _call_openai_compatible_image_api(
        self,
        prompt: str,
        model: str,
        image_base64: str,
        content_type: str,
    ) -> Dict[str, Any]:
        url = f"{self.api_url.rstrip('/')}/chat/completions"
        headers = {
            "Authorization": f"Bearer {self.api_key}",
            "Content-Type": "application/json",
        }
        payload = {
            "model": model,
            "messages": [
                {
                    "role": "user",
                    "content": [
                        {
                            "type": "image_url",
                            "image_url": {
                                "url": f"data:{content_type};base64,{image_base64}",
                            },
                        },
                        {
                            "type": "text",
                            "text": prompt,
                        },
                    ],
                }
            ],
            "max_tokens": self.max_tokens,
        }
        response = requests.post(url, headers=headers, json=payload, timeout=120)
        if response.status_code != 200:
            raise RuntimeError(f"图片分析调用失败: HTTP {response.status_code} - {response.text[:400]}")
        return response.json()

    def _extract_response_text(self, result: Dict[str, Any]) -> str:
        choices = result.get("choices") or []
        if not choices:
            raise RuntimeError(f"模型响应缺少 choices: {json.dumps(result, ensure_ascii=False)[:500]}")
        content = choices[0].get("message", {}).get("content")
        if not isinstance(content, str) or not content.strip():
            raise RuntimeError(f"模型响应缺少 message.content: {json.dumps(result, ensure_ascii=False)[:500]}")
        return content.strip()


def build_style_analysis_prompt(product_title: str, image_url: Optional[str], basic_info: str) -> str:
    styles = "\n".join(f"- {name}" for name in RECOMMENDED_STYLE_OPTIONS)
    return f"""你是一个发夹选品风格分析助手。

任务：
根据我提供的【商品标题】【商品图片】【商品基础信息】，判断该商品的发夹子类、主风格，并判断是否推荐纳入当前发夹首发备选池。

当前重点方向：
1. 轻韩系基础造型抓夹
2. 功能结构型抓夹
3. 简约韩系装饰夹
4. 轻韩系甜美发夹（仅适合作为测试池，不宜过重）

非重点方向：
- 儿童卡通风
- IP感明显风格
- 重甜礼物风
- 公主拍照风
- 过度夸张装饰风
- 低价杂货套组风

请先在内部判断该商品更接近哪一类：
- 抓夹类
- 小发卡/刘海夹/边夹类
- 装饰发夹类
- 儿童/卡通小夹类
- 其他发夹类

风格判断标准：
1. 轻韩系基础温柔风：
配色柔和，常见米白、奶油色、浅咖、雾粉、透明茶色；轮廓圆润；装饰少；整体干净、温柔、日常、好搭。

2. 韩系功能风：
偏实用整理型，常见3齿/5齿/6齿、小中号抓夹、半扎/盘发/高马尾适用结构；重点是方便、稳、日常使用。

3. 轻韩系甜美风：
整体偏韩系，带少量蝴蝶结、小花朵、轻纱、软糯布艺等元素；甜但不过分，不明显儿童化。

4. 简约通勤风：
配色克制，线条简洁利落，装饰少，整体低调、成熟、百搭。

5. 轻精致风：
有少量金属、少量钻、少量珍珠、小亮点细节；整体精致但不浮夸。

允许输出的产品风格只能从以下列表中选 1 个：
{styles}

推荐标准：
- 推荐：适合当前首发备选池，属于当前重点方向，或虽非主推但适合进入测试池。
- 不推荐：不适合当前首发备选池，明显偏儿童、卡通、礼物、公主、夸张、低价杂货感，或受众过窄。

判断原则：
- 优先看图片整体视觉，不要只看标题
- 只判断一个最接近的主风格
- 如果明显偏儿童、卡通、礼物感、拍照感，应优先判为非重点方向
- 是否推荐不是只看好不好看，而是看是否适合纳入当前首发备选池
- 如果没有图片，仅可基于标题和基础信息判断，并在详细原因中明确说明准确度有限

请严格输出 JSON，不要输出任何额外说明：
{{
  "product_style": "",
  "is_recommended": "",
  "detailed_reason": ""
}}

输入信息：
商品标题：{product_title or "无"}
商品图片：{image_url or "无图片，仅文字分析"}
商品基础信息：{basic_info or ""}
"""


def parse_style_analysis_result(response_text: str, text_only: bool = False) -> Dict[str, str]:
    try:
        data = extract_json_object(response_text)
    except Exception as exc:
        raise RuntimeError(f"模型返回非标准 JSON: {exc}") from exc

    product_style = str(data.get("product_style", "")).strip()
    is_recommended = str(data.get("is_recommended", "")).strip()
    detailed_reason = str(data.get("detailed_reason", "")).strip()

    if product_style not in RECOMMENDED_STYLE_OPTIONS:
        raise RuntimeError(f"非法产品风格: {product_style}")
    if is_recommended not in {"推荐", "不推荐"}:
        raise RuntimeError(f"非法推荐结果: {is_recommended}")
    if not detailed_reason:
        raise RuntimeError("缺少详细原因")

    if text_only and "仅" not in detailed_reason and "标题" not in detailed_reason:
        detailed_reason = f"{detailed_reason}（仅依据标题/文本判断，准确度有限）"

    return {
        "product_style": product_style,
        "is_recommended": is_recommended,
        "detailed_reason": detailed_reason,
    }


def resolve_field_name(field_names: Iterable[str], aliases: Sequence[str]) -> Optional[str]:
    existing = set(field_names)
    for alias in aliases:
        if alias in existing:
            return alias
    return None


class HairStyleReviewJob:
    def __init__(self, options: ProcessorOptions):
        self.options = options
        self.analyzer = StyleAnalyzer(max_tokens=options.max_tokens)
        self.now = datetime.now(ZoneInfo(options.timezone_name))

    def run(self) -> Dict[str, Any]:
        info = parse_feishu_bitable_url(self.options.feishu_url)
        if info is None:
            raise ValueError(f"无法解析飞书表格链接: {self.options.feishu_url}")
        app_token = resolve_bitable_app_token(info)
        client = FeishuBitableClient(app_token=app_token, table_id=info.table_id)

        fields = client.list_fields()
        field_names = [field.field_name for field in fields]
        mapping = self._resolve_mapping(field_names)

        self._ensure_output_fields(client, field_names)
        field_names = [field.field_name for field in client.list_fields()]
        mapping = self._resolve_mapping(field_names)

        candidate_field = mapping["candidate"]
        title_field = mapping["title"]
        image_field = mapping["image"]
        if not title_field and not image_field:
            raise ValueError("表格至少需要标题或图片字段之一")

        records = client.list_records()
        if self.options.record_ids:
            target_ids = set(self.options.record_ids)
            records = [record for record in records if record.record_id in target_ids]

        candidates = self.fetch_candidate_records(client, records, mapping, limit=self.options.limit)

        preview: List[Dict[str, Any]] = []
        errors: List[Dict[str, str]] = []
        processed = 0
        skipped = len(records) - len(candidates)

        if not candidates:
            summary = {
                "success": True,
                "error": None,
                "data": {
                    "message": "没有找到待分析记录",
                    "stats": {
                        "total_records": len(records),
                        "candidate_records": 0,
                        "processed_records": 0,
                        "skipped_records": skipped,
                        "error_records": 0,
                        "max_workers": self.options.max_workers,
                    },
                    "summary_file": self._write_summary_file(
                        {
                            "success": True,
                            "data": {
                                "message": "没有找到待分析记录",
                                "stats": {
                                    "total_records": len(records),
                                    "candidate_records": 0,
                                    "processed_records": 0,
                                    "skipped_records": skipped,
                                    "error_records": 0,
                                    "max_workers": self.options.max_workers,
                                },
                            },
                        }
                    ),
                },
            }
            return summary

        with ThreadPoolExecutor(max_workers=self.options.max_workers) as executor:
            future_map = {
                executor.submit(self._handle_record, app_token, info.table_id, mapping, record): record
                for record in candidates
            }
            for future in as_completed(future_map):
                record = future_map[future]
                try:
                    result = future.result()
                    processed += 1
                    preview.append(
                        {
                            "record_id": record.record_id,
                            "title": normalize_cell_value(record.fields.get(mapping["title"])) if mapping["title"] else "",
                            "result": result,
                        }
                    )
                except Exception as exc:
                    errors.append({"record_id": record.record_id, "error": str(exc)})
                    self._write_back_error(client, mapping, record.record_id, str(exc))

        summary = {
            "success": len(errors) == 0,
            "error": None if not errors else f"部分记录处理失败，共 {len(errors)} 条",
            "data": {
                "feishu_url": self.options.feishu_url,
                "table_id": info.table_id,
                "dry_run": self.options.dry_run,
                "stats": {
                    "total_records": len(records),
                    "candidate_records": len(candidates),
                    "processed_records": processed,
                    "skipped_records": skipped,
                    "error_records": len(errors),
                    "max_workers": self.options.max_workers,
                },
                "preview": preview[:20],
                "errors": errors,
            },
        }
        summary["data"]["summary_file"] = self._write_summary_file(summary)
        return summary

    def _resolve_mapping(self, field_names: Sequence[str]) -> Dict[str, Optional[str]]:
        return {
            "title": resolve_field_name(field_names, TITLE_FIELD_ALIASES),
            "image": resolve_field_name(field_names, IMAGE_FIELD_ALIASES),
            "basic_info": resolve_field_name(field_names, BASIC_INFO_FIELD_ALIASES),
            "candidate": resolve_field_name(field_names, CANDIDATE_FIELD_ALIASES),
            "product_style": OUTPUT_STYLE_FIELD if OUTPUT_STYLE_FIELD in field_names else None,
            "is_recommended": OUTPUT_RECOMMEND_FIELD if OUTPUT_RECOMMEND_FIELD in field_names else None,
            "detailed_reason": OUTPUT_REASON_FIELD if OUTPUT_REASON_FIELD in field_names else None,
            "status": STATUS_FIELD if STATUS_FIELD in field_names else None,
            "status_time": STATUS_TIME_FIELD if STATUS_TIME_FIELD in field_names else None,
            "status_error": STATUS_ERROR_FIELD if STATUS_ERROR_FIELD in field_names else None,
        }

    def _ensure_output_fields(self, client: FeishuBitableClient, field_names: Sequence[str]) -> None:
        missing = [
            field_name
            for field_name in (OUTPUT_STYLE_FIELD, OUTPUT_RECOMMEND_FIELD, OUTPUT_REASON_FIELD)
            if field_name not in set(field_names)
        ]
        for field_name in missing:
            if self.options.dry_run:
                continue
            client.create_field(field_name=field_name, field_type=1, ui_type="Text")

    def fetch_candidate_records(
        self,
        client: FeishuBitableClient,
        records: Sequence[TableRecord],
        mapping: Dict[str, Optional[str]],
        limit: Optional[int] = None,
    ) -> List[TableRecord]:
        selected: List[TableRecord] = []
        for record in records:
            fields = record.fields
            candidate_field = mapping["candidate"]
            if candidate_field and not is_yes_value(fields.get(candidate_field)):
                continue

            if not self.options.force:
                if (
                    not is_empty(fields.get(mapping["product_style"]))
                    or not is_empty(fields.get(mapping["is_recommended"]))
                    or not is_empty(fields.get(mapping["detailed_reason"]))
                ):
                    continue

            title = normalize_cell_value(fields.get(mapping["title"])) if mapping["title"] else ""
            image_url = None
            if mapping["image"]:
                image_url = client.resolve_image_url(fields.get(mapping["image"]))
            basic_info = normalize_cell_value(fields.get(mapping["basic_info"])) if mapping["basic_info"] else ""

            if not title and not image_url and not basic_info:
                continue

            selected.append(record)
            if limit is not None and len(selected) >= limit:
                break
        return selected

    def _handle_record(
        self,
        app_token: str,
        table_id: str,
        mapping: Dict[str, Optional[str]],
        record: TableRecord,
    ) -> Dict[str, str]:
        client = FeishuBitableClient(app_token=app_token, table_id=table_id)
        if mapping["status"]:
            self._write_status(client, mapping, record.record_id, "分析中", None)

        title = normalize_cell_value(record.fields.get(mapping["title"])) if mapping["title"] else ""
        image_url = client.resolve_image_url(record.fields.get(mapping["image"])) if mapping["image"] else None
        basic_info = normalize_cell_value(record.fields.get(mapping["basic_info"])) if mapping["basic_info"] else ""

        result = self.analyzer.analyze(
            title=title,
            image_url=image_url,
            basic_info=basic_info,
        )
        self.write_back_style_result(client, mapping, record.record_id, result)
        return result

    def write_back_style_result(
        self,
        client: FeishuBitableClient,
        mapping: Dict[str, Optional[str]],
        record_id: str,
        result: Dict[str, str],
    ) -> None:
        fields = {
            OUTPUT_STYLE_FIELD: result["product_style"],
            OUTPUT_RECOMMEND_FIELD: result["is_recommended"],
            OUTPUT_REASON_FIELD: result["detailed_reason"],
        }
        if mapping["status"]:
            fields[mapping["status"]] = "已完成"
        if mapping["status_time"]:
            fields[mapping["status_time"]] = int(self.now.timestamp() * 1000)
        if mapping["status_error"]:
            fields[mapping["status_error"]] = ""
        if not self.options.dry_run:
            client.update_record_fields(record_id, fields)

    def _write_back_error(
        self,
        client: FeishuBitableClient,
        mapping: Dict[str, Optional[str]],
        record_id: str,
        error_message: str,
    ) -> None:
        updates: Dict[str, Any] = {}
        if mapping["status"]:
            updates[mapping["status"]] = "失败"
        if mapping["status_error"]:
            updates[mapping["status_error"]] = error_message[:500]
        if mapping["status_time"]:
            updates[mapping["status_time"]] = int(datetime.now(ZoneInfo(self.options.timezone_name)).timestamp() * 1000)
        if updates and not self.options.dry_run:
            client.update_record_fields(record_id, updates)

    def _write_status(
        self,
        client: FeishuBitableClient,
        mapping: Dict[str, Optional[str]],
        record_id: str,
        status: str,
        error_message: Optional[str],
    ) -> None:
        updates: Dict[str, Any] = {}
        if mapping["status"]:
            updates[mapping["status"]] = status
        if mapping["status_error"] is not None:
            updates[mapping["status_error"]] = error_message or ""
        if updates and not self.options.dry_run:
            client.update_record_fields(record_id, updates)

    def _write_summary_file(self, summary: Dict[str, Any]) -> str:
        OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
        path = OUTPUT_DIR / f"run_{datetime.now(ZoneInfo(self.options.timezone_name)).strftime('%Y%m%d_%H%M%S')}.json"
        path.write_text(json.dumps(summary, ensure_ascii=False, indent=2), encoding="utf-8")
        return str(path)


def run_style_analysis_job(
    feishu_url: str = DEFAULT_FEISHU_URL,
    record_ids: Optional[Sequence[str]] = None,
    dry_run: bool = False,
    force: bool = False,
    limit: Optional[int] = None,
) -> Dict[str, Any]:
    options = ProcessorOptions(
        feishu_url=feishu_url,
        record_ids=list(record_ids) if record_ids else None,
        dry_run=dry_run,
        force=force,
        limit=limit,
    )
    return HairStyleReviewJob(options).run()


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Standalone hair style review job.")
    parser.add_argument("--feishu-url", default=DEFAULT_FEISHU_URL)
    parser.add_argument("--record-id", action="append", dest="record_ids")
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--force", action="store_true")
    parser.add_argument("--limit", type=int)
    return parser


def main() -> None:
    parser = build_parser()
    args = parser.parse_args()
    result = run_style_analysis_job(
        feishu_url=args.feishu_url,
        record_ids=args.record_ids,
        dry_run=args.dry_run,
        force=args.force,
        limit=args.limit,
    )
    print(json.dumps(result, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
