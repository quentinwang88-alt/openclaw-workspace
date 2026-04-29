#!/usr/bin/env python3
"""宽表脚本拆分、脚本 ID 生成与标题生成。"""

from __future__ import annotations

import importlib.util
import re
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Callable, Dict, Iterable, List, Optional, Sequence, Tuple

from app.models import ScriptMetadata


SCRIPT_FIELD_SPECS: List[Dict[str, Any]] = [
    {"logical_name": "script_s1", "task_suffix": "S1", "direction_index": 1, "variant_no": None, "aliases": ["脚本方向一", "脚本_S1", "脚本S1"]},
    {"logical_name": "script_s1_v1", "task_suffix": "S1V1", "direction_index": 1, "variant_no": 1, "aliases": ["脚本1变体1"]},
    {"logical_name": "script_s1_v2", "task_suffix": "S1V2", "direction_index": 1, "variant_no": 2, "aliases": ["脚本1变体2"]},
    {"logical_name": "script_s1_v3", "task_suffix": "S1V3", "direction_index": 1, "variant_no": 3, "aliases": ["脚本1变体3"]},
    {"logical_name": "script_s1_v4", "task_suffix": "S1V4", "direction_index": 1, "variant_no": 4, "aliases": ["脚本1变体4"]},
    {"logical_name": "script_s1_v5", "task_suffix": "S1V5", "direction_index": 1, "variant_no": 5, "aliases": ["脚本1变体5"]},
    {"logical_name": "script_s2", "task_suffix": "S2", "direction_index": 2, "variant_no": None, "aliases": ["脚本方向二", "脚本_S2", "脚本S2"]},
    {"logical_name": "script_s2_v1", "task_suffix": "S2V1", "direction_index": 2, "variant_no": 1, "aliases": ["脚本2变体1"]},
    {"logical_name": "script_s2_v2", "task_suffix": "S2V2", "direction_index": 2, "variant_no": 2, "aliases": ["脚本2变体2"]},
    {"logical_name": "script_s2_v3", "task_suffix": "S2V3", "direction_index": 2, "variant_no": 3, "aliases": ["脚本2变体3"]},
    {"logical_name": "script_s2_v4", "task_suffix": "S2V4", "direction_index": 2, "variant_no": 4, "aliases": ["脚本2变体4"]},
    {"logical_name": "script_s2_v5", "task_suffix": "S2V5", "direction_index": 2, "variant_no": 5, "aliases": ["脚本2变体5"]},
    {"logical_name": "script_s3", "task_suffix": "S3", "direction_index": 3, "variant_no": None, "aliases": ["脚本方向三", "脚本_S3", "脚本S3"]},
    {"logical_name": "script_s3_v1", "task_suffix": "S3V1", "direction_index": 3, "variant_no": 1, "aliases": ["脚本3变体1"]},
    {"logical_name": "script_s3_v2", "task_suffix": "S3V2", "direction_index": 3, "variant_no": 2, "aliases": ["脚本3变体2"]},
    {"logical_name": "script_s3_v3", "task_suffix": "S3V3", "direction_index": 3, "variant_no": 3, "aliases": ["脚本3变体3"]},
    {"logical_name": "script_s3_v4", "task_suffix": "S3V4", "direction_index": 3, "variant_no": 4, "aliases": ["脚本3变体4"]},
    {"logical_name": "script_s3_v5", "task_suffix": "S3V5", "direction_index": 3, "variant_no": 5, "aliases": ["脚本3变体5"]},
    {"logical_name": "script_s4", "task_suffix": "S4", "direction_index": 4, "variant_no": None, "aliases": ["脚本方向四", "脚本_S4", "脚本S4"]},
    {"logical_name": "script_s4_v1", "task_suffix": "S4V1", "direction_index": 4, "variant_no": 1, "aliases": ["脚本4变体1"]},
    {"logical_name": "script_s4_v2", "task_suffix": "S4V2", "direction_index": 4, "variant_no": 2, "aliases": ["脚本4变体2"]},
    {"logical_name": "script_s4_v3", "task_suffix": "S4V3", "direction_index": 4, "variant_no": 3, "aliases": ["脚本4变体3"]},
    {"logical_name": "script_s4_v4", "task_suffix": "S4V4", "direction_index": 4, "variant_no": 4, "aliases": ["脚本4变体4"]},
    {"logical_name": "script_s4_v5", "task_suffix": "S4V5", "direction_index": 4, "variant_no": 5, "aliases": ["脚本4变体5"]},
]

SOURCE_FIELD_ALIASES: Dict[str, List[str]] = {
    "task_no": ["任务编号", "任务ID", "任务序号", "编号", "产品编码", "商品编码", "SKU", "Product Code"],
    "store_id": ["店铺ID", "店铺", "店铺编号", "店铺名称"],
    "product_id": ["商品ID", "商品Id", "产品ID", "GeeLark商品ID", "GeeLark Product ID", "产品编码", "商品编码", "SKU", "Product Code"],
    "product_code": ["产品编码", "商品编码", "SKU", "Product Code"],
    "target_country": ["目标国家", "国家", "投放国家"],
    "product_type": ["产品类型", "商品类型", "品类", "产品品类"],
    "script_source": ["脚本来源", "来源"],
    "publish_purpose": ["发布用途", "用途"],
    "cart_enabled": ["是否挂车", "挂车"],
    "content_branch": ["内容分支"],
    "parent_slot_1": ["所属母版1"],
    "parent_slot_2": ["所属母版2"],
    "parent_slot_3": ["所属母版3"],
    "parent_slot_4": ["所属母版4"],
    "direction_1": ["母版方向1"],
    "direction_2": ["母版方向2"],
    "direction_3": ["母版方向3"],
    "direction_4": ["母版方向4"],
}

for spec in SCRIPT_FIELD_SPECS:
    SOURCE_FIELD_ALIASES[spec["logical_name"]] = list(spec["aliases"])


DEFAULT_DIRECTION_LABELS = {
    1: "日常轻分享流",
    2: "问题解决流",
    3: "场景代入流",
    4: "结论先行流",
}

VARIANT_STRENGTH_LABELS = {
    1: "轻变体",
    2: "轻变体",
    3: "轻变体",
    4: "中变体",
    5: "中变体",
}

CONTENT_ID_BLOCK_RE = re.compile(r"^【(?:内容ID|脚本ID)】\s*\n-\s*[^\n]+\n*", re.MULTILINE)
LINE_PREFIX_RE = re.compile(r"^[\-\d\.\s]+")
TITLE_LABEL_PREFIX_RE = re.compile(
    r"^(?:视频主题|视频标题|短视频标题|标题|title|caption)\s*[:：-]\s*",
    re.IGNORECASE,
)

COUNTRY_LANGUAGE_HINTS = {
    "thailand": ("泰国", "泰语"),
    "thai": ("泰国", "泰语"),
    "泰国": ("泰国", "泰语"),
    "indonesia": ("印度尼西亚", "印尼语"),
    "indonesian": ("印度尼西亚", "印尼语"),
    "印尼": ("印度尼西亚", "印尼语"),
    "印度尼西亚": ("印度尼西亚", "印尼语"),
    "malaysia": ("马来西亚", "马来语"),
    "malay": ("马来西亚", "马来语"),
    "马来西亚": ("马来西亚", "马来语"),
    "vietnam": ("越南", "越南语"),
    "vietnamese": ("越南", "越南语"),
    "越南": ("越南", "越南语"),
    "japan": ("日本", "日语"),
    "japanese": ("日本", "日语"),
    "日本": ("日本", "日语"),
    "korea": ("韩国", "韩语"),
    "south korea": ("韩国", "韩语"),
    "korean": ("韩国", "韩语"),
    "韩国": ("韩国", "韩语"),
    "philippines": ("菲律宾", "菲律宾语或当地常用英语"),
    "philippine": ("菲律宾", "菲律宾语或当地常用英语"),
    "菲律宾": ("菲律宾", "菲律宾语或当地常用英语"),
    "usa": ("美国", "英语"),
    "us": ("美国", "英语"),
    "united states": ("美国", "英语"),
    "america": ("美国", "英语"),
    "英国": ("英国", "英语"),
    "uk": ("英国", "英语"),
    "united kingdom": ("英国", "英语"),
    "singapore": ("新加坡", "英语或当地主要使用语言"),
    "新加坡": ("新加坡", "英语或当地主要使用语言"),
    "china": ("中国", "中文"),
    "中国": ("中国", "中文"),
}


def resolve_field_mapping(field_names: Sequence[str], aliases: Dict[str, List[str]]) -> Dict[str, Optional[str]]:
    mapping: Dict[str, Optional[str]] = {}
    for logical_name, candidates in aliases.items():
        mapping[logical_name] = next((candidate for candidate in candidates if candidate in field_names), None)
    return mapping


def normalize_text(raw_value: Any) -> str:
    if raw_value is None:
        return ""
    return str(raw_value).strip()


def normalize_yes_no(raw_value: Any) -> str:
    text = normalize_text(raw_value)
    if not text:
        return ""
    lowered = text.lower()
    if lowered in {"1", "true", "yes", "y", "是", "已勾选", "勾选", "checked"}:
        return "是"
    if lowered in {"0", "false", "no", "n", "否", "未勾选", "unchecked"}:
        return "否"
    return text


def is_nurture_metadata(script_source: str, publish_purpose: str, content_branch: str = "") -> bool:
    return (
        normalize_text(script_source) == "养号复刻"
        or normalize_text(publish_purpose) == "养号"
        or normalize_text(content_branch) == "非商品展示型"
    )


def parse_script_slot(task_suffix: str) -> Tuple[int, Optional[int]]:
    matched = re.fullmatch(r"S(\d)(?:V(\d))?", str(task_suffix or "").strip().upper())
    if not matched:
        raise ValueError(f"无法解析脚本槽位: {task_suffix}")
    direction_index = int(matched.group(1))
    variant_no = int(matched.group(2)) if matched.group(2) else None
    return direction_index, variant_no


def fallback_task_no(fields: Dict[str, Any], mapping: Dict[str, Optional[str]], source_record_id: str) -> str:
    for logical_name in ("task_no", "product_id", "product_code"):
        field_name = mapping.get(logical_name)
        value = normalize_text(fields.get(field_name)) if field_name else ""
        if value:
            return value
    return source_record_id


def parent_slot_value(fields: Dict[str, Any], mapping: Dict[str, Optional[str]], direction_index: int) -> str:
    field_name = mapping.get(f"parent_slot_{direction_index}")
    value = normalize_text(fields.get(field_name)) if field_name else ""
    return value or f"M{direction_index}"


def direction_label_value(fields: Dict[str, Any], mapping: Dict[str, Optional[str]], direction_index: int) -> str:
    field_name = mapping.get(f"direction_{direction_index}")
    value = normalize_text(fields.get(field_name)) if field_name else ""
    return value or DEFAULT_DIRECTION_LABELS[direction_index]


def variant_strength_label(variant_no: Optional[int]) -> str:
    if variant_no is None:
        return "母版"
    return VARIANT_STRENGTH_LABELS.get(variant_no, "中变体")


def build_script_id(task_no: str, parent_slot: str, variant_no: Optional[int]) -> str:
    suffix = "M" if variant_no is None else f"V{variant_no}"
    safe_task_no = re.sub(r"\s+", "", task_no)
    safe_parent_slot = re.sub(r"\s+", "", parent_slot)
    return f"{safe_task_no}_{safe_parent_slot}_{suffix}"


def build_canonical_script_key(source_record_id: str, script_slot: str) -> str:
    safe_source_record_id = re.sub(r"\s+", "", str(source_record_id or ""))
    safe_script_slot = re.sub(r"\s+", "", str(script_slot or ""))
    return f"{safe_source_record_id}:{safe_script_slot}"


def content_family_key(product_id: str, parent_slot: str) -> str:
    return f"{product_id}_{parent_slot}" if product_id and parent_slot else ""


def build_title_prompt(metadata: ScriptMetadata) -> str:
    country_label, language_label = language_hint_for_country(metadata.target_country)
    return (
        "你是一个短视频平台原生内容标题生成助手。\n\n"
        "你的任务是：\n"
        "根据给定的【脚本内容】与【目标国家】，生成一条适合短视频发布使用的标题。\n\n"
        "要求如下：\n"
        f"1. 标题必须使用【目标国家当地语言】输出；当前目标国家是【{country_label}】，请直接使用【{language_label}】\n"
        "2. 标题必须控制在 50 字以内\n"
        "3. 标题风格要自然、原生、像真实用户会写的短视频标题\n"
        "4. 不要有明显广告感，不要像商品详情页标题\n"
        "5. 不要出现价格、促销、下单引导、购买链接等表达\n"
        "6. 不要机械堆砌卖点\n"
        "7. 可以优先从真实场景、小感受、小结论、轻记忆点组织标题\n"
        "8. 标题要和脚本核心表达一致，但不要只是机械重复口播原句\n"
        "9. 只输出 1 条最终标题，不要解释，不要输出多个候选\n"
        "10. 不要输出“视频主题：”“视频标题：”“标题：”这类前缀，最终答案只能是标题正文本身\n"
        "11. 除非目标国家本身是中文市场，否则不要输出中文\n\n"
        f"输入信息：\n- 目标国家：{metadata.target_country}\n"
        f"- 产品类型：{metadata.product_type}\n"
        f"- 母版方向：{metadata.direction_label}\n"
        f"- 脚本内容：{metadata.script_text}\n\n"
        "输出要求：\n- 仅输出标题正文\n- 不要加引号\n- 不要加序号\n- 不要输出解释"
    )


def sanitize_title(raw_title: str) -> str:
    text = str(raw_title or "").strip()
    text = text.replace("\r", "\n")
    text = text.split("\n")[0].strip()
    text = TITLE_LABEL_PREFIX_RE.sub("", text)
    text = re.sub(r"^[\"'“”‘’]+|[\"'“”‘’]+$", "", text)
    text = re.sub(r"^\d+[\.\、\-\)]\s*", "", text)
    text = re.sub(r"\s+", " ", text)
    return text[:50].strip()


def strip_render_noise(script_text: str) -> str:
    text = CONTENT_ID_BLOCK_RE.sub("", str(script_text or "")).strip()
    lines = [line.strip() for line in text.splitlines()]
    cleaned = [line for line in lines if line and not line.startswith("【")]
    return "\n".join(cleaned).strip()


def heuristic_title(script_text: str) -> str:
    cleaned = strip_render_noise(script_text)
    if not cleaned:
        return ""
    match = re.search(r"脚本标题[:：]\s*(.+)", cleaned)
    if match:
        return sanitize_title(match.group(1))
    for line in cleaned.splitlines():
        candidate = LINE_PREFIX_RE.sub("", line).strip()
        if not candidate:
            continue
        if len(candidate) > 50:
            candidate = candidate[:50].strip()
        if len(candidate) >= 4:
            return sanitize_title(candidate)
    return sanitize_title(cleaned[:50])


def language_hint_for_country(target_country: str) -> Tuple[str, str]:
    normalized = str(target_country or "").strip()
    key = normalized.lower()
    return COUNTRY_LANGUAGE_HINTS.get(key, (normalized or "未知国家", "当地语言"))


def contains_cjk(text: str) -> bool:
    return bool(re.search(r"[\u4e00-\u9fff]", str(text or "")))


def contains_thai(text: str) -> bool:
    return bool(re.search(r"[\u0E00-\u0E7F]", str(text or "")))


def contains_japanese(text: str) -> bool:
    return bool(re.search(r"[\u3040-\u30ff\u4e00-\u9fff]", str(text or "")))


def contains_korean(text: str) -> bool:
    return bool(re.search(r"[\uAC00-\uD7AF]", str(text or "")))


def is_title_compatible_with_country(title: str, target_country: str) -> bool:
    text = sanitize_title(title)
    normalized_country = str(target_country or "").strip().lower()
    if not text:
        return False
    if TITLE_LABEL_PREFIX_RE.match(str(title or "").strip()):
        return False
    if normalized_country in {"thailand", "thai", "泰国"}:
        return contains_thai(text)
    if normalized_country in {"japan", "japanese", "日本"}:
        return contains_japanese(text)
    if normalized_country in {"korea", "south korea", "korean", "韩国"}:
        return contains_korean(text)
    if normalized_country in {"china", "中国"}:
        return contains_cjk(text)
    if normalized_country in {
        "indonesia",
        "indonesian",
        "印尼",
        "印度尼西亚",
        "malaysia",
        "malay",
        "马来西亚",
        "vietnam",
        "vietnamese",
        "越南",
        "philippines",
        "philippine",
        "菲律宾",
        "usa",
        "us",
        "united states",
        "america",
        "uk",
        "united kingdom",
        "英国",
        "singapore",
        "新加坡",
    }:
        return not contains_cjk(text)
    return True


class BaseTitleGenerator:
    source = "unknown"

    def generate(self, metadata: ScriptMetadata) -> str:
        raise NotImplementedError


class HeuristicTitleGenerator(BaseTitleGenerator):
    source = "heuristic"

    def generate(self, metadata: ScriptMetadata) -> str:
        return heuristic_title(metadata.script_text)


class LLMTitleGenerator(BaseTitleGenerator):
    source = "llm"

    def __init__(self, route: str = "auto"):
        generator_root = Path(__file__).resolve().parents[2] / "original-script-generator"
        if str(generator_root) not in sys.path:
            sys.path.insert(0, str(generator_root))
        core_dir = str(generator_root / "core")
        existing_core = sys.modules.get("core")
        if existing_core is not None and hasattr(existing_core, "__path__"):
            existing_paths = [str(path) for path in existing_core.__path__]
            if core_dir not in existing_paths:
                existing_core.__path__.append(core_dir)
        module_path = generator_root / "core" / "llm_client.py"
        spec = importlib.util.spec_from_file_location("original_script_llm_client", module_path)
        if spec is None or spec.loader is None:
            raise RuntimeError(f"无法加载标题生成 LLM 客户端: {module_path}")
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)
        self._client = module.OriginalScriptLLMClient(route=route)

    def generate(self, metadata: ScriptMetadata) -> str:
        prompt = build_title_prompt(metadata)
        result = self._client._call_raw(prompt, image_paths=[], max_tokens=160)
        return sanitize_title(self._client._extract_text(result))


class LocalizedLLMTitleGenerator(BaseTitleGenerator):
    source = "llm_localized"

    def __init__(self, preferred_route: str = "auto", extra_routes: Optional[Sequence[str]] = None):
        routes = [str(preferred_route or "auto").strip().lower()]
        routes.extend(str(route or "").strip().lower() for route in (extra_routes or ("backup", "primary", "gemini")))
        deduped: List[str] = []
        for route in routes:
            if route and route not in deduped:
                deduped.append(route)
        self.routes = deduped or ["auto"]

    def generate(self, metadata: ScriptMetadata) -> str:
        last_title = ""
        for route in self.routes:
            try:
                title = LLMTitleGenerator(route=route).generate(metadata)
            except Exception:
                continue
            sanitized = sanitize_title(title)
            if not sanitized:
                continue
            last_title = sanitized
            if is_title_compatible_with_country(sanitized, metadata.target_country):
                return sanitized
        return ""


class FallbackTitleGenerator(BaseTitleGenerator):
    source = "fallback"

    def __init__(self, primary: BaseTitleGenerator, secondary: BaseTitleGenerator):
        self.primary = primary
        self.secondary = secondary

    def generate(self, metadata: ScriptMetadata) -> str:
        try:
            title = sanitize_title(self.primary.generate(metadata))
        except Exception:
            title = ""
        if title and is_title_compatible_with_country(title, metadata.target_country):
            return title
        fallback_title = sanitize_title(self.secondary.generate(metadata))
        if fallback_title and is_title_compatible_with_country(fallback_title, metadata.target_country):
            return fallback_title
        return ""


def _resolve_title_from_existing(
    metadata: ScriptMetadata,
    existing: Optional[Dict[str, str]],
) -> Tuple[str, str]:
    if not existing:
        return "", ""
    existing_title = sanitize_title(str(existing.get("short_video_title") or ""))
    if not existing_title:
        return "", ""
    if str(existing.get("script_text") or "") != metadata.script_text:
        return "", ""
    if str(existing.get("target_country") or "") != metadata.target_country:
        return "", ""
    if not is_title_compatible_with_country(existing_title, metadata.target_country):
        return "", ""
    return existing_title, str(existing.get("title_source") or "cached")


def build_script_metadata_records(
    records: Sequence[Any],
    mapping: Dict[str, Optional[str]],
    *,
    title_generator: Optional[BaseTitleGenerator] = None,
    existing_lookup: Optional[Dict[Tuple[str, str], Dict[str, str]]] = None,
    progress_callback: Optional[Callable[[Dict[str, int]], None]] = None,
    record_id: Optional[str] = None,
    product_id: Optional[str] = None,
    limit: Optional[int] = None,
) -> List[ScriptMetadata]:
    results: List[ScriptMetadata] = []
    title_generator = title_generator or HeuristicTitleGenerator()
    stats = {
        "records_scanned": 0,
        "scripts_seen": 0,
        "titles_reused": 0,
        "titles_generated": 0,
    }

    for record in records:
        if record_id and record.record_id != record_id:
            continue
        stats["records_scanned"] += 1
        fields = record.fields
        product_value = normalize_text(fields.get(mapping.get("product_id"))) if mapping.get("product_id") else ""
        if product_id and product_value != product_id:
            continue

        task_no = fallback_task_no(fields, mapping, record.record_id)
        store_id = normalize_text(fields.get(mapping.get("store_id"))) if mapping.get("store_id") else ""
        target_country = normalize_text(fields.get(mapping.get("target_country"))) if mapping.get("target_country") else ""
        product_type = normalize_text(fields.get(mapping.get("product_type"))) if mapping.get("product_type") else ""
        script_source = normalize_text(fields.get(mapping.get("script_source"))) if mapping.get("script_source") else ""
        publish_purpose = normalize_text(fields.get(mapping.get("publish_purpose"))) if mapping.get("publish_purpose") else ""
        cart_enabled = normalize_yes_no(fields.get(mapping.get("cart_enabled"))) if mapping.get("cart_enabled") else ""
        content_branch = normalize_text(fields.get(mapping.get("content_branch"))) if mapping.get("content_branch") else ""
        is_nurture = is_nurture_metadata(script_source, publish_purpose, content_branch)

        for spec in SCRIPT_FIELD_SPECS:
            field_name = mapping.get(spec["logical_name"])
            script_text = normalize_text(fields.get(field_name)) if field_name else ""
            if not script_text:
                continue
            direction_index = int(spec["direction_index"])
            variant_no = spec["variant_no"]
            parent_slot = parent_slot_value(fields, mapping, direction_index)
            direction_label = direction_label_value(fields, mapping, direction_index)
            resolved_product_id = product_value if is_nurture else (product_value or task_no)
            resolved_family_key = content_family_key(resolved_product_id, parent_slot)
            if is_nurture and not resolved_family_key:
                resolved_family_key = f"{build_canonical_script_key(record.record_id, spec['task_suffix'])}:养号"
            metadata = ScriptMetadata(
                script_id=build_script_id(task_no, parent_slot, variant_no),
                source_record_id=record.record_id,
                script_slot=spec["task_suffix"],
                task_no=task_no,
                store_id=store_id,
                product_id=resolved_product_id,
                parent_slot=parent_slot,
                direction_label=direction_label,
                variant_strength=variant_strength_label(variant_no),
                target_country=target_country,
                product_type=product_type,
                content_family_key=resolved_family_key,
                script_text=script_text,
                short_video_title="",
                title_source="",
                canonical_script_key=build_canonical_script_key(record.record_id, spec["task_suffix"]),
                script_source=script_source,
                publish_purpose=publish_purpose,
                cart_enabled=cart_enabled,
                content_branch=content_branch,
            )
            stats["scripts_seen"] += 1
            existing_title, existing_source = _resolve_title_from_existing(
                metadata,
                existing_lookup.get((record.record_id, spec["task_suffix"])) if existing_lookup else None,
            )
            if existing_title:
                title = existing_title
                title_source = existing_source
                stats["titles_reused"] += 1
            else:
                title = sanitize_title(title_generator.generate(metadata))
                title_source = getattr(title_generator, "source", "unknown")
                stats["titles_generated"] += 1
            results.append(
                ScriptMetadata(
                    **{
                        **metadata.__dict__,
                        "short_video_title": title,
                        "title_source": title_source,
                    }
                )
            )
            if progress_callback is not None and (stats["scripts_seen"] % 20 == 0):
                progress_callback({**stats, "written": len(results)})
            if limit is not None and len(results) >= limit:
                if progress_callback is not None:
                    progress_callback({**stats, "written": len(results)})
                return results

    if progress_callback is not None:
        progress_callback({**stats, "written": len(results)})
    return results
