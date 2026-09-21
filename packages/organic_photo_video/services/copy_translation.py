"""最终成片文案的中文全文回写（2026-09-15 轮 Phase 1a）。

数据源是**最终成片包**的 copy（title/caption/hashtags/slide_texts），不是早期
规划摘要；按文案指纹缓存翻译，修订文案后指纹变化自然重译；翻译独立于图片
生成指纹——失败只影响中文展示字段，不触发重生、重排期或重复发布。

客户端复用 ``CodexVisionClient`` 的纯文本路径（paths=[]）。
"""
from __future__ import annotations

import hashlib
import json
import os
from pathlib import Path
from typing import Any, Dict, List, Mapping, Optional


class CopyTranslationError(RuntimeError):
    """翻译失败；调用方降级为占位提示，不影响成片与发布流程。"""


#: 只翻译这些键：与最终 package manifest["copy"] 的发布字段一一对应。
TRANSLATABLE_KEYS = ("title", "caption", "hashtags", "slide_texts")


def visible_page_text(page: Mapping[str, Any]) -> str:
    """提取一页的**全部可见文字**（复审方案 P1-1）。

    按渲染顺序：kicker → headline → body → 色卡可见标签（label_zh 的
    对应发布语言在渲染端取自 label/label_local；此处并入 label 兜底与
    label_zh 中文审计——翻译与核对需要全量，不塞回旧短标题布局）。

    ``headline or body`` 的旧写法会把正文吞掉：本页有 headline 也有 body
    时两段都是可见文字，必须都返回。
    """
    text = page.get("text") if isinstance(page.get("text"), Mapping) else page
    parts: List[str] = []
    for key in ("kicker", "headline", "body"):
        value = str(text.get(key) or "").strip()
        if value:
            parts.append(value)
    chips = [chip for chip in page.get("color_chips") or []
             if isinstance(chip, Mapping)]
    for chip in chips:
        # 可见标签：发布语言 label 优先；label_zh 是中文审计，也进全量文字
        # （中文回写需要它；指纹对 label_zh 敏感——改标签即重译）。
        label = str(chip.get("label") or "").strip()
        label_zh = str(chip.get("label_zh") or "").strip()
        if label:
            parts.append(label)
        if label_zh and label_zh != label:
            parts.append(label_zh)
    return "\n".join(parts)


def pages_to_slide_texts(copy_block: Mapping[str, Any]) -> List[str] | None:
    """把结构化 pages 确定性投影成可见文字页（复审 F2 + 方案 P1-1）。

    每页 = visible_page_text（kicker/headline/body/色卡标签全量，按渲染
    顺序）；无 pages 返回 None（旧任务保持原 slide_texts 不变）。
    """
    pages = [page for page in copy_block.get("pages") or []
             if isinstance(page, Mapping)]
    if not pages:
        return None
    projected = [visible_page_text(page) for page in pages]
    return projected if all(projected) else None


def _authoritative_copy(copy_block: Mapping[str, Any]) -> Dict[str, Any]:
    """pages 存在时以投影结果覆盖 slide_texts（同源保证），否则原样返回。"""
    projected = pages_to_slide_texts(copy_block)
    if projected is None:
        return dict(copy_block)
    canonical = dict(copy_block)
    canonical["slide_texts"] = projected
    return canonical


def copy_fingerprint(copy_block: Mapping[str, Any]) -> str:
    """发布文案指纹：修订任何发布文字都会改变指纹并触发重译。

    复审 F2：pages 存在时先投影成 slide_texts 再取指纹——只改 pages.body
    也会改变指纹、触发重译（此前投影缺失导致缓存不失效、中文缺页上文字）。
    """
    canonical = _authoritative_copy(copy_block)
    payload = json.dumps(
        {key: canonical.get(key) for key in TRANSLATABLE_KEYS},
        ensure_ascii=False, sort_keys=True)
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()[:32]


def _normalize_translation(raw: Any, *, slide_count: int) -> Dict[str, Any]:
    if not isinstance(raw, Mapping):
        raise CopyTranslationError("翻译结果不是 JSON 对象")
    title = str(raw.get("title_zh") or "").strip()
    caption = str(raw.get("caption_zh") or "").strip()
    hashtags = [str(v).strip() for v in raw.get("hashtags_zh") or [] if str(v).strip()]
    slides = [str(v).strip() for v in raw.get("slides_zh") or [] if str(v).strip()]
    if not title or not caption or not hashtags or not slides:
        raise CopyTranslationError("翻译结果缺少 title/caption/hashtags/slides")
    if slide_count and len(slides) != slide_count:
        raise CopyTranslationError(
            f"翻译页数 {len(slides)} 与成片页数 {slide_count} 不一致")
    return {
        "schema_version": "opv-copy-translation-v1",
        "title_zh": title, "caption_zh": caption,
        "hashtags_zh": hashtags, "slides_zh": slides,
    }


class CopyTranslationService:
    """按文案指纹缓存的泰→中（或 vi→中）整包翻译。"""

    def __init__(self, *, root: Path, client: Any = None) -> None:
        self.root = Path(root)
        self.cache_dir = self.root / "copy_translations"
        self.client = client

    # ------------------------------------------------------------------
    def translate(self, copy_block: Mapping[str, Any], *,
                  source_locale: str = "th-TH") -> Dict[str, Any]:
        # 复审 F2：pages 存在时翻译它的投影（headline\\nbody 同源），缓存
        # 指纹同源——pages 改文即触发重译。
        source = _authoritative_copy(copy_block or {})
        fingerprint = copy_fingerprint(source)
        cached = self._read_cache(fingerprint)
        if cached is not None:
            return cached
        slide_count = len(list(source.get("slide_texts") or []))
        prompt = self._prompt(source, source_locale=source_locale)
        client = self._ensure_client()
        try:
            envelope = client.chat_with_multiple_images([], prompt, max_tokens=2000)
            content = envelope.get("choices", [{}])[0].get("message", {}).get("content", "")
            payload = json.loads(str(content))
        except CopyTranslationError:
            raise
        except Exception as exc:  # 网络/JSON 解析等一律按可重试失败处理
            raise CopyTranslationError(f"翻译调用失败：{exc}") from exc
        translation = _normalize_translation(payload, slide_count=slide_count)
        translation["copy_fingerprint"] = fingerprint
        self._write_cache(fingerprint, translation)
        return translation

    # ------------------------------------------------------------------
    def _prompt(self, source: Mapping[str, Any], *, source_locale: str) -> str:
        payload = {
            key: source.get(key)
            for key in TRANSLATABLE_KEYS
        }
        return (
            f"你是社交媒体文案翻译员。把以下{source_locale}的 TikTok 图文发布文案"
            "完整翻译成简体中文。要求：\n"
            "1. title_zh/caption_zh 译出完整语义，标签（hashtags）逐条翻译含义，"
            "格式为「原文 → 中文含义」；\n"
            "2. slides_zh 逐页翻译，页数、顺序、换行结构（\\n）必须与输入 slide_texts"
            " 完全一致；页内「造型名 — 理由」保持一行；\n"
            "3. 只翻译，不改写、不扩写、不添加内容；图片上没有的信息不得补充；\n"
            "4. 只返回 JSON 对象："
            '{"title_zh":"","caption_zh":"","hashtags_zh":["#原文 → 含义"],'
            '"slides_zh":["…"]}，不要 Markdown。\n'
            f"输入文案：{json.dumps(payload, ensure_ascii=False)}"
        )

    def _ensure_client(self) -> Any:
        if self.client is not None:
            return self.client
        self._load_local_environment()
        from services.codex_vision_client import CodexVisionClient
        self.client = CodexVisionClient(
            model=os.environ.get("OPV_PHOTO_VISION_CODEX_MODEL", "gpt-5.6-sol").strip()
            or "gpt-5.6-sol",
            reasoning_effort=os.environ.get(
                "OPV_PHOTO_VISION_CODEX_EFFORT", "low").strip() or "low",
        )
        return self.client

    @staticmethod
    def _load_local_environment() -> None:
        path = Path(__file__).resolve().parents[3] / ".env.local"
        if not path.is_file():
            return
        allowed = {"OPV_PHOTO_VISION_CODEX_MODEL", "OPV_PHOTO_VISION_CODEX_EFFORT"}
        for raw_line in path.read_text(encoding="utf-8").splitlines():
            line = raw_line.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue
            key, value = line.split("=", 1)
            key = key.strip()
            if key in allowed and key not in os.environ:
                os.environ[key] = value.strip().strip('"').strip("'")

    def _cache_path(self, fingerprint: str) -> Path:
        return self.cache_dir / f"{fingerprint}.json"

    def _read_cache(self, fingerprint: str) -> Optional[Dict[str, Any]]:
        path = self._cache_path(fingerprint)
        if not path.is_file():
            return None
        try:
            return json.loads(path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError):
            return None

    def _write_cache(self, fingerprint: str, translation: Dict[str, Any]) -> None:
        self.cache_dir.mkdir(parents=True, exist_ok=True)
        temporary = self._cache_path(fingerprint).with_suffix(".tmp")
        temporary.write_text(
            json.dumps(translation, ensure_ascii=False, indent=1), encoding="utf-8")
        temporary.replace(self._cache_path(fingerprint))


def compose_full_copy_zh(translation: Mapping[str, Any], *,
                         set_index: int = 1, total_sets: int = 1) -> str:
    """把一份译文组装成运营可读的多行文本（按最终页面顺序）。"""
    header = f"【第 {set_index} 套】" if total_sets > 1 else "【本篇文案（中文）】"
    lines: List[str] = [header]
    lines.append(f"标题：{translation.get('title_zh', '')}")
    lines.append(f"正文：{translation.get('caption_zh', '')}")
    tags = [str(v) for v in translation.get("hashtags_zh") or []]
    if tags:
        lines.append("标签：" + "；".join(tags))
    slides = [str(v) for v in translation.get("slides_zh") or []]
    for index, slide in enumerate(slides, 1):
        label = "封面" if index == 1 else f"第 {index} 页"
        lines.append(f"{label}：{slide.replace(chr(10), ' / ')}")
    return "\n".join(lines)
