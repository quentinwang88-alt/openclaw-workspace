"""结构化排版渲染器（structured_v1，2026-09-15 排版轮批次 B）。

设计边界（对应交付方案）：
- 一个共享渲染器、两种文字呈现：``clean``（纯色/简洁室内：直接排字、无底框）
  与 ``scene``（旅行街景：留白排字 + 局部柔和渐变底）。
- 文字层级：小标（地点/页签）→ 主标题 → 解释 → CTA；由程序从最终
  ``slide_texts`` 确定性解析，运营不填，渲染器不私自补文案。
- 泰文断行：优先空格/语义块，长串按字符断但绝不拆组合字符（声调/元音标记
  必须跟随其基字符）。
- bbox 统一：文字墨迹范围、渐变底与对齐使用同一套 ``textbbox`` 坐标。
- 同源：渲染输入 = 最终 ``slide_texts``（与 QA/中文翻译/manifest 同一份），
  兼容旧 ``slide_texts`` 接口不变。
"""
from __future__ import annotations

from pathlib import Path
from typing import Any, Dict, List, Mapping, Optional

from PIL import Image, ImageDraw, ImageFont

from domain.photo_contracts import placeholder_errors

STRUCTURED_RENDERER_VERSION = "structured_v1"
STRUCTURED_STYLES = ("clean", "scene")

_PACKAGE_ROOT = Path(__file__).resolve().parents[1]

#: 泰文组合字符（声调/元音标记等）：断行不得把它们与前面的基字符拆开。
_THAI_COMBINING = frozenset(
    chr(code) for code in (
        list(range(0x0E31, 0x0E32)) + list(range(0x0E34, 0x0E3B))
        + list(range(0x0E47, 0x0E4F))
    )
)
_NO_BREAK_BEFORE = _THAI_COMBINING | set(")]}」”.,!?:;ๆฯ")
_NO_BREAK_AFTER = set("([{「“")


class StructuredLayoutError(RuntimeError):
    pass


# ---------------------------------------------------------------------------
# 文本解析：slide_texts → 结构化页面规格（确定性，无模型调用）
# ---------------------------------------------------------------------------

def parse_page_spec(overlay_text: str, *, index: int, cover_index: int,
                    total: int) -> Dict[str, Any]:
    """把一页的最终文字解析为 ``kicker/headline/body/cta`` 层级。

    约定（与现有 copy 生成约定一致）：
    - 封面：两行＝「地点/小标 \\n 主标题」；单行＝只有主标题；
    - 内页：`名称 — 理由`（分隔符 ``—``/``–``/``-``/``：``）拆成 headline/body；
    - 末页：第一行 headline（可含理由），第二行 CTA。
    """
    value = str(overlay_text or "").strip()
    if placeholder_errors(value):
        raise StructuredLayoutError("overlay contains an unresolved placeholder")
    lines = [line.strip() for line in value.splitlines() if line.strip()]
    if not lines:
        return {"page_kind": "detail", "kicker": "", "headline": "", "body": "", "cta": ""}
    page_kind = (
        "cover" if index == cover_index
        else "final" if total and index == total
        else "detail"
    )
    if page_kind == "cover":
        if len(lines) >= 2:
            return {"page_kind": page_kind, "kicker": lines[0],
                    "headline": lines[1], "body": "", "cta": ""}
        return {"page_kind": page_kind, "kicker": "", "headline": lines[0],
                "body": "", "cta": ""}
    cta = ""
    if page_kind == "final" and len(lines) >= 2:
        cta = lines[-1]
        lines = lines[:-1]
    headline, body = _split_name_reason(" ".join(lines))
    return {"page_kind": page_kind, "kicker": "", "headline": headline,
            "body": body, "cta": cta}


def _split_name_reason(text: str) -> tuple[str, str]:
    for separator in (" — ", " – ", " - ", "：", ": "):
        position = text.find(separator)
        if position > 0:
            return text[:position].strip(), text[position + len(separator):].strip()
    return text.strip(), ""


# ---------------------------------------------------------------------------
# 泰文安全断行
# ---------------------------------------------------------------------------

def _break_allowed(text: str, position: int) -> bool:
    if position <= 0 or position >= len(text):
        return False
    if text[position] in _NO_BREAK_BEFORE:
        return False
    if text[position - 1] in _NO_BREAK_AFTER:
        return False
    if text[position] in _THAI_COMBINING or text[position - 1] in _THAI_COMBINING:
        # 不得把组合标记与其基字符拆开（标记跟随前一个基字符）。
        return False
    return True


def wrap_text(text: str, font: ImageFont.FreeTypeFont, *, max_width: int,
              draw: ImageDraw.ImageDraw) -> List[str]:
    """按宽度断行：空格优先，长泰文串按安全字符位断；不拆组合字符。"""
    text = str(text or "").strip()
    if not text:
        return []
    max_width = max(24, int(max_width))
    if draw.textlength(text, font=font) <= max_width:
        return [text]
    lines: List[str] = []
    current = ""
    for token in text.split(" "):
        candidate = f"{current} {token}".strip()
        if draw.textlength(candidate, font=font) <= max_width or not current:
            if not current and draw.textlength(token, font=font) > max_width:
                # 单个无空格长串（泰文常态）：按安全字符位细断。
                chunk = ""
                for i, char in enumerate(token):
                    probe = chunk + char
                    if (draw.textlength(probe, font=font) > max_width
                            and chunk and _break_allowed(probe, len(chunk))):
                        lines.append(chunk)
                        chunk = char
                    else:
                        chunk = probe
                current = chunk
            else:
                current = candidate
        else:
            lines.append(current)
            current = token
    if current:
        lines.append(current)
    return [line for line in lines if line]


# ---------------------------------------------------------------------------
# 渲染
# ---------------------------------------------------------------------------

def _resolve_font(candidates: List[str], size: int) -> ImageFont.FreeTypeFont:
    for candidate in candidates:
        path = Path(str(candidate or "")).expanduser()
        if not path.is_absolute():
            path = _PACKAGE_ROOT / path
        if path.is_file():
            try:
                return ImageFont.truetype(str(path), size=int(size))
            except OSError:
                continue
    raise StructuredLayoutError(
        "structured layout requires a usable font; candidates: "
        + "、".join(str(value) for value in candidates))


def _fit_block(lines: List[str], font: ImageFont.FreeTypeFont, *,
               max_width: int, draw: ImageDraw.ImageDraw) -> List[str]:
    wrapped: List[str] = []
    for line in lines:
        wrapped.extend(wrap_text(line, font, max_width=max_width, draw=draw))
    return wrapped


def render_structured_page(
    image: Image.Image, overlay_text: str, template: Mapping[str, Any], *,
    index: int = 1, cover_index: int = 1, total: int = 1,
) -> Dict[str, Any]:
    """在一张成片上绘制结构化文字；返回实际使用的渲染信息（进 manifest）。"""
    spec = parse_page_spec(overlay_text, index=index, cover_index=cover_index, total=total)
    style = str(template.get("structured_style") or "clean")
    if style not in STRUCTURED_STYLES:
        raise StructuredLayoutError(f"unsupported structured_style: {style}")
    draw = ImageDraw.Draw(image)
    width = image.width
    margin_x = int(template.get("padding_x") or round(width * 0.07))
    max_width = width - margin_x * 2
    kicker_size = int(template.get("kicker_font_size") or 30)
    headline_size = int(template.get("headline_font_size") or 52)
    body_size = int(template.get("body_font_size") or 36)
    cta_size = int(template.get("cta_font_size") or 30)
    min_size = int(template.get("min_font_size") or 24)
    regular_candidates = list(template.get("font_candidates") or [])
    bold_candidates = list(template.get("font_candidates_bold") or regular_candidates)
    headline_color = str(template.get("headline_text_color") or "#1F1F1F")
    body_color = str(template.get("body_text_color") or "#3D3D3D")
    kicker_color = str(template.get("kicker_text_color") or "#6B6B6B")
    cta_color = str(template.get("cta_text_color") or "#5A5A5A")
    spacing = int(template.get("line_spacing") or 10)

    # 逐级降字号直到放得下（先换行，后有限缩字；不删任何层级文本）。
    for scale_step in range(0, 41, 2):
        scale = 1 - scale_step / 100
        k_font = _resolve_font(regular_candidates, max(min_size, round(kicker_size * scale)))
        h_font = _resolve_font(bold_candidates, max(min_size, round(headline_size * scale)))
        b_font = _resolve_font(regular_candidates, max(min_size, round(body_size * scale)))
        c_font = _resolve_font(regular_candidates, max(min_size, round(cta_size * scale)))
        blocks: List[tuple[str, ImageFont.FreeTypeFont, str]] = []
        if spec["kicker"]:
            blocks.append((spec["kicker"], k_font, kicker_color))
        if spec["headline"]:
            blocks.append((spec["headline"], h_font, headline_color))
        if spec["body"]:
            blocks.append((spec["body"], b_font, body_color))
        if spec["cta"]:
            blocks.append((spec["cta"], c_font, cta_color))
        laid: List[tuple[List[str], ImageFont.FreeTypeFont, str]] = [
            (_fit_block([text], font, max_width=max_width, draw=draw), font, color)
            for text, font, color in blocks
        ]
        total_height = sum(
            len(lines) * (font.size + spacing) for lines, font, _ in laid) - spacing
        zone_height = round(image.height * (
            float(template.get("zone_height_ratio") or 0.34)))
        if total_height <= zone_height:
            return _paint(image, draw, laid, spec, style, template,
                          margin_x=margin_x, spacing=spacing)
    raise StructuredLayoutError(
        "structured text cannot fit the text zone at minimum font size; "
        "consider a copy revision instead of dropping text")


def _paint(
    image: Image.Image, draw: ImageDraw.ImageDraw,
    laid: List[tuple[List[str], ImageFont.FreeTypeFont, str]],
    spec: Mapping[str, Any], style: str, template: Mapping[str, Any], *,
    margin_x: int, spacing: int,
) -> Dict[str, Any]:
    top_zone = spec["page_kind"] == "cover"
    line_heights = [
        (font.size + spacing) * len(lines) for lines, font, _ in laid]
    block_height = sum(line_heights)
    if top_zone:
        top = int(template.get("top_offset") or round(image.height * 0.055))
    else:
        bottom_margin = int(template.get("bottom_offset") or round(image.height * 0.05))
        top = image.height - bottom_margin - block_height
    # 统一 bbox：渐变底/对齐均以同一 textbbox 结果计算。
    ink_left, ink_right = margin_x, image.width - margin_x
    y = top
    for (lines, font, color), _height in zip(laid, line_heights):
        for line in lines:
            bbox = draw.textbbox((margin_x, y), line, font=font, anchor="la")
            ink_left = min(ink_left, bbox[0])
            ink_right = max(ink_right, bbox[2])
            y = bbox[3] + spacing
    if style == "scene":
        _draw_scrim(image, top_zone=top_zone,
                    top=top, bottom=top + block_height,
                    left=max(0, ink_left - 18), right=min(image.width, ink_right + 18),
                    template=template)
        draw = ImageDraw.Draw(image)
        y = top
        for lines, font, color in laid:
            for line in lines:
                draw.text((margin_x, y), line, font=font, fill=color, anchor="la")
                y += font.size + spacing
    else:
        y = top
        for lines, font, color in laid:
            for line in lines:
                draw.text((margin_x, y), line, font=font, fill=color, anchor="la")
                y += font.size + spacing
    return {
        "renderer": STRUCTURED_RENDERER_VERSION,
        "style": style,
        "page_kind": spec["page_kind"],
    }


def _draw_scrim(image: Image.Image, *, top_zone: bool, top: int, bottom: int,
                left: int, right: int, template: Mapping[str, Any]) -> None:
    """局部柔和渐变底：只在 scene 呈现使用，边缘淡出，不整块压画面。

    2026-09-16 修复：此前在同一 RGBA 层重叠绘制半透明矩形——重叠区 alpha
    累积、条带方向计算错误，文字位置得不到稳定衬底（白墙/天空上米白字
    不可读，评审 §C 探针全 255）。改为逐行单次绘制的梯形剖面：文字带
    （top–bottom）内全强度，向带外按可用边距淡出到 0，每行只画一次。
    """
    base = str(template.get("scrim_color") or "#000000")
    alpha = int(template.get("scrim_alpha") or 90)
    overlay = Image.new("RGBA", image.size, (0, 0, 0, 0))
    scrim = ImageDraw.Draw(overlay)
    hex_color = base.lstrip("#")
    rgb = tuple(int(hex_color[i:i + 2], 16) for i in (0, 2, 4))
    band_top = max(0, top - 26)
    band_bottom = min(image.height, bottom + 26)
    upper_fade = max(1, top - band_top)
    lower_fade = max(1, band_bottom - bottom)
    for y in range(band_top, band_bottom):
        if y < top:
            factor = (y - band_top) / upper_fade
        elif y >= bottom:
            factor = (band_bottom - y) / lower_fade
        else:
            factor = 1.0
        a = int(alpha * factor)
        if a > 0:
            scrim.rectangle((left, y, right, y + 1), fill=rgb + (a,))
    image.alpha = None  # 保持 RGB 画布；合成走 RGBA 蒙版
    composed = Image.alpha_composite(image.convert("RGBA"), overlay)
    image.paste(composed.convert("RGB"), (0, 0))
