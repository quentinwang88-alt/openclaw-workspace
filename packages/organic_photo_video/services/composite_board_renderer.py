"""Programmatic P1 outfit-breakdown board renderer.

The renderer only composes operator-owned images and deterministic localized
copy.  It never invents prices, merchant names, purchase history, or product
details, and therefore stays separate from the generative image adapter.
"""

from __future__ import annotations

import hashlib
from pathlib import Path
import shutil
import subprocess
import tempfile
from typing import Any, Dict, Iterable, List, Mapping, Optional, Tuple

from PIL import Image, ImageDraw, ImageFilter, ImageFont, ImageOps, ImageStat

from services.board_copy_writer import board_copy, numbered_item_lines
from services.board_layout import load_board_layout
from services.cutout_assets import edge_background_to_alpha
from services.image_generator import GenerationOutcome, ShotGenerationRequest
from services.product_reference_resolver import select_product_references_for_slot


PROVIDER = "programmatic-board"
MODEL = "pillow-layout-v1"
PACKAGE_ROOT = Path(__file__).resolve().parents[1]
PALETTE_BACKGROUNDS = {
    "REFERENCE_WHITE": "#FFFFFF",
    "WARM_WHITE": "#FAFAF7",
    "COOL_WHITE": "#F5F7FA",
}


def _local_path(value: Any) -> str:
    if isinstance(value, str):
        return value if Path(value).is_file() else ""
    if isinstance(value, Mapping):
        candidate = str(
            value.get("cutout_image_path")
            or value.get("local_path")
            or value.get("path")
            or ""
        ).strip()
        return candidate if candidate and Path(candidate).is_file() else ""
    return ""


def _fit(image: Image.Image, size: Tuple[int, int], *, contain: bool = True) -> Image.Image:
    source = image.convert("RGBA")
    return ImageOps.contain(source, size, Image.Resampling.LANCZOS) if contain else ImageOps.fit(
        source, size, Image.Resampling.LANCZOS
    )


def _near_white_background_to_alpha(image: Image.Image) -> Image.Image:
    """Remove only edge-connected near-white background.

    White garments that are enclosed by the subject outline remain opaque.
    """
    return edge_background_to_alpha(image)


def _paste_with_shadow(
    canvas: Image.Image,
    image: Image.Image,
    box: Mapping[str, Any],
    *,
    remove_white: bool = False,
    rounded_fallback: bool = False,
    enhance_contrast: bool = False,
) -> None:
    width, height = int(box["width"]), int(box["height"])
    source = _near_white_background_to_alpha(image) if remove_white else image.convert("RGBA")
    if box.get("trim_alpha"):
        bounds = source.getchannel("A").getbbox()
        if bounds is None:
            raise ValueError("board cutout contains no visible pixels")
        # Padding belongs to the extracted asset, not its visible silhouette.
        # Crop transparent margins before contain-fitting; never stretch anatomy.
        source = source.crop(bounds)
    fitted = _fit(source, (width, height))
    if rounded_fallback and fitted.getchannel("A").getextrema() == (255, 255):
        mask = Image.new("L", fitted.size, 0)
        ImageDraw.Draw(mask).rounded_rectangle(
            (0, 0, fitted.width - 1, fitted.height - 1), radius=28, fill=255
        )
        fitted.putalpha(mask)
    x = int(box["x"]) + (width - fitted.width) // 2
    align_y = str(box.get("align_y") or "bottom")
    if align_y not in {"top", "center", "bottom"}:
        raise ValueError(f"invalid board vertical alignment {align_y!r}")
    y = int(box["y"]) + (0 if align_y == "top" else
                         (height - fitted.height) // 2 if align_y == "center" else height - fitted.height)
    alpha = fitted.getchannel("A")
    if enhance_contrast and alpha.getextrema()[0] < 255:
        # Contrast belongs outside the garment; never recolour white fabric.
        visible = alpha.point(lambda a: 255 if a > 128 else 0)
        if visible.getbbox():
            foreground = ImageStat.Stat(fitted.convert("RGB"), mask=visible).mean
            background = canvas.getpixel((0, 0))[:3]
            if max(abs(a - b) for a, b in zip(foreground, background)) < 48:
                halo_mask = Image.new("L", canvas.size, 0)
                halo_mask.paste(alpha, (x, y))
                halo_mask = halo_mask.filter(ImageFilter.MaxFilter(5)).filter(ImageFilter.GaussianBlur(5))
                halo_mask = halo_mask.point(lambda a: round(a * .28))
                halo = Image.new("RGBA", canvas.size, (65, 65, 65, 0))
                halo.putalpha(halo_mask)
                canvas.alpha_composite(halo)
    if box.get("shadow", True):
        shadow = Image.new("RGBA", canvas.size, (0, 0, 0, 0))
        shadow_mask = Image.new("L", canvas.size, 0)
        shadow_mask.paste(alpha, (x + 8, y + 12))
        shadow.putalpha(shadow_mask.filter(ImageFilter.GaussianBlur(14)))
        tint = Image.new("RGBA", canvas.size, (0, 0, 0, 38))
        canvas.alpha_composite(Image.composite(tint, Image.new("RGBA", canvas.size), shadow.getchannel("A")))
    canvas.alpha_composite(fitted, (x, y))


def _font(layout: Mapping[str, Any], size_key: str) -> ImageFont.FreeTypeFont | ImageFont.ImageFont:
    text = layout.get("text") or {}
    size = int(text.get(size_key) or 34)
    for candidate in text.get("font_candidates") or []:
        if Path(str(candidate)).is_file():
            return ImageFont.truetype(str(candidate), size=size)
    return ImageFont.load_default()


def _font_path(layout: Mapping[str, Any]) -> str:
    for candidate in (layout.get("text") or {}).get("font_candidates") or []:
        if Path(str(candidate)).is_file():
            return str(candidate)
    return ""


def _ffmpeg_path() -> str:
    configured = shutil.which("ffmpeg")
    fallback = Path.home() / ".local" / "bin" / "ffmpeg"
    if configured:
        return configured
    if fallback.is_file():
        return str(fallback)
    raise RuntimeError("ffmpeg with libharfbuzz is required for shaped Thai board text")


def _filter_path(value: str) -> str:
    return value.replace("\\", "\\\\").replace(":", "\\:").replace("'", "\\'")


def _item_refs(look: Mapping[str, Any]) -> List[Dict[str, Any]]:
    recipe = dict(look.get("recipe") or {})
    raw = look.get("item_refs") or recipe.get("item_refs") or {}
    output: List[Dict[str, Any]] = []
    for role in ("top_inner", "bottom", "footwear", "bag", "accessories"):
        value = raw.get(role) if isinstance(raw, Mapping) else None
        item = dict(value) if isinstance(value, Mapping) else {}
        item["role"] = role
        item.setdefault("label", recipe.get(role) or "")
        output.append(item)
    return output


class CompositeBoardRenderer:
    def __init__(self, layout_path: Optional[Path] = None):
        self.layout = load_board_layout(layout_path)

    def generate_shot(self, request: ShotGenerationRequest) -> GenerationOutcome:
        try:
            return self._generate(request)
        except Exception as exc:  # noqa: BLE001 - adapter boundary
            return GenerationOutcome(ok=False, provider=PROVIDER, model=MODEL, error=f"{type(exc).__name__}: {exc}")

    def _generate(self, request: ShotGenerationRequest) -> GenerationOutcome:
        if not request.continuity_reference_images:
            raise ValueError("composite board requires its anchor image")
        anchor_path = _local_path(request.continuity_reference_images[0])
        if not anchor_path:
            raise ValueError("composite board anchor image is not a readable local file")

        board_spec = dict(request.plan_shot.get("board_spec") or {})
        layout_id = str(board_spec.get("layout_id") or self.layout.get("layout_id") or "")
        layout = self.layout
        if layout_id and layout_id != str(layout.get("layout_id") or ""):
            candidate = PACKAGE_ROOT / "config" / "layouts" / f"{layout_id}.json"
            layout = load_board_layout(candidate)
        variant_name = str(board_spec.get("layout_variant") or layout.get("default_variant") or "LEFT_HERO")
        variant = (layout.get("variants") or {}).get(variant_name)
        if not variant:
            raise ValueError(f"unknown board layout variant {variant_name!r}")
        canvas_spec = layout["canvas"]
        palette_name = str(board_spec.get("palette_variant") or layout.get("default_palette") or "WARM_WHITE")
        background = str(canvas_spec.get("background") or "#FAFAF7")
        if layout.get("background_policy") != "fixed":
            background = PALETTE_BACKGROUNDS.get(palette_name, background)
        profile = dict((request.recipe_execution or {}).get("presentation_profile") or {})
        if profile.get("background_mode") == "solid_color":
            background = str(profile.get("background_color") or background)
        elif board_spec.get("background_color"):
            background = str(board_spec["background_color"])
        canvas = Image.new(
            "RGBA",
            (int(canvas_spec["width"]), int(canvas_spec["height"])),
            background,
        )
        decomposition = dict(board_spec.get("decomposition_assets") or {})
        assets = dict(decomposition.get("assets") or decomposition)
        dynamic = decomposition.get("item_count_policy") == "dynamic_2_or_3"
        expected_state = str(request.plan_shot.get("outfit_state_ref") or "FINAL")
        recorded_state = str(decomposition.get("source_outfit_state_ref") or "FINAL")
        if dynamic and expected_state != recorded_state:
            raise ValueError(f"board decomposition outfit state mismatch: {recorded_state} != {expected_state}")
        item_roles = list(decomposition.get("item_roles") or ["target_product", "top_inner", "bottom"])
        if dynamic and (len(item_roles) not in (2, 3) or item_roles[0] != "target_product"
                        or len(set(item_roles)) != len(item_roles)):
            raise ValueError("dynamic board requires two or three distinct real item roles")
        if dynamic and len(item_roles) == 2:
            # A dress is one garment, not a fabricated top-plus-bottom pair.
            variant = {**variant, **dict((layout.get("two_item_overrides") or {}).get(variant_name) or {})}

        def derived_path(role: str) -> str:
            return _local_path(assets.get(role) or {})

        is_freeform = str(layout.get("composition_style") or "") == "freeform_cutout"
        if is_freeform:
            missing = [
                role for role in ("person_cutout", *item_roles)
                if not derived_path(role)
            ]
            if missing:
                raise ValueError(
                    "freeform outfit board requires decomposition assets: "
                    + ", ".join(missing)
                )
            person_asset = dict(assets.get("person_cutout") or {})
            expected_anchor_slot = int(board_spec.get("source_person_slot") or 0)
            source_slot = int(person_asset.get("source_slot") or 0)
            if expected_anchor_slot and source_slot and source_slot != expected_anchor_slot:
                raise ValueError(
                    "person_cutout source slot does not match board anchor: "
                    f"{source_slot} != {expected_anchor_slot}"
                )
            if dynamic:
                for role in ("person_cutout", *item_roles):
                    asset_state = str((assets.get(role) or {}).get("outfit_state_ref") or recorded_state)
                    if asset_state != expected_state:
                        raise ValueError(f"{role} belongs to {asset_state}, not board state {expected_state}")
        if is_freeform and board_spec.get("item_layout_policy") == "compact_stack_v2":
            # Lay out visible silhouettes, not padded asset rectangles. Keep
            # the hero untouched and preserve each garment's aspect ratio.
            variant = {key: dict(value) for key, value in variant.items()}
            box_names = ["target_product", "companion_1", "companion_2"][:len(item_roles)]
            heights = []
            for role, name in zip(item_roles, box_names):
                with Image.open(derived_path(role)) as item_image:
                    rgba = item_image.convert("RGBA")
                    bounds = rgba.getchannel("A").getbbox()
                    cropped = rgba.crop(bounds) if bounds else rgba
                    box = variant[name]
                    heights.append(_fit(cropped, (int(box["width"]), int(box["height"]))).height)
            gap = 48
            top = max(80, (canvas.height - sum(heights) - gap * (len(heights) - 1)) // 2)
            for name, height in zip(box_names, heights):
                variant[name].update(y=top, height=height, align_y="top", trim_alpha=True)
                top += height + gap
        enhance_items = board_spec.get("item_contrast_policy") == "soft_silhouette_v1"
        hero = Image.open(derived_path("person_cutout") or anchor_path)
        same_background_region = (assets.get("person_cutout") or {}).get("composition_mode") == "same_background_region"
        hero_box = dict(variant["hero"])
        if same_background_region:
            hero_box.update(dict(layout.get("same_background_hero_override") or {}))
        _paste_with_shadow(
            canvas,
            hero,
            hero_box,
            remove_white=not same_background_region and not bool(derived_path("person_cutout")),
            rounded_fallback=not same_background_region,
        )

        product_paths = [derived_path("target_product")] if derived_path("target_product") else [
            path for path in select_product_references_for_slot(request.product, "hero")
            if _local_path(path)
        ]
        if not product_paths:
            raise ValueError("composite board requires one local target-product reference")
        _paste_with_shadow(
            canvas,
            Image.open(_local_path(product_paths[0])),
            variant["target_product"],
            remove_white=not bool(derived_path("target_product")),
            enhance_contrast=enhance_items,
        )

        items = _item_refs(request.look_snapshot)
        if is_freeform:
            recipe = dict(request.outfit_state or {}) if dynamic else dict(request.look_snapshot.get("recipe") or {})
            localized = dict((request.outfit_state.get("item_labels_i18n") or request.outfit_state.get("label_i18n") or {})
                             if dynamic else request.look_snapshot.get("item_labels_i18n") or {})
            items = [
                {
                    "role": role,
                    "path": derived_path(role),
                    "label": (assets.get(role) or {}).get("label") or recipe.get(role) or "",
                    "label_i18n": dict((assets.get(role) or {}).get("label_i18n") or
                                       localized.get(role) or {}),
                }
                for role in item_roles[1:]
            ]
        visible_items = []
        for item in items:
            if item.get("label") or _local_path(item):
                visible_items.append(item)
            if len(visible_items) >= 2:
                break
        for box_name, item in zip(("companion_1", "companion_2"), visible_items):
            path = _local_path(item)
            if path:
                _paste_with_shadow(
                    canvas, Image.open(path), variant[box_name], remove_white=not is_freeform,
                    enhance_contrast=enhance_items,
                )
            elif not is_freeform:
                self._draw_placeholder(canvas, variant[box_name], item)

        locale = str((request.recipe_execution or {}).get("locale") or "th-TH")
        hook = str(
            board_spec.get("copy_variant")
            or (request.recipe_execution or {}).get("hook_strategy")
            or "outfit_formula"
        )
        text_enabled = (layout.get("text") or {}).get("enabled", True)
        copy = board_copy(hook, locale) if text_enabled else {}
        target_item = {
            "role": "target_product",
            "title_i18n": (
                request.product.get("title_i18n")
                or dict(((request.outfit_state.get("item_labels_i18n") or request.outfit_state.get("label_i18n") or {})
                         if dynamic else request.look_snapshot.get("item_labels_i18n") or {}).get("target_product") or {})
            ),
        }
        display_items = [target_item] + visible_items
        text_specs = self._draw_copy(
            canvas, variant, copy, display_items, locale, layout=layout,
            freeform=is_freeform,
        )

        output_dir = Path(request.output_dir)
        output_dir.mkdir(parents=True, exist_ok=True)
        output_path = output_dir / f"{request.task_id}_P{request.slot_index}_v{request.shot_version}_board.png"
        base_path = output_path.with_name(output_path.stem + "_base.png")
        canvas.convert("RGB").save(base_path, format="PNG", optimize=True)
        try:
            self._render_shaped_text(base_path, output_path, text_specs, layout=layout)
        finally:
            base_path.unlink(missing_ok=True)
        digest = hashlib.sha256(output_path.read_bytes()).hexdigest()
        return GenerationOutcome(
            ok=True,
            image_path=str(output_path),
            provider=PROVIDER,
            model=MODEL,
            request_id=f"{request.task_id}_P{request.slot_index}_board",
            width=canvas.width,
            height=canvas.height,
            raw={
                "sha256": digest,
                "layout_id": layout.get("layout_id"),
                "layout_version": layout.get("layout_version"),
                "layout_variant": variant_name,
                "copy_variant": hook,
                "palette_variant": palette_name,
                "background_color": background,
                "item_contrast_policy": board_spec.get("item_contrast_policy", "legacy"),
                "item_layout_policy": board_spec.get("item_layout_policy", "legacy"),
                "text_rendered": bool(text_specs),
                "item_count": len(display_items),
                "source_outfit_state_ref": recorded_state,
                "source_person_asset_id": str((assets.get("person_cutout") or {}).get("source_asset_id") or ""),
                "commerce_data_rendered": False,
            },
        )

    def _draw_placeholder(
        self, canvas: Image.Image, box: Mapping[str, Any], item: Mapping[str, Any]
    ) -> None:
        draw = ImageDraw.Draw(canvas)
        xy = (
            int(box["x"]), int(box["y"]),
            int(box["x"]) + int(box["width"]),
            int(box["y"]) + int(box["height"]),
        )
        draw.rounded_rectangle(xy, radius=28, fill="#F0F0EC", outline="#DEDED7", width=2)

    def _draw_copy(
        self,
        canvas: Image.Image,
        variant: Mapping[str, Any],
        copy: Mapping[str, str],
        items: List[Mapping[str, Any]],
        locale: str,
        *,
        layout: Mapping[str, Any],
        freeform: bool = False,
    ) -> List[Dict[str, Any]]:
        draw = ImageDraw.Draw(canvas)
        text_spec = layout.get("text") or {}
        if text_spec.get("enabled", True) is False:
            return []
        color = str(text_spec.get("color") or "#171717")
        title_box = text_spec["title_box"]
        specs = [{
            "text": str(copy["title"]),
            "x": int(title_box["x"]),
            "y": int(title_box["y"]),
            "size": int(text_spec.get("title_size") or 54),
            "color": color,
        }]
        list_box = variant["item_list"]
        list_xy = (
            int(list_box["x"]), int(list_box["y"]),
            int(list_box["x"]) + int(list_box["width"]),
            int(list_box["y"]) + int(list_box["height"]),
        )
        if freeform:
            draw.line(
                (list_xy[0], list_xy[1], list_xy[2], list_xy[1]),
                fill="#CFCFC8", width=2,
            )
        else:
            draw.rounded_rectangle(
                list_xy, radius=30, fill=(255, 255, 255, 225),
                outline="#E4E4DE", width=2,
            )
        line_y = int(list_box["y"]) + 42
        for line in numbered_item_lines(items[:3], locale):
            specs.append({
                "text": line,
                "x": int(list_box["x"]) + 28,
                "y": line_y,
                "size": int(text_spec.get("item_size") or 34),
                "color": color,
            })
            line_y += 86
        footer_box = text_spec["footer_box"]
        specs.append({
            "text": str(copy["footer"]),
            "x": int(footer_box["x"]),
            "y": int(footer_box["y"]),
            "size": int(text_spec.get("footer_size") or 32),
            "color": "#555555",
        })
        return specs

    def _render_shaped_text(
        self,
        base_path: Path,
        output_path: Path,
        specs: List[Mapping[str, Any]],
        *,
        layout: Mapping[str, Any],
    ) -> None:
        """Use FFmpeg/Harfbuzz because Pillow's local build lacks Thai shaping."""
        if not specs:
            # A reference collage has no mandatory title/list/footer and does
            # not depend on a text font or an external text-rendering process.
            shutil.copyfile(base_path, output_path)
            return
        font_path = _font_path(layout)
        if not font_path:
            raise RuntimeError("board layout has no readable Thai font")
        text_paths: List[Path] = []
        filters = []
        try:
            for spec in specs:
                with tempfile.NamedTemporaryFile(
                    mode="w",
                    encoding="utf-8",
                    suffix=".txt",
                    prefix="opv_board_text_",
                    dir=str(output_path.parent),
                    delete=False,
                ) as handle:
                    handle.write(str(spec["text"]))
                    text_path = Path(handle.name)
                text_paths.append(text_path)
                color = str(spec.get("color") or "#171717").lstrip("#")
                filters.append(
                    "drawtext="
                    f"fontfile='{_filter_path(font_path)}':"
                    f"textfile='{_filter_path(str(text_path))}':"
                    f"fontcolor=0x{color}:fontsize={int(spec['size'])}:"
                    f"x={int(spec['x'])}:y={int(spec['y'])}"
                )
            completed = subprocess.run(
                [
                    _ffmpeg_path(), "-hide_banner", "-loglevel", "error", "-y",
                    "-i", str(base_path), "-vf", ",".join(filters),
                    "-frames:v", "1", str(output_path),
                ],
                text=True,
                capture_output=True,
                check=False,
            )
            if completed.returncode != 0 or not output_path.is_file():
                raise RuntimeError(
                    "Thai text render failed: " + (completed.stderr or "unknown ffmpeg error")
                )
        finally:
            for path in text_paths:
                path.unlink(missing_ok=True)
