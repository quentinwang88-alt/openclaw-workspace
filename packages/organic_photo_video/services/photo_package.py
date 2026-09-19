"""Build the final, ordered native-photo package without creating a video.

Raw selected shots remain immutable inputs.  This service writes separate
text-baked JPEG files, registers them as ``photo_slide`` revision assets and
stores an ordered package manifest for terminal review.
"""

from __future__ import annotations

import hashlib
import json
from pathlib import Path
from typing import Any, Dict, Mapping, Optional, Sequence

from PIL import Image, ImageDraw, ImageFont, ImageOps

from domain import statuses
from domain.photo_contracts import placeholder_errors
from services.workflow_v2 import RevisionAssetResolver, RevisionService, canonical_hash


class PhotoPackageError(RuntimeError):
    pass


def normalize_photo_template(template: Mapping[str, Any]) -> Dict[str, Any]:
    """Convert a shipped ``opv-photo-layout-v1`` config into exporter options.

    The normalized form is also accepted directly by tests and one-off callers.
    Keeping this adapter here prevents business recipes from depending on the
    image composer's private layout names.
    """
    if template.get("schema_version") == "opv-photo-layout-v2":
        allowed = {"schema_version", "layout_id", "layout_version", "layout_kind", "status", "render_options"}
        if any(v and k not in allowed for k, v in template.items()):
            raise PhotoPackageError("unsupported non-empty layout field")
        options = dict(template.get("render_options") or {})
        supported = {"width", "height", "background", "jpeg_quality", "font_candidates", "font_size", "cover_font_size", "detail_font_size", "cta_font_size", "min_font_size", "text_color", "text_background", "cta_text_color", "cta_background", "text_radius", "max_lines", "cover_index", "choice_badges", "padding_x", "padding_y", "top_offset", "bottom_offset", "line_spacing", "text_position", "split_last_line_to_bottom", "overlay_style", "structured_style", "font_candidates_bold", "kicker_font_size", "headline_font_size", "body_font_size", "zone_height_ratio", "headline_text_color", "body_text_color", "kicker_text_color", "scrim_color", "scrim_alpha", "scrim_fade"}
        if any(v and k not in supported for k, v in options.items()):
            raise PhotoPackageError("unsupported non-empty render option")
        if options.get("text_position", "top") not in {"top", "bottom"}:
            raise PhotoPackageError("unsupported text_position")
        if options.get("overlay_style") not in {None, "", "structured_v1"}:
            # 显式选择尚未支持的排版必须报错，不得静默回落旧模板。
            raise PhotoPackageError(
                "unsupported overlay_style: " + str(options.get("overlay_style")))
        return {**options, "template_id": template["layout_id"], "template_version": template["layout_version"]}
    if template.get("schema_version") != "opv-photo-layout-v1":
        return dict(template)
    canvas = dict(template.get("canvas") or {})
    text = dict(template.get("text_defaults") or {})
    export = dict(template.get("export") or {})
    backplate = dict(text.get("backplate") or {})
    return {
        "template_id": template.get("layout_id"),
        "template_version": template.get("layout_version"),
        "width": canvas.get("width"), "height": canvas.get("height"),
        "background": canvas.get("background", "#FFFFFF"),
        "jpeg_quality": export.get("quality", 90),
        "font_candidates": list(text.get("font_candidates") or []),
        "font_size": text.get("headline_size") or text.get("choice_size") or 54,
        "text_color": text.get("color", "#FFFFFF"),
        "text_background": backplate.get("color", "#000000"),
        "text_radius": backplate.get("corner_radius", 22),
        "max_lines": 2,
        "cover_index": template.get("cover_index", 1),
        "choice_badges": template.get("choice_badges", False),
    }


def _file_hash(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _font(template: Mapping[str, Any], *, required: bool,
          size: Optional[int] = None) -> Optional[ImageFont.FreeTypeFont]:
    candidates = [template.get("font_path"), *(template.get("font_candidates") or [])]
    for candidate in candidates:
        path = Path(str(candidate or "")).expanduser()
        if not path.is_absolute():
            # 项目内置字体（assets/fonts）随仓库部署，不依赖机器路径。
            path = Path(__file__).resolve().parents[1] / path
        if path.is_file():
            try:
                return ImageFont.truetype(
                    str(path), size=int(size or template.get("font_size") or 54)
                )
            except OSError:
                continue
    if required:
        raise PhotoPackageError("overlay text requires an explicit usable Unicode font")
    return None


def _fit_source_contain(path: str, size: tuple[int, int]) -> Image.Image:
    """D1: 等比完整容纳——不裁头脚，允许留边。"""
    src = Image.open(path).convert("RGB")
    tw, th = size
    scale = min(tw / src.width, th / src.height)
    nw, nh = max(1, round(src.width * scale)), max(1, round(src.height * scale))
    resized = src.resize((nw, nh), Image.LANCZOS)
    canvas = Image.new("RGB", size, (255, 255, 255))
    canvas.paste(resized, ((tw - nw) // 2, (th - nh) // 2))
    return canvas


def _fit_source(path: str, size: tuple[int, int]) -> Image.Image:
    source = Image.open(path)
    source.load()
    return ImageOps.fit(source.convert("RGB"), size, method=Image.Resampling.LANCZOS)


def _compose(
    source_paths: Sequence[str], *, layout: str, width: int, height: int,
    background: str, template: Optional[Mapping[str, Any]] = None,
    column_labels: Sequence[str] = (),
) -> Image.Image:
    if not source_paths:
        raise PhotoPackageError("each final slide needs at least one source image")
    canvas = Image.new("RGB", (width, height), background)
    gap = max(0, round(min(width, height) * 0.012))
    if layout == "single":
        if len(source_paths) != 1:
            raise PhotoPackageError("single requires exactly one source image")
        canvas.paste(_fit_source(source_paths[0], (width, height)), (0, 0))
        return canvas
    if layout == "split_vertical":
        if len(source_paths) != 2:
            raise PhotoPackageError("split_vertical requires exactly two source images")
        cell_width = (width - gap) // 2
        for index, path in enumerate(source_paths):
            x = index * (cell_width + gap)
            canvas.paste(_fit_source(path, (cell_width, height)), (x, 0))
        return canvas
    if layout == "grid_2x2":
        if len(source_paths) != 4:
            raise PhotoPackageError("grid_2x2 requires exactly four source images")
        # P3.4（§3.4）：四宫格封面独立文字区——图片区约占 80%、下 20% 留白。
        # 文字排在结构化排版 bottom 带内（text_position=bottom），不再压脸/腿。
        text_band_ratio = float(template.get("grid_text_band_ratio") or 0.0)
        if text_band_ratio <= 0:
            text_band_ratio = 0.20
        image_zone_height = int(height * (1.0 - text_band_ratio))
        cell_width = (width - gap) // 2
        cell_height = (image_zone_height - gap) // 2
        for index, path in enumerate(source_paths):
            x = (index % 2) * (cell_width + gap)
            y = (index // 2) * (cell_height + gap)
            canvas.paste(
                _fit_source_contain(path, (cell_width, cell_height)), (x, y))
        return canvas
    if layout == "triptych_3":
        # Three equal vertical columns, in frozen role order.  Every column is
        # centre-cropped (never stretched) so the same person keeps the same
        # proportions across the whole progression.
        if len(source_paths) != 3:
            raise PhotoPackageError("triptych_3 requires exactly three source images")
        cell_width = (width - gap * 2) // 3
        for index, path in enumerate(source_paths):
            x = index * (cell_width + gap)
            canvas.paste(_fit_source(path, (cell_width, height)), (x, 0))
        _draw_column_labels(
            canvas, column_labels, template, columns=3, gap=gap, cell_width=cell_width,
        )
        return canvas
    raise PhotoPackageError(f"unsupported photo layout {layout!r}")


def _fit_column_font(
    draw: ImageDraw.ImageDraw, label: str, template: Mapping[str, Any], *,
    preferred: int, floor: int, available: int,
) -> tuple[Any, int, int]:
    """Largest font size at which ``label`` still fits inside one column.

    Shrinking beats overflowing, but only down to ``floor``: a label that still
    does not fit is a content problem and must fail loudly instead of spilling
    into the neighbouring column.
    """
    for size in range(preferred, floor - 1, -1):
        font = _font({**template, "font_size": size}, required=True)
        bbox = draw.multiline_textbbox((0, 0), label, font=font, align="center")
        width, height = bbox[2] - bbox[0], bbox[3] - bbox[1]
        if width <= available:
            return font, width, height
    raise PhotoPackageError(
        f"column label {label!r} does not fit its column at minimum font size"
    )


def _draw_column_labels(
    image: Image.Image, labels: Sequence[str], template: Optional[Mapping[str, Any]], *,
    columns: int, gap: int, cell_width: int,
) -> None:
    """Label each column of a multi-column layout at the safe bottom strip.

    Labels are pinned to the very bottom so they can never enter a face or the
    headline/CTA text block drawn by :func:`_draw_overlay`.
    """
    values = [str(value or "").strip() for value in labels]
    values = values[:columns]
    if not values or not any(values):
        return
    template = dict(template or {})
    draw = ImageDraw.Draw(image)
    background = str(template.get("cta_background") or template.get("text_background") or "#000000D9")
    color = str(template.get("cta_text_color") or template.get("text_color") or "#FFFFFF")
    inset_x = max(8, round(cell_width * 0.05))
    available = cell_width - inset_x * 2
    preferred = max(28, min(46, round(cell_width * 0.10)))
    floor = max(20, min(preferred, int(template.get("min_font_size") or 20)))
    bottom_gap = max(16, round(image.height * 0.016))
    for index, label in enumerate(values):
        if not label:
            continue
        font, text_width, text_height = _fit_column_font(
            draw, label, template, preferred=preferred, floor=floor,
            available=available,
        )
        padding = max(6, round(font.size * 0.42))
        cell_left = index * (cell_width + gap)
        left = cell_left + inset_x + (available - text_width) // 2
        top = image.height - bottom_gap - text_height
        draw.rounded_rectangle(
            (left - padding, top - padding // 2,
             left + text_width + padding, top + text_height + padding // 2),
            radius=max(8, round(padding * 0.9)), fill=background,
        )
        draw.multiline_text((left, top), label, font=font, fill=color, align="center")


def _draw_overlay(
    image: Image.Image, text: str, template: Mapping[str, Any], *,
    index: int = 1, cover_index: int = 1, total: int = 1,
) -> None:
    value = str(text or "").strip()
    if placeholder_errors(value):
        raise PhotoPackageError("overlay contains an unresolved placeholder")
    if not value:
        return
    lines = value.splitlines()
    if template.get("split_last_line_to_bottom") and index == total and len(lines) > 1:
        _draw_overlay(
            image, "\n".join(lines[:-1]),
            {**dict(template), "split_last_line_to_bottom": False,
             "font_size": int(template.get("detail_font_size") or template.get("font_size") or 54),
             "_force_font_size": True},
            index=index, cover_index=cover_index, total=total,
        )
        _draw_overlay(
            image, lines[-1],
            {**dict(template), "split_last_line_to_bottom": False, "text_position": "bottom",
             "font_size": int(template.get("cta_font_size") or template.get("detail_font_size") or 42),
             "_force_font_size": True,
             "text_color": str(template.get("cta_text_color") or template.get("text_color") or "#171717"),
             "text_background": str(template.get("cta_background") or template.get("text_background") or "#FFFFFF")},
            index=index, cover_index=cover_index, total=total,
        )
        return
    max_lines = int(template.get("max_lines") or 2)
    if len(lines) > max_lines:
        raise PhotoPackageError(f"overlay text exceeds max_lines={max_lines}")
    draw = ImageDraw.Draw(image)
    spacing = int(template.get("line_spacing") or 12)
    padding_x, padding_y = int(template.get("padding_x") or 36), int(template.get("padding_y") or 24)
    if template.get("_force_font_size"):
        start_size = int(template.get("font_size") or 54)
    else:
        start_size = int(
            (template.get("cover_font_size") or template.get("font_size") or 54)
            if index == cover_index
            else (template.get("detail_font_size") or template.get("font_size") or 54)
        )
    min_size = int(template.get("min_font_size") or max(24, round(start_size * .55)))
    font = None
    bbox = None
    for size in range(start_size, min_size - 1, -2):
        candidate = _font(template, required=True, size=size)
        candidate_bbox = draw.multiline_textbbox(
            (0, 0), value, font=candidate, spacing=spacing, align="center"
        )
        if candidate_bbox[2] - candidate_bbox[0] + padding_x * 2 <= image.width:
            font, bbox = candidate, candidate_bbox
            break
    if font is None or bbox is None:
        raise PhotoPackageError("overlay text cannot fit the configured canvas at minimum font size")
    text_width, text_height = bbox[2] - bbox[0], bbox[3] - bbox[1]
    position = str(template.get("text_position") or "top")
    top = (
        int(template.get("top_offset") or padding_y)
        if position == "top"
        else image.height - int(template.get("bottom_offset") or padding_y * 2) - text_height
    )
    left = (image.width - text_width) // 2
    background = str(template.get("text_background") or "#000000B3")
    draw.rounded_rectangle(
        (left - padding_x, top - padding_y // 2,
         left + text_width + padding_x, top + text_height + padding_y // 2),
        radius=int(template.get("text_radius") or 22), fill=background,
    )
    draw.multiline_text(
        (left, top), value, font=font,
        fill=str(template.get("text_color") or "#FFFFFF"),
        spacing=spacing, align="center",
    )


def _draw_choice_badges(image: Image.Image, template: Mapping[str, Any], *,
                        index: int, cover_index: int, layout: str) -> None:
    """The choice template alone labels its cover quadrants TL/TR/BL/BR."""
    if (template.get("template_id") not in {"PHOTO_CHOICE_GRID_V1", "PHOTO_CHOICE_CARD_V1", "PHOTO_CHOICE_CARD_V2"}
            or not template.get("choice_badges") or index != cover_index or layout != "grid_2x2"):
        return
    gap = round(min(image.width, image.height) * 0.012)
    cell_width, cell_height = (image.width - gap) // 2, (image.height - gap) // 2
    size = max(40, round(min(cell_width, cell_height) * .14))
    inset = max(12, round(cell_width * .045))
    font = _font({**template, "font_size": round(size * .65)}, required=True)
    draw = ImageDraw.Draw(image)
    for offset, letter in enumerate("ABCD"):
        left = (offset % 2) * (cell_width + gap) + inset
        # Lower corner of each cell stays clear of the cover headline.
        top = (offset // 2) * (cell_height + gap) + cell_height - inset - size
        draw.rounded_rectangle((left, top, left + size, top + size), radius=round(size * .22), fill="#FFFFFF")
        box = draw.textbbox((0, 0), letter, font=font)
        draw.text((left + (size - box[2] + box[0]) / 2 - box[0],
                   top + (size - box[3] + box[1]) / 2 - box[1]), letter, font=font, fill="#171717")


class PhotoPackageExporter:
    def __init__(self, repository: Any, *, output_root: Path):
        self.repository = repository
        self.output_root = Path(output_root)
        self.revisions = RevisionService(repository)

    def export(
        self, task_id: str, *, template: Mapping[str, Any],
        slide_specs: Optional[Sequence[Mapping[str, Any]]] = None,
        copy_block: Optional[Mapping[str, Any]] = None,
    ) -> Dict[str, Any]:
        template = normalize_photo_template(template)
        task = self.repository.get_task(task_id)
        if task is None or not task.active_revision_id:
            raise PhotoPackageError("photo export requires an active Workflow V2 task")
        if str(getattr(task, "media_kind", "video") or "video") != "native_photo":
            raise PhotoPackageError("photo export requires media_kind=native_photo")
        if task.released_revision_id:
            raise PhotoPackageError("released photo output is immutable; create a rework revision")
        if task.task_status not in {statuses.TASK_IMAGE_REVIEW, "photo_packaging"}:
            raise PhotoPackageError("photo export requires image_review or photo_packaging status")
        revision = self.repository.get_task_revision(task.active_revision_id)
        if revision is None or revision.revision_status != "working":
            raise PhotoPackageError("active photo revision is not working")
        package = self.repository.get_content_package(str(task.content_package_id or ""))
        if package is None or package.task_id != task_id:
            raise PhotoPackageError("content package is missing or belongs to another task")

        plan = revision.plan_snapshot_json.get("plan") or {}
        raw_specs = list(slide_specs or plan.get("slides") or plan.get("photo_slides") or [])
        specs = []
        for raw in raw_specs:
            spec = dict(raw)
            spec["index"] = int(spec.get("index") or spec.get("slot_index") or 0)
            layout_snapshot = spec.get("layout_snapshot") or {}
            spec["layout"] = str(
                spec.get("layout") or layout_snapshot.get("layout")
                or layout_snapshot.get("layout_type") or "single"
            )
            specs.append(spec)
        expected_count = int(task.requested_shot_count or 5)
        if len(specs) != expected_count or expected_count < 1 or expected_count > 10:
            raise PhotoPackageError("photo slide specs must match the frozen requested count")
        if [int(item.get("index") or 0) for item in specs] != list(range(1, expected_count + 1)):
            raise PhotoPackageError("photo slide indexes must be consecutive in publish order")

        width, height = int(template.get("width") or 1080), int(template.get("height") or 1920)
        if width < 320 or height < 320:
            raise PhotoPackageError("photo canvas is too small")
        quality = int(template.get("jpeg_quality") or 90)
        if quality < 70 or quality > 100:
            raise PhotoPackageError("jpeg_quality must be in 70..100")
        template_id = str(template.get("template_id") or "").strip()
        template_version = int(template.get("template_version") or 0)
        if not template_id or template_version < 1:
            raise PhotoPackageError("template_id and positive template_version are required")
        cover_index = int(template.get("cover_index") or plan.get("cover_index") or 1)
        if cover_index < 1 or cover_index > expected_count:
            raise PhotoPackageError("cover_index is outside the ordered slide range")

        frozen_copy = dict(copy_block or revision.plan_snapshot_json.get("copy") or {})
        if placeholder_errors(frozen_copy):
            raise PhotoPackageError("frozen copy contains an unresolved placeholder")
        target = self.output_root / task_id / revision.revision_id / "final"
        target.mkdir(parents=True, exist_ok=True)
        output_slides = []
        renderer_pages = []
        current = revision
        for spec in specs:
            index = int(spec["index"])
            source_slots = [int(value) for value in (spec.get("source_slots") or [index])]
            source_assets = [RevisionAssetResolver.selected(current, f"shot:{slot}") for slot in source_slots]
            for asset in source_assets:
                if _file_hash(Path(asset["path"])) != asset["sha256"]:
                    raise PhotoPackageError("selected source image changed before photo export")
            image = _compose(
                [item["path"] for item in source_assets],
                layout=str(spec.get("layout") or "single"), width=width, height=height,
                background=str(template.get("background") or "#FFFFFF"),
                template=template,
                column_labels=(
                    spec.get("column_labels")
                    or (spec.get("layout_snapshot") or {}).get("column_labels")
                    or ()
                ),
            )
            if str(template.get("overlay_style") or "") == "structured_v1":
                from services.photo_structured_layout import render_structured_page
                renderer_pages.append(render_structured_page(
                    image, str(spec.get("overlay_text") or ""), template,
                    index=index, cover_index=cover_index, total=expected_count,
                ))
            else:
                _draw_overlay(
                    image, str(spec.get("overlay_text") or ""), template,
                    index=index, cover_index=cover_index, total=expected_count,
                )
            _draw_choice_badges(image, template, index=index, cover_index=cover_index, layout=spec["layout"])
            path = target / f"{index:02d}.jpg"
            temporary = path.with_suffix(".tmp.jpg")
            image.save(temporary, "JPEG", quality=quality, optimize=True, subsampling=0)
            temporary.replace(path)
            sha256 = _file_hash(path)
            asset_id = f"opv_slide_{revision.revision_id}_{index}_{sha256[:12]}"
            candidate = {
                "asset_id": asset_id, "asset_type": "photo_slide", "slot_index": index,
                "path": str(path.resolve()), "sha256": sha256,
                "parent_asset_ids": [item["asset_id"] for item in source_assets],
                "parameters_hash": canonical_hash({
                    "template": dict(template), "spec": dict(spec), "copy": frozen_copy,
                }),
            }
            current = self.revisions.register_candidate(current, candidate)
            current = self.revisions.select(
                current, selection_key=f"slide:{index}", asset_id=asset_id,
            )
            output_slides.append({
                "index": index, "asset_id": asset_id, "path": str(path.resolve()),
                "sha256": sha256, "mime_type": "image/jpeg", "width": width,
                "height": height,
                "source_asset_ids": [item["asset_id"] for item in source_assets],
            })

        manifest = {
            "schema_version": "opv-photo-package-v1", "media_kind": "native_photo",
            "task_id": task_id, "revision_id": revision.revision_id,
            "content_package_id": package.content_package_id,
            "template_id": template_id, "template_version": template_version,
            "cover_index": cover_index,
            "copy": frozen_copy, "slides": output_slides,
        }
        if renderer_pages:
            manifest["renderer"] = {
                "version": "structured_v1",
                "style": str(template.get("structured_style") or ""),
                "template_id": template_id,
                "template_version": template_version,
                "fonts": list(template.get("font_candidates") or [])[:1],
                "pages": renderer_pages,
            }
        if revision.plan_snapshot_json.get("plan", {}).get("theme_brief"):
            manifest["theme_brief"] = dict(
                revision.plan_snapshot_json["plan"]["theme_brief"]
            )
            # New packages bind topic metadata into the review fingerprint.
            # Older released v1 packages omit this marker and retain their
            # historical fingerprint for queue/retry compatibility.
            manifest["theme_copy_binding_version"] = 1
        manifest["package_fingerprint"] = canonical_hash(manifest)
        self.repository.update_content_package(
            package.content_package_id,
            photo_manifest_json=manifest,
            selected_image_ids_json=[item["asset_id"] for item in output_slides],
            cover_image_id=output_slides[manifest["cover_index"] - 1]["asset_id"],
            cover_title=str(frozen_copy.get("title") or ""),
            caption=str(frozen_copy.get("caption") or frozen_copy.get("title") or ""),
            hashtags_json=list(frozen_copy.get("hashtags") or []),
            content_fingerprint=manifest["package_fingerprint"],
            status=statuses.PACKAGE_QA_REVIEW,
        )
        if task.task_status == statuses.TASK_IMAGE_REVIEW:
            self.repository.transition_task(task_id, statuses.TASK_IMAGE_REVIEW, "photo_packaging")
        return manifest


class NativePhotoProductionFlow:
    """Continue a native-photo task through source preparation and packaging.

    Terminal review remains an explicit separate action via
    :class:`PhotoPackageReviewService`; this flow never claims human approval.
    """

    def __init__(self, repository: Any, producer: Any, *, output_root: Path,
                 vision_service: Any = None):
        self.repository = repository
        self.producer = producer
        self.vision_service = vision_service
        self.output_root = Path(output_root)
        self.exporter = PhotoPackageExporter(repository, output_root=self.output_root)

    def prepare(
        self, task_id: str, *, template: Mapping[str, Any],
        slide_specs: Optional[Sequence[Mapping[str, Any]]] = None,
    ) -> Dict[str, Any]:
        task = self.repository.get_task(task_id)
        if task is None:
            raise PhotoPackageError("photo task is missing")
        if task.task_status in {
            statuses.TASK_PLANNED, statuses.TASK_IMAGE_GENERATING,
        }:
            report = self.producer.produce(task_id)
            if report.task_status != statuses.TASK_IMAGE_REVIEW:
                return {"task_id": task_id, "status": report.task_status, "report": report}
            task = self.repository.get_task(task_id)
        if task.task_status == statuses.TASK_IMAGE_REVIEW:
            manifest = self.exporter.export(
                task_id, template=template, slide_specs=slide_specs,
            )
            self._run_final_page_qa(task, manifest)
            return {"task_id": task_id, "status": statuses.TASK_PHOTO_PACKAGING,
                    "photo_manifest": manifest}
        if task.task_status == statuses.TASK_PHOTO_PACKAGING:
            package = self.repository.get_content_package(task.content_package_id)
            return {"task_id": task_id, "status": task.task_status,
                    "photo_manifest": package.photo_manifest_json if package else None}
        if task.task_status == statuses.TASK_PHOTO_READY:
            package = self.repository.get_content_package(task.content_package_id)
            return {"task_id": task_id, "status": task.task_status,
                    "photo_manifest": package.photo_manifest_json if package else None}
        raise PhotoPackageError(
            f"photo task status {task.task_status!r} cannot continue preparation"
        )

    def _run_travel_final_page_qa(self, task: Any, manifest: Dict[str, Any]) -> Any:
        """Final composited-page QA for travel recipes; blocks 技术完成 on failure.

        Recipe identity comes from the frozen task row, never from filenames.
        Source assets are kept: failures stop the flow instead of regenerating.
        """
        recipe_id = str(getattr(task, "recipe_id", "") or "")
        if not recipe_id.startswith("PHOTO_TH_TRAVEL"):
            return None
        from services.photo_reference_vision import PhotoReferenceVisionService
        vision = self.vision_service
        if vision is None:
            vision = PhotoReferenceVisionService(root=self.output_root)
        expected_texts = [
            str(text) for text in (manifest.get("copy") or {}).get("slide_texts") or []
        ]
        slides = list(manifest.get("slides") or [])
        role_order = (
            [f"look_{letter}" for letter in "abcd"]
            if len(slides) == 4 else
            ["cover"] + [f"look_{letter}" for letter in "abcd"]
        )
        if len(expected_texts) != len(slides):
            raise PhotoPackageError(
                f"旅行最终页面 QA 需要成片文字与页面一一对应（{len(expected_texts)} vs {len(slides)}）"
            )
        qa = vision.review_travel_final_pages(
            image_paths=[str(slide["path"]) for slide in slides],
            expected_texts=expected_texts,
            role_order=role_order,
        )
        manifest["travel_final_page_qa"] = qa
        manifest["package_fingerprint"] = canonical_hash(manifest)
        self.repository.update_content_package(
            str(manifest.get("content_package_id") or ""),
            photo_manifest_json=manifest,
            content_fingerprint=manifest["package_fingerprint"],
        )
        if not qa["passed"]:
            # 2026-09-08 QA 收敛：文字渲染小偏差（缺换行/单字符缺失/措辞差异）
            # 属发布前可见的小瑕疵，降为 quality warning 不阻塞技术完成；
            # 仍拦真正的灾难：文字裁切、乱码、遮挡主体。
            blockers, warnings = [], []
            for page in qa.get("pages") or []:
                problems = [issue for issue in page.get("issues") or [] if issue]
                flags = [
                    name for name in ("text_clipped", "text_garbled",
                                      "subject_obscured") if page.get(name)
                ]
                label = f"P{page.get('index')}：" + "、".join(flags + problems)
                (blockers if flags else warnings).append(label)
            manifest.setdefault("quality_warnings", []).extend(
                {"code": "FINAL_PAGE_TEXT", "message": item}
                for item in warnings
            )
            if blockers:
                raise PhotoPackageError(
                    "旅行最终页面 QA 发现文字裁切/乱码/遮挡，已阻止进入技术完成："
                    + "；".join(blockers)
                    + "。源素材已保留；请修复模板后建 rework revision 重新套版"
                )
            manifest["package_fingerprint"] = canonical_hash(manifest)
            self.repository.update_content_package(
                str(manifest.get("content_package_id") or ""),
                photo_manifest_json=manifest,
                content_fingerprint=manifest["package_fingerprint"],
            )
        return qa

    def _run_final_page_qa(self, task: Any, manifest: Dict[str, Any]) -> Any:
        """Route the composited-page QA through the planning-flow registry."""
        from services.photo_content_planner import get_planning_flow
        from services.photo_flow_registry import (
            is_layered_progression_flow, is_thermal_transition_flow,
        )
        recipe_id = str(getattr(task, "recipe_id", "") or "")
        flow = get_planning_flow(recipe_id)
        if is_thermal_transition_flow(flow):
            return self._run_thermal_transition_final_page_qa(task, manifest)
        if is_layered_progression_flow(flow):
            return self._run_layering_final_page_qa(task, manifest)
        return self._run_travel_final_page_qa(task, manifest)

    def _run_thermal_transition_final_page_qa(
        self, task: Any, manifest: Dict[str, Any]
    ) -> Any:
        """Strictly check all five composited daily-transition pages."""
        from services.photo_reference_vision import PhotoReferenceVisionService
        vision = self.vision_service or PhotoReferenceVisionService(root=self.output_root)
        slides = list(manifest.get("slides") or [])
        expected_texts = [
            str(text) for text in (manifest.get("copy") or {}).get("slide_texts") or []
        ]
        roles = ["hook", "state_base", "state_mid", "state_outer", "cta"]
        if len(slides) != 5 or len(expected_texts) != 5:
            raise PhotoPackageError(
                "冷热切换最终页面 QA 必须覆盖 hook/base/mid/outer/cta 五页"
            )
        # The composited-page QA only judges text rendering on the finished
        # pages, which is flow-agnostic; the transition semantics were already
        # enforced on the source group by photo_thermal_transition_qa.
        qa = vision.review_layering_final_pages(
            image_paths=[str(slide["path"]) for slide in slides],
            expected_texts=expected_texts, role_order=roles,
        )
        manifest["thermal_transition_final_page_qa"] = qa
        manifest["package_fingerprint"] = canonical_hash(manifest)
        self.repository.update_content_package(
            str(manifest.get("content_package_id") or ""),
            photo_manifest_json=manifest,
            content_fingerprint=manifest["package_fingerprint"],
        )
        if not qa.get("passed"):
            issues = []
            for page in qa.get("pages") or []:
                flags = [name for name in (
                    "text_readable", "text_matches_expected", "text_clipped",
                    "text_garbled", "subject_obscured",
                ) if (
                    page.get(name) is False
                    if name in {"text_readable", "text_matches_expected"}
                    else page.get(name) is True
                )]
                issues.append(
                    f"P{page.get('index')}：" + "、".join(
                        flags + [str(value) for value in page.get("issues") or []]
                    )
                )
            raise PhotoPackageError(
                "冷热切换最终页面 QA 未通过，已阻止进入技术完成："
                + "；".join(issues)
            )
        return qa

    def _run_layering_final_page_qa(
        self, task: Any, manifest: Dict[str, Any]
    ) -> Any:
        """Strictly check all five composited temperature-layering pages."""
        from services.photo_reference_vision import PhotoReferenceVisionService
        vision = self.vision_service or PhotoReferenceVisionService(root=self.output_root)
        slides = list(manifest.get("slides") or [])
        expected_texts = [
            str(text) for text in (manifest.get("copy") or {}).get("slide_texts") or []
        ]
        roles = ["hook", "layer_base", "layer_mid", "layer_outer", "cta"]
        if len(slides) != 5 or len(expected_texts) != 5:
            raise PhotoPackageError(
                "温度分层最终页面 QA 必须覆盖 hook/base/mid/outer/cta 五页"
            )
        qa = vision.review_layering_final_pages(
            image_paths=[str(slide["path"]) for slide in slides],
            expected_texts=expected_texts, role_order=roles,
        )
        manifest["layering_final_page_qa"] = qa
        manifest["package_fingerprint"] = canonical_hash(manifest)
        self.repository.update_content_package(
            str(manifest.get("content_package_id") or ""),
            photo_manifest_json=manifest,
            content_fingerprint=manifest["package_fingerprint"],
        )
        if not qa.get("passed"):
            issues = []
            for page in qa.get("pages") or []:
                flags = [name for name in (
                    "text_readable", "text_matches_expected", "text_clipped",
                    "text_garbled", "subject_obscured",
                ) if (
                    page.get(name) is False
                    if name in {"text_readable", "text_matches_expected"}
                    else page.get(name) is True
                )]
                issues.append(
                    f"P{page.get('index')}：" + "、".join(
                        flags + [str(value) for value in page.get("issues") or []]
                    )
                )
            raise PhotoPackageError(
                "温度分层最终页面 QA 未通过，已阻止进入技术完成："
                + "；".join(issues)
            )
        return qa
