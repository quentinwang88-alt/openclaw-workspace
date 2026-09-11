"""Generate and qualify the four hairstyle sources for ``mx_wig_choice_v1``.

MX-only module: Thai flows never import this.  One :meth:`prepare` call turns a
frozen per-item hair plan (four options) into four generated portrait sources,
a group-level QA verdict, and a qualified ``AssetSet`` with hair-shaped
attributes (never outerwear/bottom evidence).

Resume contract: the supply manifest records an ``input_hash`` over the flow
version, persona reference bytes, planning content and channel parameters.  A
re-run with the same hash only generates the roles that are still missing;
finished sources are never regenerated or re-billed.  Retakes force specific
roles through :meth:`regenerate_roles` and keep the retired bytes on disk.
"""

from __future__ import annotations

import hashlib
import json
from pathlib import Path
from typing import Any, Dict, List, Mapping, Optional, Sequence

from domain.models import AssetSet
from services.asset_set_service import AssetSetService, validate_asset_set
from services.image_generator import (
    ShotGenerationRequest,
    check_portrait_916,
    read_image_dimensions,
)
from services.photo_wig_flow import (
    MX_WIG_ASSET_SET_KEY,
    MX_WIG_CATEGORY,
    MX_WIG_LOGIC_KEY,
    MX_WIG_SOURCE_ROLES,
)
from services.persona_pack import select_identity_references


class PhotoWigSupplyError(ValueError):
    pass


WIG_SUPPLY_SCHEMA_VERSION = "opv-wig-supply-manifest-v1"
WIG_PROMPT_VERSION = "opv-photo-mx-wig-prompt-v1"


# ---------------------------------------------------------------------------
# Prompt composition (the MX wig prompt builder)
# ---------------------------------------------------------------------------

_HAIR_LENGTH_ZH = {
    "chin_bob": "下颌附近的经典短 Bob",
    "neck_bob": "颈背长度的短 Bob",
    "collarbone": "锁骨长度中长发",
    "long": "长发（胸口以下）",
}
_HAIR_TEXTURE_ZH = {
    "straight": "顺直",
    "soft_wave": "柔和波浪",
    "wave": "波浪",
    "curl": "卷发",
}


def _zh_or_raw(value: Any, fallback_map: Mapping[str, str]) -> str:
    text = str(value or "").strip()
    if text:
        return text
    return ""


def option_prompt_lines(option: Mapping[str, Any]) -> List[str]:
    """Render one frozen hair option as executable Chinese prompt direction."""
    length = option.get("length_zh") or _HAIR_LENGTH_ZH.get(
        str(option.get("length") or ""), ""
    )
    texture = option.get("texture_zh") or _HAIR_TEXTURE_ZH.get(
        str(option.get("texture") or ""), ""
    )
    color = option.get("color_zh") or option.get("color") or ""
    parting = option.get("parting_zh") or ""
    silhouette = option.get("silhouette_zh") or ""
    framing = option.get("framing_zh") or ""
    bangs = option.get("bangs_zh") or "无厚重刘海遮挡发际线"
    lines = [
        f"款式方向：{option.get('style_zh') or option.get('label_es') or ''}（{option.get('label_es') or ''}）",
        f"长度：{length}；整体轮廓：{silhouette}",
        f"卷度/纹理：{texture or '自然顺直'}；发色方向：{color}",
        f"分缝：{parting or '自然分缝'}；额前处理：{bangs}",
        f"取景要求：{framing or '胸部以上的半身胸像，长发时扩大取景让发尾完整可见'}",
    ]
    return [f"{index}. {line}" for index, line in enumerate(lines, 1)]


def reference_usage_lines(reference_roles: Mapping[str, Sequence[str]],
                          ordered_paths: Sequence[str]) -> List[str]:
    """【输入图片用途】lines exactly matching the actual upload order."""
    role_label = {
        "persona_identity_images": "人物身份（脸型、肤色、年龄感的唯一权威）",
        "hair_inspiration_images": "发型灵感：只参考发型长度/轮廓/卷度，绝不复制其中的模特脸、妆面或背景",
        "environment_reference_images": "环境参考：只参考场所与光线氛围，不复制其中人物",
        "visual_style_reference_images": "画面风格参考：只参考光线、色调与构图",
    }
    lines = []
    for index, path in enumerate(ordered_paths, 1):
        matched = [
            label for key, label in role_label.items()
            if str(path) in {str(value) for value in reference_roles.get(key) or []}
        ]
        lines.append(f"输入图片{index}用途：{'；'.join(matched) if matched else '一般参考'}。")
    return lines


def compose_wig_prompt(
    option: Mapping[str, Any], plan_item: Mapping[str, Any], *,
    persona: Mapping[str, Any], reference_roles: Mapping[str, Sequence[str]],
    ordered_reference_paths: Sequence[str],
) -> str:
    """Full executable prompt for one MX wig option page.

    Deliberately separate from ``compose_shot_prompt``: a wig portrait must NOT
    inherit the clothing contract (冻结穿搭/外套鞋履/全身比例/保持原发型), and
    the persona lock is scoped to face identity only — hair change is the point
    of this flow.
    """
    persona_name = str(persona.get("name") or persona.get("persona_id") or "人物 A")
    lines = [
        "生成一张竖屏 9:16 的真实美妆创作者发型展示照片"
        "（TikTok 自然流图文素材，不是电商商品图或影棚大片）。",
        "",
        "【人物身份锁】",
        "输入图片1是人物身份的唯一权威：保持其中的脸部特征、脸型、自然肤色和年龄感；"
        "不得更换五官、不得放大眼睛或削尖下巴、不得磨皮成塑料皮肤、不得把主角换成另一张脸。",
        "参考图中的发型只是人物现状，不代表目标发型：本页必须按【本页发型计划】整体更换发型，"
        "禁止照搬参考图的发型、长度、卷度或分缝。",
        f"人物：{persona_name}。表情与视线自然放松，头部端正不歪斜；不照搬参考照片的固定笑容。",
        "",
        "【本页发型计划】（必须执行）",
        *option_prompt_lines(option),
        "发丝要求：发际线自然、发缝清楚、头发边缘清晰不糊化；保留真实发丝纹理，"
        "不要把头发画成一体化的塑料壳。",
        "",
        "【本页画面】",
        f"主题氛围：{plan_item.get('topic_zh') or '周末换个发型'}",
        f"服装方向：{plan_item.get('wardrobe_direction_zh') or '简单的纯色上衣，与头发形成明暗对比，不抢焦点'}",
        f"妆容方向：{plan_item.get('makeup_direction_zh') or '自然精致的日常妆，无过度磨皮'}",
        f"背景与光线：{plan_item.get('scene_prompt_zh') or '室内窗边柔和自然光，背景简洁虚化'}",
        "构图：人物为绝对主体，头顶上方留出少量呼吸空间；四张图保持同一人物、同一类光线与构图，"
        "只有发型按各自计划不同。",
        "",
        "【输入图片用途】",
        *reference_usage_lines(reference_roles, ordered_reference_paths),
        "",
        "【画面风格】",
        "真实手机感创作者照片：自然光、白平衡自然；保留真实皮肤、发丝和织物纹理。",
        "精致但不失真：不刻意添加毛孔或疲态，也不过度磨皮。",
        "",
        "【通用负向要求】",
        "不要文字、字幕、水印或杜撰 Logo；不要畸形手指或多余肢体；不要广告大片灯光；",
        "不要明显歪头；不要把本页发型画成其他选项的发型；不要复制任何参考图中模特的脸。",
        "只输出一张完整画面。",
    ]
    return "\n".join(lines)


# ---------------------------------------------------------------------------
# Supply service
# ---------------------------------------------------------------------------


def _safe(value: str) -> str:
    return "".join(ch if ch.isalnum() or ch in "-_" else "_" for ch in str(value))[:120]


def _hash_payload(value: Any) -> str:
    return hashlib.sha256(
        json.dumps(value, ensure_ascii=False, sort_keys=True,
                   separators=(",", ":")).encode("utf-8")
    ).hexdigest()


def _persona_digest(persona: Mapping[str, Any]) -> Dict[str, Any]:
    selected = select_identity_references(persona)
    return {
        "persona_pack_id": str(
            persona.get("persona_id") or persona.get("ref_id") or ""
        ),
        "identity_reference_sha256s": [
            str(item.get("sha256") or "") for item in selected
        ],
        "identity_reference_paths": [str(item.get("local_path") or "") for item in selected],
    }


class PhotoWigSupplyService:
    """Generate/keep the four hair sources for one planned post item."""

    def __init__(self, generator: Any, root: Path, qa_reviewer: Any = None):
        self.generator = generator
        self.root = Path(root)
        self.qa_reviewer = qa_reviewer

    # -- manifest helpers -------------------------------------------------

    def item_dir(self, record_id: str) -> Path:
        return self.root / "wig_choice_supply" / _safe(record_id)

    def manifest_path(self, record_id: str) -> Path:
        return self.item_dir(record_id) / "wig_supply_manifest.json"

    def input_hash(
        self, *, plan_item: Mapping[str, Any], persona: Mapping[str, Any],
        reference_paths: Sequence[str], content_requirement: str,
        channel_params: Mapping[str, Any],
    ) -> str:
        return _hash_payload({
            "flow": "mx_wig_choice_v1",
            "prompt_version": WIG_PROMPT_VERSION,
            "plan": plan_item,
            "persona": _persona_digest(persona),
            "reference_sha256s": [
                hashlib.sha256(Path(path).read_bytes()).hexdigest()
                for path in reference_paths if Path(path).is_file()
            ],
            "content_requirement": content_requirement,
            "channel_params": dict(channel_params),
        })

    def load_manifest(self, record_id: str) -> Optional[Dict[str, Any]]:
        path = self.manifest_path(record_id)
        if not path.is_file():
            return None
        return json.loads(path.read_text(encoding="utf-8"))

    def _save_manifest(self, record_id: str, manifest: Mapping[str, Any]) -> None:
        path = self.manifest_path(record_id)
        path.parent.mkdir(parents=True, exist_ok=True)
        temporary = path.with_suffix(".tmp")
        temporary.write_text(
            json.dumps(manifest, ensure_ascii=False, indent=1), encoding="utf-8"
        )
        temporary.replace(path)

    # -- generation -------------------------------------------------------

    def _ordered_references(
        self, persona: Mapping[str, Any], reference_paths: Sequence[str],
        reference_roles: Mapping[str, Sequence[str]],
    ) -> List[str]:
        identity = [str(item.get("local_path") or "")
                    for item in select_identity_references(persona)]
        identity = [path for path in identity if Path(path).is_file()]
        ordered = list(dict.fromkeys(identity + [str(path) for path in reference_paths]))
        for path in ordered:
            if not Path(path).is_file():
                raise PhotoWigSupplyError(f"参考图缺失：{path}")
        return ordered

    def _generate_role(
        self, *, record_id: str, option: Mapping[str, Any], plan_item: Mapping[str, Any],
        persona: Mapping[str, Any], reference_paths: Sequence[str],
        reference_roles: Mapping[str, Sequence[str]], slot_index: int, attempt: int,
    ) -> Dict[str, Any]:
        role = str(option.get("role") or "")
        prompt = compose_wig_prompt(
            option, plan_item, persona=persona, reference_roles=reference_roles,
            ordered_reference_paths=reference_paths,
        )
        request = ShotGenerationRequest(
            task_id=_safe(record_id), slot_index=slot_index,
            slot_role=role, shot_version=attempt,
            plan_shot={"slot_index": slot_index, "slot_role": role,
                       "purpose": str(option.get("label_es") or "")},
            product={}, persona_snapshot=dict(persona),
            look_snapshot={}, scene_snapshot={},
            output_dir=str(self.item_dir(record_id)),
            continuity_reference_images=list(reference_paths),
            reference_roles=dict(reference_roles),
            prompt_override=prompt,
        )
        outcome = self.generator.generate_shot(request)
        if not outcome.ok or not outcome.image_path:
            raise PhotoWigSupplyError(
                f"选项 {option.get('label_es') or role} 图片生成失败："
                f"{outcome.error}（provider={outcome.provider} model={outcome.model}）"
            )
        dimensions = read_image_dimensions(str(outcome.image_path))
        if dimensions is None:
            raise PhotoWigSupplyError(f"选项 {role} 图片无法解码：{outcome.image_path}")
        if not check_portrait_916(dimensions):
            raise PhotoWigSupplyError(
                f"选项 {role} 图片不是 9:16 竖版：{dimensions}"
            )
        return {
            "role": role,
            "path": str(Path(outcome.image_path).resolve()),
            "sha256": hashlib.sha256(Path(outcome.image_path).read_bytes()).hexdigest(),
            "width": dimensions[0], "height": dimensions[1],
            "generation_provider": str(outcome.provider),
            "generation_model": str(outcome.model),
            "generation_request_id": str(outcome.request_id),
            "attempt": attempt,
            "planned_option": {key: option.get(key) for key in (
                "role", "label_es", "length", "texture", "color", "parting",
                "silhouette", "framing",
            )},
            "planned_option_hash": _hash_payload(option),
            "prompt_version": WIG_PROMPT_VERSION,
        }

    def prepare(
        self, *, record_id: str, plan_item: Mapping[str, Any],
        persona: Mapping[str, Any], reference_paths: Sequence[str] = (),
        reference_roles: Optional[Mapping[str, Sequence[str]]] = None,
        content_requirement: str = "", progress: Optional[Any] = None,
        channel_params: Optional[Mapping[str, Any]] = None,
    ) -> Dict[str, Any]:
        """Resume-safe generation of the four options; returns sources + QA."""
        roles = [str(role) for role in MX_WIG_SOURCE_ROLES]
        options = {str(item.get("role")): dict(item)
                   for item in plan_item.get("options") or []}
        missing_roles = [role for role in roles if role not in options]
        if missing_roles:
            raise PhotoWigSupplyError(
                f"发型计划缺少选项：{'、'.join(missing_roles)}"
            )
        roles_map = dict(reference_roles or {})
        ordered_refs = self._ordered_references(persona, reference_paths, roles_map)
        identity = _persona_digest(persona)
        resolved_channel = dict(channel_params or self._channel_params())
        input_hash = self.input_hash(
            plan_item=plan_item, persona=persona, reference_paths=ordered_refs,
            content_requirement=content_requirement,
            channel_params=resolved_channel,
        )
        manifest = self.load_manifest(record_id) or {}
        if manifest and manifest.get("input_hash") not in (None, input_hash):
            raise PhotoWigSupplyError("主题、人物或参考图已变化")
        manifest = {
            **manifest,
            "schema_version": WIG_SUPPLY_SCHEMA_VERSION,
            "execution_flow": "mx_wig_choice_v1",
            "record_id": record_id,
            "input_hash": input_hash,
            "channel_params": resolved_channel,
            "persona": identity,
            "reference_paths": ordered_refs,
            "reference_roles": {key: list(value) for key, value in roles_map.items()},
            "plan_hash": _hash_payload(plan_item),
            "content_requirement": content_requirement,
        }
        sources: Dict[str, Dict[str, Any]] = {
            str(item.get("role")): dict(item)
            for item in manifest.get("sources") or []
        }
        # Finished sources stay untouched; only missing or retired roles generate.
        for role in roles:
            item = sources.get(role)
            if item and Path(item.get("path") or "").is_file() and hashlib.sha256(
                    Path(item["path"]).read_bytes()).hexdigest() == item.get("sha256"):
                continue
            attempt = int((item or {}).get("attempt") or 0) + 1
            if progress:
                progress("asset_generated_pending", role=role, attempt=attempt)
            generated = self._generate_role(
                record_id=record_id, option=options[role], plan_item=plan_item,
                persona=persona, reference_paths=ordered_refs,
                reference_roles=roles_map,
                slot_index=roles.index(role) + 1, attempt=attempt,
            )
            if progress:
                progress("asset_generated", role=role)
            sources[role] = generated
            manifest["sources"] = [sources[key] for key in roles if key in sources]
            self._save_manifest(record_id, manifest)

        ordered_sources = [sources[role] for role in roles]
        hashes = [item["sha256"] for item in ordered_sources]
        if len(set(hashes)) != len(hashes):
            raise PhotoWigSupplyError("四款选项出现了完全相同的图片文件，不能作为四个不同发型")
        qa: Dict[str, Any] = dict(manifest.get("group_qa") or {})
        if self.qa_reviewer is not None and not qa.get("passed"):
            if progress:
                progress("qa_started")
            qa = self.qa_reviewer.review_group(
                persona_reference_path=(identity["identity_reference_paths"] or [""])[0],
                sources=ordered_sources, plan_item=plan_item,
            )
            manifest["group_qa"] = qa
            self._save_manifest(record_id, manifest)
            failed_roles = [str(role) for role in (qa.get("failed_roles") or [])]
            repaired: List[str] = []
            if failed_roles:
                # One targeted repair per failed role, then one group re-check.
                for role in failed_roles:
                    if role not in roles:
                        continue
                    retired = list(manifest.get("retired") or [])
                    retired.append({**sources[role], "retire_reason": "wig_group_qa"})
                    manifest["retired"] = retired
                    attempt = int(sources[role].get("attempt") or 0) + 1
                    sources[role] = self._generate_role(
                        record_id=record_id, option=options[role],
                        plan_item=plan_item, persona=persona,
                        reference_paths=ordered_refs, reference_roles=roles_map,
                        slot_index=roles.index(role) + 1, attempt=attempt,
                    )
                    repaired.append(role)
                    manifest["sources"] = [sources[key] for key in roles]
                    self._save_manifest(record_id, manifest)
                    if progress:
                        progress("repair_scheduled", roles=repaired, reason="wig_group_qa")
                ordered_sources = [sources[role] for role in roles]
                qa = self.qa_reviewer.review_group(
                    persona_reference_path=(identity["identity_reference_paths"] or [""])[0],
                    sources=ordered_sources, plan_item=plan_item,
                )
                manifest["group_qa"] = qa
                self._save_manifest(record_id, manifest)
            if not qa.get("passed"):
                detail = "；".join(
                    f"{role}: {'、'.join(issues)}"
                    for role, issues in (qa.get("issues") or {}).items()
                ) or str(qa.get("notes") or "")
                raise PhotoWigSupplyError(
                    "假发组级质检未通过（已保留素材，可按重拍列定向重生）："
                    + detail
                )
        manifest["sources"] = ordered_sources
        manifest["status"] = "complete"
        self._save_manifest(record_id, manifest)
        return {"sources": ordered_sources, "manifest_path": str(self.manifest_path(record_id)),
                "group_qa": qa}

    def regenerate_roles(self, record_id: str, roles: Sequence[str],
                         reason: str = "运营手动重拍") -> List[str]:
        """Retire specific roles so the next :meth:`prepare` regenerates them."""
        known = set(MX_WIG_SOURCE_ROLES)
        targets = [role for role in roles if role in known]
        if not targets:
            return []
        manifest = self.load_manifest(record_id)
        if manifest is None:
            raise PhotoWigSupplyError(f"该行没有假发供给清单：{record_id}")
        retired = list(manifest.get("retired") or [])
        remaining: List[Dict[str, Any]] = []
        for item in manifest.get("sources") or []:
            if str(item.get("role")) in targets:
                retired.append({**item, "retire_reason": reason})
            else:
                remaining.append(item)
        manifest["retired"] = retired
        manifest["sources"] = remaining
        manifest["status"] = "incomplete"
        manifest.pop("group_qa", None)
        self._save_manifest(record_id, manifest)
        return targets

    # -- asset registration -------------------------------------------------

    def _channel_params(self) -> Dict[str, Any]:
        """Channel identity feeding the resume hash (values, never secrets)."""
        import os

        return {
            "photo_channel": os.environ.get("OPV_PHOTO_CHANNEL", ""),
            "codex_image_model": os.environ.get("OPENAI_CODEX_IMAGE_MODEL", ""),
            "creatok_image_model": os.environ.get("OPV_CREATOK_IMAGE_MODEL", ""),
        }

    def register_asset_set(
        self, *, repository: Any, record_id: str, sources: Sequence[Mapping[str, Any]],
        plan_item: Mapping[str, Any], persona: Mapping[str, Any],
        choice_axis: str = "style",
    ) -> AssetSet:
        """Register the four generated sources as one qualified MX asset set.

        Mirrors the row shape of ``PhotoAssetSupplyService.qualify`` but with
        hair-shaped qualification evidence; clothing fields are never faked.
        """
        if len(sources) != 4:
            raise PhotoWigSupplyError("假发素材集必须恰好四个角色")
        options = {str(item.get("role")): item
                   for item in plan_item.get("options") or []}
        assets: List[Dict[str, Any]] = []
        approval_attributes: Dict[str, Any] = {}
        source_hashes: Dict[str, str] = {}
        identity = _persona_digest(persona)
        identity_id = "mx_wig_identity_" + (
            (identity["identity_reference_sha256s"] or [""])[:1][0][:16]
        )
        for index, source in enumerate(sources, 1):
            role = str(source.get("role") or "")
            option = options.get(role) or {}
            asset_id = f"{_safe(record_id)}_{role}_{str(source.get('sha256'))[:10]}"
            source_hashes[asset_id] = str(source.get("sha256") or "")
            letter = role.split("_")[-1].upper()
            label_es = str(option.get("label_es") or f"Opción {letter}")
            approval_attributes[asset_id] = {
                "identity_id": identity_id,
                # One group-level camera scale: options stay comparable; the
                # per-option framing (chest-up vs wider for long ends) is a
                # framing attribute, not a scale change.
                "camera_scale": "portrait_half_body",
                "length": str(option.get("length") or ""),
                "texture": str(option.get("texture") or ""),
                "color": str(option.get("color") or ""),
                "parting": str(option.get("parting") or ""),
                "silhouette": str(option.get("silhouette") or ""),
                "hairline_visible": True,
                "label_es": label_es,
                "planned_option_hash": str(source.get("planned_option_hash") or ""),
                "qa_role_passed": True,
            }
            assets.append({
                "asset_id": asset_id, "role": role,
                "path": str(source.get("path") or ""),
                "sha256": str(source.get("sha256") or ""),
                "tags": {"length": str(option.get("length") or ""),
                         "texture": str(option.get("texture") or ""),
                         "color": str(option.get("color") or "")},
                "display_label": {"es-MX": label_es, "zh-CN": str(option.get("label_zh") or label_es)},
                "content_plan": {
                    "item_id": str(record_id),
                    "look_signature": str(source.get("planned_option_hash") or ""),
                    "attributes": approval_attributes[asset_id],
                },
            })
        identity_digest = hashlib.sha256(
            ":".join(str(item.get("sha256")) for item in sources).encode()
        ).hexdigest()[:16]
        asset_set_id = f"ASSET_MX_WIG_CHOICE_GEN_{identity_digest}"
        # Content-addressed idempotency: identical sources reuse the exact
        # row.  New sources take the next free version for the key — the
        # table carries UNIQUE(asset_set_key, asset_set_version), so a
        # constant version would silently overwrite the previous row under a
        # foreign asset_set_id.
        existing = repository.get_asset_set(asset_set_id)
        if existing is not None:
            return existing
        current = repository.list_asset_sets(
            category_key=MX_WIG_CATEGORY, market="MX", status="enabled")
        version = max(
            (item.asset_set_version for item in current
             if item.asset_set_key == MX_WIG_ASSET_SET_KEY),
            default=0,
        ) + 1
        asset_set = AssetSet(
            asset_set_id=asset_set_id,
            asset_set_key=MX_WIG_ASSET_SET_KEY,
            asset_set_version=version,
            category_key=MX_WIG_CATEGORY, market="MX", status="enabled",
            tags_json={"use_cases": "pick_your_hair", "choice_axis": choice_axis,
                       "source": "mx_wig_reference_generated",
                       "theme_key": str(plan_item.get("theme_key") or "")},
            manifest_json={
                "assets": assets, "pairs": [],
                "content_approval": {
                    "schema_version": "opv-source-qualification-v1",
                    "reviewer": "system_wig_reference_generation",
                    "reviewer_type": "technical",
                    "allowed_logic_keys": [MX_WIG_LOGIC_KEY],
                    "source_hashes": source_hashes,
                    "attributes": approval_attributes,
                },
            },
        )
        validate_asset_set(asset_set, verify_files=True)
        return AssetSetService(repository).save(asset_set)
