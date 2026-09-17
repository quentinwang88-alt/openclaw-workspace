"""Stage operator-provided complete-look photos without requiring JSON input."""
from __future__ import annotations

import hashlib
import io
import json
import mimetypes
from pathlib import Path
from typing import Any, Mapping, Sequence

from PIL import Image, ImageOps

from services.image_generator import read_image_dimensions
from domain.models import AssetSet
from domain.photo_contracts import select_execution_profile
from services.asset_set_service import AssetSetService


class PhotoAssetSupplyError(ValueError):
    pass


# ``use_cases`` values whose flows freeze one base outfit across ordered
# states.  One set of source photos can only honestly represent one of them.
LAYERED_PROGRESSION_USE_CASE = frozenset({
    "temperature_dressing", "thermal_transition",
})


def _register_heif_support() -> None:
    """Best-effort HEIC/HEIF decode registration (iPhone uploads)."""
    try:
        import pillow_heif  # noqa: F401

        pillow_heif.register_heif_opener()
    except Exception:  # noqa: BLE001 - absence only narrows format support
        pass


_register_heif_support()


def normalize_image_bytes(content: bytes, name: str, content_type: str) -> tuple[bytes, str]:
    """Return JPG/PNG bytes; other Pillow-decodable formats (HEIC/WebP/BMP/
    TIFF) are transcoded to JPEG with EXIF orientation applied."""
    suffix = _suffix_static(name, content_type)
    if suffix:
        return content, suffix
    if not content:
        return b"", ""
    try:
        with Image.open(io.BytesIO(content)) as image:
            converted = ImageOps.exif_transpose(image.convert("RGB"))
            buffer = io.BytesIO()
            converted.save(buffer, "JPEG", quality=92)
            return buffer.getvalue(), ".jpg"
    except Exception:  # noqa: BLE001 - undecodable stays an explicit error
        return b"", ""


def _suffix_static(name: str, content_type: str) -> str:
    suffix = Path(name).suffix.lower()
    if suffix == ".jpeg":
        suffix = ".jpg"
    if suffix in {".jpg", ".png"}:
        return suffix
    mime = content_type.lower() or str(mimetypes.guess_type(name)[0] or "").lower()
    return {"image/jpeg": ".jpg", "image/png": ".png"}.get(mime, "")


class PhotoAssetSupplyService:
    """Download and describe candidate photos; qualification stays separate."""

    def __init__(self, client: Any, *, root: Path):
        self.client = client
        self.root = Path(root)

    @staticmethod
    def gap_summary(recipe: Any) -> str:
        spec = dict(getattr(recipe, "recipe_spec_json", {}) or {})
        roles = list((spec.get("asset_requirements") or {}).get("required_roles") or [])
        relations = list((spec.get("visual_rules") or {}).get("garment_relations") or [])
        relation_text = ""
        if any(item.get("relation") == "same_outerwear_different_bottom" for item in relations):
            relation_text = "；其中 B/C 需为同款外套、不同下装"
        return f"需要 {len(roles)} 张完整穿搭图（{', '.join(roles)}）{relation_text}"

    def stage(self, *, record_id: str, attachments: Sequence[Mapping[str, Any]],
              required_roles: Sequence[str], metadata: Mapping[str, Any] = None,
              verified_attributes: Mapping[str, Mapping[str, Any]] = None) -> dict[str, Any]:
        if len(attachments) != len(required_roles):
            raise PhotoAssetSupplyError(
                f"上传了 {len(attachments)} 张，当前主题需要 {len(required_roles)} 张完整穿搭图；"
                "请按当前主题要求的角色顺序上传"
            )
        folder = self.root / "staging" / self._safe(record_id)
        folder.mkdir(parents=True, exist_ok=True)
        files = []
        for index, (attachment, role) in enumerate(zip(attachments, required_roles), 1):
            if not isinstance(attachment, Mapping) or not attachment.get("file_token"):
                raise PhotoAssetSupplyError(f"第 {index} 个附件缺少 file_token")
            content, name, content_type, _size = self.client.download_attachment_bytes(dict(attachment))
            content, suffix = normalize_image_bytes(content, str(name or ""), str(content_type or ""))
            if not content or not suffix:
                raise PhotoAssetSupplyError(f"第 {index} 个附件不是支持的 JPG/PNG 图片")
            digest = hashlib.sha256(content).hexdigest()
            path = folder / f"{index:02d}_{role}_{digest[:16]}{suffix}"
            if not path.exists():
                path.write_bytes(content)
            dimensions = read_image_dimensions(str(path))
            if dimensions is None:
                raise PhotoAssetSupplyError(f"第 {index} 张图片无法解码")
            role_attributes = dict((verified_attributes or {}).get(str(role)) or {})
            files.append({
                "role": str(role), "path": str(path.resolve()), "sha256": digest,
                "width": dimensions[0], "height": dimensions[1],
                "source_file_token": str(attachment["file_token"]),
                **({"verified_attributes": role_attributes} if role_attributes else {}),
            })
        hashes = [str(item["sha256"]) for item in files]
        if len(set(hashes)) != len(hashes):
            raise PhotoAssetSupplyError("完整穿搭角色图不能使用内容完全相同的图片")
        manifest = {
            "schema_version": "opv-photo-asset-staging-v1",
            "record_id": record_id, "status": "pending_content_review", "files": files,
            "metadata": dict(metadata or {}),
        }
        manifest_path = folder / "manifest.json"
        manifest_path.write_text(json.dumps(manifest, ensure_ascii=False, indent=2), encoding="utf-8")
        return {**manifest, "manifest_path": str(manifest_path.resolve())}

    def stage_product_references(
        self, *, record_id: str, attachments: Sequence[Mapping[str, Any]]
    ) -> list[str]:
        """Download reusable product truth images; these are not post pages."""
        return self.stage_reference_images(
            record_id=record_id, attachments=attachments, reference_kind="product"
        )

    def stage_reference_images(
        self, *, record_id: str, attachments: Sequence[Mapping[str, Any]],
        reference_kind: str, allow_empty: bool = False,
    ) -> list[str]:
        """Download immutable reference bytes into a role-specific cache."""
        if not attachments:
            if allow_empty:
                # 方案 §6.5 零外部图行（外部合同零页）：无参考可下载，
                # 返回空列表交由规划器凭内容要求规划
                return []
            raise PhotoAssetSupplyError("请至少上传一张参考图")
        kind = str(reference_kind or "reference").strip().lower()
        folder_name = "product_references" if kind == "product" else f"{self._safe(kind)}_references"
        folder = self.root / folder_name / self._safe(record_id)
        folder.mkdir(parents=True, exist_ok=True)
        paths = []
        for index, attachment in enumerate(attachments, 1):
            if not isinstance(attachment, Mapping) or not attachment.get("file_token"):
                raise PhotoAssetSupplyError(f"第 {index} 个参考图附件缺少 file_token")
            content, name, content_type, _size = self.client.download_attachment_bytes(dict(attachment))
            content, suffix = normalize_image_bytes(content, str(name or ""), str(content_type or ""))
            if not content or not suffix:
                raise PhotoAssetSupplyError(f"第 {index} 张参考图不是支持的 JPG/PNG")
            digest = hashlib.sha256(content).hexdigest()
            path = folder / f"{index:02d}_{digest[:16]}{suffix}"
            if not path.exists():
                path.write_bytes(content)
            if read_image_dimensions(str(path)) is None:
                raise PhotoAssetSupplyError(f"第 {index} 张参考图无法解码")
            paths.append(str(path.resolve()))
        return paths

    def stage_existing(self, *, record_id: str, sources: Sequence[Mapping[str, Any]],
                       required_roles: Sequence[str], metadata: Mapping[str, Any] = None) -> dict[str, Any]:
        if len(sources) < len(required_roles):
            raise PhotoAssetSupplyError(
                f"旧穿搭任务仅找到 {len(sources)} 套不同 Look，当前主题需要 {len(required_roles)} 套"
            )
        folder = self.root / "staging" / self._safe(record_id)
        folder.mkdir(parents=True, exist_ok=True)
        files = []
        for index, (source, role) in enumerate(zip(sources, required_roles), 1):
            path = Path(str(source.get("path") or "")).expanduser().resolve()
            if not path.is_file():
                raise PhotoAssetSupplyError(f"旧穿搭素材 {index} 文件不存在")
            digest = hashlib.sha256(path.read_bytes()).hexdigest()
            if source.get("sha256") and source.get("sha256") != digest:
                raise PhotoAssetSupplyError(f"旧穿搭素材 {index} 哈希已变化")
            dimensions = read_image_dimensions(str(path))
            if dimensions is None:
                raise PhotoAssetSupplyError(f"旧穿搭素材 {index} 无法解码")
            files.append({
                "role": str(role), "path": str(path), "sha256": digest,
                "width": dimensions[0], "height": dimensions[1],
                "source_task_id": str(source.get("task_id") or ""),
                "source_look_ref": str(source.get("look_ref") or ""),
                "source_kind": str(source.get("source_kind") or "existing_outfit"),
                "generation_provider": str(source.get("generation_provider") or ""),
                "generation_model": str(source.get("generation_model") or ""),
                "generation_request_id": str(source.get("generation_request_id") or ""),
                "display_label": dict(source.get("display_label") or {}),
                "outerwear_signature": str(source.get("outerwear_signature") or ""),
                "planned_look_signature": str(source.get("planned_look_signature") or ""),
                "content_plan_item_id": str(source.get("content_plan_item_id") or ""),
                "planned_attributes": dict(source.get("planned_attributes") or {}),
            })
        manifest = {
            "schema_version": "opv-photo-asset-staging-v1", "record_id": record_id,
            "status": "pending_content_review", "source": "existing_outfit_tasks", "files": files,
            "metadata": dict(metadata or {}),
        }
        manifest_path = folder / "manifest.json"
        manifest_path.write_text(json.dumps(manifest, ensure_ascii=False, indent=2), encoding="utf-8")
        return {**manifest, "manifest_path": str(manifest_path.resolve())}

    def load_staged(self, record_id: str) -> dict[str, Any]:
        path = self.root / "staging" / self._safe(record_id) / "manifest.json"
        if not path.is_file():
            raise PhotoAssetSupplyError("找不到该行已暂存的图文参考图，请重新上传并执行")
        payload = json.loads(path.read_text(encoding="utf-8"))
        if payload.get("status") not in {"pending_content_review", "qualified"}:
            raise PhotoAssetSupplyError("暂存素材状态不是待内容审核")
        for item in payload.get("files") or []:
            source = Path(str(item.get("path") or ""))
            if not source.is_file() or hashlib.sha256(source.read_bytes()).hexdigest() != item.get("sha256"):
                raise PhotoAssetSupplyError("暂存图片缺失或内容已经变化")
        return payload

    def qualify(self, *, record_id: str, recipe: Any, repository: Any,
                reviewer: str = "feishu_human_operator", reviewer_type: str = "human",
                source: str = "feishu_human_confirmed_upload",
                profile_binding: Mapping[str, Any] = None,
                approval_attributes: Mapping[str, Mapping[str, Any]] = None,
                approval_evidence: Mapping[str, Any] = None,
                category_key: str = "", market: str = "") -> AssetSet:
        """Promote the exact staged bytes after the operator confirms the theme.

        ``category_key`` / ``market`` are caller-supplied overrides: 国家无关
        配方（V3）按设计不再在 recipe_spec 里声明这两项（见
        VN_SCARF_CROSS_MARKET_IMPLEMENTATION_SPEC §通用 Recipe 删除项：市场由
        Market Pack 拥有、类别由预设/行级声明提供）。未传时回落配方声明，
        既有 TH 线路逐字不变。
        """
        staged = self.load_staged(record_id)
        existing = None
        if staged.get("status") == "qualified" and staged.get("asset_set_id"):
            existing = repository.get_asset_set(str(staged["asset_set_id"]))
            if existing is not None:
                return existing
        spec = dict(getattr(recipe, "recipe_spec_json", {}) or {})
        binding = dict(profile_binding or {})
        profiles = list(spec.get("execution_profiles") or [])
        requested_profile_id = str(binding.get("profile_id") or "")
        # 国家无关配方（V3）不声明 markets/category_key：调用方传入优先，未传
        # 时回落配方声明。两者皆缺时报明确错误，而不是造出 ASSET__UPLOAD_ 这种
        # 无类别素材集后在下游炸出一句难以归因的 AssetSetError。
        # 市场要在选档**之前**算出来：选 profile 需要它。
        market = str(market or (spec.get("markets") or [""])[0])
        category = str(category_key or spec.get("category_key") or "")
        # 调用方没指名 profile 时（自动风格参考供给走的就是这条路），必须按
        # **本次请求的市场**选档，规则与发布链路共用 ``select_execution_profile``。
        # 否则回落 ``profiles[0]``——对 ``PHOTO_TRAVEL_OUTFIT_V3`` 而言 profiles[0]
        # 是泰国档，VN 请求会把素材集登记进 TH 的 asset_set_key 命名空间，而该键的
        # 版本槽位由泰国线路持有：``uq_opv_asset_set_version`` 是 (asset_set_key,
        # asset_set_version) 唯一索引，INSERT 会静默改写那行、保留它原来的
        # asset_set_id，本次算出来的 content-addressed id 根本不存在，下游按 id
        # 查不到就报 NEEDS_ASSET——图已经付过费。
        # 没有任何档位服务本市场时保持既有兜底（取第一档），让下游按既有错误路径
        # 报错，而不是在这里改变语义。
        profile = select_execution_profile(
            profiles, market=market, profile_id=requested_profile_id,
        )
        if not isinstance(profile, Mapping) and not requested_profile_id:
            profile = next(iter(profiles), None)
        if not isinstance(profile, Mapping):
            raise PhotoAssetSupplyError(
                "Recipe 缺少可执行方案" if not requested_profile_id
                else f"Recipe 不包含 profile {requested_profile_id}"
            )
        key = str(binding.get("asset_set_key") or (profile.get("asset_set_keys") or [""])[0])
        if not key:
            raise PhotoAssetSupplyError("Recipe/profile binding 缺少 asset_set_key")
        if not category:
            raise PhotoAssetSupplyError(
                "素材集缺少类别：配方未声明 category_key，调用方也未提供"
            )
        current = repository.list_asset_sets(category_key=category, market=market, status="enabled")
        # 版本槽位的真实口径是唯一索引 ``uq_opv_asset_set_version``：
        # ``(asset_set_key, asset_set_version)``，与 status / market / category
        # **无关**。一条**停用**的历史行、或属于另一个市场的行同样占着槽位，
        # 只数「本类别+本市场下 enabled 的同键行」会算出一个已被占用的版本，
        # 于是 INSERT 命中唯一索引去改写那条行。仓储能按索引口径给出权威最大值
        # 时优先用它；替身没有该方法时退化为本地候选集估算——首算偏保守只会多
        # 花一次重试，写入侧已不会再覆盖任何行。
        allocate_version = getattr(repository, "next_asset_set_version", None)
        if callable(allocate_version):
            version = int(allocate_version(key)) + 1
        else:
            version = max(
                (item.asset_set_version for item in current if item.asset_set_key == key),
                default=0,
            ) + 1
        assets = []
        frozen_approval_attributes = {}
        source_hashes = {}
        for index, item in enumerate(staged["files"]):
            role = str(item["role"])
            letter = chr(65 + index)
            asset_id = f"{self._safe(record_id)}_{role}_{item['sha256'][:10]}"
            source_hashes[asset_id] = item["sha256"]
            verified = dict(item.get("verified_attributes") or {})
            verified.update(dict((approval_attributes or {}).get(role) or {}))
            frozen_approval_attributes[asset_id] = {
                "outerwear_id": f"human_confirmed_{asset_id}_outerwear",
                "bottom_id": f"human_confirmed_{asset_id}_bottom",
                **verified,
            }
            assets.append({
                "asset_id": asset_id, "role": role, "path": item["path"],
                "sha256": item["sha256"], "tags": {},
                "display_label": dict(item.get("display_label") or {
                    "th-TH": f"ลุค {letter}", "zh-CN": f"造型 {letter}"
                }),
                "content_plan": {
                    "item_id": str(item.get("content_plan_item_id") or ""),
                    "look_signature": str(item.get("planned_look_signature") or ""),
                    "attributes": dict(item.get("planned_attributes") or {}),
                },
            })
        for relation in (spec.get("visual_rules") or {}).get("garment_relations") or []:
            if relation.get("relation") == "same_outerwear_different_bottom":
                relation_roles = list(relation.get("roles") or [])
                members = [asset for asset in assets if asset["role"] in relation_roles]
                if len(members) == 2:
                    if source == "feishu_style_reference_generated":
                        signatures = {
                            str(item.get("outerwear_signature") or "")
                            for item in staged["files"] if item.get("role") in relation_roles
                        }
                        if len(signatures) != 1 or "" in signatures:
                            raise PhotoAssetSupplyError("风格参考生成的 B/C 未保持同款外套，不能进入四选一 Recipe")
                    shared = "human_confirmed_same_outerwear_" + hashlib.sha256(
                        (record_id + ":" + ":".join(sorted(relation_roles))).encode()
                    ).hexdigest()[:12]
                    for member in members:
                        frozen_approval_attributes[member["asset_id"]]["outerwear_id"] = shared
        required_tags = dict((spec.get("asset_requirements") or {}).get("required_tags") or {})
        frozen_variables = {
            **dict(profile.get("variables") or {}),
            **dict(binding.get("variables") or {}),
        }
        # Flow bindings may expose the variables directly for callers that do
        # not need to wrap them in a second ``variables`` object.
        for key_name in spec.get("asset_match_keys") or []:
            if key_name in binding:
                frozen_variables[key_name] = binding[key_name]
        match_tags = {
            key_name: frozen_variables.get(key_name)
            for key_name in spec.get("asset_match_keys") or []
            if key_name in frozen_variables
        }
        if binding:
            identity_bytes = json.dumps({
                "source_hashes": [item["sha256"] for item in staged["files"]],
                "profile_binding": binding,
            }, ensure_ascii=False, sort_keys=True, separators=(",", ":")).encode()
        else:
            # Preserve historical ids for legacy complete-look retries.
            identity_bytes = ":".join(
                item["sha256"] for item in staged["files"]
            ).encode()
        identity = hashlib.sha256(identity_bytes).hexdigest()[:16]
        asset_set_id = f"ASSET_{market}_{category.upper()}_UPLOAD_{identity}"
        # Content-addressed idempotency: retries of the same record regenerate
        # the staging manifest as pending, but identical content must reuse the
        # already-qualified asset set instead of colliding on a drifted version.
        if existing is None:
            existing = repository.get_asset_set(asset_set_id)
        if existing is not None:
            staged.update(status="qualified", asset_set_id=existing.asset_set_id,
                          asset_set_version=existing.asset_set_version, reviewer=reviewer)
            path = Path(staged["files"][0]["path"]).parent / "manifest.json"
            path.write_text(json.dumps(staged, ensure_ascii=False, indent=2), encoding="utf-8")
            return existing
        if LAYERED_PROGRESSION_USE_CASE & set(required_tags.get("use_cases") or []):
            # Every layered line (temperature layering and the daily hot→cold
            # transition) freezes base/bottom/shoes across its states, so one
            # set of source photos can never honestly represent two families.
            incoming_hashes = set(source_hashes.values())
            for candidate in current:
                candidate_use_cases = set(
                    (candidate.tags_json or {}).get("use_cases") or []
                )
                if not (LAYERED_PROGRESSION_USE_CASE & candidate_use_cases):
                    continue
                old_hashes = set(
                    ((candidate.manifest_json or {}).get("content_approval") or {})
                    .get("source_hashes", {}).values()
                )
                if incoming_hashes & old_hashes:
                    raise PhotoAssetSupplyError(
                        "分层图文不同资产集不得复用相同源图；请为当前组合提供独立实拍"
                    )
        by_asset_role = {str(item["role"]): str(item["asset_id"]) for item in assets}
        frozen_pairs = []
        for required_pair in (spec.get("asset_requirements") or {}).get("required_pairs") or []:
            pair_roles = [str(value) for value in required_pair.get("roles") or []]
            if pair_roles and all(role in by_asset_role for role in pair_roles):
                frozen_pairs.append({
                    "relation": str(required_pair.get("relation") or ""),
                    "asset_ids": [by_asset_role[role] for role in pair_roles],
                })
        asset_set = AssetSet(
            asset_set_id=asset_set_id,
            asset_set_key=key, asset_set_version=version,
            category_key=category, market=market, status="enabled",
            tags_json={**required_tags, **match_tags, "source": source,
                       "theme_key": str((staged.get("metadata") or {}).get("theme_key") or "")},
            manifest_json={
                "assets": assets, "pairs": frozen_pairs,
                "content_approval": {
                    "schema_version": "opv-source-qualification-v1",
                    "reviewer": reviewer, "reviewer_type": reviewer_type,
                    "allowed_logic_keys": [str((spec.get("content_card") or {}).get("logic_key") or "")],
                    "source_hashes": source_hashes, "attributes": frozen_approval_attributes,
                    **({"evidence": dict(approval_evidence)} if approval_evidence else {}),
                },
                "profile_binding": binding,
            },
        )
        AssetSetService(repository).save(asset_set)
        # 写入后独立回读自证：``AssetSetService.save`` 已保证返回值来自存储，这里
        # 再核对一次「本次 content-addressed id 真的落了行、且落在预期的 key 上」。
        # 不成立就说明写入语义又绕回了「版本冲突改写了别人的行」，宁可在冻结之前
        # 响亮失败，也不能让调用方带着悬空 id 往下走——下游报 NEEDS_ASSET 时图已经
        # 付过费。登记失败不重新生图：暂存清单与已生成的图片都原样留在盘上。
        persisted = repository.get_asset_set(asset_set.asset_set_id)
        if persisted is None or str(persisted.asset_set_key) != key:
            raise PhotoAssetSupplyError(
                "素材集写入后回读不到本次登记的 id："
                f"期望 id={asset_set.asset_set_id} key={key}，"
                f"实际回读到 {getattr(persisted, 'asset_set_id', None)!r} "
                f"key={getattr(persisted, 'asset_set_key', None)!r}。"
                "已生成的素材与暂存清单均保留，可重试登记，不会重复付费。"
            )
        path = Path(staged["files"][0]["path"]).parent / "manifest.json"
        staged.update(status="qualified", asset_set_id=persisted.asset_set_id,
                      asset_set_version=persisted.asset_set_version, reviewer=reviewer)
        path.write_text(json.dumps(staged, ensure_ascii=False, indent=2), encoding="utf-8")
        return persisted

    @staticmethod
    def _safe(value: str) -> str:
        return "".join(ch if ch.isalnum() or ch in "-_" else "_" for ch in str(value))[:120]

    @staticmethod
    def _suffix(name: str, content_type: str) -> str:
        return _suffix_static(name, content_type)
