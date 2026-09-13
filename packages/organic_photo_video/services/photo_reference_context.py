"""公共参考管线门面（Phase 1：抽公共能力，零行为变化）。

本模块是一个**薄编排层**，只复用既有能力，不复制底层实现：

- ``services.photo_reference.resolve_reference_mode``：冻结参考模式；
- 调用方传入的 ``ProductReferenceResolver``：解析商品快照。

约束（与规格 §3.1 A 一致）：

- 不生成文案、不执行旅行规划；
- 不读写文件、不访问网络；
- 不把 ``STYLE + product_id`` 改造成新的 UI 枚举；
- 首次迁移必须保证 TH V2 现有输出逐字不变。

Phase 1 只落地“参考模式 + 商品快照 + 商品上下文”的解析与冻结；
``style_reference_paths`` 由后续阶段在附件暂存后回填，本阶段保持空元组。
"""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Mapping, Sequence

from services.photo_reference import (
    REFERENCE_MODE_COMPLETE_LOOK,
    REFERENCE_MODE_PRODUCT,
    REFERENCE_MODE_STYLE,
    resolve_reference_mode,
)

#: 商品上下文的稳定字段集（与 ``feishu_workflow`` 内联实现逐字一致）。
PRODUCT_CONTEXT_FIELDS: tuple[str, ...] = (
    "product_id",
    "product_name",
    "category",
    "variant_key",
    "reference_pack_id",
    "reference_pack_version",
)


@dataclass(frozen=True)
class PhotoReferenceContext:
    """单条记录参考输入的冻结结果。

    字段顺序参照规格 §3.1 A 的建议数据合同；``reference_attachments`` 是
    TH V2 运营字段当前的承载形态（飞书附件描述符），作为附加字段保留，
    以便 V2 路径在迁移后仍能取到与迁移前完全相同的对象。
    """

    reference_mode: str = ""
    reference_attachments: tuple[Any, ...] = ()
    product_snapshot: dict[str, Any] = field(default_factory=dict)
    product_context: dict[str, Any] = field(default_factory=dict)
    product_reference_paths: tuple[str, ...] = ()
    complete_look_sources: tuple[Mapping[str, Any], ...] = ()
    style_reference_paths: tuple[str, ...] = ()
    input_fingerprint: str = ""


def build_product_context(product_snapshot: Mapping[str, Any] | None) -> dict[str, Any]:
    """按稳定字段集裁出商品上下文，过滤空值（与旧内联逻辑等价）。"""
    if not product_snapshot:
        return {}
    return {
        key: product_snapshot.get(key)
        for key in PRODUCT_CONTEXT_FIELDS
        if product_snapshot.get(key) not in (None, "")
    }


def resolve_reference_inputs(
    *,
    selected_type: Any,
    unified_attachments: Sequence[Any] | None,
    legacy_complete: Sequence[Any] | None,
    legacy_product: Sequence[Any] | None,
    product_id: str,
    required_roles: Sequence[str],
    quantity: int,
) -> tuple[str, list[Any]]:
    """冻结参考模式与参考附件，逐字复刻 ``feishu_workflow`` 内联分支。

    只处理非分层（旅行/通用）路径；分层流（温度分层 / 冷热切换）不经过本函数。
    注意：无任何输入时**不抛错**（保持旧行为），交由调用方后续门禁处理。
    """
    reference_mode = ""
    reference_attachments: list[Any] = []
    unified_attachments = list(unified_attachments or [])
    legacy_complete = list(legacy_complete or [])
    legacy_product = list(legacy_product or [])
    if unified_attachments:
        reference_attachments = list(unified_attachments)
        reference_mode = resolve_reference_mode(
            selected_type=selected_type,
            attachments=reference_attachments,
            product_id=product_id,
            required_role_count=len(required_roles),
            requested_count=quantity,
            required_roles=required_roles,
        )
    elif legacy_complete:
        reference_mode = REFERENCE_MODE_COMPLETE_LOOK
        reference_attachments = list(legacy_complete)
    elif legacy_product or product_id:
        reference_mode = REFERENCE_MODE_PRODUCT
        reference_attachments = list(legacy_product)
    return reference_mode, reference_attachments


def resolve_photo_reference_context(
    *,
    selected_type: Any,
    unified_attachments: Sequence[Any] | None = None,
    legacy_complete: Sequence[Any] | None = None,
    legacy_product: Sequence[Any] | None = None,
    product_id: str = "",
    required_roles: Sequence[str] = (),
    quantity: int = 1,
    account_id: str = "",
    record_id: str = "",
    product_reference_resolver: Any = None,
) -> PhotoReferenceContext:
    """解析并冻结一条记录的参考上下文。

    与 ``feishu_workflow._generate_native_photo`` 迁移前的内联逻辑等价：

    - 参考模式/附件：见 :func:`resolve_reference_inputs`；
    - 仅当 ``STYLE + product_id`` 且提供 resolver 时解析商品快照；
    - 商品缺包时**向上抛出** ``ProductReferenceResolutionError``，由调用方按既有
      文案包装为 ``FeishuWorkflowError``（本模块不依赖飞书异常类型）。
    """
    reference_mode, reference_attachments = resolve_reference_inputs(
        selected_type=selected_type,
        unified_attachments=unified_attachments,
        legacy_complete=legacy_complete,
        legacy_product=legacy_product,
        product_id=product_id,
        required_roles=required_roles,
        quantity=quantity,
    )

    product_snapshot: dict[str, Any] = {}
    if (
        reference_mode == REFERENCE_MODE_STYLE
        and str(product_id or "").strip()
        and product_reference_resolver is not None
    ):
        product_snapshot = dict(
            product_reference_resolver.resolve_snapshot(
                product_id, selection_key=record_id, account_id=account_id,
            )
            or {}
        )

    product_context = build_product_context(product_snapshot)
    product_reference_paths = tuple(
        str(item) for item in (product_snapshot.get("reference_images") or [])
    )
    complete_look_sources = (
        tuple(reference_attachments)
        if reference_mode == REFERENCE_MODE_COMPLETE_LOOK else ()
    )
    return PhotoReferenceContext(
        reference_mode=reference_mode,
        reference_attachments=tuple(reference_attachments),
        product_snapshot=product_snapshot,
        product_context=product_context,
        product_reference_paths=product_reference_paths,
        complete_look_sources=complete_look_sources,
    )
