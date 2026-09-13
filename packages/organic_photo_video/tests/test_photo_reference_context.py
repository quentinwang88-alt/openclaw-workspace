"""Phase 1 公共参考管线门面的等价性与契约测试。

目的：钉死 ``services/photo_reference_context.resolve_photo_reference_context``
与迁移前 ``feishu_workflow._generate_native_photo`` 内联分支**逐字等价**，
防止后续 Phase 在公共入口上悄悄改变 TH V2 行为。
"""
from __future__ import annotations

import unittest
from pathlib import Path

from services.photo_reference import (
    REFERENCE_MODE_COMPLETE_LOOK,
    REFERENCE_MODE_PRODUCT,
    REFERENCE_MODE_STYLE,
    resolve_reference_mode,
)
from services.photo_reference_context import (
    PRODUCT_CONTEXT_FIELDS,
    PhotoReferenceContext,
    build_product_context,
    resolve_photo_reference_context,
)

REPO_ROOT = Path(__file__).resolve().parents[1]
LEGACY_CONTEXT_FIELDS = (
    "product_id", "product_name", "category", "variant_key",
    "reference_pack_id", "reference_pack_version",
)


class _Resolver:
    """最小 ProductReferenceResolver 替身。"""

    def __init__(self, snapshot=None, raises=None):
        self._snapshot = {} if snapshot is None else snapshot
        self._raises = raises
        self.calls = []

    def resolve_snapshot(self, product_id, *, selection_key="", account_id=""):
        self.calls.append((product_id, selection_key, account_id))
        if self._raises is not None:
            raise self._raises
        return self._snapshot


def _legacy_resolve(*, selected_type, unified_attachments, legacy_complete,
                    legacy_product, product_id, roles, quantity,
                    account_id, record_id, resolver):
    """逐字复刻迁移前 feishu_workflow 的内联分支（非分层路径）。"""
    reference_mode = ""
    reference_attachments = []
    if unified_attachments:
        reference_attachments = unified_attachments
        reference_mode = resolve_reference_mode(
            selected_type=selected_type,
            attachments=reference_attachments, product_id=product_id,
            required_role_count=len(roles), requested_count=quantity,
            required_roles=roles,
        )
    elif legacy_complete:
        reference_mode, reference_attachments = REFERENCE_MODE_COMPLETE_LOOK, legacy_complete
    elif legacy_product or product_id:
        reference_mode, reference_attachments = REFERENCE_MODE_PRODUCT, legacy_product
    style_product = {}
    if reference_mode == REFERENCE_MODE_STYLE and product_id:
        style_product = resolver.resolve_snapshot(
            product_id, selection_key=record_id, account_id=account_id,
        )
    product_context = ({
        key: style_product.get(key)
        for key in LEGACY_CONTEXT_FIELDS
        if style_product.get(key) not in (None, "")
    } if style_product else {})
    return reference_mode, reference_attachments, style_product, product_context


def _facade_resolve(*, selected_type, unified_attachments, legacy_complete,
                    legacy_product, product_id, roles, quantity,
                    account_id, record_id, resolver):
    ctx = resolve_photo_reference_context(
        selected_type=selected_type,
        unified_attachments=unified_attachments,
        legacy_complete=legacy_complete,
        legacy_product=legacy_product,
        product_id=product_id,
        required_roles=roles,
        quantity=quantity,
        account_id=account_id,
        record_id=record_id,
        product_reference_resolver=resolver,
    )
    return ctx.reference_mode, list(ctx.reference_attachments), ctx.product_snapshot, ctx.product_context


# 覆盖每个分支的输入矩阵（不含分层流；分层流不走本门面）。
_BRANCH_CASES = (
    # (name, kwargs, expect_mode)
    ("auto_no_input", dict(selected_type="自动判断", unified_attachments=[],
                           legacy_complete=[], legacy_product=[], product_id=""), ""),
    ("unified_style", dict(selected_type="风格参考",
                           unified_attachments=[{"file_token": "a"}],
                           legacy_complete=[], legacy_product=[], product_id=""),
     REFERENCE_MODE_STYLE),
    ("auto_with_product_id", dict(selected_type="自动判断", unified_attachments=[],
                                  legacy_complete=[], legacy_product=[],
                                  product_id="P-1"), REFERENCE_MODE_PRODUCT),
    ("unified_complete_look", dict(
        selected_type="完整穿搭",
        unified_attachments=[{"file_token": str(i)} for i in range(4)],
        legacy_complete=[], legacy_product=[], product_id="", roles=("front", "side", "back", "detail"),
        quantity=1), REFERENCE_MODE_COMPLETE_LOOK),
    ("legacy_complete", dict(selected_type="", unified_attachments=[],
                             legacy_complete=[{"file_token": str(i)} for i in range(4)],
                             legacy_product=[], product_id=""), REFERENCE_MODE_COMPLETE_LOOK),
    ("legacy_product", dict(selected_type="", unified_attachments=[], legacy_complete=[],
                            legacy_product=[{"file_token": "p"}], product_id=""),
     REFERENCE_MODE_PRODUCT),
    ("style_plus_product", dict(selected_type="风格参考",
                                unified_attachments=[{"file_token": "s"}],
                                legacy_complete=[], legacy_product=[], product_id="P-1"),
     REFERENCE_MODE_STYLE),
)


class FacadeEquivalenceTest(unittest.TestCase):
    """门面输出必须与旧内联逻辑逐字一致。"""

    def _run(self, kwargs, snapshot=None, raises=None):
        resolver = _Resolver(snapshot=snapshot, raises=raises)
        base = dict(selected_type="", unified_attachments=[], legacy_complete=[],
                    legacy_product=[], product_id="", roles=(), quantity=1,
                    account_id="acct", record_id="rec")
        base.update(kwargs)
        legacy = _legacy_resolve(resolver=resolver, **base)
        facade = _facade_resolve(resolver=resolver, **base)
        return legacy, facade

    def test_branch_matrix_matches_legacy(self):
        for name, kwargs, expect_mode in _BRANCH_CASES:
            with self.subTest(case=name):
                legacy, facade = self._run(kwargs)
                self.assertEqual(legacy[0], expect_mode)
                self.assertEqual(facade[0], expect_mode)
                self.assertEqual(facade[0], legacy[0], "mode")
                self.assertEqual(facade[1], list(legacy[1]), "attachments")
                self.assertEqual(facade[2], legacy[2], "snapshot")
                self.assertEqual(facade[3], legacy[3], "context")

    def test_style_plus_product_resolves_snapshot(self):
        snapshot = {"product_id": "P-1", "product_name": "外套", "category": "outerwear",
                    "variant_key": "v1", "reference_pack_id": "pack", "reference_pack_version": "1",
                    "reference_images": ["/x/1.png", "/x/2.png"], "extra": "ignored"}
        legacy, facade = self._run(
            dict(selected_type="风格参考", unified_attachments=[{"file_token": "s"}],
                 product_id="P-1"), snapshot=snapshot)
        self.assertEqual(facade[0], REFERENCE_MODE_STYLE)
        self.assertEqual(facade[2], snapshot)
        # 商品上下文只保留稳定字段，过滤空值与未知字段
        self.assertEqual(facade[3]["product_id"], "P-1")
        self.assertNotIn("extra", facade[3])
        self.assertNotIn("reference_images", facade[3])
        self.assertEqual(facade[3], legacy[3])

    def test_product_id_alone_does_not_resolve_snapshot(self):
        # PRODUCT 模式不解析快照（旧行为：仅 STYLE+product_id 才解析）
        resolver = _Resolver(snapshot={"product_id": "P-1"})
        ctx = resolve_photo_reference_context(
            selected_type="自动判断", product_id="P-1",
            product_reference_resolver=resolver,
        )
        self.assertEqual(ctx.reference_mode, REFERENCE_MODE_PRODUCT)
        self.assertEqual(ctx.product_snapshot, {})
        self.assertEqual(resolver.calls, [])

    def test_missing_resolver_skips_snapshot_without_error(self):
        ctx = resolve_photo_reference_context(
            selected_type="风格参考", unified_attachments=[{"file_token": "s"}],
            product_id="P-1", product_reference_resolver=None,
        )
        self.assertEqual(ctx.reference_mode, REFERENCE_MODE_STYLE)
        self.assertEqual(ctx.product_snapshot, {})

    def test_complete_look_sources_and_product_paths(self):
        ctx = resolve_photo_reference_context(
            selected_type="完整穿搭",
            unified_attachments=[{"file_token": str(i)} for i in range(4)],
            required_roles=("front", "side", "back", "detail"), quantity=1,
        )
        self.assertEqual(ctx.reference_mode, REFERENCE_MODE_COMPLETE_LOOK)
        self.assertEqual(len(ctx.complete_look_sources), 4)
        self.assertEqual(ctx.product_reference_paths, ())

    def test_product_reference_paths_come_from_snapshot(self):
        ctx = resolve_photo_reference_context(
            selected_type="风格参考", unified_attachments=[{"file_token": "s"}],
            product_id="P-1",
            product_reference_resolver=_Resolver(
                snapshot={"product_id": "P-1", "reference_images": ["/a.png", "/b.png"]}
            ),
        )
        self.assertEqual(ctx.product_reference_paths, ("/a.png", "/b.png"))


class FacadeErrorTest(unittest.TestCase):
    """错误传播语义与旧内联一致。"""

    def test_unknown_reference_type_raises_value_error(self):
        with self.assertRaises(ValueError):
            resolve_photo_reference_context(
                selected_type="不存在的类型", unified_attachments=[{"file_token": "s"}],
            )

    def test_no_input_falls_through_without_error(self):
        # 旧行为：无附件也无商品编码时**不抛错**，reference_mode 留空，
        # 交由调用方的后续门禁（"该图文预设需要填写产品编码或上传参考图"）处理。
        ctx = resolve_photo_reference_context(selected_type="风格参考")
        self.assertEqual(ctx.reference_mode, "")
        self.assertEqual(ctx.reference_attachments, ())

    def test_style_with_product_id_but_no_attachments(self):
        # 有商品编码时走 PRODUCT 分支（旧行为），而非 STYLE 校验失败
        ctx = resolve_photo_reference_context(selected_type="风格参考", product_id="P-1",
                                              product_reference_resolver=_Resolver())
        self.assertEqual(ctx.reference_mode, REFERENCE_MODE_PRODUCT)

    def test_resolver_error_propagates_unwrapped(self):
        class Boom(RuntimeError):
            pass

        with self.assertRaises(Boom):
            resolve_photo_reference_context(
                selected_type="风格参考", unified_attachments=[{"file_token": "s"}],
                product_id="P-1", product_reference_resolver=_Resolver(raises=Boom("no pack")),
            )


class BuildProductContextTest(unittest.TestCase):
    def test_matches_legacy_field_filter(self):
        self.assertEqual(build_product_context(None), {})
        self.assertEqual(build_product_context({}), {})
        self.assertEqual(
            build_product_context({"product_id": "P", "category": "", "product_name": None, "x": 1}),
            {"product_id": "P"},
        )

    def test_field_order_is_frozen(self):
        self.assertEqual(PRODUCT_CONTEXT_FIELDS, LEGACY_CONTEXT_FIELDS)


class ContextContractTest(unittest.TestCase):
    def test_context_is_frozen_dataclass(self):
        ctx = PhotoReferenceContext()
        with self.assertRaises(Exception):
            ctx.reference_mode = "STYLE"  # type: ignore[misc]

    def test_context_has_spec_fields(self):
        for field_name in (
            "reference_mode", "reference_attachments", "product_snapshot",
            "product_context", "product_reference_paths", "complete_look_sources",
            "style_reference_paths", "input_fingerprint",
        ):
            self.assertIn(field_name, PhotoReferenceContext.__dataclass_fields__)

    def test_style_reference_paths_reserved_for_later_phase(self):
        ctx = resolve_photo_reference_context(
            selected_type="风格参考", unified_attachments=[{"file_token": "s"}],
        )
        # Phase 1 不回填；由后续阶段在附件暂存后写入
        self.assertEqual(ctx.style_reference_paths, ())


class ModulePurityTest(unittest.TestCase):
    def test_imports_are_minimal_and_side_effect_free(self):
        source = (REPO_ROOT / "services" / "photo_reference_context.py").read_text(encoding="utf-8")
        import_lines = [
            line.strip() for line in source.splitlines()
            if line.lstrip().startswith(("import ", "from "))
        ]
        allowed = {
            "from __future__ import annotations",
            "from dataclasses import dataclass, field",
            "from typing import Any, Mapping, Sequence",
        }
        for line in import_lines:
            if line in allowed:
                continue
            self.assertTrue(
                line.startswith("from services.photo_reference import"),
                f"unexpected import: {line}",
            )
        for forbidden in ("requests", "urllib", "sqlite3", "open(", "subprocess", "os.environ"):
            self.assertNotIn(forbidden, source)

    def test_feishu_workflow_delegates_to_facade(self):
        source = (REPO_ROOT / "services" / "feishu_workflow.py").read_text(encoding="utf-8")
        self.assertIn("resolve_photo_reference_context", source)
        # 旧内联分支不得回归
        self.assertNotIn("reference_mode, reference_attachments = REFERENCE_MODE_PRODUCT, legacy_product", source)


if __name__ == "__main__":
    unittest.main()
