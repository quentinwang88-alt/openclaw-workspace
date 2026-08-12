from __future__ import annotations

import sqlite3
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace

from core.persona_selection import select_persona_contract
from scripts.ensure_persona_template_workbench import (
    _record_to_db_payload,
    sync_to_db,
)


def apparel_fields(*, status: str = "启用", with_reference: bool = True) -> dict:
    return {
        "人物模板ID（需填写）": "TH_APPAREL_TEST_001",
        "人物模板名称（需填写）": "泰国女装自然分享者",
        "模板状态（需填写，仅启用会被流程使用）": status,
        "适用国家（需填写）": "泰国",
        "一级类目（需填写）": "女装",
        "适用产品类型（需填写，可多填）": "上装, 外套, 轻上装",
        "适用展示方式（可选）": "PERSON_ON_CAMERA, CREATOR_SELF_SHOT",
        "性别（需填写）": "女性",
        "年龄段（需填写）": "22-30",
        "身形气质（需填写）": "自然匀称",
        "身形比例（可选）": "上身4下身6，头身比约1:7.2",
        "脸部可见度（需填写）": "露脸",
        "人物参考图（需上传）": (
            [{"file_token": "persona_ref", "name": "persona.jpg"}]
            if with_reference else []
        ),
        "人物身份描述（需填写）": "曼谷年轻女装创作者",
        "外貌特征（需填写）": "自然肤质和真实身体比例",
        "妆发设定（需填写）": "深色长发和自然妆",
        "说话人格（需填写）": "像朋友一样自然分享",
        "适用展示模式（系统读取）": "ON_BODY_RESULT, SCENE_USAGE",
        "人物正向提示（系统读取）": "natural Thai apparel creator",
        "人物负向提示（系统读取）": "plastic skin, commercial model",
        "优先级（可选）": 80,
        "模板版本（系统，默认V1）": "V1",
        "一致性版本（系统，默认PERSONA_V1）": "PERSONA_V1",
        "备注": "test",
    }


class FakeClient:
    def __init__(self, fields: dict):
        self.records = [SimpleNamespace(record_id="recPersona", fields=fields)]
        self.updates = []

    def list_records(self, page_size=100):
        return self.records

    def update_record_fields(self, record_id, fields):
        self.updates.append((record_id, fields))


class PersonaTemplateWorkbenchTest(unittest.TestCase):
    def test_apparel_fields_compile_to_internal_matching_scope(self):
        payload, warning = _record_to_db_payload(apparel_fields())
        source = payload["source_payload"]
        self.assertEqual("enabled", payload["status"])
        self.assertEqual(["TH"], payload["markets"])
        self.assertEqual(["女装"], source["applicable_categories"])
        self.assertEqual(
            ["light_top", "outerwear"],
            source["applicable_product_types"],
        )
        self.assertIn("GARMENT_WORN", source["supported_demonstration_modes"])
        self.assertEqual(["PERSON_ON_CAMERA"], source["supported_presentation_modes"])
        self.assertEqual(["CREATOR_SELF_SHOT"], source["supported_capture_modes"])
        self.assertEqual("上身4下身6，头身比约1:7.2", source["body_proportion_text"])
        self.assertEqual("", warning)

    def test_enabled_without_reference_is_synced_as_non_active(self):
        payload, warning = _record_to_db_payload(
            apparel_fields(with_reference=False)
        )
        self.assertEqual("testing", payload["status"])
        self.assertIn("必须先上传人物参考图", warning)

    def test_pull_to_shared_db_makes_womens_persona_selectable(self):
        with tempfile.TemporaryDirectory() as tmp:
            db_path = Path(tmp) / "light.sqlite3"
            client = FakeClient(apparel_fields())
            result = sync_to_db(client, db_path=db_path)
            self.assertEqual(1, result["updated"])
            self.assertEqual(0, result["failed"])
            contract, _, _ = select_persona_contract(
                product_type="外套", top_category="女装", country="泰国",
                presentation_mode="PERSON_ON_CAMERA",
                capture_mode="CREATOR_SELF_SHOT",
                demonstration_mode="GARMENT_WORN", seed=1,
                recent_usage=[], db_path=str(db_path),
            )
            self.assertEqual("AVAILABLE", contract["availability"])
            self.assertEqual("TH_APPAREL_TEST_001", contract["persona_id"])
            self.assertEqual(
                "PERSONA_PRODUCT_COMPOSITE_PREFERRED",
                contract["reference_strategy"],
            )
            conn = sqlite3.connect(db_path)
            row = conn.execute(
                "SELECT status FROM persona_templates WHERE persona_id=?",
                ("TH_APPAREL_TEST_001",),
            ).fetchone()
            conn.close()
            self.assertEqual(("enabled",), row)
            self.assertEqual("已同步", client.updates[-1][1]["同步状态（系统）"])


if __name__ == "__main__":
    unittest.main()
