"""目标账号进入飞书工作流（2026-09-15）：统一解析、快照冻结与投递校验。

复用 ``test_photo_feishu_batch`` 的假仓库/假客户端 harness，验证：
- 行级「目标账号（可选）」→ 账号默认主题回退 → account brief 冻结进
  request.theme_brief 与 target_publish_account_id；
- 任务创建携带 target_publish_account_id；摘要回显本篇实际配置；
- 任务显式主题优先于账号默认主题；
- 未配置账号、市场/店铺不一致均显式报错且不产生冻结批次。
"""
from __future__ import annotations

import json
import sys
import unittest
from pathlib import Path

PACKAGE_ROOT = Path(__file__).resolve().parents[1]
if str(PACKAGE_ROOT) not in sys.path:
    sys.path.insert(0, str(PACKAGE_ROOT))
if str(Path(__file__).resolve().parent) not in sys.path:
    sys.path.insert(0, str(Path(__file__).resolve().parent))

from services.feishu_workflow import (
    FIELD_CONTENT_THEME, FIELD_TARGET_ACCOUNT, FIELD_VISUAL_PRESET,
)
from services.publish_account_profile import PublishAccountProfileResolver
from test_photo_feishu_batch import PhotoBatchTest


def seed(tmp: Path, handle: str, profile: dict, store: str = "THFZ01",
         country: str = "TH") -> None:
    (tmp / f"{handle}.json").write_text(json.dumps({
        "schema_version": "opv-publish-account-seed-v1",
        "account_id": handle, "account_name": handle, "store_id": store,
        "target_country": country, "profile": profile,
    }, ensure_ascii=False), encoding="utf-8")


class TargetAccountWorkflowTest(PhotoBatchTest):
    def setUp(self):
        super().setUp()
        self.seed_root = Path(self.temp.name) / "accounts"
        self.seed_root.mkdir(parents=True, exist_ok=True)
        self.workflow.publish_account_resolver = (
            PublishAccountProfileResolver(config_dir=self.seed_root))
        self.created_requests = []
        original_create = self.create

        def recording_create(request):
            self.created_requests.append(request)
            return original_create(request)
        # 在父类 patch 之上再叠一层：stopall 会按栈序还原两层。
        from unittest.mock import patch
        patch("services.task_intake.TaskIntakeService.create_task",
              side_effect=recording_create).start()

    def run_record(self, record_id: str):
        return self.workflow.scan(record_id=record_id)

    def errors_text(self, report) -> str:
        return " ".join(str(item.get("error") if isinstance(item, dict) else item)
                        for item in report["errors"])

    def test_target_account_freezes_default_theme_and_brief(self):
        seed(self.seed_root, "acc-a", {
            "default_theme": "秋季穿搭",
            "positioning": "旅行高级感；融入景点",
            "expression_mode": "STYLE_INSPIRATION",
            "visual_baseline": "中性奶白底、自然肤色",
        })
        self.client.fields[FIELD_TARGET_ACCOUNT] = "acc-a"
        self.client.fields.pop(FIELD_CONTENT_THEME, None)
        report = self.run_record("rec-target-1")
        self.assertEqual(report["errors"], [], self.errors_text(report))
        entry = self.repo.batch.manifest_json["entries"][0]
        request = entry["request"]
        # 账号默认主题生效且来源被记录；主题配置进入冻结请求。
        self.assertEqual(request["theme_brief"]["theme_key"], "AUTUMN_OUTFIT")
        account = request["theme_brief"]["account"]
        self.assertEqual(account["target_publish_account_id"], "acc-a")
        self.assertEqual(account["theme_source"], "account_default")
        self.assertEqual(account["expression_mode"], "STYLE_INSPIRATION")
        self.assertIn("旅行高级感", account["positioning"])
        self.assertTrue(account["profile_fingerprint"])
        self.assertEqual(request["target_publish_account_id"], "acc-a")
        # 任务创建携带目标账号。
        self.assertTrue(self.created_requests)
        self.assertEqual(
            self.created_requests[0].target_publish_account_id, "acc-a")
        # 摘要向运营回显本篇实际配置。
        summary = self.client.fields.get("内容方案摘要") or ""
        self.assertIn("acc-a", summary)
        self.assertIn("账号默认", summary)

    def test_visual_preset_task_override_and_freeze(self):
        seed(self.seed_root, "acc-a", {})
        self.client.fields[FIELD_TARGET_ACCOUNT] = "acc-a"
        self.client.fields[FIELD_CONTENT_THEME] = "凉爽旅行"
        self.client.fields[FIELD_VISUAL_PRESET] = "清爽室内穿搭"
        report = self.run_record("rec-preset-1")
        self.assertEqual(report["errors"], [], self.errors_text(report))
        request = self.repo.batch.manifest_json["entries"][0]["request"]
        preset = request["theme_brief"]["visual_preset"]
        self.assertEqual((preset["preset_id"], preset["background_mode"], preset["source"]),
                         ("VP_CLEAN_INDOOR_V1", "fixed", "task"))
        self.assertTrue(preset["fingerprint"])
        # 摘要回显视觉预设。
        self.assertIn("视觉预设 清爽室内穿搭", self.client.fields.get("内容方案摘要") or "")

    def test_visual_preset_entry_default_for_bound_row(self):
        seed(self.seed_root, "acc-a", {})  # 无 default_visual_preset
        self.client.fields[FIELD_TARGET_ACCOUNT] = "acc-a"
        self.client.fields[FIELD_CONTENT_THEME] = "凉爽旅行"
        report = self.run_record("rec-preset-2")
        self.assertEqual(report["errors"], [], self.errors_text(report))
        request = self.repo.batch.manifest_json["entries"][0]["request"]
        preset = request["theme_brief"]["visual_preset"]
        # 本 harness 的预设是四选一（product_supply）入口：默认＝纯色搭配解析。
        self.assertEqual((preset["preset_id"], preset["source"]),
                         ("VP_SOLID_COLOR_V1", "entry_default"))

    def test_visual_preset_overrides_frozen_layout(self):
        """排版接线（2026-09-15 B2）：预设声明 structured_v1 时，冻结请求的
        layout_snapshot 按背景类型替换为新模板；未知 family 显式报错。"""
        seed(self.seed_root, "acc-a", {})
        self.client.fields[FIELD_TARGET_ACCOUNT] = "acc-a"
        self.client.fields[FIELD_CONTENT_THEME] = "凉爽旅行"
        self.client.fields[FIELD_VISUAL_PRESET] = "旅行场景穿搭"
        report = self.run_record("rec-layout-1")
        self.assertEqual(report["errors"], [], self.errors_text(report))
        request = self.repo.batch.manifest_json["entries"][0]["request"]
        layout = request["layout_snapshot"]
        self.assertEqual(layout["layout_id"], "PHOTO_STRUCTURED_SCENE_V1")
        self.assertEqual(layout["render_options"]["overlay_style"], "structured_v1")
        self.assertEqual(layout["render_options"]["structured_style"], "scene")
        # 指纹校验通过（冻结请求合法）。
        from services.photo_request_factory import validate_frozen_request
        validate_frozen_request(request)

    def test_task_theme_overrides_account_default(self):
        seed(self.seed_root, "acc-a", {"default_theme": "秋季穿搭"})
        self.client.fields[FIELD_TARGET_ACCOUNT] = "acc-a"
        self.client.fields[FIELD_CONTENT_THEME] = "凉爽旅行"
        report = self.run_record("rec-target-2")
        self.assertEqual(report["errors"], [], self.errors_text(report))
        request = self.repo.batch.manifest_json["entries"][0]["request"]
        self.assertEqual(request["theme_brief"]["theme_key"], "COOL_WEATHER_TRAVEL")
        self.assertEqual(request["theme_brief"]["account"]["theme_source"], "task")

    def test_unknown_target_account_fails_before_any_freeze(self):
        self.client.fields[FIELD_TARGET_ACCOUNT] = "ghost"
        report = self.run_record("rec-target-3")
        self.assertIn("无法解析", self.errors_text(report))
        self.assertIsNone(self.repo.batch)

    def test_market_mismatch_rejected(self):
        seed(self.seed_root, "acc-vn", {}, store="VNPS01", country="VN")
        self.client.fields[FIELD_TARGET_ACCOUNT] = "acc-vn"
        report = self.run_record("rec-target-4")
        self.assertIn("市场", self.errors_text(report))
        self.assertIsNone(self.repo.batch)

    def test_store_mismatch_rejected(self):
        seed(self.seed_root, "acc-other-store", {}, store="OTHER01")
        self.client.fields[FIELD_TARGET_ACCOUNT] = "acc-other-store"
        report = self.run_record("rec-target-5")
        self.assertIn("店铺", self.errors_text(report))
        self.assertIsNone(self.repo.batch)


if __name__ == "__main__":
    unittest.main()
