"""Target-account claim isolation: 定向候选只归目标账号 + 图文领取范围。

All databases and media are temporary.  These tests cover the 2026-09-15
account-positioning round:

1. ``script_metadata.target_publish_account_id`` 列与 context JSON 双通道；
2. ``filter_candidates_for_account`` 的双向隔离（定向候选 / own_tasks_only）；
3. ``schedule_slots`` 端到端：B 账号不能领 A 绑定内容；仅本账号任务的账号
   不领未绑定公共图文；定向候选在目标账号无槽位时保持待排期不转号；
4. ``sync_accounts`` 把账号表定位列收敛成 photo_content_profile JSON 与
   图文领取范围，空值不覆盖库内既有配置；
5. 提交前 ``_validate_opv_upload`` 核对冻结清单目标与领取账号。
"""

from dataclasses import replace
from datetime import datetime
import hashlib
import json
from pathlib import Path
import sys
import tempfile
import unittest
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from app.db import AutoPublishDB
from app.models import AccountConfig, PublishTaskStatus, ScriptMetadata
from app.publishers import BasePublishAdapter, RoutedPublishAdapter
from app.scheduler import (
    _validate_opv_upload, filter_candidates_for_account, schedule_slots,
    sync_accounts,
)


def signed_manifest(payload):
    unsigned = {key: value for key, value in payload.items() if key != "manifest_sha256"}
    digest = hashlib.sha256(json.dumps(unsigned, ensure_ascii=False, sort_keys=True,
                                      separators=(",", ":")).encode()).hexdigest()
    return {**unsigned, "manifest_sha256": digest}


class PhotoAdapter(BasePublishAdapter):
    def __init__(self):
        self.requests = []

    def create_scheduled_task(self, **kwargs):
        raise AssertionError("Photos must never enter the video-only interface")

    def create_publish_task(self, request):
        self.requests.append(request)
        return "photo:job-1"

    def query_task_status(self, **kwargs):
        return PublishTaskStatus("pending", "待执行")


class Record:
    def __init__(self, fields, record_id="rec-1"):
        self.fields = fields
        self.record_id = record_id


class TargetAccountClaimTest(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.root = Path(self.temp.name)
        self.env = patch.dict("os.environ", {
            "OPENCLAW_SHARED_DATA_DIR": str(self.root),
            "SHORT_VIDEO_AUTO_PUBLISH_CONFIG_PATH": str(self.root / "unused.json"),
        })
        self.env.start()
        self.db = AutoPublishDB(self.root / "queue.sqlite3")
        self.now = datetime(2026, 9, 15, 11)
        self.adapter = PhotoAdapter()
        self.publisher = RoutedPublishAdapter(
            default_adapter=self.adapter, channel_adapters={"CreatOK": self.adapter},
            account_channels={}, task_prefix_adapters={"photo:": self.adapter},
        )

    def tearDown(self):
        self.env.stop()
        self.temp.cleanup()

    # ------------------------------------------------------------------
    def add_photo_account(self, account_id, *, scope="", times=("12:00", "", "")):
        account = AccountConfig(
            account_id, account_id, "THFZ01", "可用", *times,
            publish_channel="CreatOK", nurture_enabled=True, nurture_only=True,
            photo_claim_scope=scope,
        )
        self.db.upsert_account_configs([account])
        self.db.update_account_provider_info(account_id, content_photo=True)
        self.publisher.account_channels[account_id] = "CreatOK"
        return account

    def write_photo(self, key, *, target="", manifest_target=None):
        task_id = key.split(":", 1)[1]
        slides = []
        for index in range(1, 6):
            path = self.root / f"{task_id}-page-{index}.png"
            path.write_bytes(f"{task_id}-{index}".encode())
            slides.append({
                "index": index, "asset_id": f"slide-{index}", "path": str(path),
                "sha256": hashlib.sha256(path.read_bytes()).hexdigest(),
                "mime_type": "image/png", "width": 1080, "height": 1920,
            })
        manifest_payload = {
            "schema_version": "opv-photo-release-v1", "media_kind": "native_photo",
            "task_id": task_id, "revision_id": "revision-1",
            "content_package_id": f"package-{task_id}", "review_id": "review-1",
            "reviewer_type": "human", "reviewer": "feishu_operator",
            "review_decision": "passed",
            "content_review": {
                "operator_preview": True, "content_alignment": True,
                "language_confirmed": True,
            },
            "input_fingerprint": "fingerprint-1", "selection_hash": "selection-1",
            "input_snapshot_hash": "snapshot-1",
            "template_id": "pick", "template_version": 1, "cover_index": 1,
            "copy": {"title": "Choose a look", "caption": "Pick A B C or D",
                     "hashtags": ["#outfit"]},
            "slides": slides,
        }
        if manifest_target:
            manifest_payload["target_publish_account_id"] = manifest_target
        manifest = signed_manifest(manifest_payload)
        context = {"workflow_version": 2, "publish_title": "Choose a look",
                   "release_manifest": manifest, "recipe_id": "pick"}
        if target:
            context["target_publish_account_id"] = target
        metadata = ScriptMetadata(
            script_id=task_id, canonical_script_key=key, source_record_id=f"source-{task_id}",
            script_slot=f"OPV:{task_id}", task_no="1", store_id="THFZ01", product_id="",
            parent_slot="OPV", direction_label="pick", variant_strength="",
            target_country="TH", product_type="apparel",
            content_family_key=f"family-{task_id}",
            script_text=json.dumps(context), short_video_title="Choose a look",
            title_source="opv_copy", script_source="图文养号", publish_purpose="养号",
            cart_enabled="否", content_branch="非商品展示型",
            audio_mode="silent_source_platform_bgm",
            target_publish_account_id=target,
        )
        self.db.upsert_script_metadata([metadata])
        self.db.upsert_video_asset(
            canonical_script_key=key, script_id=task_id,
            run_manager_record_id=f"source-{task_id}",
            video_source_type="opv_photo_package", video_source_value="package-1",
            local_file_path=None, download_status="下载成功", run_video_status="已完成",
            media_kind="native_photo", photo_manifest_json=manifest,
        )
        return manifest

    def schedule(self):
        return schedule_slots(self.db, self.publisher, now=self.now, window_hours=2)

    def slot_reasons(self):
        with self.db._connect() as conn:
            return [
                str(row["error_message"] or "")
                for row in conn.execute(
                    "SELECT error_message FROM publish_slots "
                    "WHERE schedule_status='待排期'"
                ).fetchall()
            ]

    # ------------------------------------------------------------------
    def test_new_columns_exist(self):
        with self.db._connect() as conn:
            accounts = {row["name"] for row in conn.execute("PRAGMA table_info(account_configs)")}
            scripts = {row["name"] for row in conn.execute("PRAGMA table_info(script_metadata)")}
        self.assertIn("photo_content_profile_json", accounts)
        self.assertIn("photo_claim_scope", accounts)
        self.assertIn("target_publish_account_id", scripts)

    def test_candidate_exposes_target_from_column_and_context(self):
        self.write_photo("opv:t1", target="photo-a")
        self.write_photo("opv:t2")  # 无列值：context 也不带 → 公共池
        candidates = {c.script_id: c for c in self.db.list_ready_candidates("THFZ01")}
        self.assertEqual(candidates["t1"].target_publish_account_id, "photo-a")
        self.assertEqual(candidates["t2"].target_publish_account_id, "")

    def test_filter_candidates_isolation_rules(self):
        photo_bound = type("C", (), {})()
        photo_bound.target_publish_account_id = "acc-a"
        photo_bound.content_type = "photo"
        photo_unbound = type("C", (), {})()
        photo_unbound.target_publish_account_id = ""
        photo_unbound.content_type = "photo"
        video_unbound = type("C", (), {})()
        video_unbound.target_publish_account_id = ""
        video_unbound.content_type = "video"
        account = {"photo_claim_scope": "own_tasks_only"}

        kept, stats = filter_candidates_for_account(
            [photo_bound, photo_unbound, video_unbound], "acc-a", account)
        self.assertEqual([id(c) for c in kept], [id(photo_bound), id(video_unbound)])
        self.assertEqual(stats, {"targeted_for_other": 0, "unbound_photo_skipped": 1})

        kept, stats = filter_candidates_for_account(
            [photo_bound, photo_unbound], "acc-b", {"photo_claim_scope": "store_pool"})
        self.assertEqual([id(c) for c in kept], [id(photo_unbound)])
        self.assertEqual(stats["targeted_for_other"], 1)

        kept, _ = filter_candidates_for_account(
            [photo_unbound], "acc-c", None)
        self.assertEqual(len(kept), 1)

    def test_other_account_cannot_claim_bound_candidate(self):
        self.add_photo_account("photo-b")
        self.write_photo("opv:t1", target="photo-a", manifest_target="photo-a")
        with patch("app.scheduler._validate_opv_upload") as verify:
            stats = self.schedule()
        verify.assert_not_called()
        self.assertEqual(stats.scheduled, 0)
        self.assertTrue(any("其他目标账号" in reason for reason in self.slot_reasons()))
        # 候选保持待排期，等待目标账号自己的槽位。
        self.assertEqual(
            self.db.get_video_asset("opv:t1")["publish_status"], "待排期")

    def test_own_tasks_only_account_skips_unbound_pool_photo(self):
        self.add_photo_account("photo-a", scope="own_tasks_only")
        self.write_photo("opv:t1")  # 未绑定公共池图文
        with patch("app.scheduler._validate_opv_upload") as verify:
            stats = self.schedule()
        verify.assert_not_called()
        self.assertEqual(stats.scheduled, 0)
        self.assertTrue(any("仅本账号任务" in reason for reason in self.slot_reasons()))

    def test_own_tasks_only_account_claims_bound_candidate(self):
        self.add_photo_account("photo-a", scope="own_tasks_only")
        self.write_photo("opv:t1", target="photo-a", manifest_target="photo-a")
        with patch("app.scheduler._validate_opv_upload"):
            stats = self.schedule()
        self.assertEqual(stats.scheduled, 1)
        self.assertEqual(len(self.adapter.requests), 1)
        self.assertEqual(self.adapter.requests[0].account_id, "photo-a")
        # 提交上下文冻结目标账号，供提交前复核与审计。
        with self.db._connect() as conn:
            context = json.loads(conn.execute(
                "SELECT submission_context_json FROM publish_slots "
                "WHERE schedule_status='已排期'"
            ).fetchone()[0])
        self.assertEqual(context["target_publish_account_id"], "photo-a")
        assigned = self.db.get_video_asset("opv:t1")
        self.assertEqual(assigned["account_id"], "photo-a")

    def test_store_pool_account_still_claims_unbound_photo(self):
        self.add_photo_account("photo-b", scope="store_pool")
        self.write_photo("opv:t1")
        with patch("app.scheduler._validate_opv_upload"):
            self.assertEqual(self.schedule().scheduled, 1)
        self.assertEqual(self.adapter.requests[0].account_id, "photo-b")

    def test_submit_validation_rejects_wrong_account(self):
        self.write_photo("opv:t1", target="photo-a", manifest_target="photo-a")
        candidate = self.db.list_ready_candidates("THFZ01")[0]
        with patch("app.scheduler._verify_live_opv_release"):
            _validate_opv_upload(candidate, account_id="photo-a")  # 匹配：不抛错
            with self.assertRaises(RuntimeError) as ctx:
                _validate_opv_upload(candidate, account_id="photo-b")
        self.assertIn("不是冻结目标账号", str(ctx.exception))

    def test_submit_validation_rejects_queue_context_mismatch(self):
        # 冻结清单与 context 都指向 photo-b（领取账号也一致），但队列列被改成
        # photo-a：列与 context 双通道互证必须拦截这类入队后被改绑的写入。
        self.write_photo("opv:t1", target="photo-b", manifest_target="photo-b")
        candidate = self.db.list_ready_candidates("THFZ01")[0]
        from app.models import PublishCandidate
        fields = {
            f: getattr(candidate, f) for f in candidate.__dataclass_fields__
            if f != "target_publish_account_id"
        }
        tampered = PublishCandidate(**{**fields, "target_publish_account_id": "photo-a"})
        with patch("app.scheduler._verify_live_opv_release"), \
                self.assertRaises(ValueError) as ctx:
            _validate_opv_upload(tampered, account_id="photo-b")
        self.assertIn("队列目标账号", str(ctx.exception))

    # ------------------------------------------------------------------
    def test_sync_accounts_builds_photo_content_profile(self):
        mapping = {
            "account_id": "账号ID", "account_name": "账号名称", "store_id": "店铺ID",
            "account_status": "账号状态", "publish_channel": "发布渠道",
            "positioning": "账号定位", "default_theme": "默认主题",
            "expression_mode": "内容表达", "visual_style": "视频风格",
            "photo_claim_scope": "图文领取范围",
        }
        records = [Record({
            "账号ID": "photo-a", "账号名称": "A", "店铺ID": "THFZ01",
            "账号状态": "可用", "发布渠道": "CreatOK",
            "账号定位": "旅行高级感；融入景点",
            "默认主题": "凉爽旅行",
            "内容表达": "实用指南",
            "视频风格": "中性奶白底、自然肤色",
            "图文领取范围": "仅本账号任务",
        })]
        self.assertEqual(sync_accounts(records, mapping, self.db), 1)
        row = self.db.get_account_config("photo-a")
        profile = json.loads(row["photo_content_profile_json"])
        self.assertEqual(profile["positioning"], "旅行高级感；融入景点")
        self.assertEqual(profile["default_theme"], "凉爽旅行")
        self.assertEqual(profile["expression_mode"], "PRACTICAL_GUIDE")
        self.assertEqual(profile["visual_baseline"], "中性奶白底、自然肤色")
        self.assertEqual(row["photo_claim_scope"], "own_tasks_only")

    def test_sync_accounts_empty_profile_preserves_stored_values(self):
        self.add_photo_account("photo-a", scope="own_tasks_only")
        with self.db._connect() as conn:
            conn.execute(
                "UPDATE account_configs SET photo_content_profile_json=? "
                "WHERE account_id='photo-a'",
                (json.dumps({"positioning": "手工配置"}, ensure_ascii=False),))
        mapping = {"account_id": "账号ID", "store_id": "店铺ID",
                   "account_status": "账号状态", "publish_channel": "发布渠道"}
        sync_accounts([Record({
            "账号ID": "photo-a", "店铺ID": "THFZ01", "账号状态": "可用",
            "发布渠道": "CreatOK",
        })], mapping, self.db)
        row = self.db.get_account_config("photo-a")
        # 账号表没维护定位列（mapping 缺失）时不得清空已有配置。
        self.assertEqual(json.loads(row["photo_content_profile_json"])["positioning"], "手工配置")
        self.assertEqual(row["photo_claim_scope"], "own_tasks_only")

    def test_account_without_scope_defaults_to_store_pool(self):
        self.add_photo_account("photo-b")
        row = self.db.get_account_config("photo-b")
        self.assertIn("photo_claim_scope", row.keys())
        self.assertEqual(str(row["photo_claim_scope"] or ""), "")


if __name__ == "__main__":
    unittest.main()
