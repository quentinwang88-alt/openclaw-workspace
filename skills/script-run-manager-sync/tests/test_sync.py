#!/usr/bin/env python3
"""同步逻辑单元测试。"""

from pathlib import Path
import sys
import unittest

TESTS_DIR = Path(__file__).resolve().parent
SKILL_DIR = TESTS_DIR.parent
if str(SKILL_DIR) not in sys.path:
    sys.path.insert(0, str(SKILL_DIR))

from core.bitable import TableRecord
from core.sync import (
    SCRIPT_TYPE_NURTURE,
    SCRIPT_TYPE_ORIGINAL,
    SCRIPT_TYPE_SHORT_VIDEO_REMAKE,
    SOURCE_FIELD_ALIASES,
    SCRIPT_FIELD_SPECS,
    TARGET_FIELD_ALIASES,
    compact_anchor_text,
    has_any_sync_enabled,
    is_variant_slot,
    build_prompt_with_anchor,
    build_target_fields,
    build_source_failure_fields,
    build_source_success_fields,
    build_sync_tasks,
    now_text,
    normalize_run_manager_script_type,
    prepend_script_id_header,
    prompt_has_script_id_header,
    resolved_task_target_language,
    resolve_field_mapping,
    summarize_sync_scope,
)
from core.source_voiceover_plan import build_original_voiceover_payload
from run_pipeline import (
    build_existing_target_updates,
    build_target_record_indexes,
    existing_target_conflict_reason,
    resolve_task_action,
)


class ScriptRunManagerSyncTest(unittest.TestCase):
    def setUp(self) -> None:
        source_field_names = [
            "产品编码",
            "产品类型",
            "目标语言",
            "一级类目",
            "产品参数信息",
            "任务编号",
            "店铺ID",
            "产品图片",
            "脚本类型",
            "脚本来源",
            "源复刻任务ID",
            "发布用途",
            "内容分支",
            "视频时长",
            "是否可同步",
            "是否可同步母版",
            "是否可同步子变体",
            "同步状态",
            "同步时间",
            "所属母版1",
            "所属母版2",
            "母版方向1",
            "母版方向2",
            "脚本方向一",
            "脚本1变体1",
            "脚本方向二",
            "脚本4变体5",
        ]
        target_field_names = [
            "任务名",
            "提示词",
            "参考图",
            "脚本ID",
            "店铺ID",
            "内部脚本键",
            "脚本来源",
            "脚本类型",
            "目标语言",
            "发布用途",
            "是否挂车",
            "内容分支",
            "免参考图",
            "视频时长",
            "状态",
        ]
        self.mapping = resolve_field_mapping(source_field_names, SOURCE_FIELD_ALIASES)
        self.target_mapping = resolve_field_mapping(target_field_names, TARGET_FIELD_ALIASES)

    def test_script_specs_cover_24_slots(self) -> None:
        self.assertEqual(len(SCRIPT_FIELD_SPECS), 24)

    def test_script_type_normalization_covers_original_remake_and_nurture(self) -> None:
        self.assertEqual(normalize_run_manager_script_type(), SCRIPT_TYPE_ORIGINAL)
        self.assertEqual(
            normalize_run_manager_script_type(source_script_type="短视频复刻"),
            SCRIPT_TYPE_ORIGINAL,
        )
        self.assertEqual(
            normalize_run_manager_script_type(
                source_script_type="短视频复刻",
                source_remake_record_id="rec_remake_1",
            ),
            SCRIPT_TYPE_SHORT_VIDEO_REMAKE,
        )
        self.assertEqual(
            normalize_run_manager_script_type(script_source="养号复刻"),
            SCRIPT_TYPE_NURTURE,
        )
        self.assertEqual(
            normalize_run_manager_script_type(publish_purpose="养号"),
            SCRIPT_TYPE_NURTURE,
        )

    def test_build_sync_tasks_creates_one_task_per_non_empty_script(self) -> None:
        records = [
            TableRecord(
                record_id="rec_1",
                fields={
                    "产品编码": "ABC001",
                    "产品类型": "手镯",
                    "目标语言": "泰语",
                    "一级类目": "配饰",
                    "产品参数信息": "细手圈，内径约56mm，圈宽2mm，开口可微调",
                    "任务编号": "003",
                    "店铺ID": "MYPS01",
                    "是否可同步": True,
                    "产品图片": [{"file_token": "file_1"}],
                    "视频时长": "28秒",
                    "所属母版1": "M1",
                    "所属母版2": "M2",
                    "母版方向1": "日常轻分享流",
                    "母版方向2": "问题解决流",
                    "脚本方向一": "script one",
                    "脚本1变体1": "script one v1",
                    "脚本方向二": "script two",
                    "脚本4变体5": "script four v5",
                },
            ),
        ]

        tasks = build_sync_tasks(records, self.mapping)

        self.assertEqual(
            [task.task_name for task in tasks],
            ["ABC001.003_M1_M", "ABC001.003_M1_V1", "ABC001.003_M2_M", "ABC001.003_M4_V5"],
        )
        self.assertEqual(tasks[0].reference_images, [{"file_token": "file_1"}])
        self.assertEqual(tasks[0].product_type, "手镯")
        self.assertEqual(tasks[0].business_category, "配饰")
        self.assertEqual(tasks[0].product_params, "细手圈，内径约56mm，圈宽2mm，开口可微调")
        self.assertEqual(tasks[0].video_duration, 28)
        self.assertEqual(tasks[0].store_id, "MYPS01")
        self.assertEqual(tasks[0].script_id, "003_M1_M")
        self.assertEqual(tasks[0].internal_script_key, "rec_1:S1")
        self.assertEqual(tasks[1].script_id, "003_M1_V1")
        self.assertEqual(tasks[2].script_id, "003_M2_M")
        self.assertEqual(tasks[3].script_id, "003_M4_V5")

    def test_sync_preserves_source_voiceover_contract_and_execution_plan(self) -> None:
        source_mapping = resolve_field_mapping(
            list(self.mapping.values()) + ["口播表达合同", "口播执行计划"],
            SOURCE_FIELD_ALIASES,
        )
        target_mapping = resolve_field_mapping(
            list(self.target_mapping.values()) + ["口播表达合同", "口播执行计划"],
            TARGET_FIELD_ALIASES,
        )
        contract = '{"schema_version":"voiceover-expression-contract-v2"}'
        plan = '{"schema_version":"voiceover-execution-plan-v1","mode":"REUSE_APPROVED_COPY"}'
        task = build_sync_tasks([
            TableRecord("rec_voice", {
                "产品编码": "ABC001",
                "是否可同步母版": True,
                "脚本方向一": "source script",
                "口播表达合同": contract,
                "口播执行计划": plan,
            })
        ], source_mapping)[0]
        fields = build_target_fields(task, target_mapping)
        self.assertEqual(fields["口播表达合同"], contract)
        self.assertEqual(fields["口播执行计划"], plan)

    def test_original_script_voiceover_becomes_a_fresh_copy_generation_route(self) -> None:
        source = """【商品】短款外搭让比例更利落
【镜头1|0-2s|hook】
画面:正面上身
口播:ดูทรงนี้ก่อนนะ
【镜头2|2s|proof】
画面:腰线近景
口播:ชายเสื้ออยู่ใกล้เอวสูง
【情绪】像朋友自然分享"""
        payload = build_original_voiceover_payload(source)
        self.assertIsNotNone(payload)
        plan = payload["execution_plan"]
        self.assertEqual(plan["mode"], "GENERATE_FROM_VERIFIED_VISUAL_FACTS")
        self.assertNotIn("target_text", plan)
        self.assertNotIn("ดูทรงนี้ก่อนนะ", str(payload))
        contract = payload["expression_contract"]
        self.assertEqual(contract["schema_version"], "voiceover-expression-contract-v2")
        self.assertFalse(contract["hook_preconditions"]["newness_authorized"])
        self.assertFalse(contract["hook_preconditions"]["audience_tension_authorized"])
        self.assertFalse(contract["hook_preconditions"]["comparison_authorized"])
        self.assertFalse(contract["hook_preconditions"]["social_proof_authorized"])
        self.assertFalse(contract["hook_preconditions"]["audience_need_authorized"])
        self.assertFalse(contract["hook_preconditions"]["visual_result_authorized"])
        self.assertFalse(contract["speech_policy"]["soft_warning_polish_enabled"])
        self.assertEqual(
            contract["speech_policy"]["plan_mode"],
            "deterministic_semantic_segments",
        )

    def test_original_script_exports_explicit_visual_facts(self) -> None:
        source = """【商品】深色短款牛仔外套
【镜头1|2s|hook】
画面:翻领、前襟四颗纽扣和左右胸前口袋清楚可见，衣长落在腰部附近
口播:ดูตัวนี้ก่อนนะ
【镜头2|2s|proof】
画面:近景可见深色牛仔车线和纽扣
口播:ดูรายละเอียดตรงนี้
"""
        payload = build_original_voiceover_payload(source)
        self.assertEqual(
            payload["expression_contract"]["verified_visual_facts"],
            [
                {
                    "concept_key": "pocket",
                    "exact_fact_zh": "胸前带有口袋",
                    "normalizer_text": "口袋",
                    "operator_priority": "core",
                },
                {
                    "concept_key": "cropped_length",
                    "exact_fact_zh": "短款衣长落在腰部附近",
                    "normalizer_text": "短款",
                    "operator_priority": "normal",
                },
                {
                    "concept_key": "closure_detail",
                    "exact_fact_zh": "前襟有四颗可见纽扣",
                    "normalizer_text": "前襟扣位",
                    "operator_priority": "core",
                },
                {
                    "concept_key": "seam_detail",
                    "exact_fact_zh": "可见明线与车线细节",
                    "normalizer_text": "分割明线",
                    "operator_priority": "optional",
                },
            ],
        )
        self.assertEqual(
            payload["expression_contract"]["product_category"],
            {"category_zh": "牛仔夹克", "target_language_hint": "แจ็กเก็ตยีนส์"},
        )

    def test_build_sync_tasks_uses_original_script_for_master_slot(self) -> None:
        records = [
            TableRecord(
                record_id="rec_master_prompt",
                fields={
                    "产品编码": "ABC099",
                    "任务编号": "099",
                    "是否可同步": True,
                    "脚本方向一": "original script one",
                    "脚本1变体1": "variant one",
                },
            ),
        ]

        tasks = build_sync_tasks(records, self.mapping)

        self.assertEqual(tasks[0].task_name, "ABC099.099_M1_M")
        self.assertEqual(tasks[0].prompt_text, "original script one")
        self.assertEqual(tasks[1].prompt_text, "variant one")

    def test_build_sync_tasks_falls_back_to_task_no_when_product_code_missing(self) -> None:
        records = [
            TableRecord(
                record_id="rec_missing_code",
                fields={
                    "任务编号": "053",
                    "是否可同步母版": True,
                    "产品图片": [{"file_token": "file_1"}],
                    "脚本方向一": "master one",
                    "脚本方向二": "master two",
                },
            ),
        ]

        tasks = build_sync_tasks(records, self.mapping)

        self.assertEqual([task.task_name for task in tasks], ["053.053_M1_M", "053.053_M2_M"])
        self.assertTrue(all(task.product_code == "053" for task in tasks))
        self.assertEqual(tasks[0].script_id, "053_M1_M")

    def test_split_sync_checkboxes_only_sync_master_slots_when_master_checked(self) -> None:
        records = [
            TableRecord(
                record_id="rec_master",
                fields={
                    "产品编码": "ABC010",
                    "是否可同步母版": True,
                    "是否可同步子变体": False,
                    "产品图片": [{"file_token": "file_1"}],
                    "脚本方向一": "master one",
                    "脚本1变体1": "variant one",
                    "脚本方向二": "master two",
                },
            ),
        ]

        tasks = build_sync_tasks(records, self.mapping)

        self.assertEqual([task.task_name for task in tasks], ["ABC010.ABC010_M1_M", "ABC010.ABC010_M2_M"])

    def test_split_sync_checkboxes_only_sync_variant_slots_when_variant_checked(self) -> None:
        records = [
            TableRecord(
                record_id="rec_variant",
                fields={
                    "产品编码": "ABC011",
                    "是否可同步母版": False,
                    "是否可同步子变体": True,
                    "产品图片": [{"file_token": "file_1"}],
                    "脚本方向一": "master one",
                    "脚本1变体1": "variant one",
                    "脚本4变体5": "variant four v5",
                },
            ),
        ]

        tasks = build_sync_tasks(records, self.mapping)

        self.assertEqual([task.task_name for task in tasks], ["ABC011.ABC011_M1_V1", "ABC011.ABC011_M4_V5"])

    def test_build_sync_tasks_respects_checkbox(self) -> None:
        records = [
            TableRecord(
                record_id="rec_1",
                fields={
                    "产品编码": "ABC001",
                    "是否可同步": False,
                    "产品图片": [{"file_token": "file_1"}],
                    "脚本方向一": "script one",
                },
            ),
        ]

        tasks = build_sync_tasks(records, self.mapping)
        self.assertEqual(tasks, [])

    def test_has_any_sync_enabled_supports_split_fields(self) -> None:
        fields = {"是否可同步母版": True}
        self.assertTrue(has_any_sync_enabled(fields, self.mapping))

    def test_is_variant_slot(self) -> None:
        self.assertFalse(is_variant_slot("S1"))
        self.assertTrue(is_variant_slot("S1V1"))

    def test_summarize_sync_scope(self) -> None:
        tasks = build_sync_tasks(
            [
                TableRecord(
                    record_id="rec_scope",
                    fields={
                        "产品编码": "ABC012",
                        "是否可同步": True,
                        "产品图片": [{"file_token": "file_1"}],
                        "脚本方向一": "master one",
                        "脚本1变体1": "variant one",
                        "脚本4变体5": "variant four v5",
                    },
                ),
            ],
            self.mapping,
        )

        self.assertEqual(summarize_sync_scope(tasks), "母版+子变体（母版 1 条，子变体 2 条）")

    def test_build_target_fields_includes_script_id(self) -> None:
        records = [
            TableRecord(
                record_id="rec_1",
                fields={
                    "产品编码": "ABC001",
                    "产品类型": "手镯",
                    "目标语言": "泰语",
                    "一级类目": "配饰",
                    "产品参数信息": "细手圈，内径约56mm，圈宽2mm，开口可微调",
                    "任务编号": "003",
                    "店铺ID": "MYPS01",
                    "是否可同步": True,
                    "产品图片": [{"file_token": "file_1"}],
                    "所属母版1": "M1",
                    "母版方向1": "日常轻分享流",
                    "脚本方向一": "script one",
                },
            ),
        ]

        tasks = build_sync_tasks(records, self.mapping)
        fields = build_target_fields(tasks[0], self.target_mapping)

        self.assertEqual(fields["任务名"], "ABC001.003_M1_M")
        self.assertTrue(fields["提示词"].startswith("【脚本ID】\n- 003_M1_M\n\n产品锚点：手镯｜细手圈，内径约56mm，圈宽2mm，开口可微调\n"))
        self.assertTrue(fields["提示词"].endswith("script one"))
        self.assertEqual(fields["脚本ID"], "003_M1_M")
        self.assertEqual(fields["店铺ID"], "MYPS01")
        self.assertEqual(fields["内部脚本键"], "rec_1:S1")
        self.assertEqual(fields["脚本类型"], "原创脚本")
        self.assertEqual(fields["目标语言"], "泰语")
        self.assertEqual(fields["视频时长"], 15)

    def test_thai_store_fills_target_language_when_source_field_is_empty(self) -> None:
        records = [
            TableRecord(
                record_id="rec_th_store",
                fields={
                    "产品编码": "ABC_TH",
                    "产品类型": "外套",
                    "目标语言": "",
                    "店铺ID": "THFZ01",
                    "是否可同步": True,
                    "产品图片": [{"file_token": "file_1"}],
                    "脚本方向一": "script one",
                },
            ),
        ]

        task = build_sync_tasks(records, self.mapping)[0]
        fields = build_target_fields(task, self.target_mapping)

        self.assertEqual(resolved_task_target_language(task), "泰语")
        self.assertEqual(fields["目标语言"], "泰语")
        self.assertIn("目标语言：泰语", fields["提示词"])

    def test_existing_script_id_backfills_store_id_when_empty(self) -> None:
        existing_target = TableRecord(
            record_id="target_1",
            fields={
                "任务名": "OLD.S1",
                "内部脚本键": "OLD.S1",
                "脚本ID": "003_M1_M",
                "状态": "待处理",
            },
        )
        task = build_sync_tasks(
            [
                TableRecord(
                    record_id="source_1",
                    fields={
                        "产品编码": "ABC001",
                        "任务编号": "003",
                        "店铺ID": "TH01",
                        "是否可同步母版": True,
                        "脚本方向一": "same script",
                    },
                )
            ],
            self.mapping,
        )[0]

        fields = build_target_fields(task, self.target_mapping)
        updates = build_existing_target_updates(
            existing_target,
            fields,
            self.target_mapping,
            allow_full_patch=False,
        )

        self.assertEqual(updates["店铺ID"], "TH01")
        self.assertEqual(updates["脚本类型"], "原创脚本")

    def test_build_target_fields_carries_video_duration(self) -> None:
        records = [
            TableRecord(
                record_id="rec_duration",
                fields={
                    "产品编码": "ABC088",
                    "任务编号": "088",
                    "是否可同步母版": True,
                    "视频时长": 31,
                    "脚本方向一": "duration script",
                },
            ),
        ]

        tasks = build_sync_tasks(records, self.mapping)
        fields = build_target_fields(tasks[0], self.target_mapping)

        self.assertEqual(tasks[0].video_duration, 31)
        self.assertEqual(fields["视频时长"], 31)

    def test_existing_internal_script_key_updates_instead_of_create(self) -> None:
        existing_records = [
            TableRecord(
                record_id="target_1",
                fields={
                    "任务名": "ABC001.003_M1_M",
                    "内部脚本键": "ABC001.S1",
                    "脚本ID": "003_M1_M",
                    "状态": "待处理",
                },
            )
        ]
        task = build_sync_tasks(
            [
                TableRecord(
                    record_id="source_1",
                    fields={
                        "产品编码": "ABC001",
                        "任务编号": "004",
                        "是否可同步母版": True,
                        "脚本方向一": "new script",
                    },
                )
            ],
            self.mapping,
        )[0]

        indexes = build_target_record_indexes(existing_records, self.target_mapping)

        self.assertEqual(resolve_task_action(task, indexes, self.target_mapping), "create")

    def test_existing_script_id_skips_create(self) -> None:
        existing_records = [
            TableRecord(
                record_id="target_1",
                fields={
                    "任务名": "OLD.S1",
                    "内部脚本键": "OLD.S1",
                    "脚本ID": "003_M1_M",
                    "状态": "待处理",
                },
            )
        ]
        task = build_sync_tasks(
            [
                TableRecord(
                    record_id="source_1",
                    fields={
                        "产品编码": "ABC001",
                        "任务编号": "003",
                        "是否可同步母版": True,
                        "脚本方向一": "same script",
                    },
                )
            ],
            self.mapping,
        )[0]

        indexes = build_target_record_indexes(existing_records, self.target_mapping)

        self.assertEqual(resolve_task_action(task, indexes, self.target_mapping), "skip(script_id exists)")

    def test_prepend_script_id_header_does_not_duplicate_standard_header(self) -> None:
        prompt = prepend_script_id_header("【脚本ID】\n- old_id\n\n正文内容", "003_M1_V1")

        self.assertEqual(prompt.count("【脚本ID】"), 1)
        self.assertTrue(prompt.startswith("【脚本ID】\n- 003_M1_V1\n\n正文内容"))
        self.assertTrue(prompt_has_script_id_header(prompt))

    def test_prepend_script_id_header_keeps_legacy_content_id_below_script_id(self) -> None:
        prompt = prepend_script_id_header("【内容ID】\n- 123456\n\n正文内容", "003_M1_V1")

        self.assertTrue(prompt.startswith("【脚本ID】\n- 003_M1_V1\n\n【内容ID】\n- 123456"))

    def test_nurture_task_sets_reference_free_flag(self) -> None:
        source_field_names = [
            "产品编码",
            "任务编号",
            "是否可同步母版",
            "脚本方向一",
            "脚本来源",
            "发布用途",
            "是否挂车",
            "内容分支",
        ]
        mapping = resolve_field_mapping(source_field_names, SOURCE_FIELD_ALIASES)
        tasks = build_sync_tasks(
            [
                TableRecord(
                    record_id="rec_nurture",
                    fields={
                        "产品编码": "YR028",
                        "任务编号": "028",
                        "是否可同步母版": True,
                        "脚本方向一": "final storyboard",
                        "脚本来源": "养号复刻",
                        "发布用途": "养号",
                        "是否挂车": "否",
                        "内容分支": "非商品展示型",
                    },
                ),
            ],
            mapping,
        )

        fields = build_target_fields(tasks[0], self.target_mapping)

        self.assertEqual(fields["脚本来源"], "养号复刻")
        self.assertEqual(fields["脚本类型"], "养号脚本")
        self.assertEqual(fields["发布用途"], "养号")
        self.assertEqual(fields["是否挂车"], "否")
        self.assertEqual(fields["内容分支"], "非商品展示型")
        self.assertEqual(fields["免参考图"], "是")

    def test_nurture_task_with_reference_frames_disables_reference_free_flag(self) -> None:
        source_field_names = [
            "产品编码",
            "任务编号",
            "是否可同步母版",
            "产品图片",
            "脚本方向一",
            "脚本来源",
            "发布用途",
        ]
        mapping = resolve_field_mapping(source_field_names, SOURCE_FIELD_ALIASES)
        tasks = build_sync_tasks(
            [
                TableRecord(
                    record_id="rec_nurture_ref",
                    fields={
                        "产品编码": "YR029",
                        "任务编号": "029",
                        "是否可同步母版": True,
                        "产品图片": [{"file_token": "reference_1"}],
                        "脚本方向一": "final storyboard",
                        "脚本来源": "养号复刻",
                        "发布用途": "养号",
                    },
                ),
            ],
            mapping,
        )

        fields = build_target_fields(tasks[0], self.target_mapping)

        self.assertEqual(fields["参考图"], [{"file_token": "reference_1"}])
        self.assertEqual(fields["免参考图"], "否")

    def test_build_prompt_with_anchor_falls_back_to_raw_script_without_anchor_fields(self) -> None:
        prompt = build_prompt_with_anchor(
            build_sync_tasks(
                [
                    TableRecord(
                        record_id="rec_2",
                        fields={
                            "产品编码": "ABC002",
                            "任务编号": "004",
                            "是否可同步": True,
                            "产品图片": [{"file_token": "file_2"}],
                            "脚本方向一": "plain script",
                        },
                    ),
                ],
                self.mapping,
            )[0]
        )

        self.assertIn("【口播/字幕语言强制约束】", prompt)
        self.assertTrue(prompt.startswith("【脚本ID】\n- 004_M1_M\n\n"))
        self.assertIn("plain script", prompt)

    def test_compact_anchor_text_flattens_multiline_and_truncates(self) -> None:
        text = compact_anchor_text(
            "细手圈\n内径约56mm\n圈宽2mm\n开口可微调\n适合日常轻佩戴\n避免过度拉伸",
            max_length=24,
        )

        self.assertIn("细手圈；内径约56mm", text)
        self.assertTrue(text.endswith("…"))

    def test_source_backwrite_fields(self) -> None:
        ts = now_text()
        success_fields = build_source_success_fields(
            self.mapping,
            synced_count=24,
            synced_at=ts,
            sync_scope="母版（4 条）",
            cleared_legacy=True,
            cleared_master=True,
            cleared_variant=True,
        )
        failure_fields = build_source_failure_fields(
            self.mapping,
            error_message="boom",
            synced_at=ts,
            sync_scope="子变体（20 条）",
        )

        self.assertFalse(success_fields["是否可同步"])
        self.assertFalse(success_fields["是否可同步母版"])
        self.assertFalse(success_fields["是否可同步子变体"])
        self.assertIn("母版（4 条）", success_fields["同步状态"])
        self.assertIn("新增 24 条", success_fields["同步状态"])
        self.assertEqual(success_fields["同步时间"], ts)
        self.assertIn("子变体（20 条）", failure_fields["同步状态"])
        self.assertIn("同步失败", failure_fields["同步状态"])

    def test_source_success_fields_report_actual_created_and_patched_counts(self) -> None:
        ts = now_text()
        fields = build_source_success_fields(
            self.mapping,
            synced_count=1,
            synced_at=ts,
            sync_scope="母版（4 条）",
            patched_count=2,
            existing_count=1,
        )

        self.assertIn("新增 1 条", fields["同步状态"])
        self.assertIn("补写 2 条", fields["同步状态"])
        self.assertIn("已存在 1 条", fields["同步状态"])

    def test_existing_target_conflict_blocks_legacy_task_name_collision(self) -> None:
        task = build_sync_tasks(
            [
                TableRecord(
                    record_id="source_1",
                    fields={
                        "产品编码": "ABC001",
                        "任务编号": "824",
                        "是否可同步母版": True,
                        "脚本方向一": "new script",
                    },
                )
            ],
            self.mapping,
        )[0]
        existing = TableRecord(
            record_id="target_1",
            fields={
                "任务名": "ABC001.S1",
                "提示词": "【脚本ID】\n- 022_M1_M\n\nold script",
                "脚本ID": "022_M1_M",
                "状态": "已发布",
            },
        )

        reason = existing_target_conflict_reason(task, existing, "任务名", self.target_mapping)

        self.assertIn("脚本ID不一致", reason)


if __name__ == "__main__":
    unittest.main()
