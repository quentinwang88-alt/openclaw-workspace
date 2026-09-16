#!/usr/bin/env python3
"""提示词压缩守卫测试。

压缩是"无损重排"，所以测试的重点不是"变短了"，而是**关键信息一条不少**：
硬约束段、逐镜维度、动作主线、身份锁、口播原文都必须在压缩后逐字成立。
"""
from __future__ import annotations

import os
import unittest
from pathlib import Path
import sys
from unittest.mock import patch

TESTS_DIR = Path(__file__).resolve().parent
SKILL_DIR = TESTS_DIR.parent
if str(SKILL_DIR) not in sys.path:
    sys.path.insert(0, str(SKILL_DIR))

from core.video_prompt_compaction import (  # noqa: E402
    DEFAULT_VIDEO_PROMPT_CHAR_LIMIT,
    VIDEO_PROMPT_CHAR_LIMIT_ENV,
    audit_protected_text,
    audit_shot_protection,
    compact_video_prompt,
    compact_video_prompt_report,
    video_prompt_char_limit,
)


def _long_prompt() -> str:
    """A prompt shaped like the UGC-native renderer's output."""
    return "\n".join(
        [
            "【视频任务】",
            "竖屏手机短视频，时长15秒。",
            "",
            "【商品身份锁｜最高优先级】",
            "商品外观以参考图为唯一准则；商品一致性优先于人物美感、场景氛围和镜头效果。",
            "",
            "【整片语义主线｜不做逐句逐镜绑定】",
            "主消费情境：约会、上班、度假、旅游都适合",
            "核心购买理由：细链条耳线设计，不会勾到头巾和头发",
            "必须保持：蝴蝶造型主体；细链连接直条穿引端的耳线结构；整体尺寸约长4.5cm、宽1cm",
            "",
            "【商品负向约束】",
            "禁止把商品替换成相似款或根据常见配饰重新设计；禁止新增参考图中没有的宝石或文字",
            "",
            "【全片不露脸｜硬约束】",
            "任何一镜都不得出现眼睛入画、鼻子入画、嘴部入画、正面全脸。",
            "人物只能以这些局部入画：耳廓与耳垂近景、耳侧与颈侧关系、少量下颌边缘。",
            "",
            "【人物身份锁｜与商品参考分权】",
            "人物模板ID：TH_APPAREL_REAL_01_001",
            "人物比例：UNAVAILABLE",
            "人物身份、年龄感、体型、发型和自然肤质以人物模板参考为准；商品参考图只决定商品外观，其中模特、滤镜、姿态和背景均无人物权威。",
            "参考策略：PERSONA_PRODUCT_COMPOSITE_REQUIRED",
            "",
            "【配饰佩戴与展示关系】",
            "商品关系：商品已经正确佩戴在耳部，耳侧保持清楚可见",
            "必要结果：从已经佩戴好的状态开始，至少有一段清楚看到耳饰与耳部或整套穿搭的关系",
            "角色：耳饰是耳侧与领口关系中的主要造型焦点。",
            "",
            "【本条动作主线｜只执行这一条】",
            "动作类型：RESULT_SHOW",
            "核心动作：人物完成一次自然上半身角度变化，下一段重新放置手机回到耳侧与颈侧近景",
            "",
            "【拍摄方式｜UGC_NATIVE_V2_MULTICLIP】",
            "普通用户使用手机竖屏随手记录，使用现场已有自然光或普通室内光；机位简单，允许轻微手持感、轻微构图不完美和真实环境层次，人物皮肤、衣物和背景保留自然质感",
            "",
            "【人物、穿搭与生活场景】",
            "出镜方式：PERSON_ON_CAMERA（本片全片不露脸，人物只以耳侧、耳廓、颈侧等局部身体入画）",
            "人物：准备离开室内的日常配饰使用者；自然未精修肤质",
            "基础穿搭：不适用",
            "地点与时刻：客厅窗边通往门口的自然动线中，入口窄墙面与鞋柜旁的同一小片区域。；准备离开室内前，创作者用同一部手机连续补录商品细节与佩戴效果。",
            "生活痕迹：鞋柜边自然放置一只准备带出门的素色布袋",
            "",
            "【拍摄片段01｜0.0-4.0s｜HOOK】",
            "画面事件：耳饰安放在鞋柜上的哑光灰台面，蝴蝶造型主体占据清楚焦点，细链与直条穿引端自然延伸",
            "人物动作：耳饰从第一帧已经清楚可见，人物的肩部和耳侧正处在一个很小的自然角度变化中，不先静止等待再开始",
            "本段相对上一段的新信息：RESULT_OR_ENTRY_ESTABLISHMENT",
            "商品执行关系：PRODUCT_RESULT_CLOSE",
            "本段手机构图：独立录制商品已经佩戴完成的结果近景，让小商品第一眼清楚可辨",
            "商品必须可见：蝴蝶造型主体；细链连接直条穿引端的耳线结构",
            "",
            "【直接剪切｜开始另一段独立手机素材】",
            "",
            "【拍摄片段02｜4.0-7.0s｜PROOF】",
            "画面事件：直接切至哑光灰背景前的纯手部画面，手指稳定承托耳饰本体，蝴蝶主体、细链和直条穿引端共同处于可辨范围",
            "人物动作：人物完成一次自然上半身角度变化，下一段重新放置手机回到耳侧与颈侧近景",
            "本段相对上一段的新信息：PRIMARY_PRODUCT_EVIDENCE",
            "商品执行关系：NATURAL_MOTION_RELATION",
            "本段手机构图：同一地点重新放置手机，录制一次连续的上半身或拍摄关系变化，不用重复摆头支撑整段",
            "商品必须可见：蝴蝶造型主体；细链连接直条穿引端的耳线结构",
            "",
            "【连续口播｜必须原样使用目标语言】",
            "Saya baru jumpa subang ni, rasa sesuai nak pakai pergi kerja.",
            "",
            "【统一执行】",
            "第一优先保持商品身份、佩戴状态、人物肢体和穿搭连续；第二优先完整执行以上独立可见片段并真实直接剪切；第三优先卖点关系与原生手机可行性。发生冲突时先简化场景陈设和人物表演，不得合并片段或退回一镜到底。",
        ]
    )


def _shot_block(index: int, start: float, end: float, role: str, anchor: str) -> List[str]:
    return [
        f"【拍摄片段{index:02d}｜{start}-{end}s｜{role}】",
        f"画面事件：第{index}段独立录制的商品可见事件，商品始终处于可辨范围",
        f"人物动作：第{index}段只做很小的自然角度变化，不重复上一段的动作",
        f"商品必须可见：{anchor}",
        "",
    ]


def _two_shot_prompt(*, first: str, second: str) -> str:
    """Two shots that name *different* observation requirements."""
    lines = [
        "【视频任务】",
        "竖屏手机短视频，时长15秒。",
        "",
        "【商品负向约束】",
        "禁止把商品替换成相似款或根据常见配饰重新设计；禁止新增参考图中没有的宝石或文字",
        "",
        "【全片不露脸｜硬约束】",
        "任何一镜都不得出现眼睛入画、鼻子入画、嘴部入画、正面全脸。",
        "",
        "【拍摄片段01｜0.0-7.5s｜HOOK】",
        "画面事件：商品安放在哑光灰台面上，主体占据清楚焦点",
        "人物动作：保持已完成的佩戴，只做很小的自然角度变化",
        f"商品必须可见：{first}",
        "",
        "【直接剪切｜开始另一段独立手机素材】",
        "",
        "【拍摄片段02｜7.5-15.0s｜PROOF】",
        "画面事件：手指稳定承托商品本体，本体处于可辨范围",
        "人物动作：手指稳定承托商品，小幅转动本体",
        f"商品必须可见：{second}",
        "",
        "【统一执行】",
        "保持同一人物、商品、穿搭、场景和连续事件；商品一致性优先于场景美感和镜头效果。",
    ]
    return "\n".join(lines)


def _four_shot_prompt(anchors) -> str:
    lines = [
        "【视频任务】",
        "竖屏手机短视频，时长15秒。",
        "",
        "【商品负向约束】",
        "禁止把商品替换成相似款或根据常见配饰重新设计",
        "",
        "【全片不露脸｜硬约束】",
        "任何一镜都不得出现眼睛入画、鼻子入画、嘴部入画、正面全脸。",
        "",
    ]
    for index, anchor in enumerate(anchors, 1):
        start = (index - 1) * 3.75
        lines.extend(_shot_block(index, start, start + 3.75, "PROOF", anchor))
    lines.extend(
        [
            "【统一执行】",
            "保持同一人物、商品、穿搭、场景和连续事件；商品一致性优先于场景美感和镜头效果。",
        ]
    )
    return "\n".join(lines)


class CompactionRuleTest(unittest.TestCase):
    def test_short_prompt_is_untouched(self):
        prompt = "【视频任务】\n竖屏手机短视频，时长15秒。"
        self.assertEqual(compact_video_prompt(prompt, limit=4000), prompt)

    def test_disabled_limit_returns_original(self):
        prompt = _long_prompt()
        self.assertEqual(compact_video_prompt(prompt, limit=0), prompt)
        self.assertEqual(compact_video_prompt(prompt, limit=-5), prompt)

    def test_over_limit_prompt_is_compacted(self):
        prompt = _long_prompt()
        compacted = compact_video_prompt(prompt, limit=600)
        self.assertLess(len(compacted), len(prompt))
        self.assertNotEqual(compacted, prompt)

    def test_protected_sections_survive_verbatim(self):
        prompt = _long_prompt()
        compacted = compact_video_prompt(prompt, limit=600)
        for section, body in (
            ("【商品负向约束】", "禁止把商品替换成相似款或根据常见配饰重新设计"),
            ("【全片不露脸｜硬约束】", "任何一镜都不得出现眼睛入画"),
            ("【全片不露脸｜硬约束】", "人物只能以这些局部入画：耳廓与耳垂近景"),
        ):
            self.assertIn(section, compacted)
            self.assertIn(body, compacted)

    def test_placeholder_lines_dropped(self):
        compacted = compact_video_prompt(_long_prompt(), limit=600)
        self.assertNotIn("人物比例：UNAVAILABLE", compacted)
        self.assertNotIn("基础穿搭：不适用", compacted)

    def test_protocol_tokens_dropped_but_shot_role_kept(self):
        compacted = compact_video_prompt(_long_prompt(), limit=600)
        self.assertNotIn("本段相对上一段的新信息", compacted)
        self.assertNotIn("参考策略：PERSONA_PRODUCT_COMPOSITE_REQUIRED", compacted)
        # 商品执行关系是可读的镜头职能，保留
        self.assertIn("商品执行关系：PRODUCT_RESULT_CLOSE", compacted)

    def test_duplicate_anchor_merged_into_one_global_line(self):
        compacted = compact_video_prompt(_long_prompt(), limit=600)
        self.assertEqual(compacted.count("每段商品必须可见："), 1)
        self.assertNotIn("\n商品必须可见：", compacted)
        self.assertIn("细链连接直条穿引端的耳线结构", compacted)

    def test_shot_dimensions_survive(self):
        compacted = compact_video_prompt(_long_prompt(), limit=600)
        self.assertIn("画面事件：", compacted)
        self.assertEqual(compacted.count("画面事件："), 2)
        self.assertIn("人物动作：", compacted)

    def test_action_spine_is_not_deduplicated_away(self):
        """核心动作 与第二段 人物动作 同文，但两处都是权威，必须都留。"""
        compacted = compact_video_prompt(_long_prompt(), limit=600)
        self.assertIn("核心动作：人物完成一次自然上半身角度变化", compacted)
        self.assertIn("动作类型：RESULT_SHOW", compacted)

    def test_identity_lock_is_not_weakened(self):
        compacted = compact_video_prompt(_long_prompt(), limit=600)
        self.assertIn("必须保持：蝴蝶造型主体；细链连接直条穿引端的耳线结构；整体尺寸约长4.5cm、宽1cm", compacted)

    def test_voiceover_is_untouched(self):
        prompt = _long_prompt()
        compacted = compact_video_prompt(prompt, limit=600)
        voice = "Saya baru jumpa subang ni, rasa sesuai nak pakai pergi kerja."
        self.assertIn(voice, prompt)
        self.assertIn(voice, compacted)

    def test_guard_lines_are_never_dropped(self):
        compacted = compact_video_prompt(_long_prompt(), limit=600)
        self.assertIn("不得合并片段或退回一镜到底", compacted)

    def test_repeated_scene_clause_is_deduplicated(self):
        """地点与时刻 的前半句与其它行重复时让位，时刻信息保留。"""
        compacted = compact_video_prompt(_long_prompt(), limit=600)
        self.assertIn("准备离开室内前", compacted)
        self.assertLess(
            compacted.count("客厅窗边通往门口的自然动线中"),
            2,
        )

    def test_no_blank_line_is_left_behind_by_deletions(self):
        compacted = compact_video_prompt(_long_prompt(), limit=600)
        self.assertNotIn("\n\n\n", compacted)
        self.assertFalse(compacted.startswith("\n"))
        self.assertFalse(compacted.endswith("\n\n"))

    def test_over_budget_without_redundancy_is_returned_not_truncated(self):
        """无冗余可去时宁可超长，也不截断合同。"""
        prompt = "\n".join(
            [f"【片段{i:02d}】" + "甲乙丙丁戊己庚辛壬癸" * 8 for i in range(20)]
        )
        compacted = compact_video_prompt(prompt, limit=100)
        self.assertIn("甲乙丙丁戊己庚辛壬癸", compacted)
        self.assertTrue(compacted.rstrip().endswith("甲乙丙丁戊己庚辛壬癸"))


class PerShotAnchorProtectionTest(unittest.TestCase):
    """T13 / Review #9：压缩不得把某镜的观察要求冒充成每段的要求。

    修复前的行为：取第一条 ``商品必须可见`` 改写成 ``每段商品必须可见``，
    删掉其余各镜自己的那一行 —— 一句话同时犯了两个错：
    把第一镜的要求断言到第二~四镜，并静默删除它们自己的要求。
    """

    def test_T13_different_per_shot_anchors_keep_their_own_shot(self):
        prompt = _two_shot_prompt(first="耳饰整体轮廓", second="背部耳针连接处")
        compacted = compact_video_prompt(prompt, limit=200)
        shot1, _, shot2 = compacted.partition("【拍摄片段02")
        self.assertIn("商品必须可见：耳饰整体轮廓", shot1)
        self.assertIn("商品必须可见：背部耳针连接处", shot2)
        # 不得提升，也不得让第二镜的要求出现在第一镜区块里。
        self.assertNotIn("每段商品必须可见", compacted)
        self.assertNotIn("背部耳针连接处", shot1)

    def test_T13_identical_per_shot_anchors_are_still_merged(self):
        """相同要求仍应合理去重，否则这次修复就变成了「什么都不删」。"""
        prompt = _two_shot_prompt(first="蝴蝶造型主体", second="蝴蝶造型主体")
        compacted = compact_video_prompt(prompt, limit=200)
        self.assertEqual(compacted.count("每段商品必须可见：蝴蝶造型主体"), 1)
        self.assertNotIn("\n商品必须可见：", compacted)

    def test_T13_a_punctuation_difference_is_still_the_same_rule(self):
        """只差标点不构成「不同要求」，否则去重会被标点噪声废掉。"""
        prompt = _two_shot_prompt(first="蝴蝶造型主体", second="蝴蝶造型主体。")
        compacted = compact_video_prompt(prompt, limit=200)
        self.assertEqual(compacted.count("每段商品必须可见："), 1)

    def test_one_disagreeing_shot_blocks_the_lift_for_all(self):
        """四个镜头里只要有一个不同，就整体不提升。"""
        prompt = _four_shot_prompt(
            ["整体轮廓", "整体轮廓", "整体轮廓", "背部连接处"]
        )
        compacted = compact_video_prompt(prompt, limit=200)
        self.assertNotIn("每段商品必须可见", compacted)
        self.assertIn("商品必须可见：背部连接处", compacted)

    def test_the_lifted_rule_is_not_reported_as_a_loss(self):
        """合法提升后审计必须放行，否则守卫会恒红。"""
        prompt = _two_shot_prompt(first="蝴蝶造型主体", second="蝴蝶造型主体")
        report = compact_video_prompt_report(prompt, limit=200)
        self.assertTrue(report.ok, report.shot_migrations)

    def test_a_prompt_that_fits_is_returned_untouched(self):
        prompt = _two_shot_prompt(first="耳饰整体轮廓", second="背部耳针连接处")
        report = compact_video_prompt_report(prompt, limit=100000)
        self.assertFalse(report.applied)
        self.assertEqual(report.text, prompt)


class ShotOwnershipAuditTest(unittest.TestCase):
    """要求 5：压缩后核对每镜保护项归属，而不是只搜关键字还在不在全文里。"""

    def test_audit_catches_a_constraint_that_migrated_between_shots(self):
        before = _two_shot_prompt(first="整体轮廓", second="背部连接")
        after = before.replace("商品必须可见：整体轮廓", "商品必须可见：占位甲")
        after = after.replace("商品必须可见：背部连接", "商品必须可见：整体轮廓")
        problems = audit_shot_protection(before, after)
        self.assertTrue(
            any("挪到" in item and "整体轮廓" in item for item in problems), problems
        )

    def test_audit_catches_a_deleted_shot_constraint(self):
        before = _two_shot_prompt(first="整体轮廓", second="背部连接")
        after = before.replace("\n商品必须可见：背部连接", "")
        problems = audit_shot_protection(before, after)
        self.assertTrue(any("丢失" in item and "背部连接" in item for item in problems))

    def test_audit_is_silent_when_every_shot_keeps_its_own(self):
        prompt = _two_shot_prompt(first="整体轮廓", second="背部连接")
        self.assertEqual(audit_shot_protection(prompt, prompt), ())

    def test_audit_is_silent_for_a_legitimate_lift(self):
        before = _two_shot_prompt(first="蝴蝶造型主体", second="蝴蝶造型主体")
        after = before.replace(
            "商品必须可见：蝴蝶造型主体", "每段商品必须可见：蝴蝶造型主体", 1
        ).replace("\n商品必须可见：蝴蝶造型主体", "")
        self.assertEqual(
            audit_shot_protection(before, after, lifted=("商品必须可见",)), ()
        )

    def test_a_migrated_constraint_would_pass_a_whole_text_search(self):
        """证明这个审计不是多余的：纯关键字搜索会误判通过。"""
        before = _two_shot_prompt(first="整体轮廓", second="背部连接")
        after = before.replace("商品必须可见：整体轮廓", "商品必须可见：占位甲")
        after = after.replace("商品必须可见：背部连接", "商品必须可见：整体轮廓")
        self.assertIn("整体轮廓", after)  # 全文搜索：还在
        self.assertTrue(audit_shot_protection(before, after))  # 归属审计：挪走了

    def test_protected_text_audit_catches_a_rewritten_shot_header(self):
        before = _two_shot_prompt(first="整体轮廓", second="背部连接")
        after = before.replace("【拍摄片段01｜0.0-7.5s｜HOOK】", "【片段01】")
        losses = audit_protected_text(before, after)
        self.assertTrue(any("镜头标题" in item for item in losses), losses)


class ProtectedConstraintLabelTest(unittest.TestCase):
    """要求 3：承载、裁切、动作边界不得被当作重复修饰删掉。"""

    def test_the_action_spine_survives_a_verbatim_duplicate_earlier_in_the_text(self):
        """修复前 ``核心动作`` 不在保护名单里，会被同值行挤掉。"""
        prompt = "\n".join(
            [
                "【视频任务】",
                "竖屏手机短视频，时长15秒。",
                "",
                "【人物、穿搭与生活场景】",
                "地点身份：人物完成一次自然上半身角度变化",
                "",
                "【本条动作主线｜只执行这一条】",
                "动作类型：RESULT_SHOW",
                "开始状态：商品已经佩戴完成",
                "核心动作：人物完成一次自然上半身角度变化",
                "",
                "【拍摄片段01｜0.0-15.0s｜HOOK】",
                "画面事件：商品安放在哑光灰台面上，主体清楚可见",
                "人物动作：保持已完成的佩戴，只做很小的自然角度变化",
                "商品必须可见：整体轮廓",
                "",
                "【统一执行】",
                "保持同一人物、商品、穿搭、场景和连续事件；商品一致性优先于场景美感和镜头效果。",
            ]
        )
        compacted = compact_video_prompt(prompt, limit=200)
        self.assertIn("核心动作：人物完成一次自然上半身角度变化", compacted)
        self.assertIn("动作类型：RESULT_SHOW", compacted)
        self.assertIn("开始状态：商品已经佩戴完成", compacted)

    def test_carrier_and_crop_lines_are_not_clause_stripped(self):
        prompt = "\n".join(
            [
                "【视频任务】",
                "竖屏手机短视频，时长15秒。",
                "",
                "【人物、穿搭与生活场景】",
                "出镜方式：静态商品与哑光灰台面的独立关系；商品始终位于画面中心",
                "",
                "【拍摄片段01｜0.0-15.0s｜HOOK】",
                "画面事件：静态商品与哑光灰台面的独立关系；商品始终位于画面中心",
                "人物动作：保持静置，只做很小的自然角度变化",
                "商品必须可见：整体轮廓",
                "",
                "【统一执行】",
                "保持同一人物、商品、穿搭、场景和连续事件；商品一致性优先于场景美感和镜头效果。",
            ]
        )
        compacted = compact_video_prompt(prompt, limit=200)
        self.assertIn(
            "出镜方式：静态商品与哑光灰台面的独立关系；商品始终位于画面中心",
            compacted,
        )


class OverLimitStatusTest(unittest.TestCase):
    """要求 4：超限要给明确状态，不截断、不静默丢镜头。"""

    def test_report_marks_an_unshrinkable_prompt_as_over_limit(self):
        prompt = "\n".join(
            [f"【片段{i:02d}】" + "甲乙丙丁戊己庚辛壬癸" * 8 for i in range(20)]
        )
        report = compact_video_prompt_report(prompt, limit=100)
        self.assertTrue(report.over_limit)
        self.assertGreater(len(report.text), 100)
        # 没有被截断：最后一个片段仍然完整存在
        self.assertTrue(report.text.rstrip().endswith("甲乙丙丁戊己庚辛壬癸"))
        self.assertTrue(report.ok)

    def test_report_stays_under_the_budget_for_the_long_fixture(self):
        report = compact_video_prompt_report(_long_prompt(), limit=1500)
        self.assertTrue(report.applied)
        self.assertFalse(report.over_limit)
        self.assertTrue(report.ok, (report.protected_dropped, report.shot_migrations))

    def test_report_admits_when_the_floor_is_above_the_budget(self):
        """去掉解释性长句后仍有 1452 字符的硬约束，就该如实报超限。"""
        report = compact_video_prompt_report(_long_prompt(), limit=600)
        self.assertTrue(report.applied)
        self.assertTrue(report.over_limit)
        # 超限不等于可以牺牲约束：审计仍然必须是干净的。
        self.assertTrue(report.ok, (report.protected_dropped, report.shot_migrations))
        self.assertIn("核心动作：人物完成一次自然上半身角度变化", report.text)

    def test_the_string_helper_matches_the_report_text(self):
        prompt = _long_prompt()
        self.assertEqual(
            compact_video_prompt(prompt, limit=600),
            compact_video_prompt_report(prompt, limit=600).text,
        )

    def test_a_still_over_budget_prompt_is_reported_not_truncated(self):
        """宁可超长也不截断合同：超限只反映在状态里。"""
        prompt = _two_shot_prompt(
            first="整体轮廓" + "甲" * 300, second="背部连接" + "乙" * 300
        )
        report = compact_video_prompt_report(prompt, limit=200)
        self.assertTrue(report.over_limit)
        self.assertIn("整体轮廓" + "甲" * 300, report.text)
        self.assertIn("背部连接" + "乙" * 300, report.text)


class CharLimitConfigTest(unittest.TestCase):
    def test_default_limit(self):
        with patch.dict(os.environ, {}, clear=False):
            os.environ.pop(VIDEO_PROMPT_CHAR_LIMIT_ENV, None)
            self.assertEqual(video_prompt_char_limit(), DEFAULT_VIDEO_PROMPT_CHAR_LIMIT)

    def test_explicit_limit(self):
        with patch.dict(os.environ, {VIDEO_PROMPT_CHAR_LIMIT_ENV: "3800"}):
            self.assertEqual(video_prompt_char_limit(), 3800)

    def test_disable_tokens(self):
        for token in ("0", "off", "false", "disable"):
            with patch.dict(os.environ, {VIDEO_PROMPT_CHAR_LIMIT_ENV: token}):
                self.assertEqual(video_prompt_char_limit(), 0, token)

    def test_unparsable_falls_back_to_default(self):
        with patch.dict(os.environ, {VIDEO_PROMPT_CHAR_LIMIT_ENV: "abc"}):
            self.assertEqual(video_prompt_char_limit(), DEFAULT_VIDEO_PROMPT_CHAR_LIMIT)


class RendererWiringTest(unittest.TestCase):
    def test_render_video_generation_prompt_applies_compaction(self):
        """渲染出口必须真的过预算，而不是只在模块里可用。"""
        from core import production_script_renderer as renderer

        long_prompt = _long_prompt()
        with patch.object(renderer, "load_item_result", return_value={"script": {}}), \
                patch.object(
                    renderer,
                    "_video_prompt_profile",
                    return_value=renderer.UGC_NATIVE_PROFILE,
                ), \
                patch.object(
                    renderer,
                    "_render_ugc_native_video_generation_prompt",
                    return_value=long_prompt,
                ), \
                patch.dict(os.environ, {VIDEO_PROMPT_CHAR_LIMIT_ENV: "600"}):
            rendered = renderer.render_video_generation_prompt(
                item=object(), duration_seconds=15.0
            )
        self.assertEqual(rendered, compact_video_prompt(long_prompt, limit=600))
        self.assertLess(len(rendered), len(long_prompt))

    def test_legacy_profile_is_not_compacted(self):
        """legacy 分支有自己的预算逻辑，不能被二次改写。"""
        from core import production_script_renderer as renderer

        legacy_prompt = _long_prompt()
        with patch.object(renderer, "load_item_result", return_value={"script": {}}), \
                patch.object(
                    renderer,
                    "_video_prompt_profile",
                    return_value=renderer.LEGACY_PROFILE,
                ), \
                patch.object(
                    renderer,
                    "_render_legacy_video_generation_prompt",
                    return_value=legacy_prompt,
                ), \
                patch.dict(os.environ, {VIDEO_PROMPT_CHAR_LIMIT_ENV: "600"}):
            rendered = renderer.render_video_generation_prompt(
                item=object(), duration_seconds=15.0
            )
        self.assertEqual(rendered, legacy_prompt)

    def test_the_report_variant_agrees_with_the_string_variant(self):
        """同一入口的两种返回形式必须描述同一次压缩。"""
        from core import production_script_renderer as renderer

        long_prompt = _long_prompt()
        with patch.object(renderer, "load_item_result", return_value={"script": {}}), \
                patch.object(
                    renderer,
                    "_video_prompt_profile",
                    return_value=renderer.UGC_NATIVE_PROFILE,
                ), \
                patch.object(
                    renderer,
                    "_render_ugc_native_video_generation_prompt",
                    return_value=long_prompt,
                ), \
                patch.dict(os.environ, {VIDEO_PROMPT_CHAR_LIMIT_ENV: "1500"}):
            report = renderer.render_video_generation_prompt_report(
                item=object(), duration_seconds=15.0
            )
            rendered = renderer.render_video_generation_prompt(
                item=object(), duration_seconds=15.0
            )
        self.assertEqual(report.text, rendered)
        self.assertTrue(report.applied)
        self.assertTrue(report.ok)

    def test_the_legacy_path_reports_itself_as_untouched(self):
        from core import production_script_renderer as renderer

        legacy_prompt = _long_prompt()
        with patch.object(renderer, "load_item_result", return_value={"script": {}}), \
                patch.object(
                    renderer,
                    "_video_prompt_profile",
                    return_value=renderer.LEGACY_PROFILE,
                ), \
                patch.object(
                    renderer,
                    "_render_legacy_video_generation_prompt",
                    return_value=legacy_prompt,
                ):
            report = renderer.render_video_generation_prompt_report(
                item=object(), duration_seconds=15.0
            )
        self.assertEqual(report.text, legacy_prompt)
        self.assertFalse(report.applied)
        self.assertFalse(report.over_limit)


if __name__ == "__main__":
    unittest.main()
