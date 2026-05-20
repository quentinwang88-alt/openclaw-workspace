#!/usr/bin/env python3
"""
轻量级养号视频复刻流水线。

读取飞书多维表格中状态为“待开始”的记录，直接把视频传给模型分析，
按顺序生成：
1. 素材筛选与高光DNA提取
2. 轻微改写复刻方案
3. 最终执行分镜
"""

import argparse
import os
import re
import sys
from pathlib import Path
from typing import Dict, List, Optional


SKILL_DIR = Path(__file__).parent.absolute()
REPO_ROOT = SKILL_DIR.parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))
sys.path.insert(0, str(SKILL_DIR))

from workspace_support import load_repo_env

load_repo_env()

from core.bitable import (  # noqa: E402
    FeishuBitableClient,
    RemakeRecord,
    normalize_cell_value,
    resolve_field_mapping,
    resolve_video_url,
    resolve_wiki_bitable_app_token,
)
from core.feishu_url_parser import parse_feishu_bitable_url  # noqa: E402
from core.llm_client import VideoRemakeLLMClient  # noqa: E402
from core.prompts import (  # noqa: E402
    CONTENT_BRANCH_NON_PRODUCT,
    CONTENT_BRANCH_PRODUCT,
    build_final_storyboard_prompt,
    build_highlight_dna_prompt,
    build_light_rewrite_prompt,
)


FEISHU_APP_TOKEN = os.environ.get("FEISHU_APP_TOKEN", "")
FEISHU_TABLE_ID = os.environ.get("FEISHU_TABLE_ID", "")

STATUS_PENDING = "待开始"
STATUS_PROCESSING = "处理中"
STATUS_DONE = "已完成"
STATUS_FAILED = "失败"
STATUS_NOT_SUITABLE = "不适合复刻"


def resolve_feishu_config(feishu_url: Optional[str]) -> (str, str):
    """支持通过飞书链接动态切表。"""
    if feishu_url:
        info = parse_feishu_bitable_url(feishu_url)
        if info:
            app_token = info.app_token
            if "/wiki/" in info.original_url:
                resolved_token = resolve_wiki_bitable_app_token(info.app_token)
                print(f"🔄 检测到 wiki 链接，已解析底层 bitable app_token: {resolved_token}")
                app_token = resolved_token
            print("🔗 从 URL 解析飞书配置:")
            print(f"   app_token: {app_token}")
            print(f"   table_id: {info.table_id}")
            return app_token, info.table_id
        print("⚠️ 无法解析飞书 URL，回退到默认配置")

    if not FEISHU_APP_TOKEN or not FEISHU_TABLE_ID:
        raise RuntimeError(
            "缺少飞书配置。请先填写仓库根目录 .env 中的 FEISHU_APP_TOKEN / FEISHU_TABLE_ID，"
            "或运行时使用 --feishu-url。"
        )
    return FEISHU_APP_TOKEN, FEISHU_TABLE_ID


def validate_required_fields(mapping: Dict[str, Optional[str]]) -> None:
    """校验运行所需字段。"""
    required = {
        "status": "状态字段",
        "video": "视频字段",
        "highlight_dna": "高光DNA提取结果字段",
        "light_rewrite_plan": "轻微改写复刻方案字段",
        "final_storyboard": "最终固定分镜字段",
    }
    missing = [label for key, label in required.items() if not mapping.get(key)]
    if missing:
        raise Exception(f"表格缺少必需字段: {', '.join(missing)}")


def build_context(record: RemakeRecord, mapping: Dict[str, Optional[str]]) -> Dict[str, str]:
    """抽取单条任务上下文。"""
    fields = record.fields
    replicate_mode = (
        normalize_cell_value(fields.get(mapping.get("remake_mode")))
        if mapping.get("remake_mode") else ""
    ) or "轻本地化复刻"
    raw_branch = normalize_cell_value(fields.get(mapping.get("content_branch"))) if mapping.get("content_branch") else ""
    if raw_branch == "商品展示型":
        content_branch = CONTENT_BRANCH_PRODUCT
    else:
        raw_branch = "非商品展示型"
        content_branch = CONTENT_BRANCH_NON_PRODUCT
    return {
        "content_branch": content_branch,
        "content_branch_label": raw_branch,
        "store_id": normalize_cell_value(fields.get(mapping.get("store_id"))) if mapping.get("store_id") else "",
        "target_country": normalize_cell_value(fields.get(mapping.get("target_country"))) if mapping.get("target_country") else "",
        "target_language": normalize_cell_value(fields.get(mapping.get("target_language"))) if mapping.get("target_language") else "",
        "product_type": normalize_cell_value(fields.get(mapping.get("product_type"))) if mapping.get("product_type") else "",
        "remake_mode": replicate_mode,
        "replicate_mode": replicate_mode,
    }


def load_pending_records(
    client: FeishuBitableClient,
    mapping: Dict[str, Optional[str]],
    limit: Optional[int] = None,
) -> List[RemakeRecord]:
    """读取待开始任务。"""
    records = client.list_records(page_size=100)
    status_field = mapping["status"]
    pending = [
        record for record in records
        if normalize_cell_value(record.fields.get(status_field)) == STATUS_PENDING
    ]
    if limit:
        pending = pending[:limit]
    return pending


def has_text_value(value: object) -> bool:
    """判断字段是否已有可用文本。"""
    return isinstance(value, str) and bool(value.strip())


def extract_negative_words(final_storyboard: str) -> str:
    """从 Step3 输出中拆出“负面限制词”小节，供单独字段写回。

    Step3 prompt 升级后负面限制词章节序号从「三、」改为「四、」，
    保留旧序号匹配以便兼容历史已写入的内容。
    """
    text = str(final_storyboard or "").strip()
    patterns = [
        r"(?:^|\n)#+\s*[三四]、负面限制词\s*\n(?P<body>[\s\S]+?)$",
        r"(?:^|\n)[三四]、负面限制词\s*\n(?P<body>[\s\S]+?)$",
    ]
    for pattern in patterns:
        match = re.search(pattern, text)
        if match:
            return match.group("body").strip()
    return ""


# 复刻策略关键词 → 是否适合后续生成
_STRATEGY_NOT_SUITABLE = "不建议复刻"
_STRATEGY_KEEP_LENGTH = "原长复刻"
_STRATEGY_COMPRESS = "选段压缩复刻"


def extract_duration_decision(highlight_dna: str) -> Dict[str, str]:
    """从 Step1 输出的「零、时长决策」段中抽取关键决策字段。

    返回字典：
      - original_duration: 原视频实际秒数（字符串，可能为空）
      - strategy: 原长复刻 / 选段压缩复刻 / 不建议复刻 / 空字符串
      - target_duration: 复刻目标时长秒数（字符串，可能为空；不建议复刻时为 0）
      - not_suitable_reason: 仅在不建议复刻时给出原因摘要（截断 500 字符）
    """
    text = str(highlight_dna or "")
    # 去掉 Markdown 加粗/斜体标记，避免 `**原视频实际时长**：12` 抓不到
    flat = re.sub(r"\*+", "", text)
    result = {
        "original_duration": "",
        "strategy": "",
        "target_duration": "",
        "not_suitable_reason": "",
    }

    # 原视频实际时长（秒）
    m = re.search(r"原视频实际时长\s*[（(]?\s*秒\s*[)）]?\s*[:：]\s*([0-9]+(?:\.[0-9]+)?)", flat)
    if m:
        result["original_duration"] = m.group(1).strip()

    # 复刻策略
    m = re.search(r"复刻策略\s*[:：]\s*([^\n]+)", flat)
    if m:
        raw = m.group(1).strip()
        for keyword in (_STRATEGY_NOT_SUITABLE, _STRATEGY_COMPRESS, _STRATEGY_KEEP_LENGTH):
            if keyword in raw:
                result["strategy"] = keyword
                break

    # 复刻目标时长（秒）
    m = re.search(r"复刻目标时长\s*[（(]?\s*秒\s*[)）]?\s*[:：]\s*([0-9]+(?:\.[0-9]+)?)", flat)
    if m:
        result["target_duration"] = m.group(1).strip()

    # 不建议复刻原因（取「不建议复刻」之后的第一段简短描述）
    if result["strategy"] == _STRATEGY_NOT_SUITABLE:
        # 优先匹配「原因：xxx」或「不适合的原因：xxx」
        reason_match = re.search(
            r"(?:不适合的?原因|原因|理由)\s*[:：]\s*([^\n]+(?:\n(?!\s*[一二三四五六七八九零])[^\n]+)*)",
            flat,
        )
        if reason_match:
            result["not_suitable_reason"] = reason_match.group(1).strip()[:500]

    return result


class NotSuitableForRemake(Exception):
    """Step1 判定原视频不适合复刻；用于跳过 Step2/3 并打上专用状态。"""

    def __init__(self, reason: str):
        super().__init__(reason or "原视频不适合复刻")
        self.reason = reason


class VideoRemakePipeline:
    """三阶段高保真轻量复刻流水线。"""

    def __init__(self, client: FeishuBitableClient, mapping: Dict[str, Optional[str]]):
        self.client = client
        self.mapping = mapping
        self.llm_client = VideoRemakeLLMClient()
        self.stats = {
            "total": 0,
            "success": 0,
            "failed": 0,
            "not_suitable": 0,
        }

    def process_records(self, records: List[RemakeRecord], dry_run: bool = False) -> Dict[str, int]:
        self.stats["total"] = len(records)

        if dry_run:
            print("🔍 Dry run 仅展示待处理记录：")
            for index, record in enumerate(records, 1):
                context = build_context(record, self.mapping)
                print(
                    f"  {index}. record_id={record.record_id} | "
                    f"分支={context['content_branch_label'] or '未提供'} | "
                    f"店铺={context['store_id'] or '未提供'} | "
                    f"国家={context['target_country'] or '未提供'} | "
                    f"语言={context['target_language'] or '未提供'} | "
                    f"商品={context['product_type'] or '未提供'}"
                )
            return self.stats

        for index, record in enumerate(records, 1):
            print(f"\n{'=' * 70}")
            print(f"🎬 处理任务 {index}/{len(records)}: {record.record_id}")
            print(f"{'=' * 70}")
            try:
                self._process_single_record(record)
                self.stats["success"] += 1
            except NotSuitableForRemake as exc:
                self.stats["not_suitable"] += 1
                print(f"⏭️  当前记录被判定为不适合复刻: {exc.reason or '未提供原因'}")
                self._mark_not_suitable(record.record_id, exc.reason)
            except Exception as exc:
                self.stats["failed"] += 1
                print(f"❌ 当前记录失败: {exc}")
                self._mark_failed(record.record_id, str(exc))

        return self.stats

    def _process_single_record(self, record: RemakeRecord) -> None:
        fields = record.fields
        context = build_context(record, self.mapping)
        video_field = self.mapping["video"]
        status_field = self.mapping["status"]
        error_field = self.mapping.get("error_message")

        video_url = resolve_video_url(self.client, fields.get(video_field))
        print(f"  🎞️ 视频地址已解析: {video_url[:120]}{'...' if len(video_url) > 120 else ''}")
        print(
            f"  🧭 分支={context['content_branch_label'] or '未提供'} | "
            f"店铺={context['store_id'] or '未提供'} | "
            f"国家={context['target_country'] or '未提供'} | "
            f"语言={context['target_language'] or '未提供'} | "
            f"商品={context['product_type'] or '未提供'} | "
            f"模式={context['remake_mode'] or '轻本地化复刻'}"
        )

        processing_fields = {status_field: STATUS_PROCESSING}
        if error_field:
            processing_fields[error_field] = ""
        self.client.update_record_fields(record.record_id, processing_fields)

        highlight_dna = fields.get(self.mapping["highlight_dna"])
        if has_text_value(highlight_dna):
            print("  1/3 跳过高光DNA提取，复用已写入结果...")
        else:
            print("  1/3 生成高光DNA提取结果...")
            highlight_dna = self.llm_client.chat_with_video(
                video_url=video_url,
                prompt=build_highlight_dna_prompt(context),
                max_tokens=2500,
            )
            self.client.update_record_fields(
                record.record_id,
                {self.mapping["highlight_dna"]: highlight_dna},
            )

        # 抽取 Step1 输出的时长决策；不适合复刻则直接分流，跳过 Step2/3
        decision = extract_duration_decision(str(highlight_dna or ""))
        if decision["original_duration"] or decision["target_duration"] or decision["strategy"]:
            print(
                f"  📏 时长决策：原视频={decision['original_duration'] or '?'}s"
                f" | 策略={decision['strategy'] or '?'}"
                f" | 目标={decision['target_duration'] or '?'}s"
            )

        if decision["strategy"] == _STRATEGY_NOT_SUITABLE:
            # 把 0 秒目标时长也回写一下，便于下游识别
            duration_field = self.mapping.get("video_duration")
            if duration_field:
                try:
                    self.client.update_record_fields(
                        record.record_id,
                        {duration_field: 0},
                    )
                except Exception as exc:
                    print(f"    ⚠️ 回写视频时长(0)失败: {exc}")
            raise NotSuitableForRemake(decision["not_suitable_reason"])

        # 回写复刻目标时长，供下游脚本管理表/视频生成模型消费
        if decision["target_duration"]:
            duration_field = self.mapping.get("video_duration")
            if duration_field:
                try:
                    self.client.update_record_fields(
                        record.record_id,
                        {duration_field: int(float(decision["target_duration"]))},
                    )
                except Exception as exc:
                    print(f"    ⚠️ 回写复刻目标时长失败: {exc}")

        light_rewrite_plan = fields.get(self.mapping["light_rewrite_plan"])
        if has_text_value(light_rewrite_plan):
            print("  2/3 跳过轻微改写复刻方案，复用已写入结果...")
        else:
            print("  2/3 生成轻微改写复刻方案...")
            light_rewrite_plan = self.llm_client.chat_text(
                prompt=build_light_rewrite_prompt(context, str(highlight_dna or "")),
                max_tokens=2500,
            )
            self.client.update_record_fields(
                record.record_id,
                {self.mapping["light_rewrite_plan"]: light_rewrite_plan},
            )

        final_storyboard = fields.get(self.mapping["final_storyboard"])
        if has_text_value(final_storyboard):
            print("  3/3 跳过最终固定分镜，复用已写入结果...")
            if self.mapping.get("negative_words") and not has_text_value(fields.get(self.mapping["negative_words"])):
                negative_words = extract_negative_words(str(final_storyboard or ""))
                if negative_words:
                    self.client.update_record_fields(
                        record.record_id,
                        {self.mapping["negative_words"]: negative_words},
                    )
        else:
            print("  3/3 生成最终固定分镜...")
            final_storyboard = self.llm_client.chat_text(
                prompt=build_final_storyboard_prompt(context, str(light_rewrite_plan or "")),
                max_tokens=2500,
            )
            final_update = {self.mapping["final_storyboard"]: final_storyboard}
            negative_words = extract_negative_words(str(final_storyboard or ""))
            if self.mapping.get("negative_words") and negative_words:
                final_update[self.mapping["negative_words"]] = negative_words
            self.client.update_record_fields(record.record_id, final_update)

        done_fields = {
            status_field: STATUS_DONE,
        }
        already_synced = (
            self.mapping.get("synced_script_id")
            and has_text_value(fields.get(self.mapping["synced_script_id"]))
        )
        if self.mapping.get("sync_status") and not already_synced:
            done_fields[self.mapping["sync_status"]] = "待同步"
        if error_field:
            done_fields[error_field] = ""
        self.client.update_record_fields(record.record_id, done_fields)
        print("  ✅ 当前记录完成")

    def _mark_failed(self, record_id: str, error_message: str) -> None:
        status_field = self.mapping["status"]
        error_field = self.mapping.get("error_message")
        fields = {status_field: STATUS_FAILED}
        if error_field:
            fields[error_field] = error_message[:1000]
        try:
            self.client.update_record_fields(record_id, fields)
        except Exception as exc:
            print(f"⚠️ 回写失败状态也失败了: {exc}")

    def _mark_not_suitable(self, record_id: str, reason: str) -> None:
        """标记为不适合复刻：状态=不适合复刻，原因写入错误信息字段，跳过同步。

        前置：飞书状态字段的选项里必须已经添加「不适合复刻」选项；
        若未添加，飞书 API 会返回 InvalidRequest，此时降级为「失败」并附说明。
        """
        status_field = self.mapping["status"]
        error_field = self.mapping.get("error_message")
        fields = {status_field: STATUS_NOT_SUITABLE}
        if error_field:
            fields[error_field] = (reason or "原视频不适合复刻")[:1000]
        try:
            self.client.update_record_fields(record_id, fields)
        except Exception as exc:
            print(
                f"⚠️ 回写「不适合复刻」状态失败（请确认飞书状态字段已添加该选项）: {exc}\n"
                f"   降级为「失败」状态。"
            )
            fallback = {status_field: STATUS_FAILED}
            if error_field:
                fallback[error_field] = (
                    f"[不适合复刻] {reason or '原视频不适合复刻'}\n"
                    f"⚠️ 飞书状态选项缺少「{STATUS_NOT_SUITABLE}」，已降级为失败。"
                )[:1000]
            try:
                self.client.update_record_fields(record_id, fallback)
            except Exception as exc2:
                print(f"⚠️ 降级回写失败状态也失败了: {exc2}")


def main() -> None:
    parser = argparse.ArgumentParser(
        description="轻量级养号视频复刻流水线",
    )
    parser.add_argument("--feishu-url", "-u", help="飞书多维表格链接")
    parser.add_argument("--limit", "-n", type=int, help="限制处理数量")
    parser.add_argument("--dry-run", action="store_true", help="只查看待处理任务")
    args = parser.parse_args()

    app_token, table_id = resolve_feishu_config(args.feishu_url)
    client = FeishuBitableClient(app_token=app_token, table_id=table_id)

    field_names = client.list_field_names()
    mapping = resolve_field_mapping(field_names)
    validate_required_fields(mapping)

    print("📋 字段映射:")
    for key, value in mapping.items():
        if value:
            print(f"   {key}: {value}")

    records = load_pending_records(client, mapping, limit=args.limit)
    print(f"\n📌 找到待处理任务 {len(records)} 条")
    if not records:
        return

    pipeline = VideoRemakePipeline(client, mapping)
    stats = pipeline.process_records(records, dry_run=args.dry_run)

    print(f"\n{'=' * 70}")
    print("📊 执行完成")
    print(f"{'=' * 70}")
    print(f"总任务数: {stats['total']}")
    print(f"成功: {stats['success']}")
    print(f"不适合复刻: {stats.get('not_suitable', 0)}")
    print(f"失败: {stats['failed']}")


if __name__ == "__main__":
    main()
