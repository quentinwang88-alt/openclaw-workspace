#!/usr/bin/env python3
"""修复版视频复刻流水线 - 直接处理指定记录。"""

import sys
from pathlib import Path

SKILL_DIR = Path(__file__).parent.absolute()
REPO_ROOT = SKILL_DIR.parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))
sys.path.insert(0, str(SKILL_DIR))

from core.bitable import (
    FeishuBitableClient,
    resolve_wiki_bitable_app_token,
    normalize_cell_value,
)
from core.feishu_url_parser import parse_feishu_bitable_url
from core.llm_client import VideoRemakeLLMClient
from core.prompts import (
    CONTENT_BRANCH_NON_PRODUCT,
    CONTENT_BRANCH_PRODUCT,
    build_final_video_prompt,
    build_remade_script_prompt,
    build_remake_card_prompt,
    build_script_breakdown_prompt,
)

FEISHU_URL = "https://gcngopvfvo0q.feishu.cn/wiki/OzeJwBCzXit0mfkyylOcYioVnSd?table=tbliIkHtmm82qa5A&view=vewHPPmtMB"

STATUS_PENDING = "待开始"
STATUS_PROCESSING = "处理中"
STATUS_DONE = "已完成"
STATUS_FAILED = "失败"


def has_text_value(value):
    return isinstance(value, str) and bool(value.strip())


def main():
    print("🚀 启动修复版视频复刻流水线")
    print("=" * 70)

    # 初始化客户端
    info = parse_feishu_bitable_url(FEISHU_URL)
    app_token = resolve_wiki_bitable_app_token(info.app_token)
    client = FeishuBitableClient(app_token=app_token, table_id=info.table_id)
    llm_client = VideoRemakeLLMClient()

    # 获取所有记录
    records = client.list_records(page_size=500)

    # 找到待处理任务
    pending = []
    for r in records:
        status = r.fields.get("任务状态", "")
        task_no = r.fields.get("任务编号", "")
        if status == STATUS_PENDING and task_no in ["095", "098", "101"]:
            pending.append(r)

    print(f"📋 找到 {len(pending)} 条待处理任务")
    if not pending:
        print("✅ 没有待处理任务")
        return

    success_count = 0
    failed_count = 0

    for idx, record in enumerate(pending, 1):
        fields = record.fields
        record_id = record.record_id
        task_no = fields.get("任务编号", "N/A")

        print(f"\n{'=' * 70}")
        print(f"🎬 处理任务 {idx}/{len(pending)}: 任务编号 {task_no}")
        print(f"{'=' * 70}")

        try:
            # 获取上下文信息
            raw_branch = normalize_cell_value(fields.get("内容分支"))
            if raw_branch == "商品展示型":
                content_branch = CONTENT_BRANCH_PRODUCT
            else:
                content_branch = CONTENT_BRANCH_NON_PRODUCT

            context = {
                "content_branch": content_branch,
                "content_branch_label": raw_branch,
                "target_country": normalize_cell_value(fields.get("目标国家")),
                "target_language": normalize_cell_value(fields.get("目标语言")),
                "product_type": normalize_cell_value(fields.get("商品类型")),
                "remake_mode": "轻本地化复刻",
                "replicate_mode": "轻本地化复刻",
            }

            print(f"  🧭 分支={context['content_branch_label']} | "
                  f"国家={context['target_country']} | "
                  f"语言={context['target_language']} | "
                  f"商品={context['product_type']}")

            # 标记处理中
            client.update_record_fields(record_id, {"任务状态": STATUS_PROCESSING})

            # 获取视频URL
            video_field = fields.get("视频", [])
            if not video_field:
                raise Exception("没有视频字段")
            file_token = video_field[0].get("file_token")
            if not file_token:
                raise Exception("没有文件token")
            video_url = client.get_tmp_download_url(file_token)
            print(f"  🎞️ 视频地址已解析")

            # 步骤1: 脚本拆解
            script_breakdown = fields.get("脚本拆解", "")
            if has_text_value(script_breakdown):
                print("  1/4 跳过脚本拆解，复用已写入结果...")
            else:
                print("  1/4 生成脚本拆解...")
                script_breakdown = llm_client.chat_with_video(
                    video_url=video_url,
                    prompt=build_script_breakdown_prompt(context),
                    max_tokens=2500,
                )
                client.update_record_fields(
                    record_id, {"脚本拆解": script_breakdown}
                )
                print(f"     ✓ 已生成 ({len(script_breakdown)} 字符)")

            # 步骤2: 复刻卡
            remake_card = fields.get("复刻卡", "")
            if has_text_value(remake_card):
                print("  2/4 跳过复刻卡，复用已写入结果...")
            else:
                print("  2/4 生成复刻卡...")
                remake_card = llm_client.chat_text(
                    prompt=build_remake_card_prompt(context, script_breakdown),
                    max_tokens=2500,
                )
                client.update_record_fields(
                    record_id, {"复刻卡": remake_card}
                )
                print(f"     ✓ 已生成 ({len(remake_card)} 字符)")

            # 步骤3: 复刻后的脚本
            remade_script = fields.get("复刻后的脚本", "")
            if has_text_value(remade_script):
                print("  3/4 跳过复刻后的脚本，复用已写入结果...")
            else:
                print("  3/4 生成复刻后的脚本...")
                remade_script = llm_client.chat_text(
                    prompt=build_remade_script_prompt(context, remake_card),
                    max_tokens=2500,
                )
                client.update_record_fields(
                    record_id, {"复刻后的脚本": remade_script}
                )
                print(f"     ✓ 已生成 ({len(remade_script)} 字符)")

            # 步骤4: 最终复刻视频提示词
            final_prompt = fields.get("最终复刻视频提示词", "")
            if has_text_value(final_prompt):
                print("  4/4 跳过最终复刻视频提示词，复用已写入结果...")
            else:
                print("  4/4 生成最终复刻视频提示词...")
                final_prompt = llm_client.chat_text(
                    prompt=build_final_video_prompt(context, remade_script),
                    max_tokens=2500,
                )
                print(f"     ✓ 已生成 ({len(final_prompt)} 字符)")

            # 标记完成
            client.update_record_fields(
                record_id,
                {
                    "任务状态": STATUS_DONE,
                    "最终复刻视频提示词": final_prompt,
                }
            )
            print("  ✅ 当前记录完成")
            success_count += 1

        except Exception as exc:
            failed_count += 1
            print(f"  ❌ 当前记录失败: {exc}")
            try:
                client.update_record_fields(
                    record_id,
                    {
                        "任务状态": STATUS_FAILED,
                    }
                )
            except Exception:
                pass

    print(f"\n{'=' * 70}")
    print("📊 执行完成")
    print(f"{'=' * 70}")
    print(f"总任务数: {len(pending)}")
    print(f"成功: {success_count}")
    print(f"失败: {failed_count}")


if __name__ == "__main__":
    main()
