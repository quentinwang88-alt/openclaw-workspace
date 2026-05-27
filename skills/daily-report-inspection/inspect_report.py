#!/usr/bin/env python3
"""
每日日报巡检主脚本

流程：
1. 从飞书知识库自动定位当天的日报（配饰 + 女装）
2. 读取日报正文内容
3. 解析为结构化字段
4. 应用巡检规则，识别异常
5. 生成 Markdown 巡检摘要卡片
6. 通过老王助理飞书机器人发送给老板

使用方法：
    python3 inspect_report.py                       # 巡检当天日报
    python3 inspect_report.py --date 2026-05-20     # 指定日期
    python3 inspect_report.py --no-send             # 只生成不发送
    python3 inspect_report.py --dry-run             # 查看摘要不发送
"""

import argparse
import json
import sys
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Any, Optional

SKILL_DIR = Path(__file__).resolve().parent
sys.path.insert(0, str(SKILL_DIR))

from feishu_reader import FeishuWikiReader
from report_parser import ReportParser
from inspection_engine import InspectionEngine
from summary_builder import SummaryBuilder
from feishu_sender import FeishuSender
from llm_inspector import LLMInspector


def parse_date(date_str: str) -> datetime:
    """解析日期字符串"""
    formats = [
        "%Y-%m-%d",
        "%Y%m%d",
        "%m-%d",
        "%m%d",
    ]
    for fmt in formats:
        try:
            dt = datetime.strptime(date_str, fmt)
            if dt.year == 1900:
                dt = dt.replace(year=datetime.now().year)
            return dt
        except ValueError:
            continue
    raise ValueError(f"Cannot parse date: {date_str}")


def run_inspection(
    target_date: Optional[datetime] = None,
    send_message: bool = True,
    use_card: bool = False,
    use_llm: bool = True,
) -> Dict[str, Any]:
    """
    执行完整巡检流程

    Args:
        target_date: 巡检日期，默认今天
        send_message: 是否发送飞书消息
        use_card: 是否使用卡片消息（否则用文本消息）

    Returns:
        巡检结果汇总
    """
    if target_date is None:
        target_date = datetime.now()

    print(f"\n{'='*60}")
    print(f"🔍 日报巡检开始 - {target_date.strftime('%Y-%m-%d')}")
    print(f"{'='*60}")

    # ========== Step 1: 定位并读取日报 ==========
    print(f"\n📂 Step 1/5: 定位并读取日报...")
    reader = FeishuWikiReader()
    reports = reader.find_and_read_reports(target_date)

    found_count = sum(1 for r in reports if r["found"])
    print(f"  日报定位结果: {found_count}/{len(reports)} 找到")

    # ========== Step 2: 解析日报内容（正则预提取） ==========
    print(f"\n📝 Step 2/5: 解析日报内容（正则预提取）...")
    parser = ReportParser()
    for report in reports:
        if report.get("found") and report.get("content"):
            content = report["content"]
            parsed = parser.parse(content)
            report["parsed"] = parsed
            gen = parsed.get("short_video_generation", {})
            print(f"  {report['business_line']}: 解析完成 (生成{gen.get('generated_count')}, 可用{gen.get('directly_usable_count')}, 发布{parsed.get('video_publish',{}).get('total_publish_count')})")
        else:
            report["parsed"] = {}

    # ========== Step 3: LLM 巡检增强（校正字段 + 巡检 + 生成摘要） ==========
    print(f"\n🤖 Step 3/5: LLM 巡检增强...")
    llm_summary = None
    llm_result = None
    if not use_llm:
        print(f"  ⏭️ LLM 已禁用（--no-llm）")
    else:
        try:
            llm = LLMInspector()
            if llm.enabled:
                llm_result = llm.inspect(reports, target_date)
                if "error" not in llm_result:
                    llm_summary = llm.get_summary(llm_result)
                    if llm_summary:
                        print(f"  ✅ LLM 摘要生成成功 ({len(llm_summary)} 字)")
                        # 用 LLM 校正后的字段替换
                        llm_reports = llm_result.get("reports", [])
                        for lr in llm_reports:
                            biz = lr.get("business_line", "")
                            for report in reports:
                                if report.get("business_line") == biz:
                                    corrected = lr.get("corrected_fields", {})
                                    if corrected:
                                        report["parsed"] = {**report["parsed"], **corrected}
                else:
                    print(f"  ⚠️ LLM 返回解析失败: {llm_result.get('error')}")
            else:
                print(f"  ⏭️ LLM 已禁用（配置 llm.enabled=false）")
        except Exception as e:
            print(f"  ⚠️ LLM 调用失败，降级到规则引擎: {e}")

    # ========== Step 4: 规则引擎兜底巡检 + 生成摘要 ==========
    print(f"\n🔎 Step 4/5: 规则引擎巡检 + 生成摘要...")
    if llm_summary:
        # LLM 已生成摘要，规则引擎只做计数统计
        summary = llm_summary
        engine = InspectionEngine()
        result = engine.inspect(reports)
        total_issues = result.get("total_issues", 0)
        has_critical = result.get("has_critical", False)
        print(f"  使用 LLM 摘要（token: {llm_result.get('_usage', {}) if llm_result else 'N/A'}）")
        print(f"  规则引擎复核: {total_issues} 个需关注问题")
    else:
        # 降级：纯规则引擎
        engine = InspectionEngine()
        result = engine.inspect(reports)
        total_issues = result.get("total_issues", 0)
        has_critical = result.get("has_critical", False)
        builder = SummaryBuilder()
        summary = builder.build(result, reports, target_date)
        print(f"  规则引擎: {total_issues} 个需关注问题")

    print(f"\n{'─'*60}")
    print(summary)
    print(f"{'─'*60}")

    # ========== Step 5: 发送飞书消息 ==========
    send_result = None
    if send_message:
        print(f"\n📤 Step 5/5: 发送飞书消息...")
        try:
            sender = FeishuSender()

            if use_card:
                title = f"{target_date.strftime('%m%d')} 日报巡检摘要"
                send_result = sender.send_interactive_card(title, summary)
            else:
                send_result = sender.send_text_message(summary)

            if send_result and send_result.get("code") == 0:
                print(f"  ✅ 消息发送成功 (messageId: {send_result.get('data', {}).get('message_id', 'N/A')})")
            else:
                error_msg = send_result.get("msg", "Unknown error") if send_result else "No response"
                print(f"  ❌ 消息发送失败: {error_msg}")
                print(f"  💡 提示: 请确认在 config.json 中配置了 feishu.receive_id")
        except Exception as e:
            print(f"  ❌ 发送异常: {e}")
            print(f"  💡 提示: 检查 bot 应用凭证和 API 权限")
    else:
        print(f"\n📤 Step 5/5: 跳过发送（--no-send）")

    # ========== 汇总 ==========
    print(f"\n{'='*60}")
    print(f"✅ 巡检完成")
    print(f"  日报: {found_count}/{len(reports)} 找到")
    print(f"  问题: {total_issues} 个")
    print(f"  发送: {'是' if (send_result and send_result.get('code') == 0) else '否/失败'}")
    print(f"{'='*60}\n")

    return {
        "date": target_date.strftime("%Y-%m-%d"),
        "reports_found": found_count,
        "reports_total": len(reports),
        "total_issues": total_issues,
        "has_critical": has_critical,
        "summary": summary,
        "send_result": send_result,
        "reports": reports,
        "findings": result.get("findings", []),
    }


SAMPLE_REPORT_DATA = [
    {
        "business_line": "配饰",
        "found": True,
        "match_level": 1,
        "match_desc": "精准匹配: '0520 配饰'",
        "title": "0520 配饰",
        "doc_token": "",
        "content": """短视频生成：今日暂无。明日24个待生成。
视频发布：马来配饰成功发布1个，越南配饰成功发布0个，越南发夹成功发布6个，失败0个。
选品及上品：泰国本土店商品31。
达人建联：越南配饰建联3389人，越南发饰建联3615。
样品审批：越南配饰新增样品申请19人，批出样品1人。
其它事项补充：已完成洞察报告越南首饰必备动作。
明日计划：优先完成24个视频生成。""",
    },
    {
        "business_line": "女装",
        "found": True,
        "match_level": 1,
        "match_desc": "精准匹配: '0520 女装'",
        "title": "0520 女装",
        "doc_token": "",
        "content": """短视频生成：今日生成9个，直接可用3个，修改后可用2个，不能使用4个。
视频发布：泰国女装发布4个，女装店发布3个。
选品及上品：今日暂无。
达人建联：今日未进行达人建联。
样品审批：新增样品申请12人，批出0人。
其它事项补充：无。
明日计划：继续推进。""",
    },
]


def run_local_test(target_date=None, send_message=False):
    """使用本地示例数据运行完整检流程"""
    if target_date is None:
        target_date = datetime.now()

    print(f"\n{'='*60}")
    print(f"🔍 日报巡检本地测试 - {target_date.strftime('%Y-%m-%d')}")
    print(f"{'='*60}")

    print(f"\n📂 Step 1/2: 使用示例数据...")
    reports = SAMPLE_REPORT_DATA
    found_count = sum(1 for r in reports if r["found"])
    print(f"  日报数据: {found_count}/{len(reports)} 可用")

    print(f"\n📝 Step 2/2: 解析 + 巡检 + 生成摘要...")
    parser = ReportParser()
    for report in reports:
        if report.get("found") and report.get("content"):
            report["parsed"] = parser.parse(report["content"])

    engine = InspectionEngine()
    result = engine.inspect(reports)

    builder = SummaryBuilder()
    summary = builder.build(result, reports, target_date)

    print(f"\n{'─'*60}")
    print(summary)
    print(f"{'─'*60}")

    print(f"\n{'='*60}")
    print(f"✅ 本地测试完成")
    print(f"  日报: {found_count}/{len(reports)}")
    print(f"  问题: {result.get('total_issues', 0)} 个")
    print(f"{'='*60}\n")

    return {
        "date": target_date.strftime("%Y-%m-%d"),
        "reports_found": found_count,
        "reports_total": len(reports),
        "total_issues": result.get("total_issues", 0),
        "has_critical": result.get("has_critical", False),
        "summary": summary,
        "reports": reports,
        "findings": result.get("findings", []),
    }


def main():
    parser_args = argparse.ArgumentParser(
        description="每日日报巡检 - 自动定位日报、解析内容、检测异常、生成摘要",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
示例:
  python3 inspect_report.py                        # 巡检今天日报
  python3 inspect_report.py --date 2026-05-20      # 巡检指定日期
  python3 inspect_report.py --no-send              # 只生成摘要不发送
  python3 inspect_report.py --dry-run              # 查看摘要不发送（同 --no-send）
  python3 inspect_report.py --card                 # 用卡片消息发送
        """,
    )
    parser_args.add_argument(
        "--date", "-d",
        help="巡检日期（格式: YYYY-MM-DD 或 MM-DD 或 MMDD）",
    )
    parser_args.add_argument(
        "--no-send", action="store_true",
        help="不发送飞书消息，只生成并打印摘要",
    )
    parser_args.add_argument(
        "--dry-run", action="store_true",
        help="同 --no-send",
    )
    parser_args.add_argument(
        "--card", action="store_true",
        help="使用飞书卡片消息发送（默认文本消息）",
    )
    parser_args.add_argument(
        "--json", action="store_true",
        help="输出 JSON 格式结果",
    )
    parser_args.add_argument(
        "--local-test", action="store_true",
        help="使用本地示例数据测试（不调用飞书 API）",
    )
    parser_args.add_argument(
        "--no-llm", action="store_true",
        help="禁用 LLM 增强，仅使用规则引擎",
    )

    args = parser_args.parse_args()

    target_date = None
    if args.date:
        target_date = parse_date(args.date)

    send = not (args.no_send or args.dry_run)
    use_llm = not args.no_llm

    if args.local_test:
        result = run_local_test(target_date=target_date, send_message=False)
    elif args.json:
        result = run_inspection(target_date=target_date, send_message=False, use_llm=use_llm)
    else:
        run_inspection(
            target_date=target_date,
            send_message=send,
            use_card=args.card,
            use_llm=use_llm,
        )


if __name__ == "__main__":
    main()
