#!/usr/bin/env python3
"""生成达人画像卡 CLI 入口。"""
import argparse
import json
import logging
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]

sys.path.insert(0, str(REPO_ROOT))
try:
    from workspace_support import load_repo_env
    load_repo_env()
except ImportError:
    pass

from app.services.profile_generator import generate_profile_card

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)


def main():
    parser = argparse.ArgumentParser(description="生成达人画像卡 V1")
    parser.add_argument("--creator-url", required=True, help="达人 TikTok 主页链接")
    parser.add_argument("--market", required=True, help="市场: VN/TH/MY/PH")
    parser.add_argument("--history-relation", required=True,
                        help="历史关系: 出过单/发过视频/申请过样品/聊过未合作/陌生")
    parser.add_argument("--cover-collage", action="append", default=[],
                        help="封面拼图路径（可多次指定）")
    parser.add_argument("--profile-header", default=None,
                        help="达人主页头部截图路径（可选）")
    parser.add_argument("--recent-video-meta", default="",
                        help="近期视频文字信息（可选）")
    parser.add_argument("--product-candidates", default=None,
                        help="商品候选池 JSON 文件路径（可选）")
    parser.add_argument("--feishu-table-url", default=None,
                        help="飞书表格 URL，如 https://.../base/{app_token}?table={table_id}")
    parser.add_argument("--no-write", action="store_true",
                        help="不写入飞书，仅输出结果")
    parser.add_argument("--json", action="store_true",
                        help="以 JSON 格式输出")
    parser.add_argument("--verbose", action="store_true",
                        help="详细输出")

    args = parser.parse_args()

    if args.verbose:
        logging.getLogger().setLevel(logging.DEBUG)

    if not args.cover_collage:
        print("❌ 至少需要一张封面拼图 (--cover-collage)", file=sys.stderr)
        sys.exit(1)

    # 加载商品候选池
    product_candidates = None
    if args.product_candidates:
        try:
            product_candidates = json.loads(Path(args.product_candidates).read_text())
        except Exception as e:
            print(f"⚠️  无法加载商品候选池: {e}", file=sys.stderr)

    # 解析飞书 URL
    app_token = None
    table_id = None
    if args.feishu_table_url:
        import re
        m = re.search(r'/base/([^/?]+)', args.feishu_table_url)
        if m:
            app_token = m.group(1)
        m = re.search(r'table=([^&]+)', args.feishu_table_url)
        if m:
            table_id = m.group(1)

    # 执行
    if not args.json:
        print(f"🔍 正在分析达人: {args.creator_url}")
        print(f"   市场: {args.market}")
        print(f"   历史关系: {args.history_relation}")
        print(f"   封面拼图: {len(args.cover_collage)} 张")
        print()

    result = generate_profile_card(
        creator_url=args.creator_url,
        market=args.market,
        history_relation=args.history_relation,
        cover_collage_images=args.cover_collage,
        profile_header_image=args.profile_header,
        recent_video_meta_text=args.recent_video_meta,
        product_candidates=product_candidates,
        app_token=app_token,
        table_id=table_id,
        write_to_feishu=not args.no_write,
    )

    if result.get("error"):
        print(f"❌ 错误: {result['error']}", file=sys.stderr)
        sys.exit(1)

    if args.json:
        print(json.dumps(result, ensure_ascii=False, indent=2))
    else:
        print("📊 分析结果:")
        print(f"   Creator: {result['creator_url']}")
        if result.get("writable_fields"):
            print(f"   可写入字段:")
            for k, v in result["writable_fields"].items():
                display_v = str(v)
                if len(display_v) > 80:
                    display_v = display_v[:80] + "..."
                print(f"     {k}: {display_v}")
        print(f"   需人工复核: {'是' if result['manual_review_required'] else '否'}")
        if result.get("feishu_result"):
            fr = result["feishu_result"]
            print(f"   飞书: {fr.get('action', '?')} → record_id={fr.get('record_id', '?')}")
        print(f"   日志 ID: {result.get('log_id', 'N/A')}")

        # 校验详情
        if result.get("validation_results"):
            issues = [r for r in result["validation_results"] if r["action"] in ("reject", "write_with_review")]
            if issues:
                print(f"\n  ⚠️  校验问题:")
                for r in issues:
                    for e in r.get("errors", []):
                        print(f"     ❌ [{r['field']}] {e}")
                    for w in r.get("warnings", []):
                        print(f"     ⚠️  [{r['field']}] {w}")


if __name__ == "__main__":
    main()
