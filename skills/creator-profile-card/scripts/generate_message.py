#!/usr/bin/env python3
"""生成达人私信话术 CLI 入口 — V1.1 优化版。

V1.1 新增：
- 内容机会卡中间层
- 商品具体类型、拍摄场景、内容钩子等新字段
- 质量评分和自动重写
"""
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

from app.services.message_generator import generate_message

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)


def main():
    parser = argparse.ArgumentParser(
        description="生成达人合作私信话术 — V1.1 优化版",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
商品信息示例（--product-json）:
{
  "product_name": "宽松轻薄开衫",
  "product_category": "轻上装",
  "specific_product_type": "薄开衫",
  "target_scene": ["空调房", "通勤", "出门前搭配"],
  "creator_shooting_scene": ["镜前半身试穿", "一衣两穿", "出门前快速换装"],
  "main_content_hook": "空调房冷、外面热，一件薄开衫刚好过渡",
  "fit_body_or_style": ["宽松", "遮手臂", "日常休闲"],
  "selling_points": ["轻薄透气", "多色可选"],
  "shooting_scenarios": ["镜前半身试穿", "出门前快速换装"],
  "price_tier": "中等",
  "avoid_claims": ["防晒效果不要夸大", "不要承诺显瘦"],
  "sample_available": "可寄样",
  "commission_info": "佣金 15%",
  "support_info": "提供拍摄参考和文案模板"
}
        """,
    )
    parser.add_argument("--creator-url", required=True, help="达人 TikTok 主页链接")
    parser.add_argument("--market", required=True, help="市场: VN/TH/MY/PH")
    parser.add_argument("--target-language", required=True,
                        help="目标语言: 越南语/泰语/马来语/菲律宾语")
    parser.add_argument("--history-relation", required=True,
                        help="历史关系: 出过单/发过视频/申请过样品/聊过未合作/陌生")

    # ── 商品信息（V1.1 扩展） ──
    parser.add_argument("--product-name", required=True, help="选定商品名称")
    parser.add_argument("--product-category", required=True, help="商品类目")
    parser.add_argument("--product-json", default=None,
                        help="商品详细信息 JSON 文件路径（V1.1 推荐方式，包含 specific_product_type/target_scene 等）")

    # ── 旧字段（兼容，会被 --product-json 覆盖） ──
    parser.add_argument("--specific-product-type", default="",
                        help="商品具体类型（如'薄开衫'）")
    parser.add_argument("--selling-points", default="",
                        help="商品卖点，逗号分隔")
    parser.add_argument("--shooting-scenarios", default="",
                        help="拍摄场景，逗号分隔")
    parser.add_argument("--price-tier", default="", help="价格层级")
    parser.add_argument("--sample-available", default="", help="是否可寄样")
    parser.add_argument("--commission-info", default="", help="佣金信息")
    parser.add_argument("--support-info", default="", help="优惠/支持")

    # ── 达人画像卡 ──
    parser.add_argument("--profile-json", default=None,
                        help="达人画像卡 JSON 文件路径")

    # ── 封面拼图（用于内容机会卡） ──
    parser.add_argument("--cover-collage", action="append", default=[],
                        help="封面拼图路径（可多次指定，用于内容机会卡）")
    parser.add_argument("--cover-count", type=int, default=20,
                        help="封面总数（默认 20）")
    parser.add_argument("--recent-video-meta", default="",
                        help="近期视频文字信息（可选）")

    # ── 选项 ──
    parser.add_argument("--skip-opportunity", action="store_true",
                        help="跳过内容机会卡生成（V1.0 兼容模式）")
    parser.add_argument("--json", action="store_true",
                        help="以 JSON 格式输出")
    parser.add_argument("--verbose", action="store_true",
                        help="详细输出")

    args = parser.parse_args()

    if args.verbose:
        logging.getLogger().setLevel(logging.DEBUG)

    # ── 加载商品信息 ──
    product_info = {
        "product_name": args.product_name,
        "product_category": args.product_category,
        "specific_product_type": args.specific_product_type,
        "selling_points": [s.strip() for s in args.selling_points.split(",") if s.strip()],
        "shooting_scenarios": [s.strip() for s in args.shooting_scenarios.split(",") if s.strip()],
        "price_tier": args.price_tier,
        "sample_available": args.sample_available,
        "commission_info": args.commission_info,
        "support_info": args.support_info,
    }

    # 如果提供了 --product-json，它覆盖所有商品字段
    if args.product_json:
        try:
            product_json_data = json.loads(Path(args.product_json).read_text())
            product_info.update(product_json_data)
        except Exception as e:
            print(f"⚠️  无法加载商品 JSON: {e}", file=sys.stderr)

    # ── 加载画像卡 ──
    profile_card = {}
    if args.profile_json:
        try:
            profile_card = json.loads(Path(args.profile_json).read_text())
        except Exception as e:
            print(f"⚠️  无法加载画像卡: {e}", file=sys.stderr)

    if not args.json:
        print(f"✍️  正在生成私信话术 (V1.1)...")
        print(f"   达人: {args.creator_url}")
        print(f"   商品: {args.product_name} ({product_info.get('specific_product_type', '未指定具体类型')})")
        print(f"   语言: {args.target_language}")
        if args.skip_opportunity:
            print(f"   ⚠️  跳过内容机会卡生成")
        print()

    result = generate_message(
        creator_url=args.creator_url,
        market=args.market,
        target_language=args.target_language,
        history_relation=args.history_relation,
        product_name=args.product_name,
        product_category=args.product_category,
        profile_card=profile_card,
        product_info=product_info,
        cover_collage_images=args.cover_collage,
        cover_count=args.cover_count,
        recent_video_meta_text=args.recent_video_meta,
        skip_opportunity=args.skip_opportunity,
    )

    if result.get("error"):
        print(f"❌ 错误: {result['error']}", file=sys.stderr)
        sys.exit(1)

    if args.json:
        print(json.dumps(result, ensure_ascii=False, indent=2))
    else:
        # 内容机会卡
        opp = result.get("content_opportunity", {})
        if opp:
            print("🎯 内容机会卡:")
            obs = opp.get("observable_detail", {})
            print(f"   可观察特征: {obs.get('value', '')} (置信度: {obs.get('confidence', 0)})")
            co = opp.get("creator_content_opportunity", {})
            print(f"   内容机会: {co.get('value', '')}")
            scene = opp.get("recommended_shooting_scene", {})
            print(f"   推荐拍摄: {scene.get('value', '')}")
            ca = opp.get("message_core_angle", {})
            print(f"   话术角度: {ca.get('value', '')}")
            av = opp.get("avoid_angle", {})
            print(f"   避免: {av.get('value', '')}")
            print()

        # 话术
        print("📝 运营参考（中文）:")
        print(f"   {result.get('message_cn_for_operator', '(空)')}")
        print()
        print(f"🌐 私信版 ({args.target_language}):")
        print(f"   {result.get('message_local', '(空)')}")
        print()
        print(f"💡 为什么这样写: {result.get('why_this_message', '')}")
        print()

        # 质量评分
        quality = result.get("quality_score", 0)
        breakdown = result.get("quality_breakdown", {})
        print(f"⭐ 质量评分: {quality}/10 {'✅' if quality >= 8 else '⚠️ 需重写'} (重写 {result.get('rewrite_count', 0)} 次)")
        if breakdown:
            print(f"   具体观察: {breakdown.get('specific_observation', 0)}/2")
            print(f"   商品具体度: {breakdown.get('product_specificity', 0)}/2")
            print(f"   拍摄场景: {breakdown.get('shooting_scene', 0)}/2")
            print(f"   达人收益感: {breakdown.get('creator_benefit', 0)}/1")
            print(f"   低压力 CTA: {breakdown.get('low_pressure_cta', 0)}/1")
            print(f"   非模板化: {breakdown.get('non_template', 0)}/1")
            print(f"   风险控制: {breakdown.get('risk_control', 0)}/1")
        print()

        risk = result.get("risk_check", {})
        if any(risk.values()):
            print("⚠️  风险提示:")
            if risk.get("has_template_feeling"):
                print("   - 包含模板化表达")
            if risk.get("has_overpromise"):
                print("   - 包含过度承诺表达")
            if risk.get("has_monitoring_feeling"):
                print("   - 包含监控感表达")
            if risk.get("uses_unprovided_policy"):
                print("   - 使用了未提供的合作政策")
            if risk.get("too_general"):
                print("   - 话术太泛")
        else:
            print("✅ 风险检查通过")
        print(f"\n   日志 ID: {result.get('log_id', 'N/A')}")


if __name__ == "__main__":
    main()
