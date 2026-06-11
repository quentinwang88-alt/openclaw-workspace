#!/usr/bin/env python3
"""周度达人巡检工具 — V1.1 达人关系运营闭环。

功能：
1. 读取飞书达人画像表中的所有达人
2. 根据层级、关系阶段、冷却期、回复状态判断本周动作
3. 生成本周建议动作 + 建议原因（回写飞书）
4. 可选：为建议触达的达人生成话术草稿并回写飞书
"""
import json, os, sys, requests
from datetime import datetime, timezone
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
WORKSPACE_ROOT = REPO_ROOT.parents[1]
sys.path.insert(0, str(REPO_ROOT))
sys.path.insert(0, str(WORKSPACE_ROOT))

# 加载 .env
try:
    from workspace_support import load_repo_env
    load_repo_env()
except ImportError:
    pass

from app.services.weekly_decision import decide_weekly_action, days_since


# ── 飞书凭证 ──
cfg = json.loads((Path.home() / ".openclaw" / "openclaw.json").read_text())

feishu = cfg["channels"]["feishu"]
resp = requests.post(
    "https://open.feishu.cn/open-apis/auth/v3/tenant_access_token/internal",
    json={"app_id": feishu["appId"], "app_secret": feishu["appSecret"]}, timeout=30,
)
TOKEN = resp.json()["tenant_access_token"]
H = {"Authorization": f"Bearer {TOKEN}", "Content-Type": "application/json"}

WIKI_TOKEN = "GNaHw1xM9ik7tDkBS6Kcfdf8nwg"
TABLE_ID = "tbluyKELrrCc5qPT"
resp = requests.get(
    "https://open.feishu.cn/open-apis/wiki/v2/spaces/get_node",
    headers=H, params={"token": WIKI_TOKEN}, timeout=30,
)
APP_TOKEN = resp.json()["data"]["node"]["obj_token"]


def fetch_all_records() -> list:
    resp = requests.get(
        f"https://open.feishu.cn/open-apis/bitable/v1/apps/{APP_TOKEN}/tables/{TABLE_ID}/records",
        headers=H, params={"page_size": 500}, timeout=30,
    )
    return resp.json().get("data", {}).get("items", [])


def get_field(fields: dict, key: str, default=None):
    v = fields.get(key)
    if v is None:
        return default
    if isinstance(v, dict):
        return v.get("link") or v.get("text") or str(v)
    if isinstance(v, list) and v and isinstance(v[0], dict):
        return [x.get("text", str(x)) for x in v]
    return v


def generate_draft_for_record(record: dict, decision: dict) -> str:
    """为单条记录生成话术草稿。

    Returns:
        话术文本，失败则返回空字符串。
    """
    try:
        from app.services.message_generator import generate_message

        flds = record.get("fields", {})
        url_field = flds.get("达人链接", {})
        creator_url = url_field.get("link", "") if isinstance(url_field, dict) else str(url_field)

        profile = {"writable_fields": {
            "内容类型": get_field(flds, "内容类型", ""),
            "画面风格": get_field(flds, "画面风格", ""),
            "适配类目": get_field(flds, "适配类目", []),
            "推荐商品/品类": get_field(flds, "推荐商品/品类", ""),
            "活跃度": get_field(flds, "活跃度", ""),
        }}

        history_relation = get_field(flds, "历史关系", "陌生")
        message_purpose = decision.get("message_purpose", "product_invitation")
        product_name = get_field(flds, "推荐商品/品类", get_field(flds, "选定商品", ""))
        product_category = "轻上装"  # 默认，实际应读商品表

        # 下载封面拼图（如果表里有的话）
        covers = flds.get("封面拼图", [])
        cover_imgs = []
        if covers:
            import tempfile
            tmp_dir = Path(tempfile.mkdtemp(prefix="feishu_covers_"))
            for cv in covers:
                try:
                    file_token = cv.get("file_token", "")
                    dl_resp = requests.get(
                        "https://open.feishu.cn/open-apis/drive/v1/medias/batch_get_tmp_download_url",
                        headers=H, params={"file_tokens": file_token}, timeout=15,
                    )
                    urls = dl_resp.json().get("data", {}).get("tmp_download_urls", [])
                    if urls:
                        tmp_url = urls[0].get("tmp_download_url", "")
                        if tmp_url:
                            img_resp = requests.get(tmp_url, timeout=30)
                            img_path = tmp_dir / f"{file_token}.png"
                            img_path.write_bytes(img_resp.content)
                            cover_imgs.append(str(img_path))
                except Exception:
                    pass

        # 下载商品图
        product_imgs = []
        product_field = flds.get("计划带货商品", [])
        if product_field:
            import tempfile
            tmp_dir = Path(tempfile.mkdtemp(prefix="feishu_product_"))
            for pv in (product_field if isinstance(product_field, list) else [product_field]):
                try:
                    file_token = pv.get("file_token", "")
                    dl_resp = requests.get(
                        "https://open.feishu.cn/open-apis/drive/v1/medias/batch_get_tmp_download_url",
                        headers=H, params={"file_tokens": file_token}, timeout=15,
                    )
                    urls = dl_resp.json().get("data", {}).get("tmp_download_urls", [])
                    if urls:
                        tmp_url = urls[0].get("tmp_download_url", "")
                        if tmp_url:
                            img_resp = requests.get(tmp_url, timeout=30)
                            img_path = tmp_dir / f"{file_token}.png"
                            img_path.write_bytes(img_resp.content)
                            product_imgs.append(str(img_path))
                except Exception:
                    pass

        # 从商品图分析商品信息
        product_info = {"product_name": product_name, "product_category": product_category}
        if product_imgs:
            try:
                from app.services.llm_client import get_llm_client
                llm = get_llm_client()
                product_info = llm.call_json(
                    prompt="分析这张商品图片，输出JSON：product_name, product_category, specific_product_type, target_scene, creator_shooting_scene, main_content_hook, fit_body_or_style, selling_points, avoid_claims",
                    image_paths=product_imgs[:1],
                    system_prompt="你是电商商品分析助手。只基于图片内容分析，不编造信息。",
                )
                product_info["sample_available"] = "可寄样"
                product_info["commission_info"] = "佣金 15%"
            except Exception:
                pass

        result = generate_message(
            creator_url=creator_url,
            market="TH",
            target_language="泰语",
            history_relation=history_relation,
            product_name=product_name or product_info.get("product_name", ""),
            product_category=product_category,
            profile_card=profile,
            product_info=product_info,
            cover_collage_images=cover_imgs + product_imgs,
            cover_count=20,
            message_purpose=message_purpose,
            relationship_context={
                "creator_tier": get_field(flds, "达人层级", ""),
                "relationship_stage": get_field(flds, "关系阶段", ""),
                "days_since_last_contact": "未知",
                "last_contact_type": get_field(flds, "上次联系类型", ""),
                "last_contact_at": str(get_field(flds, "上次联系时间", "")),
                "last_message_summary": get_field(flds, "本次话术草稿", "")[:100],
            },
        )
        return result.get("message_cn_for_operator", "")

    except Exception as e:
        print(f"      ⚠️ 话术生成失败: {e}")
        return ""


def scan_and_update(generate_messages: bool = False, dry_run: bool = False, limit: int = 0, offset: int = 0):
    items = fetch_all_records()
    total = len(items)
    if offset > 0:
        items = items[offset:]
    if limit > 0:
        items = items[:limit]
    print(f"📊 全表 {total} 条记录，本次处理 {len(items)} 条 (offset={offset})\n")

    actions_count = {}
    total_updated = 0
    total_messages = 0

    needs_action_records = []

    # 第一轮：决策
    for item in items:
        flds = item.get("fields", {})
        rid = item["record_id"]
        num = flds.get("编号", "?")

        creator_tier = get_field(flds, "达人层级", "")
        relationship_stage = get_field(flds, "关系阶段", "冷")
        activity = get_field(flds, "活跃度", "中")
        history_relation = get_field(flds, "历史关系", "陌生")
        last_contact_at = get_field(flds, "上次联系时间", None)
        last_contact_type = get_field(flds, "上次联系类型", None)
        no_reply_count = int(get_field(flds, "连续未回复次数", 0) or 0)
        next_contact_after = get_field(flds, "下次可联系时间", None)
        fit_categories = get_field(flds, "适配类目", [])

        if not creator_tier:
            if history_relation in ("出过单", "发过视频"):
                creator_tier = "A 类"
            elif history_relation in ("申请过样品", "聊过未合作"):
                creator_tier = "B 类"
            else:
                creator_tier = "C 类"

        if not relationship_stage:
            if history_relation in ("出过单", "发过视频"):
                relationship_stage = "温"
            else:
                relationship_stage = "冷"

        decision = decide_weekly_action(
            creator_tier=creator_tier, relationship_stage=relationship_stage,
            activity=activity, history_relation=history_relation,
            last_contact_at=last_contact_at, last_contact_type=last_contact_type,
            no_reply_count=no_reply_count, next_contact_after=next_contact_after,
            fit_categories=fit_categories,
        )

        action = decision["action"]
        reason = decision["reason"]
        actions_count[action] = actions_count.get(action, 0) + 1

        status_icon = {"商品邀约": "🎯", "关系维护": "💬", "轻跟进": "👋", "暂缓": "⏸️", "放弃": "❌", "人工查看": "👁️"}.get(action, "")
        print(f"  {status_icon} #{num} [{creator_tier}] {action} — {reason}")

        if dry_run:
            needs_action = action in ("商品邀约", "关系维护", "轻跟进")
            if needs_action:
                needs_action_records.append((item, decision))
            continue

        update_fields = {
            "达人层级": creator_tier,
            "关系阶段": relationship_stage,
            "本周建议动作": action,
            "本周建议原因": reason,
            "处理状态": "待处理",
        }

        try:
            resp = requests.put(
                f"https://open.feishu.cn/open-apis/bitable/v1/apps/{APP_TOKEN}/tables/{TABLE_ID}/records/{rid}",
                headers=H, json={"fields": update_fields}, timeout=15,
            )
            if resp.json().get("code") == 0:
                total_updated += 1
            needs_action = action in ("商品邀约", "关系维护", "轻跟进")
            if needs_action:
                needs_action_records.append((item, decision))
        except Exception as e:
            print(f"    ⚠️ 回写失败: {e}")

    print(f"\n📊 动作分布:")
    for a, c in sorted(actions_count.items()):
        print(f"  {a}: {c}")

    if not dry_run:
        print(f"✅ 已更新 {total_updated} 条记录")

    # 第二轮：生成本周话术草稿
    if generate_messages and needs_action_records:
        action_records = [r for r, d in needs_action_records if d["action"] in ("商品邀约", "关系维护", "轻跟进")]
        print(f"\n✍️ 正在为 {len(action_records)} 条待触达达人生成话术草稿...\n")

        for item, decision in needs_action_records:
            flds = item.get("fields", {})
            rid = item["record_id"]
            num = flds.get("编号", "?")
            action = decision["action"]

            print(f"  生成 #{num} [{action}] ... ", end="", flush=True)
            draft = generate_draft_for_record(item, decision)
            if draft:
                # 回写话术草稿到飞书
                try:
                    resp = requests.put(
                        f"https://open.feishu.cn/open-apis/bitable/v1/apps/{APP_TOKEN}/tables/{TABLE_ID}/records/{rid}",
                        headers=H,
                        json={"fields": {"本次话术草稿": draft, "处理状态": "待处理"}},
                        timeout=15,
                    )
                    if resp.json().get("code") == 0:
                        total_messages += 1
                        print(f"✅ ({len(draft)} 字)")
                    else:
                        print(f"⚠️ 回写失败")
                except Exception as e:
                    print(f"⚠️ {e}")
            else:
                print("⏭️ 生成失败")

        print(f"\n📝 已生成 {total_messages} 条话术草稿")

    return {"actions_count": actions_count, "total_updated": total_updated, "total_messages": total_messages}


def main():
    import argparse
    parser = argparse.ArgumentParser(description="周度达人池巡检")
    parser.add_argument("--dry-run", action="store_true", help="仅输出，不回写")
    parser.add_argument("--generate-messages", action="store_true", help="为本周建议触达的达人生成话术草稿并回写飞书")
    parser.add_argument("--limit", type=int, default=0, help="限制处理条数（0=全部）")
    parser.add_argument("--offset", type=int, default=0, help="跳过前 N 条")
    parser.add_argument("--verbose", action="store_true")
    args = parser.parse_args()

    scan_and_update(generate_messages=args.generate_messages, dry_run=args.dry_run, limit=args.limit, offset=args.offset)


if __name__ == "__main__":
    main()
