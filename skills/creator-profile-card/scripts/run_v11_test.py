#!/usr/bin/env python3
"""V1.1 试跑脚本：读飞书达人画像卡，拿计划带货商品图，跑内容机会卡 + 话术 + 质量评分。"""
import json, os, sys, requests
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from app.services.message_generator import generate_message
from app.services.llm_client import get_llm_client

# ── 凭证 ──
cfg = json.loads(Path("/sessions/serene-gallant-fermat/mnt/.openclaw/openclaw.json").read_text())
feishu = cfg["channels"]["feishu"]
resp = requests.post("https://open.feishu.cn/open-apis/auth/v3/tenant_access_token/internal",
    json={"app_id": feishu["appId"], "app_secret": feishu["appSecret"]}, timeout=30)
token = resp.json()["tenant_access_token"]
H = {"Authorization": f"Bearer {token}"}

# ── 解析表 ──
resp = requests.get("https://open.feishu.cn/open-apis/wiki/v2/spaces/get_node",
    headers=H, params={"token": "GNaHw1xM9ik7tDkBS6Kcfdf8nwg"}, timeout=30)
app_token = resp.json()["data"]["node"]["obj_token"]
TABLE_ID = "tbluyKELrrCc5qPT"

# ── 读记录 ──
resp = requests.get(
    f"https://open.feishu.cn/open-apis/bitable/v1/apps/{app_token}/tables/{TABLE_ID}/records",
    headers=H, params={"page_size": 500}, timeout=30)
items = resp.json()["data"]["items"]

# ── 选 #6 测试 ──
def find(num):
    return [r for r in items if r["fields"].get("编号") == str(num)][0]

def download_attachment(file_token, name, dest_dir):
    dl = requests.get(
        "https://open.feishu.cn/open-apis/drive/v1/medias/batch_get_tmp_download_url",
        headers=H, params={"file_tokens": file_token}, timeout=30)
    urls = dl.json()["data"]["tmp_download_urls"]
    if urls:
        content = requests.get(urls[0]["tmp_download_url"], timeout=60).content
        p = Path(dest_dir) / name
        p.write_bytes(content)
        return str(p)
    return None

# ── 准备数据 ──
tmp = "/sessions/serene-gallant-fermat/mnt/outputs/test_images"
rec = find(6)
flds = rec["fields"]

# 下载封面
Path(f"{tmp}/rec_6").mkdir(parents=True, exist_ok=True)
cover_imgs = []
for ci, cv in enumerate(flds.get("封面拼图", [])):
    p = download_attachment(cv["file_token"], cv.get("name", f"cover_{ci}.png"), f"{tmp}/rec_6")
    if p: cover_imgs.append(p)

# 下载商品图
Path(f"{tmp}/products").mkdir(parents=True, exist_ok=True)
product_imgs = []
for pi, pv in enumerate(flds.get("计划带货商品", [])):
    p = download_attachment(pv["file_token"], pv.get("name", f"product_{pi}.png"), f"{tmp}/products")
    if p: product_imgs.append(p)

# ── LLM 从商品图分析商品信息 ──
print("🔍 分析商品图...")
llm = get_llm_client()
product_prompt = """请分析这张商品图片。只基于图片内容判断。输出 JSON：
{
  "product_name": "商品名称",
  "product_category": "发饰/耳饰/项链/围巾/帽子/轻上装/女装",
  "specific_product_type": "具体类型（薄开衫/宽松T恤/短款夹克/珍珠发夹等）",
  "color_style": "颜色风格",
  "fit_body_or_style": ["适合的身型或风格"],
  "target_scene": ["目标使用场景"],
  "creator_shooting_scene": ["达人拍摄场景建议"],
  "main_content_hook": "一句话内容钩子",
  "avoid_claims": ["避免的过度承诺"],
  "selling_points": ["核心卖点"],
  "sample_available": "可寄样",
  "commission_info": "佣金 15%",
  "support_info": "提供拍摄参考和文案模板"
}"""
product_info = llm.call_json(
    prompt=product_prompt, image_paths=product_imgs[:1],
    system_prompt="你是电商商品分析助手。只基于图片内容分析，不编造信息。")
product_info["sample_available"] = "可寄样"
product_info["commission_info"] = "佣金 15%"
product_info["support_info"] = "提供拍摄参考"
print(json.dumps(product_info, ensure_ascii=False, indent=2))

# ── 构建画像卡 ──
profile_card = {"writable_fields": {
    "内容类型": flds.get("内容类型", ""),
    "画面风格": flds.get("画面风格", ""),
    "适配类目": flds.get("适配类目", []),
    "推荐商品/品类": flds.get("推荐商品/品类", ""),
}}

# ── 跑 V1.1 ──
url_field = flds.get("达人链接", {})
creator_url = url_field.get("link", "") if isinstance(url_field, dict) else str(url_field)
print(f"\n📊 #6 | {creator_url[:50]}")
print(f"商品: {product_info['product_name']} ({product_info.get('specific_product_type','?')})")
print(f"旧话术: {flds.get('沟通切入点','?')[:80]}")

# 图片：封面 + 商品图一起给 LLM 看
all_imgs = cover_imgs + product_imgs

r = generate_message(
    creator_url=creator_url, market="TH", target_language="泰语",
    history_relation="陌生",
    product_name=product_info["product_name"],
    product_category=product_info.get("product_category", "轻上装"),
    profile_card=profile_card, product_info=product_info,
    cover_collage_images=all_imgs, cover_count=20,
)

print(f"\n新话术: {r.get('message_cn_for_operator')}")
print(f"评分: {r.get('quality_score')}/10 (重写{r.get('rewrite_count')}次)")
print(f"细节: {r.get('quality_breakdown', {})}")
opp = r.get("content_opportunity", {})
if opp:
    print(f"观察: {opp.get('observable_detail',{}).get('value','')[:100]}")
    print(f"拍摄: {opp.get('recommended_shooting_scene',{}).get('value','')[:100]}")
    print(f"角度: {opp.get('message_core_angle',{}).get('value','')[:100]}")
risks = [k for k,v in r.get("risk_check",{}).items() if v]
print(f"风险: {risks if risks else '✅'}")
