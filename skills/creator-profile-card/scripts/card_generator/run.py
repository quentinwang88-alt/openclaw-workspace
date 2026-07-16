#!/usr/bin/env python3
"""
达人沟通小卡片生成器 V2 — 支持多产品+当地语言。

读取飞书「达人沟通小卡片表」中待生成的记录：
1. LLM 分析产品图 → 总结卖点（多品每品1条，单品1-2条），文案按国家输出当地语言
2. 调用 Codex image_generation → 生成 4:5 竖版卡片
3. 回写飞书

用法：
  python3 scripts/card_generator/run.py          # dry-run
  python3 scripts/card_generator/run.py --apply  # 正式写入
"""
import base64, importlib.util, json, os, re, sys, tempfile, time, requests
from datetime import datetime
from pathlib import Path

SKILL_ROOT = Path('/Users/likeu3/.openclaw/workspace/skills/creator-profile-card')
sys.path.insert(0, str(SKILL_ROOT))

IS_DRY_RUN = '--apply' not in sys.argv
MESSAGE_ONLY = '--message-only' in sys.argv
FORCE_MESSAGE = '--force-message' in sys.argv

def cli_value(name, default=''):
    if name not in sys.argv:
        return default
    idx = sys.argv.index(name)
    if idx + 1 >= len(sys.argv):
        return default
    return sys.argv[idx + 1]

LIMIT = int(cli_value('--limit', '0') or 0)

from app.services.message_generator import generate_batch_content_opportunity

# ── 飞书 ──
cfg = json.loads((Path.home() / '.openclaw' / 'openclaw.json').read_text())
feishu = cfg['channels']['feishu']
r = requests.post('https://open.feishu.cn/open-apis/auth/v3/tenant_access_token/internal',
    json={'app_id': feishu['appId'], 'app_secret': feishu['appSecret']}, timeout=15)
TOKEN = r.json()['tenant_access_token']
H = {'Authorization': f'Bearer {TOKEN}', 'Content-Type': 'application/json'}
r = requests.get('https://open.feishu.cn/open-apis/wiki/v2/spaces/get_node',
    headers=H, params={'token': 'N4QNw8n15iX9otkmRrVcjdoonHg'})
APP_TOKEN = r.json()['data']['node']['obj_token']
TABLE_ID = 'tblXnkMOwXd74mh6'

# ── Codex ──
openai_image_config_path = Path('/Users/likeu3/.openclaw/workspace/skills/openai-image/app/config.py')
spec = importlib.util.spec_from_file_location('openai_image_config', openai_image_config_path)
openai_image_config = importlib.util.module_from_spec(spec)
assert spec and spec.loader
sys.modules['openai_image_config'] = openai_image_config
spec.loader.exec_module(openai_image_config)
resolve_codex_access_token = openai_image_config.resolve_codex_access_token
resolve_codex_base_url = openai_image_config.resolve_codex_base_url
resolve_codex_model = openai_image_config.resolve_codex_model
CODEX_TOKEN = resolve_codex_access_token()
CODEX_BASE = resolve_codex_base_url()
CODEX_IMG_MODEL = resolve_codex_model()
sys.path.insert(0, str(SKILL_ROOT))

# ── LLM (doubao) ──
from openai import OpenAI
from app.services.llm_client import get_llm_client
LLM_URL = os.environ.get('CREATOR_PROFILE_LLM_API_URL', os.environ.get('LLM_API_URL', 'https://ark.cn-beijing.volces.com/api/coding/v3'))
LLM_KEY = os.environ.get('CREATOR_PROFILE_LLM_API_KEY', os.environ.get('LLM_API_KEY', ''))
LLM_MODEL = os.environ.get('CREATOR_PROFILE_LLM_MODEL', os.environ.get('LLM_MODEL', 'Doubao-Seed-2.0-pro'))
import httpx
_USE_CODEX_LLM = 'codex' in LLM_URL.lower() or 'chatgpt.com' in LLM_URL.lower()
_http = httpx.Client(transport=httpx.HTTPTransport(retries=0), timeout=180) if 'volces' in LLM_URL else None
llm_client = None if _USE_CODEX_LLM else (OpenAI(api_key=LLM_KEY, base_url=LLM_URL, timeout=180, http_client=_http) if _http else OpenAI(api_key=LLM_KEY, base_url=LLM_URL, timeout=180))
profile_llm_client = get_llm_client() if _USE_CODEX_LLM else None

def call_json(prompt, system_prompt='', image_paths=None, max_tokens=500):
    if profile_llm_client:
        return profile_llm_client.call_json(
            prompt=prompt,
            image_paths=image_paths or [],
            system_prompt=system_prompt,
            max_tokens=max_tokens,
        )
    msgs = [];
    if system_prompt: msgs.append({'role':'system','content':system_prompt})
    content = [{'type':'text','text':prompt}]
    if image_paths:
        for p in image_paths:
            if Path(p).exists():
                b64 = base64.b64encode(Path(p).read_bytes()).decode()
                content.append({'type':'image_url','image_url':{'url':f'data:image/png;base64,{b64}'}})
    msgs.append({'role':'user','content':content})
    r = llm_client.chat.completions.create(model=LLM_MODEL, messages=msgs, max_tokens=max_tokens, temperature=0.3)
    text = r.choices[0].message.content or ''
    import re
    try: return json.loads(text)
    except:
        m = re.search(r'\{.*\}', text, re.DOTALL)
        if m: return json.loads(m.group(0))
    return {}

# ── 语言映射 ──
LANG_MAP = {'TH': '泰语', 'VN': '越南语', 'MY': '英语', 'PH': '英语', '其他': '英语'}
LANG_CODE = {'TH': 'th', 'VN': 'vi', 'MY': 'en', 'PH': 'en', '其他': 'en'}
COUNTRY_NAMES = {'TH': 'ไทย', 'VN': 'Việt Nam', 'MY': 'Malaysia', 'PH': 'Philippines', '其他': 'Other'}

BATCH_MESSAGE_FIELDS = [
    {"field_name": "图片推导产品信息", "type": 1, "ui_type": "Text"},
    {"field_name": "批量建联话术", "type": 1, "ui_type": "Text"},
    {"field_name": "批量建联话术本地语言", "type": 1, "ui_type": "Text"},
    {"field_name": "话术生成状态", "type": 1, "ui_type": "Text"},
    {"field_name": "话术失败原因", "type": 1, "ui_type": "Text"},
    {"field_name": "话术版本", "type": 1, "ui_type": "Text"},
    {"field_name": "话术生成时间", "type": 1, "ui_type": "Text"},
]

MESSAGE_VERSION = "content_opportunity_batch_v1"

def download_feishu_img(file_token):
    try:
        dl = requests.get('https://open.feishu.cn/open-apis/drive/v1/medias/batch_get_tmp_download_url',
            headers=H, params={'file_tokens': file_token}, timeout=15).json()
        for u in dl.get('data',{}).get('tmp_download_urls',[]):
            if u.get('file_token') == file_token:
                data = requests.get(u['tmp_download_url'], timeout=30).content
                p = Path(tempfile.mkdtemp()) / f'{file_token}.png'
                p.write_bytes(data)
                return str(p)
    except: pass
    return None

def list_fields():
    names = set()
    page_token = None
    while True:
        params = {'page_size': 100}
        if page_token:
            params['page_token'] = page_token
        resp = requests.get(
            f'https://open.feishu.cn/open-apis/bitable/v1/apps/{APP_TOKEN}/tables/{TABLE_ID}/fields',
            headers=H, params=params, timeout=30).json()
        if resp.get('code') != 0:
            raise RuntimeError(f'读取字段失败: {resp}')
        data = resp.get('data', {})
        for item in data.get('items', []):
            names.add(item.get('field_name'))
        if not data.get('has_more'):
            return names
        page_token = data.get('page_token')

def ensure_batch_message_fields():
    existing = list_fields()
    for spec in BATCH_MESSAGE_FIELDS:
        if spec['field_name'] in existing:
            continue
        payload = {'field_name': spec['field_name'], 'type': spec['type'], 'ui_type': spec['ui_type']}
        resp = requests.post(
            f'https://open.feishu.cn/open-apis/bitable/v1/apps/{APP_TOKEN}/tables/{TABLE_ID}/fields',
            headers=H, json=payload, timeout=30).json()
        if resp.get('code') != 0:
            raise RuntimeError(f"创建字段失败 {spec['field_name']}: {resp}")

def infer_product_category(products, supplement):
    text = ' '.join(products + [supplement or ''])
    if any(k in text for k in ['发饰', '发夹', '发圈', 'hair', 'clip']):
        return '发饰'
    if any(k in text for k in ['耳饰', '耳环', '项链', '戒指', '手链', 'jewelry']):
        return '配饰'
    if any(k in text for k in ['裤', 'pants', 'trousers']):
        return '裤装'
    if any(k in text for k in ['裙', 'dress', 'skirt']):
        return '裙装'
    if any(k in text for k in ['外套', '上衣', '衬衫', '开衫', 'top', 'shirt']):
        return '轻上装'
    return '轻上装'

def strip_batch_greeting(message):
    text = (message or '').strip()
    if not text:
        return ''
    patterns = [
        r'^(?:哈喽|你好|您好|嗨|Hi|Hello|Hey)\s*@?[\w.\-\u4e00-\u9fff]+[～~,!！,，\s]+',
        r'^(?:สวัสดีค่ะ|สวัสดีครับ|สวัสดี)\s*@?[\w.\-]+[～~,!！,，\s]+',
    ]
    for pattern in patterns:
        text = re.sub(pattern, '', text, count=1, flags=re.IGNORECASE).strip()
    return text

def soften_batch_message(message):
    text = strip_batch_greeting(message)
    replacements = {
        '很适合': '比较适合',
        '非常适合': '可以考虑',
        '马上安排': '再发具体信息',
        '我这边安排': '我再发具体信息',
        '给你安排': '发你具体信息',
        '适合你账号': '适合短视频展示',
        'เหมาะกับแผนไหมค่อยคืนข้อความ': 'ถ้าคิดว่าแนวนี้โอเค ค่อยทักกลับมาได้',
        'ค่อยคืนข้อความ': 'ค่อยทักกลับมาได้',
    }
    for old, new in replacements.items():
        text = text.replace(old, new)
    return text.strip()

def generate_batch_message(card_data, country, product_category, supplement):
    products = card_data.get('products', []) or []
    product_names = [p.get('short_product_name', '').strip() for p in products if p.get('short_product_name')]
    selling_points = [p.get('selling_point', '').strip() for p in products if p.get('selling_point')]
    recommended_product = '、'.join(product_names) or card_data.get('title') or '时尚单品'
    selling_text = '；'.join(selling_points[:2]) or supplement or '日常好搭、适合低成本短视频展示'
    target_language = LANG_MAP.get(country, '英语')
    product_context = dict(card_data)
    product_context['product_name'] = recommended_product
    product_context['product_category'] = product_category
    product_context['selling_points'] = selling_text
    result = generate_batch_content_opportunity(
        product_context=product_context,
        market=country,
        target_language=target_language,
        product_name=recommended_product,
        product_category=product_category,
        selling_points=selling_text,
        relationship_stage='冷',
    )
    return result

def build_message_fields(card_data, batch_message):
    tone_guard = batch_message.get('tone_guard') or {}
    has_message = bool(batch_message.get('message_local') or batch_message.get('message_cn_for_operator'))
    failed_reason = batch_message.get('error') or '；'.join(tone_guard.get('issues') or [])
    status = '已生成' if has_message else '失败'
    if has_message and tone_guard and not tone_guard.get('passed', True):
        status = '需人工复核'
    return {
        '图片推导产品信息': json.dumps(card_data, ensure_ascii=False, indent=2),
        '批量建联话术': batch_message.get('message_cn_for_operator', ''),
        '批量建联话术本地语言': batch_message.get('message_local', ''),
        '话术生成状态': status,
        '话术失败原因': '' if status == '已生成' else failed_reason,
        '话术版本': MESSAGE_VERSION,
        '话术生成时间': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
    }

def needs_message(flds):
    if FORCE_MESSAGE:
        return True
    status = str(flds.get('话术生成状态', '') or '').strip()
    if status in {'已生成', '已确认'}:
        return False
    if str(flds.get('批量建联话术本地语言', '') or '').strip():
        return False
    return True

def generate_card_data(products, country, commission, supplement, product_imgs):
    """LLM 生成卡片文案（当地语言）。"""
    lang = LANG_MAP.get(country, '英语')
    lines = [f"• {p}" for p in products] if products else ['（从图片识别）']
    prompt = f"""请为达人沟通小卡片生成当地语言文案。

产品列表: {', '.join(products) if products else '请从图片识别'}
产品数量: {len(products) if products else '请从图片识别'} 个
国家/市场: {country}
卡片语言: {lang}
佣金: {commission}
卖点补充: {supplement or '无'}

规则:
- 如果只有1个产品 → 1-2条卖点
- 2-3个产品 → 每品1条短卖点，每个卖点12-20字
- >3个产品 → need_split=true
- 所有文案（标题/卖点/底部）必须用{lang}，不要中文
- 卖点突出：好展示/好搭配/上镜清晰

输出JSON:
{{"need_split":false,"card_language":"{lang}",
 "title":"(当地语言标题，如\"สินค้าแนะนำ\"等)","market_text":"(市场名)","commission_text":"(佣金文本)",
 "products":[{{"short_product_name":"(当地语言短商品名，≤12字)","selling_point":"(当地语言，12-20字)"}}],
 "footer_text":"(当地语言CTA，如\"ติดต่อสอบถามรายละเอียดเพิ่มเติมได้ค่ะ\")"}}"""

    try:
        result = call_json(prompt=prompt, image_paths=product_imgs, system_prompt='达人合作商品摘要+翻译助手。只输出合法JSON。', max_tokens=600)
        return result
    except Exception as e:
        print(f'    文案生成失败: {e}')
        return None

def identify_products_from_images(product_imgs, fallback_products=None):
    """多张产品图逐图识别产品名，避免把多个不同产品合成单品卡。"""
    fallback_products = fallback_products or []
    if not product_imgs:
        return fallback_products
    try:
        expected = min(len(product_imgs), 3)
        hint = '、'.join(fallback_products) if fallback_products else '无'
        result = call_json(
            prompt=f'''这批图片是建联小卡片的产品图，请逐张识别商品。

要求：
- 一张图片对应一个产品名
- 如果多张图是不同产品，必须输出多个产品
- 如果表格里的产品名只是泛称或只写了其中一个产品，不要合并
- 最多输出 {expected} 个产品名
- 产品名用中文短名，便于后续翻译

表格产品名提示：{hint}
图片数量：{len(product_imgs)}

只输出JSON：{{"products":["产品1","产品2","产品3"]}}''',
            system_prompt='电商商品识别助手。只输出合法JSON。',
            image_paths=product_imgs[:3],
            max_tokens=300,
        )
        inferred = [str(p).strip() for p in result.get('products', []) if str(p).strip()]
        return inferred[:3] or fallback_products
    except Exception as e:
        print(f'    产品图识别失败: {e}')
        return fallback_products

def generate_card_image(card_data, product_imgs):
    """Codex image_generation 生成卡片。"""
    if not CODEX_TOKEN: raise RuntimeError('Codex token 不可用')

    products = card_data.get('products', [])
    is_multi = len(products) > 1

    # 构建产品描述
    prod_lines = []
    for p in products:
        name = p.get('short_product_name', '')
        sp = p.get('selling_point', '')
        prod_lines.append(f"• {name}: {sp}")

    prompt = f"""Generate a creator collaboration product card, 4:5 vertical, clean style for mobile chat.

Title: {card_data.get('title','')}
Market: {card_data.get('market_text','')}
Commission: {card_data.get('commission_text','')}

Products:
{chr(10).join(prod_lines)}

Footer: {card_data.get('footer_text','')}

Design:
1. Product image(s) as main visual. Multi-product cards: arrange {len(products)} products evenly
2. Clean layout, easy to read on phone
3. Light, soft background
4. NOT an e-commerce detail page
5. NO aggressive promo elements
6. Looks like a "creator collab card", not consumer ad
7. All text must be in the provided language, do NOT translate to Chinese"""

    proxy = os.environ.get('HTTPS_PROXY', os.environ.get('https_proxy', ''))
    client = httpx.Client(proxy=proxy, timeout=180) if proxy else httpx.Client(transport=httpx.HTTPTransport(retries=0), timeout=180)

    content_items = [{"type": "input_text", "text": prompt}]
    for img in product_imgs:
        if img and Path(img).exists():
            b64 = base64.b64encode(Path(img).read_bytes()).decode()
            content_items.append({"type": "input_image", "image_url": f"data:image/png;base64,{b64}"})

    body = {
        "model": "gpt-5.5", "instructions": "You are an image generation assistant.",
        "input": [{"role": "user", "content": content_items}],
        "tools": [{"type": "image_generation", "model": CODEX_IMG_MODEL, "quality": "medium",
                    "size": "1024x1280", "output_format": "png", "background": "opaque", "partial_images": 1}],
        "tool_choice": {"type": "allowed_tools", "mode": "required", "tools": [{"type": "image_generation"}]},
        "stream": True, "store": False,
    }

    url = f"{CODEX_BASE}/responses"
    headers = {"Authorization": f"Bearer {CODEX_TOKEN}", "Content-Type": "application/json"}

    image_b64 = ""
    with client.stream("POST", url, json=body, headers=headers) as resp:
        for line in resp.iter_lines():
            if not line or not line.startswith("data: "): continue
            ds = line[6:]
            if ds.strip() == "[DONE]": break
            try: event = json.loads(ds)
            except: continue
            if event.get("type") == "response.image_generation_call.partial_image":
                c = str(event.get("partial_image_b64", "")).strip()
                if c: image_b64 = c
            elif event.get("type") == "response.output_item.done":
                item = event.get("item", {})
                if item.get("type") == "image_generation_call":
                    r2 = str(item.get("result", "")).strip()
                    if r2: image_b64 = r2
    if not image_b64: raise RuntimeError('Codex 未返回图片')
    return image_b64

def upload_to_feishu(image_b64, filename):
    img_data = base64.b64decode(image_b64)
    r = requests.post('https://open.feishu.cn/open-apis/drive/v1/medias/upload_all',
        headers={'Authorization': f'Bearer {TOKEN}'},
        data={'file_name':filename, 'parent_type':'bitable_image', 'parent_node':APP_TOKEN, 'size':str(len(img_data))},
        files={'file':(filename, img_data, 'image/png')}, timeout=60)
    return r.json()

# ── 主流程 ──
print(f"Codex: {'***'+CODEX_TOKEN[-6:] if CODEX_TOKEN else 'EMPTY'}")
if IS_DRY_RUN: print("🔍 DRY-RUN\n")
if MESSAGE_ONLY: print("💬 MESSAGE-ONLY：只生成/回写批量建联话术，不重画卡片\n")
if not IS_DRY_RUN:
    ensure_batch_message_fields()

r = requests.get(f'https://open.feishu.cn/open-apis/bitable/v1/apps/{APP_TOKEN}/tables/{TABLE_ID}/records',
    headers=H, params={'page_size': 50})
records = r.json()['data']['items']
if MESSAGE_ONLY:
    pending = [i for i in records if needs_message(i.get('fields', {}))]
else:
    pending = [i for i in records if i['fields'].get('生成状态') == '待生成']
if LIMIT > 0:
    pending = pending[:LIMIT]
print(f"待生成: {len(pending)}/{len(records)}\n")

for rec in pending:
    flds = rec['fields']
    rid = rec['record_id']
    raw_product = flds.get('本次带货产品', '') or ''
    country = flds.get('国家/市场', '') or ''
    commission = flds.get('佣金比例', '') or ''
    supplement = flds.get('卖点补充', '') or ''

    if not country or not commission:
        print(f'  ⏭️ 缺国家/佣金')
        continue

    # 解析产品列表（换行/逗号分隔）
    products = [p.strip() for p in raw_product.replace('\n', ',').split(',') if p.strip()]

    # 下载产品图
    product_imgs = []
    for att in flds.get('产品图片', []):
        ft = att.get('file_token', '') if isinstance(att, dict) else str(att)
        if ft:
            path = download_feishu_img(ft)
            if path: product_imgs.append(path)

    # 无产品名，或 1 个泛称产品名对应多张产品图时，按图片逐个识别产品。
    if product_imgs and (not products or (len(products) == 1 and len(product_imgs) > 1)):
        inferred_products = identify_products_from_images(product_imgs, products)
        if len(inferred_products) > len(products):
            products = inferred_products
        elif not products:
            products = inferred_products or ['时尚单品']
    elif not products:
        products = ['时尚单品']

    # >3 个产品 → 建议拆卡
    if len(products) > 3:
        print(f'  {products[0][:20]}... (共{len(products)}品) → 超过3个，建议拆卡')
        if not IS_DRY_RUN:
            requests.put(f'https://open.feishu.cn/open-apis/bitable/v1/apps/{APP_TOKEN}/tables/{TABLE_ID}/records/{rid}',
                headers=H, json={'fields': {'生成状态': '失败'}}, timeout=10)
        continue

    prefix = '多品' if len(products) > 1 else '单品'
    print(f'  [{prefix}{len(products)}] {products[0][:20]} [{country}] 佣金={commission}', end=' ', flush=True)

    # Step 1: LLM 生成当地语言文案
    card_data = generate_card_data(products, country, commission, supplement, product_imgs)
    if not card_data or card_data.get('need_split'):
        if not IS_DRY_RUN:
            requests.put(f'https://open.feishu.cn/open-apis/bitable/v1/apps/{APP_TOKEN}/tables/{TABLE_ID}/records/{rid}',
                headers=H, json={'fields': {'生成状态': '失败'}}, timeout=10)
        print('❌ 文案生成失败')
        continue

    sps = [p.get('selling_point','') for p in card_data.get('products',[])]
    print(f'卖点={sps[:2]} ', end='', flush=True)

    product_names = [p.get('short_product_name', '').strip() for p in card_data.get('products', []) if p.get('short_product_name')]
    product_category = infer_product_category(product_names or products, supplement)
    try:
        batch_message = generate_batch_message(card_data, country, product_category, supplement)
        message_fields = build_message_fields(card_data, batch_message)
        print(f"话术={message_fields['话术生成状态']} ", end='', flush=True)
    except Exception as e:
        message_fields = {
            '图片推导产品信息': json.dumps(card_data, ensure_ascii=False, indent=2),
            '话术生成状态': '失败',
            '话术版本': MESSAGE_VERSION,
            '话术生成时间': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
        }
        print(f'话术失败: {e} ', end='', flush=True)

    if IS_DRY_RUN:
        lang = card_data.get("card_language", "?")
        print(f'(dry-run) {lang}')
        continue

    if MESSAGE_ONLY:
        r2 = requests.put(f'https://open.feishu.cn/open-apis/bitable/v1/apps/{APP_TOKEN}/tables/{TABLE_ID}/records/{rid}',
            headers=H, json={'fields': message_fields}, timeout=10)
        print(f'✅ message code={r2.json().get("code", -1)}')
        time.sleep(1)
        continue

    # Step 2: Codex 生成卡片
    try:
        card_b64 = generate_card_image(card_data, product_imgs)

        # Step 3: 上传飞书
        upload_result = upload_to_feishu(card_b64, f'card_{products[0][:8]}.png')
        if upload_result.get('code') != 0:
            print(f'❌ 上传失败: {upload_result.get("msg","")}')
            requests.put(f'https://open.feishu.cn/open-apis/bitable/v1/apps/{APP_TOKEN}/tables/{TABLE_ID}/records/{rid}',
                headers=H, json={'fields': {'生成状态': '失败'}}, timeout=10)
            continue

        file_token = upload_result['data']['file_token']
        fields_to_write = {
            '生成状态': '已生成',
            '生成卡片': [{'file_token': file_token, 'name': f'card_{products[0][:8]}.png'}],
        }
        fields_to_write.update(message_fields)
        r2 = requests.put(f'https://open.feishu.cn/open-apis/bitable/v1/apps/{APP_TOKEN}/tables/{TABLE_ID}/records/{rid}',
            headers=H, json={'fields': fields_to_write}, timeout=10)
        print(f'✅ code={r2.json().get("code", -1)}')
        time.sleep(3)
    except Exception as e:
        print(f'❌ {e}')
        if not IS_DRY_RUN:
            requests.put(f'https://open.feishu.cn/open-apis/bitable/v1/apps/{APP_TOKEN}/tables/{TABLE_ID}/records/{rid}',
                headers=H, json={'fields': {'生成状态': '失败'}}, timeout=10)

print(f'\n完成')
