#!/usr/bin/env python3
"""离线批量脚本：逐条生成话术草稿并回写飞书，每条独立运行，失败不影响其他。"""
import json, os, sys, time, traceback, requests
from pathlib import Path

sys.path.insert(0, '/sessions/serene-gallant-fermat/mnt/workspace/skills/creator-profile-card')

from app.services.message_generator import generate_message
from app.services.llm_client import get_llm_client

# ── 初始化 ──
cfg = json.loads(open('/sessions/serene-gallant-fermat/mnt/.openclaw/openclaw.json').read())
feishu = cfg['channels']['feishu']
r = requests.post('https://open.feishu.cn/open-apis/auth/v3/tenant_access_token/internal',
    json={'app_id': feishu['appId'], 'app_secret': feishu['appSecret']}, timeout=15)
TOKEN = r.json()['tenant_access_token']
H = {'Authorization': f'Bearer {TOKEN}', 'Content-Type': 'application/json'}

r = requests.get('https://open.feishu.cn/open-apis/wiki/v2/spaces/get_node',
    headers=H, params={'token': 'GNaHw1xM9ik7tDkBS6Kcfdf8nwg'}, timeout=15)
APP_TOKEN = r.json()['data']['node']['obj_token']
TABLE_ID = 'tbluyKELrrCc5qPT'

# 读任务列表
tasks = json.loads(Path('/sessions/serene-gallant-fermat/mnt/outputs/v11_batch_tasks.json').read_text())
print(f"共 {len(tasks)} 条待生成")

BASE = Path('/sessions/serene-gallant-fermat/mnt/outputs/v11_batch')
RESULT_FILE = BASE / 'results.jsonl'

def download_img(file_token, name, dest):
    try:
        dl = requests.get(
            'https://open.feishu.cn/open-apis/drive/v1/medias/batch_get_tmp_download_url',
            headers=H, params={'file_tokens': file_token}, timeout=15)
        urls = dl.json().get('data', {}).get('tmp_download_urls', [])
        if urls:
            content = requests.get(urls[0]['tmp_download_url'], timeout=30).content
            (dest / name).write_bytes(content)
            return str(dest / name)
    except Exception:
        pass
    return None

def run_one(task, idx):
    """处理单条记录。"""
    try:
        rid = task['rid']
        num = task['num']
        action = task['action']

        # 确定 purpose
        purpose_map = {'商品邀约': 'product_invitation', '关系维护': 'relationship_maintenance', '轻跟进': 'follow_up'}
        purpose = purpose_map.get(action, 'product_invitation')

        # 关系维护不需要商品，直接生成
        if purpose == 'relationship_maintenance':
            profile = {'writable_fields': {
                '内容类型': task.get('content_type', ''),
                '画面风格': task.get('visual', ''),
                '适配类目': task.get('fit_cat', []),
                '活跃度': task.get('activity', ''),
            }}
            result = generate_message(
                creator_url=task['url'], market='TH', target_language='泰语',
                history_relation=task['history'],
                product_name='', product_category='',
                profile_card=profile, product_info={},
                message_purpose=purpose,
                relationship_context={
                    'creator_tier': task['tier'],
                    'days_since_last_contact': '未知',
                },
            )
            draft = result.get('message_cn_for_operator', '')
            print(f"  [{idx}] #{num} 关系维护 → {len(draft)}字")
            return draft

        # 商品邀约需要下载商品图分析
        tmp_covers = BASE / f'covers_{num}'
        tmp_products = BASE / f'products_{num}'
        tmp_covers.mkdir(parents=True, exist_ok=True)
        tmp_products.mkdir(parents=True, exist_ok=True)

        # 查飞书记录拿附件
        r = requests.get(
            f'https://open.feishu.cn/open-apis/bitable/v1/apps/{APP_TOKEN}/tables/{TABLE_ID}/records/{rid}',
            headers=H, timeout=15)
        flds = r.json().get('data', {}).get('record', {}).get('fields', {})

        # 下载商品图
        product_imgs = []
        for pv in flds.get('计划带货商品', []):
            path = download_img(pv['file_token'], pv.get('name', 'product.png'), tmp_products)
            if path: product_imgs.append(path)

        # 商品分析
        product_info = {'product_name': task.get('product', '日常女装'), 'product_category': '轻上装'}
        if product_imgs:
            try:
                llm = get_llm_client()
                pi = llm.call_json(
                    prompt='分析商品图输出JSON：product_name,product_category,specific_product_type,target_scene,creator_shooting_scene,main_content_hook,fit_body_or_style,selling_points,avoid_claims',
                    image_paths=product_imgs[:1], system_prompt='电商商品分析助手')
                product_info = pi
            except Exception as e:
                print(f"  [{idx}] #{num} 商品分析失败: {e}")
        product_info['sample_available'] = '可寄样'
        product_info['commission_info'] = '佣金 15%'

        # 下载封面
        cover_imgs = []
        for cv in flds.get('封面拼图', []):
            path = download_img(cv['file_token'], cv.get('name', 'cover.png'), tmp_covers)
            if path: cover_imgs.append(path)

        profile = {'writable_fields': {
            '内容类型': task.get('content_type', ''),
            '画面风格': task.get('visual', ''),
            '适配类目': task.get('fit_cat', []),
            '活跃度': task.get('activity', ''),
        }}

        result = generate_message(
            creator_url=task['url'], market='TH', target_language='泰语',
            history_relation=task['history'],
            product_name=product_info.get('product_name', ''),
            product_category=product_info.get('product_category', '轻上装'),
            profile_card=profile, product_info=product_info,
            cover_collage_images=cover_imgs + product_imgs, cover_count=20,
            message_purpose=purpose,
        )

        draft = result.get('message_cn_for_operator', '')
        score = result.get('quality_score', 0)
        print(f"  [{idx}] #{num} {action} score={score} → {len(draft)}字")
        return draft

    except Exception as e:
        print(f"  [{idx}] #{task.get('num','?')} ❌ {e}")
        return None


# ── 主循环 ──
BASE.mkdir(parents=True, exist_ok=True)
results = []

for idx, task in enumerate(tasks, 1):
    print(f"\n[{idx}/{len(tasks)}] #{task['num']} {task['action']}", flush=True)
    t0 = time.time()

    draft = run_one(task, idx)

    elapsed = time.time() - t0
    success = bool(draft)

    rec = {
        'rid': task['rid'],
        'num': task['num'],
        'action': task['action'],
        'success': success,
        'draft_len': len(draft) if draft else 0,
        'elapsed': f'{elapsed:.0f}s',
    }

    if success:
        # 回写飞书
        try:
            r = requests.put(
                f'https://open.feishu.cn/open-apis/bitable/v1/apps/{APP_TOKEN}/tables/{TABLE_ID}/records/{task["rid"]}',
                headers=H, json={'fields': {'本次话术草稿': draft}}, timeout=15)
            rec['feishu_code'] = r.json().get('code', -1)
        except Exception as e:
            rec['feishu_error'] = str(e)[:100]
    else:
        rec['error'] = '生成失败'

    results.append(rec)
    # 每条都写日志
    with open(RESULT_FILE, 'a') as f:
        f.write(json.dumps(rec, ensure_ascii=False) + '\n')

# ── 总结 ──
success_count = sum(1 for r in results if r['success'])
print(f'\n{"="*50}')
print(f'完成: {success_count}/{len(results)} 成功')
for r in results:
    status = '✅' if r['success'] else '❌'
    extra = f" code={r.get('feishu_code')}" if r.get('feishu_code') is not None else f" err={r.get('error','')}"
    print(f"  {status} #{r['num']} [{r['action']}] {r['elapsed']}{extra}")
