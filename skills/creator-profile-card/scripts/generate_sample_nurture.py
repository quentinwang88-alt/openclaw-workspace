#!/usr/bin/env python3
"""批量生成样品批前沟通话术。

用法：
  python3 scripts/generate_sample_nurture.py          # dry-run
  python3 scripts/generate_sample_nurture.py --apply  # 正式写入
"""
import json, sys, requests
from pathlib import Path

sys.path.insert(0, '/Users/likeu3/.openclaw/workspace')
sys.path.insert(0, '/Users/likeu3/.openclaw/workspace/skills/creator-profile-card')
from workspace_support import load_repo_env
load_repo_env()

from app.services.message_generator import generate_message

cfg = json.loads((Path.home() / '.openclaw' / 'openclaw.json').read_text())
f = cfg['channels']['feishu']
r = requests.post('https://open.feishu.cn/open-apis/auth/v3/tenant_access_token/internal',
    json={'app_id': f['appId'], 'app_secret': f['appSecret']}, timeout=15)
TOKEN = r.json()['tenant_access_token']
H = {'Authorization': f'Bearer {TOKEN}', 'Content-Type': 'application/json'}
APP_TOKEN = 'KotZbL8ydalWQcsdgKYcOIl3nVf'
TABLE_ID = 'tbluyKELrrCc5qPT'
IS_DRY_RUN = '--apply' not in sys.argv

r = requests.get(
    f'https://open.feishu.cn/open-apis/bitable/v1/apps/{APP_TOKEN}/tables/{TABLE_ID}/records',
    headers=H, params={'page_size': 100})
records = r.json()['data']['items']
print(f"共 {len(records)} 条")
if IS_DRY_RUN:
    print("🔍 DRY-RUN (--apply 写入)\n")

count = 0
for rec in records:
    flds = rec['fields']
    rid = rec['record_id']

    # 触发条件
    status = flds.get('样品申请状态', '') or ''
    product = flds.get('申请样品商品', '') or ''
    action = flds.get('本周建议动作', '') or ''
    content_mode = flds.get('达人擅长内容形式', '短视频') or '短视频'
    activity = flds.get('活跃度', '') or ''

    if status not in ('待审核', '拟通过'):
        continue
    if not product:
        continue
    if action != '样品批前沟通':
        continue
    if activity == '停更':
        continue

    url = flds.get('达人链接', {})
    creator_url = isinstance(url, dict) and url.get('link', '') or str(url)
    handle = creator_url.split('@')[1].split('?')[0].split('/')[0] if '@' in creator_url else '?'

    profile = {'writable_fields': {
        '内容类型': flds.get('内容类型', ''),
        '画面风格': flds.get('画面风格', ''),
        '适配类目': flds.get('适配类目', []),
        '活跃度': activity,
    }}

    product_info = {
        'applied_sample_product': product,
        'sample_application_status': status,
        'creator_content_mode': content_mode,
        'product_name': product,
        'product_category': flds.get('适配类目', ['轻上装'])[0] if flds.get('适配类目') else '轻上装',
    }

    print(f"  @{handle:18s} [{content_mode}] 生成中...", end=' ', flush=True)
    try:
        result = generate_message(
            creator_url=creator_url, market='TH', target_language='泰语',
            history_relation=flds.get('历史关系', '陌生') or '陌生',
            profile_card=profile, product_info=product_info,
            message_purpose='sample_pre_approval_nurture',
            relationship_context={
                'creator_tier': flds.get('达人层级', ''),
                'days_since_last_contact': '未知',
            },
        )
        draft = result.get('message_cn_for_operator', '')
        score = result.get('quality_score', 0)
        if draft and not IS_DRY_RUN:
            r2 = requests.put(
                f'https://open.feishu.cn/open-apis/bitable/v1/apps/{APP_TOKEN}/tables/{TABLE_ID}/records/{rid}',
                headers=H, json={'fields': {'本次话术草稿': draft}}, timeout=15)
            code = r2.json().get('code', -1)
            print(f'✅ score={score} ({len(draft)}字) code={code}')
        elif draft:
            print(f'(dry-run) score={score} ({len(draft)}字)')
        else:
            err = result.get('error', '生成失败')
            print(f'❌ {err}')
        count += 1
    except Exception as e:
        print(f'❌ {e}')

print(f'\n处理: {count} 条')
if IS_DRY_RUN:
    print('⚠️  加 --apply 正式写入')
