#!/usr/bin/env python3
"""
发送/回复后批量回写脚本 — 统一刷新冷却期、关系阶段、未回复计数。

运营在飞书表更新了「处理状态」「最新回复状态」「发送结果」后，
跑这个脚本一键刷新所有关联字段。

检测规则：
  1. 已发送未回写：处理状态=已发送 且 上次联系时间为空 → 回写冷却期
  2. 已回复未回写：最新回复状态有值 且 上次回复时间为空 → 回写关系流转
  3. 超时未回复递增：处理状态=已发送 且 已过7天 且 无回复 → 递增连续未回复次数

用法：
  python3 scripts/post_send_writeback.py          # dry-run
  python3 scripts/post_send_writeback.py --apply  # 正式写入
"""
import json, os, sys, requests
from datetime import datetime, timezone, timedelta
from pathlib import Path

sys.path.insert(0, '/Users/likeu3/.openclaw/workspace')
sys.path.insert(0, '/Users/likeu3/.openclaw/workspace/skills/creator-profile-card')
from workspace_support import load_repo_env
load_repo_env()

from app.services.weekly_decision import (
    update_after_send, update_after_reply,
    COOLDOWN_DAYS, days_since,
)

# ── 飞书 ──
cfg = json.loads(open(Path.home() / '.openclaw' / 'openclaw.json').read())
feishu = cfg['channels']['feishu']
r = requests.post('https://open.feishu.cn/open-apis/auth/v3/tenant_access_token/internal',
    json={'app_id': feishu['appId'], 'app_secret': feishu['appSecret']}, timeout=15)
TOKEN = r.json()['tenant_access_token']
H = {'Authorization': f'Bearer {TOKEN}', 'Content-Type': 'application/json'}
APP_TOKEN = 'KotZbL8ydalWQcsdgKYcOIl3nVf'
TABLE_ID = 'tbluyKELrrCc5qPT'

IS_DRY_RUN = '--apply' not in sys.argv

# ── 读取全表 ──
r = requests.get(
    f'https://open.feishu.cn/open-apis/bitable/v1/apps/{APP_TOKEN}/tables/{TABLE_ID}/records',
    headers=H, params={'page_size': 100})
records = r.json().get('data', {}).get('items', [])
print(f"总记录: {len(records)} 条")
if IS_DRY_RUN:
    print("🔍 DRY-RUN (--apply 正式写入)\n")

send_writes = []      # 场景1: 已发送回写
reply_writes = []     # 场景2: 已回复回写
stale_writes = []     # 场景3: 超时未回复递增
skipped = []

for rec in records:
    flds = rec['fields']
    rid = rec['record_id']
    status = flds.get('处理状态', '') or ''
    action = flds.get('本周建议动作', '') or ''
    tier = flds.get('达人层级', '') or ''
    last_contact = flds.get('上次联系时间')
    reply_status = flds.get('最新回复状态', '') or ''
    last_reply = flds.get('上次回复时间')
    consecutive = int(flds.get('连续未回复次数', 0) or 0)
    stage = flds.get('关系阶段', '') or ''

    info = lambda: (flds.get('达人链接', {}).get('link', '?') or '?').split('@')[1].split('?')[0].split('/')[0] if '@' in str(flds.get('达人链接', '')) else '?'

    # ── 场景1: 已发送但未回写冷却期 ──
    if status == '已发送' and not last_contact:
        if action in ('商品邀约', '关系维护', '轻跟进'):
            fields = update_after_send(action, tier)
            send_writes.append((rid, flds, action, tier, fields))
        else:
            skipped.append((rid, flds, f'已发送但本周动作={action}，无法确定类型'))

    # ── 场景2: 有回复状态但未回写关系流转 ──
    elif reply_status and not last_reply:
        fields = update_after_reply(reply_status)
        # 未回复/已读未回 → 递增连续未回复次数
        if reply_status in ('未回复', '已读未回'):
            fields['连续未回复次数'] = consecutive + 1
            if consecutive + 1 >= 4:
                fields['关系阶段'] = '放弃'
                fields['本周建议动作'] = '放弃'
                fields['本周建议原因'] = f'连续未回复 {consecutive + 1} 次，自动放弃'
        # 无效回复也递增
        elif reply_status == '无效回复':
            fields['连续未回复次数'] = consecutive + 1
        reply_writes.append((rid, flds, reply_status, fields))

    # ── 场景3: 已发送超7天无回复 → 递增未回复次数 ──
    elif status == '已发送' and last_contact and not reply_status:
        days = days_since(str(last_contact))
        if days is not None and days >= 7:
            new_count = consecutive + 1
            fields = {
                '连续未回复次数': new_count,
                '关系阶段': '冷却',
            }
            if new_count >= 4:
                fields['关系阶段'] = '放弃'
                fields['本周建议动作'] = '放弃'
                fields['本周建议原因'] = f'连续未回复 {new_count} 次，自动放弃'
            stale_writes.append((rid, flds, days, consecutive, fields))

    # ── 已回写的跳过 ──
    elif status == '已发送' and last_contact:
        skipped.append((rid, flds, '发送已回写（冷却期已设置）'))
    elif reply_status and last_reply:
        skipped.append((rid, flds, '回复已回写（上次回复时间已存在）'))
    else:
        skipped.append((rid, flds, '无需处理'))

# ── 输出 ──
def _h(flds):
    try:
        u = flds.get('达人链接', {})
        link = u.get('link', '?') if isinstance(u, dict) else str(u)
        return link.split('@')[1].split('?')[0].split('/')[0] if '@' in link else '?'
    except:
        return '?'

if send_writes:
    print(f"📤 已发送回写 ({len(send_writes)} 条):")
    for rid, flds, action, tier, fields in send_writes:
        cd = fields.get('下次可联系时间', '')
        cd_str = 'N/A'
        if cd:
            try:
                cd_str = datetime.fromtimestamp(cd / 1000).strftime('%m/%d')
            except:
                pass
        print(f"  @{_h(flds):18s} [{action}] → 关系=冷却, 冷却至={cd_str}")
        if not IS_DRY_RUN:
            r = requests.put(
                f'https://open.feishu.cn/open-apis/bitable/v1/apps/{APP_TOKEN}/tables/{TABLE_ID}/records/{rid}',
                headers=H, json={'fields': fields}, timeout=15)
            print(f"    ↳ code={r.json().get('code', -1)}")
    print()

if reply_writes:
    print(f"💬 已回复回写 ({len(reply_writes)} 条):")
    for rid, flds, reply_status, fields in reply_writes:
        print(f"  @{_h(flds):18s} [{reply_status}] → 关系={fields.get('关系阶段','?')}, 未回复次数={fields.get('连续未回复次数','?')}")
        if not IS_DRY_RUN:
            r = requests.put(
                f'https://open.feishu.cn/open-apis/bitable/v1/apps/{APP_TOKEN}/tables/{TABLE_ID}/records/{rid}',
                headers=H, json={'fields': fields}, timeout=15)
            print(f"    ↳ code={r.json().get('code', -1)}")
    print()

if stale_writes:
    print(f"⏰ 超时未回复递增 ({len(stale_writes)} 条):")
    for rid, flds, days, old_count, fields in stale_writes:
        new_count = fields['连续未回复次数']
        print(f"  @{_h(flds):18s} 已过{days}天无回复 → 未回复: {old_count}→{new_count}")
        if not IS_DRY_RUN:
            r = requests.put(
                f'https://open.feishu.cn/open-apis/bitable/v1/apps/{APP_TOKEN}/tables/{TABLE_ID}/records/{rid}',
                headers=H, json={'fields': fields}, timeout=15)
            print(f"    ↳ code={r.json().get('code', -1)}")
    print()

print(f"⏭️  跳过: {len(skipped)} 条")

if IS_DRY_RUN:
    print(f"\n⚠️  以上为预览，加 --apply 正式写入")
print(f"\n总计: 发送回写={len(send_writes)} | 回复回写={len(reply_writes)} | 超时递增={len(stale_writes)} | 跳过={len(skipped)}")
