#!/usr/bin/env python3
import subprocess, sys, time
from pathlib import Path

skill = Path('/Users/likeu3/.openclaw/workspace/skills/tk-title-rewriter')
sys.path.insert(0, str(skill))
from core.feishu import parse_feishu_bitable_url, resolve_bitable_app_token, FeishuBitableClient
from run_pipeline import normalize_cell_value

url = 'https://gcngopvfvo0q.feishu.cn/wiki/BTKYwAflViFj6ikg9D9cOOzin7b?table=tblXfldWIjH1ZPo0&view=vewQvEaBtD'
info = parse_feishu_bitable_url(url)
app = resolve_bitable_app_token(info)
client = FeishuBitableClient(app, info.table_id)
records = client.list_records(limit=None)
pending = []
for r in records:
    title = normalize_cell_value(r.fields.get('产品标题'))
    out = normalize_cell_value(r.fields.get('TK标题'))
    cat = normalize_cell_value(r.fields.get('产品类目'))
    if title and not out and cat == '轻上装':
        pending.append(r.record_id)

print(f'pending={len(pending)}', flush=True)
chunk_size = 15
ok = 0
for i in range(0, len(pending), chunk_size):
    chunk = pending[i:i+chunk_size]
    cmd = [
        'python3', str(skill / 'run_pipeline.py'),
        '--feishu-url', url,
        '--title-field', '产品标题',
        '--cn-summary-field', '标题翻译（中文）',
        '--llm-batch-size', '5',
        '--timeout-seconds', '180',
    ]
    for rid in chunk:
        cmd += ['--record-id', rid]
    print(f'\n=== chunk {i//chunk_size+1}/{(len(pending)+chunk_size-1)//chunk_size}: {len(chunk)} records ===', flush=True)
    start = time.time()
    p = subprocess.run(cmd, cwd='/Users/likeu3/.openclaw/workspace', text=True, capture_output=True, timeout=900)
    print(p.stdout, flush=True)
    if p.stderr:
        print(p.stderr, file=sys.stderr, flush=True)
    if p.returncode != 0:
        print(f'chunk failed returncode={p.returncode}', file=sys.stderr, flush=True)
        sys.exit(p.returncode)
    ok += len(chunk)
    print(f'chunk done in {time.time()-start:.1f}s, cumulative={ok}', flush=True)
print('all chunks done', flush=True)
