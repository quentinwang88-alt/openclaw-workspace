"""Scoped operator recovery of the nine verified pre-upload failures."""
import json
from pathlib import Path
import sqlite3
import sys
from datetime import datetime

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from run_pipeline import exclusive_run_lock
from app.scheduler import _verify_live_opv_release

SUFFIXES = ('fc5e1a8c1b65', '5f912aa1fc1a', '82bbd1311dc2', '349acbb4d840',
            '7cd80645ee20', '0c4366386bc7', 'eeca4df44001', 'cd726ee2a4c0', 'd4d40dc2a22a')
SLOTS = (123191, 123379, 124595, 123755, 123735, 124596, 124203, 124327, 124597)
DB = Path('/Users/likeu3/.openclaw/shared/data/short_video_auto_publish.sqlite3')

def main():
    apply = '--apply' in sys.argv
    with exclusive_run_lock('schedule'):
        conn = sqlite3.connect(f'{DB.as_uri()}?mode=ro', uri=True)
        conn.row_factory = sqlite3.Row
        audit = {'assets': [], 'retries': [], 'slots': []}
        for suffix in SUFFIXES:
            task = 'opv_task_20260904_' + suffix
            key = 'opv:' + task
            asset = conn.execute('SELECT * FROM video_assets WHERE canonical_script_key=?', (key,)).fetchone()
            retry = conn.execute('SELECT * FROM publish_candidate_retries WHERE canonical_script_key=?', (key,)).fetchone()
            assert asset and not asset['publish_task_id'] and asset['publish_status'] == '待排期', key
            assert retry and retry['blocked'] and 'release 核验失败' in retry['last_error'], key
            assert not conn.execute("SELECT 1 FROM publish_slots WHERE canonical_script_key=? AND schedule_status IN ('提交中','提交结果不明','已排期','已发布')", (key,)).fetchone(), key
            metadata = conn.execute('SELECT script_text FROM script_metadata WHERE canonical_script_key=?', (key,)).fetchone()
            _verify_live_opv_release(task, json.loads(metadata[0])['release_manifest']['manifest_sha256'])
            audit['assets'].append(dict(asset)); audit['retries'].append(dict(retry))
            print(task + ' release_verified', flush=True)
        for slot_id in SLOTS:
            row = conn.execute('SELECT * FROM publish_slots WHERE slot_id=?', (slot_id,)).fetchone()
            assert row and row['schedule_status'] == '已取消' and not row['publish_task_id'] and not row['canonical_script_key']
            assert 'release 核验失败' in row['error_message']
            audit['slots'].append(dict(row))
        conn.close()
        if not apply:
            print('verified_only; recoverable_assets=9; recoverable_slots=9'); return
        output = ROOT / 'output' / ('opv-recovery-' + datetime.now().strftime('%Y%m%d-%H%M%S') + '.json')
        output.write_text(json.dumps(audit, ensure_ascii=False, indent=2), encoding='utf-8')
        with sqlite3.connect(DB) as conn:
            conn.execute('BEGIN IMMEDIATE')
            for asset in audit['assets']:
                key = asset['canonical_script_key']
                assert conn.execute("UPDATE video_assets SET error_message=NULL,updated_at=? WHERE canonical_script_key=? AND publish_status='待排期' AND COALESCE(publish_task_id,'')=''", (datetime.now().strftime('%Y-%m-%d %H:%M:%S'),key)).rowcount == 1
                conn.execute('DELETE FROM publish_candidate_retries WHERE canonical_script_key=? AND blocked=1', (key,))
            for row in audit['slots']:
                assert conn.execute("UPDATE publish_slots SET schedule_status='待排期',error_message='人工恢复：上传前核验已通过',updated_at=? WHERE slot_id=? AND schedule_status='已取消' AND COALESCE(publish_task_id,'')='' AND COALESCE(canonical_script_key,'')=''", (datetime.now().strftime('%Y-%m-%d %H:%M:%S'),row['slot_id'])).rowcount == 1
        print(json.dumps({'recovered_assets':9,'recovered_slots':9,'audit_path':str(output)},ensure_ascii=False))

if __name__ == '__main__':
    main()
