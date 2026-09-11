#!/usr/bin/env python3
"""Isolated 2x2 text comparison; never updates production jobs or Feishu."""
from __future__ import annotations

import argparse
import copy
import hashlib
import json
import os
from pathlib import Path
import random
import shlex
import sqlite3
import subprocess
import sys
import time

ROOT = Path(__file__).resolve().parents[1]
ENGINE = Path('/Users/likeu3/voiceover_copy_engine')
sys.path[:0] = [str(ROOT), str(ENGINE)]
from core.longform.storage import DEFAULT_DB_PATH
from core.longform.voiceover import (build_longform_voiceover_payload,
    DEFAULT_MODEL_COMMAND, _estimated_spoken_seconds, _section_duration_fit)
from core.longform.voiceover_resources import resolve_longform_voiceover_resources
from voiceover_copy_engine.services.spoken_writer import compile_spoken_writer_input
from voiceover_copy_engine.services.writing_cases import usable_writing_case


def save(path, value):
    path.write_text(json.dumps(value, ensure_ascii=False, indent=2, default=str), encoding='utf-8')


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument('--job-id', required=True)
    p.add_argument('--output-dir', type=Path, required=True)
    p.add_argument('--model-command', default=DEFAULT_MODEL_COMMAND)
    p.add_argument('--prepare-only', action='store_true')
    a = p.parse_args()
    out = a.output_dir
    out.mkdir(parents=True, exist_ok=True)
    snapshot = out / 'input_snapshot.json'
    command_path = ENGINE / 'scripts/codex_model_command.py'
    code_hash = hashlib.sha256(command_path.read_bytes()).hexdigest()
    if snapshot.exists():
        frozen = json.loads(snapshot.read_text())
        if (frozen['job_id'] != a.job_id or frozen['model_command'] != a.model_command
                or frozen['command_hash'] != code_hash):
            raise ValueError('Snapshot differs; use a new output directory')
    else:
        with sqlite3.connect(f'file:{DEFAULT_DB_PATH}?mode=ro', uri=True) as conn:
            conn.row_factory = sqlite3.Row
            row = conn.execute('SELECT * FROM longform_job WHERE job_id=?', (a.job_id,)).fetchone()
            if row is None:
                raise ValueError('Job not found')
            master = json.loads(row['master_contract_json'])
            plan = json.loads(row['plan_json'])
        os.environ['LONGFORM_WRITING_CASE_ENABLED'] = '1'
        resources = resolve_longform_voiceover_resources(master)
        case = resources.get('writing_case_reference') or {}
        if not usable_writing_case(case):
            raise ValueError('No usable RDS writing reference: ' + str(resources.get('writing_case_audit')))
        payload = build_longform_voiceover_payload(master, plan)
        for key in ('hook_guidance', 'relationship_language', 'approved_style_references',
                    'native_rhetoric_contract', 'writing_case_reference'):
            payload[key] = resources[key]
        control = copy.deepcopy(payload)
        control.pop('writing_case_reference', None)
        frozen = dict(job_id=a.job_id, master=master, plan=plan, resources=resources,
                      payloads={'control': control, 'full_case': payload},
                      model_command=a.model_command, command_hash=code_hash)
        save(snapshot, frozen)
    projections = {}
    for arm, payload in frozen['payloads'].items():
        projected, audit = compile_spoken_writer_input('creative_longform_single_v1', payload)
        projections[arm] = dict(input=projected, audit=audit)
    left = copy.deepcopy(projections['control']['input'])
    right = copy.deepcopy(projections['full_case']['input'])
    left.pop('expression_references', None)
    right.pop('expression_references', None)
    if left != right:
        raise ValueError('Unexpected non-reference difference')
    save(out / 'writer_inputs.json', projections)
    print('PREPARED: expression references are the only writer-input difference', flush=True)
    if a.prepare_only:
        return
    outputs = []
    for arm, replica in [('control', 1), ('full_case', 1), ('control', 2), ('full_case', 2)]:
        dest = out / f'{arm}_{replica}.json'
        if dest.exists():
            outputs.append(json.loads(dest.read_text()))
            continue
        payload = frozen['payloads'][arm]
        request = dict(contract_name='creative_longform_single_v1', payload=payload)
        print(f'START {arm}_{replica}', flush=True)
        start = time.monotonic()
        completed = subprocess.run(shlex.split(a.model_command),
            input=json.dumps(request, ensure_ascii=False), text=True,
            capture_output=True, timeout=600)
        if completed.returncode:
            save(out / f'{arm}_{replica}_failure.json', dict(returncode=completed.returncode,
                 error=(completed.stderr or completed.stdout)[-1500:]))
            raise RuntimeError(f'{arm}_{replica} failed; checkpoint saved')
        result = json.loads(completed.stdout)
        if not all(result.get(k) for k in ('target_text', 'chinese_translation', 'semantic_sections')):
            save(out / f'{arm}_{replica}_invalid.json', result)
            raise ValueError('Missing required voiceover fields')
        item = dict(arm=arm, replica=replica, result=result,
                    elapsed_seconds=round(time.monotonic()-start, 2),
                    estimated_seconds=_estimated_spoken_seconds(result['target_text'], payload['target_language']),
                    section_fit=_section_duration_fit(result, payload))
        save(dest, item)
        outputs.append(item)
        print(f'DONE {arm}_{replica}: {item["estimated_seconds"]}s estimated', flush=True)
    random.Random(2909).shuffle(outputs)
    review = ['# 35秒口播参考对照｜待人工审阅', '',
              '仅文本估时，未做TTS或母语人工审核。比较开头、具体性、分享感与冗余。', '']
    key = {}
    for i, row in enumerate(outputs, 1):
        name = f'T{i:02d}'
        key[name] = dict(arm=row['arm'], replica=row['replica'])
        r = row['result']
        review += [f'## {name}', '', r['target_text'], '', r['chinese_translation'], '',
                   f'字符估时：{row["estimated_seconds"]}秒（非实测）', '']
    (out / 'blind_review.md').write_text('\n'.join(review), encoding='utf-8')
    save(out / 'answer_key.json', key)
    save(out / 'result.json', dict(status='TEXT_READY_FOR_REVIEW', results=outputs))
    print(f'COMPLETE {out}', flush=True)


if __name__ == '__main__':
    main()
