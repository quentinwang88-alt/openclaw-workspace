"""Isolated visual-writer A/B. Read frozen inputs; never mutate production rows."""
import argparse
import copy
import hashlib
import importlib.util
import json
import os
from pathlib import Path
import random
import sqlite3
import sys
import time
from types import SimpleNamespace

ROOT = Path(__file__).resolve().parents[1]
ENGINE = Path('/Users/likeu3/voiceover_copy_engine')
sys.path.insert(0, str(ROOT))
from core.simplified_complete_script import (
    build_simplified_script_prompt, normalize_simplified_visual_script,
    validate_simplified_visual_script, assemble_simplified_complete_script,
)
from core.production_script_renderer import render_video_generation_prompt


def render_handoff(script):
    return render_video_generation_prompt(item=SimpleNamespace(
        result_json=json.dumps({'script': script}, ensure_ascii=False)), duration_seconds=15)


def digest(value):
    return hashlib.sha256(json.dumps(value, ensure_ascii=False, sort_keys=True).encode()).hexdigest()


def save(path, value):
    path.write_text(json.dumps(value, ensure_ascii=False, indent=2), encoding='utf-8')


def freeze(script_id):
    db = 'file:/Users/likeu3/.openclaw/shared/data/original_script_generator.sqlite3?mode=ro'
    with sqlite3.connect(db, uri=True) as conn:
        conn.row_factory = sqlite3.Row
        row = conn.execute('''SELECT i.*, b.target_country, b.target_language
            FROM original_content_item i JOIN original_content_batch b ON b.batch_id=i.batch_id
            WHERE i.script_id=? AND i.status='SCRIPT_READY' ''', (script_id,)).fetchone()
    if row is None:
        raise ValueError('No completed frozen source: ' + script_id)
    package = json.loads(row['frozen_direction_package_json'])
    seed = package.get('simplified_creative_seed')
    voice = json.loads(row['stage_checkpoint_json'])['stages']['voiceover']['plan']
    if not seed or not voice.get('lines'):
        raise ValueError('Missing frozen seed or voiceover; no replanning permitted')
    prompt = build_simplified_script_prompt(copy.deepcopy(seed),
        target_country=row['target_country'], target_language=row['target_language'], duration_seconds=15)
    prompt += '\n\n仅输出所要求的JSON；不调用工具，不读取或写入文件。'
    return dict(product_code=row['product_code'], source_script_id=script_id,
        seed=seed, frozen_voiceover=voice, prompt=prompt, prompt_sha256=digest(prompt))


def report(out, snapshots, rows):
    for row in rows:
        row['video_prompt'] = render_handoff(row['assembled'])
        save(out / f'{row["product_code"]}_{row["arm"]}.json', row)
    shuffled = list(rows)
    random.Random(90915).shuffle(shuffled)
    lines = ['# 完整视觉脚本模型对照｜乱序审阅', '',
        '15秒视觉层实验；每产品的商品、卖点、结构、人物、穿搭、场景和旧口播冻结。',
        '未生成图片/视频，未写飞书。口播只原样挂载供参照，不评价本轮口播改善。', '']
    key = {}
    for i, row in enumerate(shuffled, 1):
        label = f'T{i:02d}'
        key[label] = dict(product_code=row['product_code'], arm=row['arm'])
        script = row['assembled']
        production = script.get('production_design') or {}
        lines += [f'## {label}｜产品 {row["product_code"]}', '']
        for field in ('character', 'outfit', 'scene', 'emotion', 'life_event'):
            lines += [f'### {field}', '']
            for k, v in (production.get(field) or {}).items():
                if v:
                    lines.append(f'- {k}：{v}')
            lines.append('')
        lines += ['### 实际分镜', '', '| 时段 | 片段 | 画面 | 动作 | 机位 |', '|---|---|---|---|---|']
        for shot in script.get('storyboard') or []:
            values = [str(shot.get(k, '')).replace('|', '／').replace('\n', ' ')
                      for k in ('time_range', 'capture_unit_id', 'visual_content', 'character_action', 'camera')]
            lines.append('| ' + ' | '.join(values) + ' |')
        voice = script.get('continuous_voiceover') or {}
        lines += ['', '### 冻结旧口播（未重写）', '', voice.get('chinese_translation', ''), '',
            '### 最终视频提示词（现有渲染器）', '', row['video_prompt'], '']
    (out / 'blind_review.md').write_text('\n'.join(lines), encoding='utf-8')
    save(out / 'answer_key.json', key)
    save(out / 'result.json', dict(status='VISUAL_TEXT_READY_FOR_REVIEW' if len(rows)==4 else 'PARTIAL',
        results=rows, production_defaults_changed=False, media_generated=False, feishu_written=False))


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument('--source-script-id', action='append', required=True)
    p.add_argument('--output-dir', type=Path, required=True)
    p.add_argument('--prepare-only', action='store_true')
    p.add_argument('--report-only', action='store_true', help='Re-render existing results without model calls')
    args = p.parse_args()
    if len(args.source_script_id) != 2:
        raise ValueError('This experiment is bounded to two products, two models each')
    out = args.output_dir.resolve()
    out.mkdir(parents=True, exist_ok=True)
    code_paths = [ROOT / 'core/simplified_complete_script.py', ROOT / 'core/script_renderer.py',
                  ENGINE / 'scripts/codex_model_command.py']
    versions = {str(path): hashlib.sha256(path.read_bytes()).hexdigest() for path in code_paths}
    cli = '/Applications/ChatGPT.app/Contents/Resources/codex'
    versions['codex_binary_hash'] = hashlib.sha256(Path(cli).read_bytes()).hexdigest()
    snap_path = out / 'input_snapshot.json'
    if snap_path.exists():
        frozen = json.loads(snap_path.read_text())
        if frozen['source_script_ids'] != args.source_script_id or frozen['code_versions'] != versions:
            raise ValueError('Source/code changed: use a new output directory')
    else:
        frozen = dict(source_script_ids=args.source_script_id, code_versions=versions,
            inputs=[freeze(s) for s in args.source_script_id], reasoning_effort='high',
            codex_bin=cli, models={'sol':'gpt-5.6-sol', 'astra':'gpt-6-astra'},
            cross_model_fallback=False, scope='15S_VISUAL_WRITER_ONLY')
        save(snap_path, frozen)
    for snap in frozen['inputs']:
        print('PREPARED ' + snap['product_code'] + ' prompt_chars=' + str(len(snap['prompt'])), flush=True)
    if args.prepare_only:
        return
    if args.report_only:
        rows = [json.loads(path.read_text()) for snap in frozen['inputs']
                for arm in ('sol', 'astra')
                if (path := out / f'{snap["product_code"]}_{arm}.json').exists()]
        report(out, frozen['inputs'], rows)
        return
    spec = importlib.util.spec_from_file_location('isolated_model_transport', code_paths[-1])
    model = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(model)
    model.CODEX_BIN = cli
    model.FALLBACK_MODEL = ''
    model.CREATIVE_ROUTE_ATTEMPT_TIMEOUT_SEC = 300
    rows = []
    for index, snap in enumerate(frozen['inputs']):
        for arm in (('sol','astra') if index == 0 else ('astra','sol')):
            dest = out / f'{snap["product_code"]}_{arm}.json'
            if dest.exists():
                rows.append(json.loads(dest.read_text()))
                continue
            name = frozen['models'][arm]
            model.MODEL = name
            print('START ' + snap['product_code'] + ' ' + arm, flush=True)
            start = time.monotonic()
            response, actual, attempts = model._run_creative_model_with_fallback(
                prompt=snap['prompt'], additional_args=['-c','model_reasoning_effort=high',
                    '-c','sandbox_mode="read-only"','-c','approval_policy="never"'],
                env=os.environ.copy(), cwd=str(out))
            if response.returncode:
                save(out / f'{snap["product_code"]}_{arm}_error.json', dict(
                    returncode=response.returncode, error=(response.stderr or response.stdout)[-1500:]))
                raise RuntimeError('Model call failed; completed arms remain checkpointed')
            if actual != name:
                raise ValueError('Unexpected cross-model substitution')
            raw = json.loads(model._extract_json(response.stdout))
            # The normalizer uses shallow copies internally: keep raw evidence untouched.
            visual = normalize_simplified_visual_script(copy.deepcopy(raw), copy.deepcopy(snap['seed']),
                generation_provenance=dict(model=actual, reasoning_effort='high', experiment='visual-model-ab-v1'))
            validation = validate_simplified_visual_script(visual, snap['seed'])
            assembled = assemble_simplified_complete_script(copy.deepcopy(visual), copy.deepcopy(snap['seed']),
                copy.deepcopy(snap['frozen_voiceover']))
            prompt = render_handoff(assembled)
            row = dict(product_code=snap['product_code'], arm=arm, actual_model=actual,
                reasoning_effort='high', attempts=attempts, prompt_sha256=snap['prompt_sha256'],
                elapsed_seconds=round(time.monotonic()-start,2), raw=raw, normalized=visual,
                validation=validation, assembled=assembled, video_prompt=prompt,
                normalization_changed_storyboard=raw.get('storyboard') != visual.get('storyboard'))
            save(dest, row)
            rows.append(row)
            report(out, frozen['inputs'], rows)
            print('DONE ' + snap['product_code'] + ' ' + arm + ' ' + json.dumps(validation, ensure_ascii=False), flush=True)
    report(out, frozen['inputs'], rows)
    print('COMPLETE ' + str(out), flush=True)


if __name__ == '__main__':
    main()
