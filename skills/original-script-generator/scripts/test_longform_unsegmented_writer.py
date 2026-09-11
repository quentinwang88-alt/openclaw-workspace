#!/usr/bin/env python3
"""Text-only ablation of first-pass segment planning; no production writes."""
import argparse
import copy
import hashlib
import importlib.util
import json
import os
from pathlib import Path
import random
import sys
import time

ROOT = Path(__file__).resolve().parents[1]
ENGINE = Path('/Users/likeu3/voiceover_copy_engine')
sys.path[:0] = [str(ROOT), str(ENGINE)]
from core.longform.voiceover import _estimated_spoken_seconds


def save(path, value):
    path.write_text(json.dumps(value, ensure_ascii=False, indent=2), encoding='utf-8')


def make_unsegmented(writer, system):
    output = copy.deepcopy(writer)
    segments = output['delivery'].pop('segments')
    output['delivery']['scene_backgrounds'] = list(dict.fromkeys(
        x['scene_location'] for x in segments if x.get('scene_location')))
    start = system.index('6. Fit delivery.total_seconds')
    end = system.index('\n7.', start)
    system = system[:start] + (
        '6. Fit delivery.total_seconds at a normal creator speaking rate. '
        'Do not add a redundant summary merely to occupy time. '
        'Scene locations are optional background context, not evidence or required narration.'
    ) + system[end:]
    system = system.replace(', semantic_sections, recommended_bridge_pause_ms.', '.')
    start = system.index('semantic_sections must contain exactly')
    end = system.index('Use only claim_key', start)
    system = system[:start] + system[end:]
    assert 'semantic_sections' not in system
    assert 'segments' not in output['delivery']
    return output, system


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument('--source-dir', type=Path, required=True)
    p.add_argument('--output-dir', type=Path, required=True)
    p.add_argument('--prepare-only', action='store_true')
    p.add_argument('--model-comparison', action='store_true',
                   help='Compare Sol/high and Astra/high with identical segmented input')
    p.add_argument('--codex-bin', type=Path, help='Use an already installed CLI for this isolated test')
    args = p.parse_args()
    out = args.output_dir
    out.mkdir(parents=True, exist_ok=True)
    module_path = ENGINE / 'scripts/codex_model_command.py'
    spec = importlib.util.spec_from_file_location('central_model', module_path)
    model = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(model)
    if args.codex_bin:
        if not args.codex_bin.is_absolute() or not args.codex_bin.is_file():
            raise ValueError('codex-bin must be an existing absolute file path')
        model.CODEX_BIN = str(args.codex_bin)
    code_hash = hashlib.sha256(module_path.read_bytes()).hexdigest()
    original = json.loads((args.source_dir / 'writer_inputs.json').read_text())['full_case']['input']
    system = model.SYSTEM_PROMPTS['creative_longform_single_v1']
    free_input, free_system = make_unsegmented(original, system)
    # Every content field remains byte-for-byte equivalent; only delivery changes.
    a, b = copy.deepcopy(original), copy.deepcopy(free_input)
    a.pop('delivery'); b.pop('delivery')
    assert a == b
    assert original['delivery']['total_seconds'] == free_input['delivery']['total_seconds']
    frozen = dict(source_dir=str(args.source_dir), command_hash=code_hash,
                  arms={'segmented': dict(input=original, system=system),
                        'unsegmented': dict(input=free_input, system=free_system)})
    if args.model_comparison:
        frozen['arms'] = {arm: dict(input=original, system=system, model=name)
                          for arm, name in [('sol', 'gpt-5.6-sol'), ('astra', 'gpt-6-astra')]}
        frozen['reasoning_effort'] = 'high'
        frozen['cross_model_fallback'] = False
        model.FALLBACK_MODEL = ''  # Isolated imported module; production file unchanged.
    frozen['codex_bin'] = model.CODEX_BIN
    frozen['codex_binary_hash'] = hashlib.sha256(Path(model.CODEX_BIN).read_bytes()).hexdigest()
    snapshot = out / 'experiment_inputs.json'
    if snapshot.exists() and json.loads(snapshot.read_text()) != frozen:
        raise ValueError('Input/version changed; use a fresh output directory')
    save(snapshot, frozen)
    print('PREPARED: ' + ('only model differs' if args.model_comparison
                         else 'only first-pass delivery segmentation differs'), flush=True)
    if args.prepare_only:
        return
    results = []
    trials = ([('sol', 1), ('astra', 1), ('astra', 2), ('sol', 2)] if args.model_comparison
              else [('segmented', 1), ('unsegmented', 1), ('unsegmented', 2), ('segmented', 2)])
    for arm, replica in trials:
        path = out / f'{arm}_{replica}.json'
        if path.exists():
            results.append(json.loads(path.read_text()))
            continue
        entry = frozen['arms'][arm]
        if args.model_comparison:
            model.MODEL = entry['model']
        prompt = entry['system'] + '\n\nInput data:\n' + json.dumps(entry['input'], ensure_ascii=False, indent=2)
        prompt += '\n\nGenerate the response. Return ONLY valid JSON, no other text.'
        print(f'START {arm}_{replica}', flush=True)
        start = time.monotonic()
        response, actual_model, attempts = model._run_creative_model_with_fallback(
            prompt=prompt, additional_args=['-c', f'model_reasoning_effort={model.REASONING}'],
            env=os.environ.copy(), cwd=str(ENGINE))
        if args.model_comparison and actual_model != entry['model']:
            raise ValueError('Unexpected model substitution')
        if response.returncode:
            save(out / f'{arm}_{replica}_failure.json', dict(returncode=response.returncode,
                 error=(response.stderr or response.stdout)[-1500:]))
            raise RuntimeError('Model failed; completed trials are checkpointed')
        result = json.loads(model._extract_json(response.stdout.strip()))
        if not result.get('target_text') or not result.get('chinese_translation'):
            save(out / f'{arm}_{replica}_invalid.json', result)
            raise ValueError('Missing text fields')
        item = dict(arm=arm, replica=replica, result=result,
                    actual_model=actual_model, attempts=attempts,
                    elapsed_seconds=round(time.monotonic()-start, 2),
                    estimated_seconds=_estimated_spoken_seconds(result['target_text'], '泰语'))
        save(path, item)
        results.append(item)
        print(f'DONE {arm}_{replica}: {item["estimated_seconds"]}s estimated', flush=True)
    random.Random(9035).shuffle(results)
    title = '模型对照实验' if args.model_comparison else '分段写作隔离实验'
    key = {}; review = [f'# {title}｜完整原文与中文', '', '时长为字符估算，未做TTS/母语人工审核。', '']
    for i, row in enumerate(results, 1):
        name = f'T{i:02d}'
        key[name] = dict(arm=row['arm'], replica=row['replica'])
        review += [f'## {name}', '', row['result']['target_text'], '',
                   row['result']['chinese_translation'], '', f'估时：{row["estimated_seconds"]}秒', '']
    save(out / 'result.json', dict(status='TEXT_READY_FOR_REVIEW', results=results))
    save(out / 'answer_key.json', key)
    (out / 'blind_review.md').write_text('\n'.join(review), encoding='utf-8')
    print('COMPLETE ' + str(out), flush=True)


if __name__ == '__main__':
    main()
