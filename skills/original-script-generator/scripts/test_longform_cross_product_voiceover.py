"""Read-only source reuse and isolated longform voiceover validation."""
import argparse
import json
import os
from pathlib import Path
import sqlite3
import sys
import time

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from core.longform.source_adapter import source_from_product_plan
from core.longform.planner import plan_segment_durations
from core.longform.voiceover import build_longform_voiceover_payload, _invoke_voiceover_model, DEFAULT_MODEL_COMMAND, _estimated_spoken_seconds, _section_duration_fit
from core.longform.voiceover_resources import resolve_longform_voiceover_resources


def save(path, data):
    path.write_text(json.dumps(data, ensure_ascii=False, indent=2, default=str), encoding='utf-8')


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument('--product-code', action='append', required=True)
    p.add_argument('--output-dir', type=Path, required=True)
    p.add_argument('--prepare-only', action='store_true')
    p.add_argument('--source-script-id', action='append', required=True,
                   help='One inspected source per product, in the same order')
    args = p.parse_args()
    out = args.output_dir
    out.mkdir(parents=True, exist_ok=True)
    os.environ['LONGFORM_WRITING_CASE_ENABLED'] = '1'
    records = []
    if len(args.source_script_id) != len(args.product_code):
        raise ValueError('Each product needs one inspected source')
    for product, script_id in zip(args.product_code, args.source_script_id):
        snap = out / f'{product}_input.json'
        if snap.exists():
            frozen = json.loads(snap.read_text())
        else:
            with sqlite3.connect('file:/Users/likeu3/.openclaw/shared/data/original_script_generator.sqlite3?mode=ro', uri=True) as conn:
                conn.row_factory = sqlite3.Row
                row = conn.execute('''SELECT i.script_id,i.updated_at,i.result_json,i.frozen_direction_package_json,b.top_category,b.product_type,
                    b.target_country,b.target_language FROM original_content_item i
                    JOIN original_content_batch b ON b.batch_id=i.batch_id
                    WHERE i.product_code=? AND i.script_id=? AND i.status='SCRIPT_READY'
                    ORDER BY i.updated_at DESC,i.script_id DESC LIMIT 1''', (product,script_id)).fetchone()
            if row is None:
                raise ValueError('No completed source for ' + product)
            package = json.loads(row['frozen_direction_package_json'])
            context = {k:row[k] for k in ('top_category','product_type','target_country','target_language')}
            master = source_from_product_plan(context, package, duration_seconds=35,
                                              product_code=product, source_plan_item_id=script_id)
            # Historical adapter can stringify an UNAVAILABLE context object.
            # Preserve its actual empty text, not the Python dictionary repr.
            market = (package.get('semantic_spine_contract') or {}).get('product_market_context') or {}
            usage = market.get('primary_usage_world')
            if isinstance(usage, dict) and master['semantic_spine'].get('primary_narrative_context') == str(usage):
                master['semantic_spine']['primary_narrative_context'] = usage.get('text') or ''
            for field in ('top_category','product_type','target_country','target_language'):
                master[field] = row[field]
            scene = master['production_world'].get('scene', '')
            plan = {'scope':'AUDIO_CAPACITY_ONLY_NOT_A_VISUAL_PLAN', 'segments':[
                dict(segment_id=chr(65+i), duration_seconds=d, scene_id='SOURCE_SCENE',
                     scene_block={'location':scene}) for i,d in enumerate(plan_segment_durations(35))]}
            payload = build_longform_voiceover_payload(master, plan)
            resources = resolve_longform_voiceover_resources(master)
            for key in ('hook_guidance','relationship_language','approved_style_references','native_rhetoric_contract','writing_case_reference'):
                payload[key] = resources[key]
            if not (payload.get('selling_argument') or {}).get('text'):
                raise ValueError('Missing selling argument')
            frozen = dict(product_code=product, source_script_id=row['script_id'], source_updated_at=row['updated_at'],
                          master=master, plan=plan, resources=resources, payload=payload)
            save(snap, frozen)
        print('PREPARED ' + product + ' ' + json.dumps({
            'source':frozen['source_script_id'],
            'context':frozen['payload']['primary_narrative_context'],
            'primary':frozen['payload']['selling_argument'].get('text'),
            'capacity':frozen['master']['longform_argument_bundle']['content_capacity'],
            'reference':frozen['resources']['writing_case_reference'].get('video_id')},ensure_ascii=False),flush=True)
        if args.prepare_only:
            continue
        for i in (1,2):
            dest = out / f'{product}_{i}.json'
            if dest.exists():
                row = json.loads(dest.read_text())
            else:
                print(f'START {product}_{i}',flush=True)
                start = time.monotonic()
                result = _invoke_voiceover_model(dict(contract_name='creative_longform_single_v1',payload=frozen['payload']),DEFAULT_MODEL_COMMAND)
                if not all(result.get(k) for k in ('target_text','chinese_translation','semantic_sections')):
                    save(out / f'{product}_{i}_invalid.json',result)
                    raise ValueError('Incomplete response')
                row = dict(product_code=product,replica=i,result=result,elapsed_seconds=round(time.monotonic()-start,2),
                    estimated_seconds=_estimated_spoken_seconds(result['target_text'],frozen['payload']['target_language']),
                    section_fit=_section_duration_fit(result,frozen['payload']))
                save(dest,row)
            records.append(row)
            print(f'DONE {product}_{i} '+json.dumps(row['result'].get('_model_provenance'),ensure_ascii=False),flush=True)
    if records:
        save(out/'result.json',dict(status='TEXT_READY_FOR_REVIEW',records=records))
        lines=['# 两产品长口播灰度｜中文与泰语','', '35秒目标；字符估时非TTS实测。仅口播，不是完整视频脚本。','']
        for row in records:
            lines += [f'## {row["product_code"]} — {row["replica"]}','',row['result']['chinese_translation'],'',row['result']['target_text'],'',f'字符估时：{row["estimated_seconds"]}秒','']
        (out/'voiceovers.md').write_text('\n'.join(lines),encoding='utf-8')


if __name__ == '__main__':
    main()
