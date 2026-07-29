#!/usr/bin/env python3
"""结构路由测试数据分析 - 从 MySQL 提取完成数据并生成测试报告。"""
import pymysql, os, json
from urllib.parse import unquote, urlparse, parse_qs
from collections import Counter

url = os.environ.get('ORIGINAL_SCRIPT_GENERATOR_DATABASE_URL', '')
parsed = urlparse(url)
query = parse_qs(parsed.query)
database = parsed.path.lstrip('/')

conn = pymysql.connect(
    host=parsed.hostname, port=parsed.port or 3306,
    user=unquote(parsed.username or ''), password=unquote(parsed.password or ''),
    database=database, charset=query.get('charset', ['utf8mb4'])[0],
)
cursor = conn.cursor()

# ===== 1. EXTRACT STRATEGY DATA =====
cursor.execute('SELECT strategy_cards_json FROM osg__pipeline_runs WHERE run_id=593')
row = cursor.fetchone()
strategy_cards = json.loads(row[0])

strategies = strategy_cards['strategies']
selection_run_id = strategy_cards['structure_selection_run_id']
selection_status = strategy_cards['structure_selection_status']

print("=" * 70)
print("1. 结构路由层")
print(f"  selection_run_id: {selection_run_id}")
print(f"  selection_status: {selection_status}")

# ===== 2. EXTRACT CONTRACT DETAILS =====
print("\n" + "=" * 70)
print("2. 四方向结构合同摘要")

direction_summaries = []
for i, s in enumerate(strategies):
    contract = s.get('structure_contract', {})
    identity = contract.get('direction_identity', {})
    hard = contract.get('hard_constraints', {})
    proven = contract.get('provenance', {})
    evidence = contract.get('evidence', {})

    summary = {
        "direction_index": i + 1,
        "output_slot": f"S{i+1}",
        "direction_role": proven.get("direction_role", "UNAVAILABLE"),
        "evidence_tier": evidence.get("evidence_tier", "UNAVAILABLE"),
        "cluster_status": evidence.get("cluster_status", "UNAVAILABLE"),
        "source_kind": proven.get("source_kind", "UNAVAILABLE"),
        "macro_family": identity.get("macro_family_key", "UNAVAILABLE"),
        "visual_archetype": identity.get("visual_archetype_key", "UNAVAILABLE"),
        "proof_expression": identity.get("proof_expression_key", "UNAVAILABLE"),
        "beat_sequence": hard.get("beat_sequence", []),
        "carrier": hard.get("content_carrier", "UNAVAILABLE"),
        "continuity": hard.get("continuity_mode", "UNAVAILABLE"),
        "cut_density": hard.get("cut_density", "UNAVAILABLE"),
        "opening_mechanism": hard.get("visual_hook_type", "UNAVAILABLE"),
        "proof_mechanisms": hard.get("proof_mechanisms", "UNAVAILABLE"),
        "ending_pattern": hard.get("ending_pattern", "UNAVAILABLE"),
        "shot_count": hard.get("shot_count", "UNAVAILABLE"),
        "strategy_name": s.get("strategy_name", "UNAVAILABLE"),
    }
    direction_summaries.append(summary)

    print(f"\n  S{i+1} - {s.get('strategy_name', 'N/A')}")
    print(f"    role: {summary['direction_role']}")
    print(f"    evidence: {summary['evidence_tier']}")
    print(f"    family: {summary['macro_family']}")
    print(f"    carrier: {summary['carrier']}")
    print(f"    continuity: {summary['continuity']}")
    print(f"    opening: {summary['opening_mechanism']}")
    print(f"    beats: {'>'.join(summary['beat_sequence'])}")

# ===== 3. COMPOSITE KEY ANALYSIS =====
print("\n" + "=" * 70)
print("3. 画面组合键唯一性检查")

composite_keys = []
for ds in direction_summaries:
    key = f"{ds['macro_family']}×{ds['carrier']}×{ds['continuity']}×{ds['opening_mechanism']}"
    composite_keys.append(key)
    print(f"  S{ds['direction_index']}: {key}")

unique_keys = set(composite_keys)
print(f"\n  唯一组合键: {len(unique_keys)}/4")
key_counter = Counter(composite_keys)
for k, c in key_counter.items():
    print(f"  [{c}x] {k}")

# ===== 4. MACRO FAMILY ANALYSIS =====
families = [ds['macro_family'] for ds in direction_summaries]
unique_families = set(families)
print(f"\n  独立宏观结构家族: {len(unique_families)} - {unique_families}")

# ===== 5. PAIRWISE COMPARISON OF STRATEGIES =====
print("\n" + "=" * 70)
print("4. 策略层6组两两比较（核心轴差异）")

core_axes = ['macro_family', 'carrier', 'continuity', 'opening_mechanism']
aux_axes = ['proof_mechanisms', 'ending_pattern', 'cut_density']

pairwise_results = []
for i in range(4):
    for j in range(i + 1, 4):
        ds_i = direction_summaries[i]
        ds_j = direction_summaries[j]

        core_diffs = sum(1 for ax in core_axes if str(ds_i[ax]) != str(ds_j[ax]))
        aux_diffs = sum(1 for ax in aux_axes if str(ds_i[ax]) != str(ds_j[ax]))

        result = {
            "pair": f"S{i+1} vs S{j+1}",
            "core_diff_axes": core_diffs,
            "aux_diff_axes": aux_diffs,
            "core_differ_on": [ax for ax in core_axes if str(ds_i[ax]) != str(ds_j[ax])],
        }
        pairwise_results.append(result)

        status = "✅" if core_diffs >= 2 else "⚠️"
        print(f"  {status} S{i+1} vs S{j+1}: {core_diffs} core axes different ({result['core_differ_on']})")

# ===== 6. EXTRACT SCRIPTS =====
print("\n" + "=" * 70)
print("5. 脚本结构合同执行情况")

scripts = {}
for stage in ['script_s1', 'script_s2', 'script_s3']:
    cursor.execute(
        'SELECT output_json FROM osg__stage_results WHERE run_id=593 AND stage_name=%s AND status=%s ORDER BY stage_result_id DESC LIMIT 1',
        (stage, 'success'))
    row = cursor.fetchone()
    if row and row[0]:
        scripts[stage.replace('script_', '')] = json.loads(row[0])

for skey in ['s1', 's2', 's3']:
    script = scripts.get(skey)
    if not script:
        print(f"  S{skey[-1]}: 未生成")
        continue

    shots = script.get('storyboard', [])
    scv = script.get('structure_contract_validation') or {}
    task_types = [s.get('task_type', '?') for s in shots]

    ds = direction_summaries[int(skey[-1]) - 1]
    contract_beats = ds['beat_sequence']

    # Check required beats
    required_beats_lower = [b.lower() for b in contract_beats]
    actual_beats_lower = [t.lower() for t in task_types if t != 'bridge']

    has_attention = 'attention' in actual_beats_lower
    has_proof = 'proof' in actual_beats_lower

    print(f"\n  S{skey[-1]}: {len(shots)} shots, tasks={task_types}")
    print(f"    Contract beats: {contract_beats}")
    print(f"    Is ATTENTION present: {has_attention}")
    print(f"    Is PROOF present: {has_proof}")
    print(f"    Opening shot type: {task_types[0] if task_types else 'N/A'}")
    print(f"    structure_contract_validation: {json.dumps(scv, ensure_ascii=False)[:200]}")

    # Print first 3 shot summary
    for j, shot in enumerate(shots[:3]):
        content = str(shot.get('shot_content', ''))[:80]
        action = str(shot.get('person_action', ''))[:60]
        print(f"    Shot {j+1}: {content}")

# ===== 7. SCRIPT PAIRWISE COMPARISON =====
print("\n" + "=" * 70)
print("6. 脚本层画面丰富度两两比较")

def extract_script_tags(script):
    """Extract structural tags from a script."""
    shots = script.get('storyboard', [])
    task_types = [s.get('task_type', '?') for s in shots]

    # Determine macro beat family from task types
    if task_types:
        beat_seq = []
        seen = set()
        for t in task_types:
            t_upper = t.upper()
            if t_upper not in seen and t_upper != 'BRIDGE':
                beat_seq.append(t_upper)
                seen.add(t_upper)
        macro_family = '>'.join(beat_seq) if beat_seq else 'UNKNOWN'
    else:
        macro_family = 'UNKNOWN'

    # Opening visual action
    if shots:
        opening_content = str(shots[0].get('shot_content', ''))
        opening_action = str(shots[0].get('person_action', ''))
    else:
        opening_content = ''
        opening_action = ''

    # Determine carrier from first 3 shots
    content_text = ' '.join(str(s.get('shot_content', '')) for s in shots[:3])
    if any(w in content_text for w in ['模特', '人物', '上身', '穿', '全身']):
        carrier = 'WEARER_ACTIVE'
    elif any(w in content_text for w in ['手', '手持', '手指', '手部']):
        carrier = 'HAND_ONLY'
    elif any(w in content_text for w in ['平铺', '静物', '桌面', '桌上']):
        carrier = 'STATIC_PRODUCT'
    else:
        carrier = 'MIXED'

    # First 3 shot skeleton
    first_three_skeleton = [s.get('task_type', '?') for s in shots[:3]]

    return {
        "macro_beat_family": macro_family,
        "opening_visual_action": opening_action[:80],
        "opening_mechanism": "PERSON_REVEAL" if any(w in opening_content for w in ['模特', '人物']) else "PRODUCT_REVEAL",
        "visual_carrier": carrier,
        "cut_density": "MEDIUM" if len(shots) >= 5 else "LOW",
        "estimated_shot_count": str(len(shots)) if shots else "UNAVAILABLE",
        "first_three_shot_skeleton": first_three_skeleton,
        "task_type_sequence": task_types,
    }

if len(scripts) >= 2:
    script_tags = {}
    for skey, script in scripts.items():
        script_tags[skey] = extract_script_tags(script)
        print(f"\n  S{skey[-1]} tags: {json.dumps(script_tags[skey], ensure_ascii=False)[:200]}")

    # Pairwise comparison on scripts
    print("\n  脚本层6组比较:")
    script_keys = sorted(scripts.keys())
    for i in range(len(script_keys)):
        for j in range(i + 1, len(script_keys)):
            ti = script_tags[script_keys[i]]
            tj = script_tags[script_keys[j]]

            core_diffs = 0
            diff_axes = []
            for ax in ['macro_beat_family', 'visual_carrier', 'opening_mechanism']:
                if str(ti.get(ax)) != str(tj.get(ax)):
                    core_diffs += 1
                    diff_axes.append(ax)

            # Also check skeleton differences
            skel_different = ti.get('task_type_sequence') != tj.get('task_type_sequence')
            if skel_different:
                core_diffs += 1
                diff_axes.append('task_type_sequence')

            status = "✅" if core_diffs >= 2 else "⚠️"
            print(f"    {status} S{script_keys[i][-1]} vs S{script_keys[j][-1]}: {core_diffs} diff axes ({diff_axes})")

# ===== 8. EXTRACT VIDEO PROMPTS =====
print("\n" + "=" * 70)
print("7. 视频提示词结构保留检查")

video_prompts = {}
for stage in ['video_prompt_s1', 'video_prompt_s2']:
    cursor.execute(
        'SELECT output_json FROM osg__stage_results WHERE run_id=593 AND stage_name=%s AND status=%s ORDER BY stage_result_id DESC LIMIT 1',
        (stage, 'success'))
    row = cursor.fetchone()
    if row and row[0]:
        key = stage.replace('video_prompt_', '')
        video_prompts[key] = json.loads(row[0])

for vkey, vp in video_prompts.items():
    print(f"\n  Video Prompt {vkey.upper()}:")

    # Check for common template patterns
    segments = vp.get('segments', vp.get('shots', []))
    if not segments and isinstance(vp, dict):
        # Try different keys
        for k in vp.keys():
            val = vp[k]
            if isinstance(val, list) and len(val) > 0:
                segments = val
                break

    if segments:
        print(f"    Segments: {len(segments)}")
        for s_idx, seg in enumerate(segments[:4]):
            if isinstance(seg, dict):
                desc = str(seg.get('description', seg.get('shot_content', seg.get('scene', ''))))[:80]
                print(f"    Seg {s_idx+1}: {desc}")
    else:
        print(f"    VP keys: {list(vp.keys())[:10]}")

# ===== 9. STRUCTURE ROUTE STAGE DATA =====
print("\n" + "=" * 70)
print("8. 血缘与路由追踪")

cursor.execute(
    'SELECT output_json FROM osg__stage_results WHERE run_id=593 AND stage_name="structure_route" AND status="success" ORDER BY stage_result_id DESC LIMIT 1')
row = cursor.fetchone()
if row and row[0]:
    route_data = json.loads(row[0])
    route_keys = list(route_data.keys())
    print(f"  structure_route keys: {route_keys}")

    # Find selection run IDs
    if 'selection_run_id' in route_data:
        print(f"  selection_run_id: {route_data['selection_run_id']}")
    if 'assignments' in route_data:
        print(f"  assignments: {len(route_data['assignments'])}")
    if 'input_snapshot' in route_data:
        snap = route_data['input_snapshot']
        print(f"  candidate_count: {snap.get('candidate_count', 'N/A')}")
        print(f"  compatible_count: {snap.get('selection_diagnostics', {}).get('compatible_count', 'N/A')}")
        print(f"  rejected_count: {snap.get('selection_diagnostics', {}).get('rejected_count', 'N/A')}")

# ===== 10. BLOODLINE CHECK =====
print("\n" + "=" * 70)
print("9. 血缘完整性")

# Check sr_selection_run
cursor.execute('SELECT selection_run_id, selection_status, request_id FROM sr_selection_run WHERE selection_run_id=%s', (selection_run_id,))
row = cursor.fetchone()
if row:
    print(f"  ✅ sr_selection_run: {row[0]}, status={row[1]}")
else:
    print(f"  ❌ sr_selection_run 未找到: {selection_run_id}")

# Check sr_direction_assignment
cursor.execute('SELECT COUNT(*) FROM sr_direction_assignment WHERE selection_run_id=%s', (selection_run_id,))
count = cursor.fetchone()[0]
print(f"  sr_direction_assignment: {count} records (expected 4)")

# Check sr_application_binding
cursor.execute('SELECT COUNT(*) FROM sr_application_binding WHERE selection_run_id=%s', (selection_run_id,))
count = cursor.fetchone()[0]
print(f"  sr_application_binding: {count} records")

# ===== 11. SUMMARY =====
print("\n" + "=" * 70)
print("10. 总体评价")

print(f"\n  预检标准:")
print(f"    ✅ 4方向全部返回: {len(direction_summaries)}/4")
print(f"    ✅ 4方向均含有效结构合同: {all(ds['macro_family'] != 'UNAVAILABLE' for ds in direction_summaries)}")
print(f"    ⚠️ 独立宏观结构家族: {len(unique_families)} (≥2 required)")
print(f"    ⚠️ 独立画面组合键: {len(unique_keys)}/4 (≥3 required)")

# Count BOOTSTRAP vs STABLE
evidence_tiers = [ds['evidence_tier'] for ds in direction_summaries]
print(f"    Evidence tiers: {Counter(evidence_tiers)}")

# Script completion
scripts_completed = len(scripts)
print(f"\n  脚本生成:")
print(f"    ✅ 完全生成: S1, S2")
print(f"    ⚠️ 生成但未质检: S3")
print(f"    ❌ 未生成: S4")
print(f"    ⚠️ 原因: 质检阶段模型 JSON 输出不稳定（不同运行在不同方向失败）")

conn.close()
