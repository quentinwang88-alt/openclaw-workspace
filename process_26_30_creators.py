#!/usr/bin/env python3
"""
⚠️ DEPRECATED - 此脚本已废弃

请使用新的批处理系统:
    python batch_runner.py batch_26_30

详见: BATCH_REFACTOR_README.md

---

原功能: 处理第26-30个达人 - 生成两张宫图（各12张封面）并上传飞书
"""
import sys, json
from pathlib import Path
sys.path.insert(0, str(Path.home() / ".openclaw/workspace/skills/creator-crm"))
from core.data_fetchers import OEmbedFetcher
from core.image_processor import GridCanvasGenerator
from core.sub_agents import FeishuUploaderAgent
from config import FEISHU_APP_TOKEN as APP_TOKEN, FEISHU_TABLE_ID as TABLE_ID, MIN_COVER_COUNT
MAX_VIDEOS_PER_CREATOR = 24
CREATORS = [
    {"record_id": "recvdmfE3EIubP", "tk_handle": "mamiawja8", "video_ids": ["7391049061064822023", "7615242551225896210", "7615222420152093960", "7615202334632578312", "7615195189279280391", "7615184455350734087", "7615160521767095560", "7615145338059476231", "7615123215471365394", "7615112485951065352", "7615098204241153298", "7615052041865530642"]},
    {"record_id": "recvdmfEwSWJGX", "tk_handle": "mr.boss2536", "video_ids": ["7598791156440485128", "7601095633235119381", "7539085865679637778", "7615225182193749268", "7615207584915836180", "7615180427963174165", "7615131564183129365", "7615124443467238677", "7615093035075915029", "7615088439360376085", "7615034106736315668", "7615028398989036821"]},
    {"record_id": "recvdmfF4tRkEc", "tk_handle": "puy2960", "video_ids": ["7599832862401088776", "7553156911147896082", "7512992019879365896", "7615234102823767317", "7615233692772093204", "7615227485763931412", "7615217601135889685", "7615209908014812436", "7615132700504001813", "7615132049657122068", "7615122990795181333", "7615119914516450581"]},
    {"record_id": "recvdmfFwln25t", "tk_handle": "benz220011", "video_ids": ["7605963865897110791", "7566542786980318472", "7585167654680268050", "7615243589152476423", "7615236466486103304", "7615230628144925970", "7615223512176872711", "7615216742331092242", "7615211905745014024", "7615200670152412423", "7615196452557573383", "7615192083875335442"]},
    {"record_id": "recvdmfG36xi7p", "tk_handle": "wstyle119", "video_ids": ["7612595625741389076", "7582638894559055125", "7591046319330921748", "7613989877713112340", "7613986654961126677", "7613983618091715861", "7613051494539087124", "7613344090528189716", "7613341613674335508", "7613320074195193109", "7613315489384779028", "7613311151769668885"]}
]
OUTPUT_DIR = Path.home() / ".openclaw/workspace/output/grids"
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

def process_creator(creator_data):
    """
    处理单个达人：获取最多 24 个视频封面，生成两张宫图并分别上传飞书

    - 宫图 1：第 1-12 张封面
    - 宫图 2：第 13-24 张封面（如果封面不足 12 张则跳过）
    """
    tk_handle, record_id, video_ids = creator_data['tk_handle'], creator_data['record_id'], creator_data['video_ids']
    print(f"\n{'='*70}\n处理达人: {tk_handle}\nRecord ID: {record_id}\n视频数量: {len(video_ids)}（最多取 {MAX_VIDEOS_PER_CREATOR} 个）\n{'='*70}")
    try:
        fetch_count = min(len(video_ids), MAX_VIDEOS_PER_CREATOR)
        print(f"⏳ 步骤 1/3: 获取封面（共 {fetch_count} 个）...")
        fetcher = OEmbedFetcher(cache_enabled=True, timeout=10)
        cover_urls = []
        for i, vid in enumerate(video_ids[:fetch_count], 1):
            print(f"  [{i}/{fetch_count}] 获取视频 {vid}...", end=" ")
            url = fetcher.fetch_cover_url(tk_handle, vid)
            if url: cover_urls.append(url); print("✅")
            else: print("❌")
        print(f"  封面获取结果: {len(cover_urls)}/{fetch_count}")
        if len(cover_urls) < MIN_COVER_COUNT:
            print(f"❌ 封面不足（需要至少 {MIN_COVER_COUNT} 个，实际 {len(cover_urls)} 个）")
            return {'status': 'failed', 'reason': 'insufficient_covers', 'tk_handle': tk_handle}
        print("⏳ 步骤 2/3: 生成宫图...")
        generator = GridCanvasGenerator()
        batch1 = cover_urls[:12]
        batch2 = cover_urls[12:24] if len(cover_urls) >= MIN_COVER_COUNT + 12 else []
        grid_paths = []
        for batch_idx, batch in enumerate([batch1, batch2], 1):
            if not batch:
                print(f"  ⏭️ 宫图 {batch_idx}：封面不足，跳过"); continue
            print(f"  下载图片（宫图 {batch_idx}，共 {len(batch)} 张）...")
            images = generator.downloader.download_images_batch(batch)
            print(f"  下载结果: {len(images)}/{len(batch)}")
            if len(images) < MIN_COVER_COUNT:
                print(f"  ⚠️ 宫图 {batch_idx}：下载成功图片不足 {MIN_COVER_COUNT} 张，跳过"); continue
            print(f"  创建宫格 {batch_idx}...")
            canvas = generator.create_canvas(images, max_images=12)
            output_path = OUTPUT_DIR / f"{tk_handle}_grid_{batch_idx}.jpg"
            canvas.save(output_path, format='JPEG', quality=85)
            print(f"  ✅ 宫图 {batch_idx} 已生成: {output_path}")
            grid_paths.append(str(output_path))
        if not grid_paths:
            print("❌ 所有宫图生成失败"); return {'status': 'failed', 'reason': 'all_grids_failed', 'tk_handle': tk_handle}
        print(f"⏳ 步骤 3/3: 上传到飞书（共 {len(grid_paths)} 张宫图）...")
        uploader = FeishuUploaderAgent()
        file_tokens = []
        for grid_path in grid_paths:
            upload_result = uploader.execute({'tk_handle': tk_handle, 'grid_path': grid_path, 'record_id': record_id, 'app_token': APP_TOKEN, 'table_id': TABLE_ID})
            file_tokens.append(upload_result['file_token'])
            print(f"  ✅ 上传成功: {upload_result['file_token']} ({Path(grid_path).name})")
        return {'status': 'success', 'tk_handle': tk_handle, 'record_id': record_id, 'grid_paths': grid_paths, 'file_tokens': file_tokens, 'grids_generated': len(grid_paths)}
    except Exception as e:
        print(f"❌ 错误: {e}"); import traceback; traceback.print_exc()
        return {'status': 'failed', 'reason': str(e), 'tk_handle': tk_handle}

def main():
    import sys
    print("\n" + "="*70)
    print("⚠️  警告: 此脚本已废弃")
    print("="*70)
    print("请使用新的批处理系统:")
    print("  python batch_runner.py batch_26_30")
    print("\n详见: BATCH_REFACTOR_README.md")
    print("="*70)
    
    response = input("\n是否继续使用旧脚本? (y/N): ")
    if response.lower() != 'y':
        print("已取消")
        sys.exit(0)
    
    print("\n继续使用旧脚本...\n")
    
    print("="*70 + "\n批量处理达人 (第26-30个)\n" + "="*70 + f"\n待处理达人数量: {len(CREATORS)}\n")
    results, success_count, failed_count = [], 0, 0
    for i, creator in enumerate(CREATORS, 1):
        print(f"\n[{i}/{len(CREATORS)}]")
        result = process_creator(creator)
        results.append(result)
        if result['status'] == 'success': success_count += 1
        else: failed_count += 1
    summary = {'total': len(CREATORS), 'success': success_count, 'failed': failed_count, 'results': results}
    summary_path = OUTPUT_DIR / 'batch_26_30_summary.json'
    with open(summary_path, 'w', encoding='utf-8') as f: json.dump(summary, f, indent=2, ensure_ascii=False)
    print(f"\n{'='*70}\n处理完成!\n{'='*70}\n成功: {success_count}\n失败: {failed_count}\n总计: {len(CREATORS)}\n\n汇总结果已保存到: {summary_path}\n{'='*70}")

if __name__ == "__main__": main()
