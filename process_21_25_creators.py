#!/usr/bin/env python3
"""处理第21-25个达人 - 生成两张宫图（各12张封面）并上传飞书"""
import sys, json
from pathlib import Path
sys.path.insert(0, str(Path.home() / ".openclaw/workspace/skills/creator-crm"))
from core.data_fetchers import OEmbedFetcher
from core.image_processor import GridCanvasGenerator
from core.sub_agents import FeishuUploaderAgent
from config import FEISHU_APP_TOKEN as APP_TOKEN, FEISHU_TABLE_ID as TABLE_ID, MIN_COVER_COUNT
MAX_VIDEOS_PER_CREATOR = 24
CREATORS = [
    {"record_id": "recvdmfq87M4ue", "tk_handle": "padcha5888", "video_ids": ["7598788850680335637", "7567640037701979413", "7534701677929139463", "7615245142366407956", "7615201956998417684", "7615197981955951892", "7615133495081569556", "7615118706913398036", "7615115424648285460", "7615106418881907988", "7615088028104609044", "7614900417792675093"]},
    {"record_id": "recvdmfBYxVg2T", "tk_handle": "unchapqko4e", "video_ids": ["7558471640900734215", "7567696077508463879", "7603415467813817608", "7615158344906476807", "7615147828934905106", "7615147178343877896", "7615146373880614162", "7615144815252000018", "7615144289328254215", "7615143732161023239", "7614935363919809799", "7614916807341657362"]},
    {"record_id": "recvdmfCtT16pH", "tk_handle": "malila694", "video_ids": ["7584854986308259088", "7578531717258972432", "7580192476720401681", "7615236348345158928", "7615218952226032912", "7615218873507351824", "7615218855778012417", "7615210304271682832", "7615204552123387153", "7615187428072049936", "7615170554458410257", "7615061723489307920"]},
    {"record_id": "recvdmfD0tWLMq", "tk_handle": "dia.officialth", "video_ids": ["7546953243901971719", "7503532042580233490", "7514278326706588936", "7614874231741975816", "7614376297040547090", "7613796537130372370", "7613754779029884178", "7613421792295652616", "7613249929837825287"]},
    {"record_id": "recvdmfDwRgG3O", "tk_handle": "bank.nara456", "video_ids": ["7606417995950230792", "7614948222401891592", "7614947929815567634", "7614947830834138376", "7614947617407094023", "7614947490751646983", "7614947337793735944", "7614947249612737810", "7614947099339230472", "7614947024688925960", "7614946883332541703", "7614946828500356370"]}
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
    print("="*70 + "\n批量处理达人 (第21-25个)\n" + "="*70 + f"\n待处理达人数量: {len(CREATORS)}\n")
    results, success_count, failed_count = [], 0, 0
    for i, creator in enumerate(CREATORS, 1):
        print(f"\n[{i}/{len(CREATORS)}]")
        result = process_creator(creator)
        results.append(result)
        if result['status'] == 'success': success_count += 1
        else: failed_count += 1
    summary = {'total': len(CREATORS), 'success': success_count, 'failed': failed_count, 'results': results}
    summary_path = OUTPUT_DIR / 'batch_21_25_summary.json'
    with open(summary_path, 'w', encoding='utf-8') as f: json.dump(summary, f, indent=2, ensure_ascii=False)
    print(f"\n{'='*70}\n处理完成!\n{'='*70}\n成功: {success_count}\n失败: {failed_count}\n总计: {len(CREATORS)}\n\n汇总结果已保存到: {summary_path}\n{'='*70}")

if __name__ == "__main__": main()
