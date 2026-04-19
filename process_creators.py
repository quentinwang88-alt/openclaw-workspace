#!/usr/bin/env python3
"""
处理飞书达人管理库中未完成的任务
"""

import sys
from pathlib import Path

# 添加路径
sys.path.insert(0, str(Path.home() / ".openclaw/workspace/skills/creator-crm"))

from core.sub_agents import (
    VideoFetcherAgent,
    CoverFetcherAgent,
    GridGeneratorAgent,
    FeishuUploaderAgent,
    AgentOrchestrator
)
from config import FEISHU_APP_TOKEN as APP_TOKEN, FEISHU_TABLE_ID as TABLE_ID

# 需要处理的达人列表（缺少视频截图的）
PENDING_CREATORS = [
    {
        "record_id": "recvdmcVxL2Q1m",
        "tk_handle": "soe..moe..kyi",
        "tk_url": "https://www.tiktok.com/@soe..moe..kyi"
    },
    {
        "record_id": "recvdmcW2lzIvC",
        "tk_handle": "fluke_0171",
        "tk_url": "https://www.tiktok.com/@fluke_0171"
    },
    {
        "record_id": "recvdmeQnHq3xE",
        "tk_handle": "beholiday.official",
        "tk_url": "https://www.tiktok.com/@beholiday.official"
    },
    {
        "record_id": "recvdmeQQOzExE",
        "tk_handle": "narikapuy",
        "tk_url": "https://www.tiktok.com/@narikapuy"
    },
    {
        "record_id": "recvdmeRpcoCJw",
        "tk_handle": "maymapayyaa",
        "tk_url": "https://www.tiktok.com/@maymapayyaa"
    },
    {
        "record_id": "recvdmeRSE5USn",
        "tk_handle": "ei.phyu.zin104",
        "tk_url": "https://www.tiktok.com/@ei.phyu.zin104"
    },
    {
        "record_id": "recvdmeSq6EVWS",
        "tk_handle": "me_diary1992",
        "tk_url": "https://www.tiktok.com/@me_diary1992"
    },
    {
        "record_id": "recvdmeSUHwbg6",
        "tk_handle": "zagasakai",
        "tk_url": "https://www.tiktok.com/@zagasakai"
    },
    {
        "record_id": "recvdmeTok7BBk",
        "tk_handle": "prasit2938",
        "tk_url": "https://www.tiktok.com/@prasit2938"
    },
    {
        "record_id": "recvdmeTZ0T2Nc",
        "tk_handle": "nong.nitcha",
        "tk_url": "https://www.tiktok.com/@nong.nitcha"
    },
    {
        "record_id": "recvdmeUrPskrR",
        "tk_handle": "peepo_89",
        "tk_url": "https://www.tiktok.com/@peepo_89"
    },
    {
        "record_id": "recvdmflCKk6Yi",
        "tk_handle": "nunoy2568",
        "tk_url": "https://www.tiktok.com/@nunoy2568"
    },
    {
        "record_id": "recvdmfm3qMTpW",
        "tk_handle": "jai1jai",
        "tk_url": "https://www.tiktok.com/@jai1jai"
    },
    {
        "record_id": "recvdmfmE1pP4l",
        "tk_handle": "babell_22",
        "tk_url": "https://www.tiktok.com/@babell_22"
    },
    {
        "record_id": "recvdmfn92C27b",
        "tk_handle": "aaalarnz.en",
        "tk_url": "https://www.tiktok.com/@aaalarnz.en"
    },
    {
        "record_id": "recvdmfnKasUKn",
        "tk_handle": "thananya4506",
        "tk_url": "https://www.tiktok.com/@thananya4506"
    },
    {
        "record_id": "recvdmfocj6GuL",
        "tk_handle": "pae__2540",
        "tk_url": "https://www.tiktok.com/@pae__2540"
    },
    {
        "record_id": "recvdmfoDPSdjA",
        "tk_handle": "prayutsunet2",
        "tk_url": "https://www.tiktok.com/@prayutsunet2"
    },
    {
        "record_id": "recvdmfpbV9ker",
        "tk_handle": "sawanitnim",
        "tk_url": "https://www.tiktok.com/@sawanitnim"
    },
    {
        "record_id": "recvdmfpFdQz7b",
        "tk_handle": "bum_thanatchaporn",
        "tk_url": "https://www.tiktok.com/@bum_thanatchaporn"
    },
    {
        "record_id": "recvdmfq87M4ue",
        "tk_handle": "padcha5888",
        "tk_url": "https://www.tiktok.com/@padcha5888"
    },
    {
        "record_id": "recvdmfBYxVg2T",
        "tk_handle": "unchapqko4e",
        "tk_url": "https://www.tiktok.com/@unchapqko4e"
    },
    {
        "record_id": "recvdmfCtT16pH",
        "tk_handle": "malila694",
        "tk_url": "https://www.tiktok.com/@malila694"
    },
    {
        "record_id": "recvdmfD0tWLMq",
        "tk_handle": "dia.officialth",
        "tk_url": "https://www.tiktok.com/@dia.officialth"
    },
    {
        "record_id": "recvdmfDwRgG3O",
        "tk_handle": "bank.nara456",
        "tk_url": "https://www.tiktok.com/@bank.nara456"
    },
    {
        "record_id": "recvdmfE3EIubP",
        "tk_handle": "mamiawja8",
        "tk_url": "https://www.tiktok.com/@mamiawja8"
    },
    {
        "record_id": "recvdmfEwSWJGX",
        "tk_handle": "mr.boss2536",
        "tk_url": "https://www.tiktok.com/@mr.boss2536"
    },
    {
        "record_id": "recvdmfF4tRkEc",
        "tk_handle": "puy2960",
        "tk_url": "https://www.tiktok.com/@puy2960"
    },
    {
        "record_id": "recvdmfFwln25t",
        "tk_handle": "benz220011",
        "tk_url": "https://www.tiktok.com/@benz220011"
    },
    {
        "record_id": "recvdmfG36xi7p",
        "tk_handle": "wstyle119",
        "tk_url": "https://www.tiktok.com/@wstyle119"
    },
    {
        "record_id": "recvdmfGvgJ5oY",
        "tk_handle": "richcasephone",
        "tk_url": "https://www.tiktok.com/@richcasephone"
    },
    {
        "record_id": "recvdmfRMJoPK0",
        "tk_handle": "11luckly",
        "tk_url": "https://www.tiktok.com/@11luckly"
    },
    {
        "record_id": "recvdmfSijwZ77",
        "tk_handle": "purin8116",
        "tk_url": "https://www.tiktok.com/@purin8116"
    },
    {
        "record_id": "recvdmfSLfXowU",
        "tk_handle": "loverrstore_",
        "tk_url": "https://www.tiktok.com/@loverrstore_"
    },
    {
        "record_id": "recvdmfTgdkcXy",
        "tk_handle": "parnntp",
        "tk_url": "https://www.tiktok.com/@parnntp"
    },
    {
        "record_id": "recvdmfTHprUFb",
        "tk_handle": "gdswigs",
        "tk_url": "https://www.tiktok.com/@gdswigs"
    },
    {
        "record_id": "recvdmfU8YtPiT",
        "tk_handle": "np_review",
        "tk_url": "https://www.tiktok.com/@np_review"
    },
    {
        "record_id": "recvdmfUCU4cnT",
        "tk_handle": "baifern1.2",
        "tk_url": "https://www.tiktok.com/@baifern1.2"
    },
    {
        "record_id": "recvdmfV9adSDH",
        "tk_handle": "pond_k4",
        "tk_url": "https://www.tiktok.com/@pond_k4"
    },
    {
        "record_id": "recvdmfVAD31y1",
        "tk_handle": "phuengthitii",
        "tk_url": "https://www.tiktok.com/@phuengthitii"
    },
    {
        "record_id": "recvdmfW5C4kgp",
        "tk_handle": "sshop8005",
        "tk_url": "https://www.tiktok.com/@sshop8005"
    },
    {
        "record_id": "recvdmfWNfgMFT",
        "tk_handle": "movefast.mutelu",
        "tk_url": "https://www.tiktok.com/@movefast.mutelu"
    },
    {
        "record_id": "recvdmfXgCniSw",
        "tk_handle": "noonkookkai",
        "tk_url": "https://www.tiktok.com/@noonkookkai"
    },
    {
        "record_id": "recvdmfXIpkD8u",
        "tk_handle": "panitarshop2",
        "tk_url": "https://www.tiktok.com/@panitarshop2"
    }
]


def main():
    """主流程"""
    print("="*70)
    print("达人视频封面处理流程")
    print("="*70)
    print(f"待处理达人数量: {len(PENDING_CREATORS)}")
    print()
    
    # 初始化编排器
    orchestrator = AgentOrchestrator()
    
    # 注册子智能体
    orchestrator.register_agent("video_fetcher", VideoFetcherAgent())
    orchestrator.register_agent("cover_fetcher", CoverFetcherAgent())
    orchestrator.register_agent("grid_generator", GridGeneratorAgent())
    orchestrator.register_agent("feishu_uploader", FeishuUploaderAgent())
    
    print()
    
    # 处理每个达人
    success_count = 0
    failed_count = 0
    
    for i, creator in enumerate(PENDING_CREATORS, 1):
        print(f"\n[{i}/{len(PENDING_CREATORS)}] 处理达人: {creator['tk_handle']}")
        print("-" * 70)
        
        try:
            # Step 1: 获取视频ID（这里需要实际实现）
            print("  ⏭️  跳过视频ID获取（需要浏览器工具）")
            
            # TODO: 此处需要通过浏览器工具获取真实的视频 ID 列表
            # 当前使用占位符，实际运行时封面获取会失败
            # 参考 process_creators_with_browser.py 中的浏览器操作流程
            
            # Step 2: 获取封面URL
            cover_agent = orchestrator.get_agent("cover_fetcher")
            cover_result = cover_agent.execute({
                'tk_handle': creator['tk_handle'],
                'video_ids': [f"video_{j}" for j in range(12)]  # 占位符，需替换为真实视频ID
            })
            
            if cover_result['cover_count'] < 12:
                print(f"  ⚠️  封面数量不足: {cover_result['cover_count']}/12")
                failed_count += 1
                continue
            
            # Step 3: 生成宫图
            grid_agent = orchestrator.get_agent("grid_generator")
            grid_result = grid_agent.execute({
                'tk_handle': creator['tk_handle'],
                'cover_urls': cover_result['cover_urls']
            })
            
            print(f"  ✅ 宫图生成: {grid_result['grid_path']}")
            
            # Step 4: 上传到飞书
            uploader_agent = orchestrator.get_agent("feishu_uploader")
            upload_result = uploader_agent.execute({
                'tk_handle': creator['tk_handle'],
                'grid_path': grid_result['grid_path'],
                'record_id': creator['record_id'],
                'app_token': APP_TOKEN,
                'table_id': TABLE_ID
            })
            
            print(f"  ✅ 上传成功: {upload_result['file_token']}")
            success_count += 1
            
        except Exception as e:
            print(f"  ❌ 处理失败: {e}")
            failed_count += 1
    
    # 打印统计
    print("\n" + "="*70)
    print("处理完成")
    print("="*70)
    print(f"成功: {success_count}")
    print(f"失败: {failed_count}")
    print(f"总计: {len(PENDING_CREATORS)}")
    print()
    
    # 打印子智能体统计
    orchestrator.print_stats()


if __name__ == "__main__":
    main()
