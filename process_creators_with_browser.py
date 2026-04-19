#!/usr/bin/env python3
"""
使用 OpenClaw 浏览器工具处理飞书达人管理库中未完成的任务
"""

import sys
import json
from pathlib import Path

# 添加路径
sys.path.insert(0, str(Path.home() / ".openclaw/workspace/skills/creator-crm"))

from core.sub_agents import (
    CoverFetcherAgent,
    GridGeneratorAgent,
    FeishuUploaderAgent,
    AgentOrchestrator
)
from core.tiktok_fetcher import extract_tiktok_page_data_js
from config import FEISHU_APP_TOKEN as APP_TOKEN, FEISHU_TABLE_ID as TABLE_ID

# 需要处理的达人列表（缺少视频截图的）
PENDING_CREATORS = [
    {"record_id": "recvdmcVxL2Q1m", "tk_handle": "soe..moe..kyi", "tk_url": "https://www.tiktok.com/@soe..moe..kyi"},
    {"record_id": "recvdmcW2lzIvC", "tk_handle": "fluke_0171", "tk_url": "https://www.tiktok.com/@fluke_0171"},
    {"record_id": "recvdmeQnHq3xE", "tk_handle": "beholiday.official", "tk_url": "https://www.tiktok.com/@beholiday.official"},
    {"record_id": "recvdmeQQOzExE", "tk_handle": "narikapuy", "tk_url": "https://www.tiktok.com/@narikapuy"},
    {"record_id": "recvdmeRpcoCJw", "tk_handle": "maymapayyaa", "tk_url": "https://www.tiktok.com/@maymapayyaa"},
    {"record_id": "recvdmeRSE5USn", "tk_handle": "ei.phyu.zin104", "tk_url": "https://www.tiktok.com/@ei.phyu.zin104"},
    {"record_id": "recvdmeSq6EVWS", "tk_handle": "me_diary1992", "tk_url": "https://www.tiktok.com/@me_diary1992"},
    {"record_id": "recvdmeSUHwbg6", "tk_handle": "zagasakai", "tk_url": "https://www.tiktok.com/@zagasakai"},
    {"record_id": "recvdmeTok7BBk", "tk_handle": "prasit2938", "tk_url": "https://www.tiktok.com/@prasit2938"},
    {"record_id": "recvdmeTZ0T2Nc", "tk_handle": "nong.nitcha", "tk_url": "https://www.tiktok.com/@nong.nitcha"},
    {"record_id": "recvdmeUrPskrR", "tk_handle": "peepo_89", "tk_url": "https://www.tiktok.com/@peepo_89"},
    {"record_id": "recvdmflCKk6Yi", "tk_handle": "nunoy2568", "tk_url": "https://www.tiktok.com/@nunoy2568"},
    {"record_id": "recvdmfm3qMTpW", "tk_handle": "jai1jai", "tk_url": "https://www.tiktok.com/@jai1jai"},
    {"record_id": "recvdmfmE1pP4l", "tk_handle": "babell_22", "tk_url": "https://www.tiktok.com/@babell_22"},
    {"record_id": "recvdmfn92C27b", "tk_handle": "aaalarnz.en", "tk_url": "https://www.tiktok.com/@aaalarnz.en"},
    {"record_id": "recvdmfnKasUKn", "tk_handle": "thananya4506", "tk_url": "https://www.tiktok.com/@thananya4506"},
    {"record_id": "recvdmfocj6GuL", "tk_handle": "pae__2540", "tk_url": "https://www.tiktok.com/@pae__2540"},
    {"record_id": "recvdmfoDPSdjA", "tk_handle": "prayutsunet2", "tk_url": "https://www.tiktok.com/@prayutsunet2"},
    {"record_id": "recvdmfpbV9ker", "tk_handle": "sawanitnim", "tk_url": "https://www.tiktok.com/@sawanitnim"},
    {"record_id": "recvdmfpFdQz7b", "tk_handle": "bum_thanatchaporn", "tk_url": "https://www.tiktok.com/@bum_thanatchaporn"},
    {"record_id": "recvdmfq87M4ue", "tk_handle": "padcha5888", "tk_url": "https://www.tiktok.com/@padcha5888"},
    {"record_id": "recvdmfBYxVg2T", "tk_handle": "unchapqko4e", "tk_url": "https://www.tiktok.com/@unchapqko4e"},
    {"record_id": "recvdmfCtT16pH", "tk_handle": "malila694", "tk_url": "https://www.tiktok.com/@malila694"},
    {"record_id": "recvdmfD0tWLMq", "tk_handle": "dia.officialth", "tk_url": "https://www.tiktok.com/@dia.officialth"},
    {"record_id": "recvdmfDwRgG3O", "tk_handle": "bank.nara456", "tk_url": "https://www.tiktok.com/@bank.nara456"},
    {"record_id": "recvdmfE3EIubP", "tk_handle": "mamiawja8", "tk_url": "https://www.tiktok.com/@mamiawja8"},
    {"record_id": "recvdmfEwSWJGX", "tk_handle": "mr.boss2536", "tk_url": "https://www.tiktok.com/@mr.boss2536"},
    {"record_id": "recvdmfF4tRkEc", "tk_handle": "puy2960", "tk_url": "https://www.tiktok.com/@puy2960"},
    {"record_id": "recvdmfFwln25t", "tk_handle": "benz220011", "tk_url": "https://www.tiktok.com/@benz220011"},
    {"record_id": "recvdmfG36xi7p", "tk_handle": "wstyle119", "tk_url": "https://www.tiktok.com/@wstyle119"},
    {"record_id": "recvdmfGvgJ5oY", "tk_handle": "richcasephone", "tk_url": "https://www.tiktok.com/@richcasephone"},
    {"record_id": "recvdmfRMJoPK0", "tk_handle": "11luckly", "tk_url": "https://www.tiktok.com/@11luckly"},
    {"record_id": "recvdmfSijwZ77", "tk_handle": "purin8116", "tk_url": "https://www.tiktok.com/@purin8116"},
    {"record_id": "recvdmfSLfXowU", "tk_handle": "loverrstore_", "tk_url": "https://www.tiktok.com/@loverrstore_"},
    {"record_id": "recvdmfTgdkcXy", "tk_handle": "parnntp", "tk_url": "https://www.tiktok.com/@parnntp"},
    {"record_id": "recvdmfTHprUFb", "tk_handle": "gdswigs", "tk_url": "https://www.tiktok.com/@gdswigs"},
    {"record_id": "recvdmfU8YtPiT", "tk_handle": "np_review", "tk_url": "https://www.tiktok.com/@np_review"},
    {"record_id": "recvdmfUCU4cnT", "tk_handle": "baifern1.2", "tk_url": "https://www.tiktok.com/@baifern1.2"},
    {"record_id": "recvdmfV9adSDH", "tk_handle": "pond_k4", "tk_url": "https://www.tiktok.com/@pond_k4"},
    {"record_id": "recvdmfVAD31y1", "tk_handle": "phuengthitii", "tk_url": "https://www.tiktok.com/@phuengthitii"},
    {"record_id": "recvdmfW5C4kgp", "tk_handle": "sshop8005", "tk_url": "https://www.tiktok.com/@sshop8005"},
    {"record_id": "recvdmfWNfgMFT", "tk_handle": "movefast.mutelu", "tk_url": "https://www.tiktok.com/@movefast.mutelu"},
    {"record_id": "recvdmfXgCniSw", "tk_handle": "noonkookkai", "tk_url": "https://www.tiktok.com/@noonkookkai"},
    {"record_id": "recvdmfXIpkD8u", "tk_handle": "panitarshop2", "tk_url": "https://www.tiktok.com/@panitarshop2"},
]


def print_task_info():
    """打印任务信息"""
    print("="*70)
    print("达人视频封面处理任务")
    print("="*70)
    print(f"📊 待处理达人数量: {len(PENDING_CREATORS)}")
    print(f"📋 飞书表格: {APP_TOKEN}")
    print()
    print("处理流程:")
    print("  1. 使用 OpenClaw 浏览器工具访问 TikTok 主页")
    print("  2. 提取视频 ID 和封面 URL")
    print("  3. 使用 oEmbed API 获取长期有效的封面 URL")
    print("  4. 下载封面并生成 3x4 宫格图")
    print("  5. 上传到飞书多维表格")
    print()
    print("="*70)
    print()


def generate_browser_instructions():
    """生成浏览器操作指令"""
    instructions = []
    
    for i, creator in enumerate(PENDING_CREATORS, 1):
        instructions.append({
            "step": i,
            "record_id": creator["record_id"],
            "tk_handle": creator["tk_handle"],
            "tk_url": creator["tk_url"],
            "actions": [
                f"browser.open('{creator['tk_url']}')",
                "browser.wait(5000)  # 等待页面加载",
                f"page_data = browser.eval(extract_tiktok_page_data_js())",
                "# 提取视频 ID",
                "video_ids = [v['video_id'] for v in page_data['videos'][:12]]",
                "# 使用 CoverFetcherAgent 获取封面",
                "# 使用 GridGeneratorAgent 生成宫图",
                "# 使用 FeishuUploaderAgent 上传"
            ]
        })
    
    return instructions


def main():
    """主流程"""
    print_task_info()
    
    print("📝 生成处理指令...")
    instructions = generate_browser_instructions()
    
    # 保存指令到文件
    output_file = Path("creator_processing_instructions.json")
    with open(output_file, 'w', encoding='utf-8') as f:
        json.dump(instructions, f, indent=2, ensure_ascii=False)
    
    print(f"✅ 指令已保存到: {output_file}")
    print()
    print("="*70)
    print("下一步操作:")
    print("="*70)
    print("1. 由于需要使用浏览器工具，建议使用 OpenClaw 的 browser 工具")
    print("2. 或者使用 sessions_spawn 创建一个子智能体来处理")
    print("3. 每个达人的处理流程:")
    print("   - 打开 TikTok 主页")
    print("   - 执行 JavaScript 提取视频数据")
    print("   - 使用 oEmbed API 获取封面")
    print("   - 生成宫格图")
    print("   - 上传到飞书")
    print()
    print("建议: 让 OpenClaw 主智能体使用 browser 工具逐个处理")
    print("="*70)


if __name__ == "__main__":
    main()
