#!/usr/bin/env python3
"""
FastMoss 数据导出脚本 V2
改进版：先展开下拉菜单再选择子类目
"""

import asyncio
from playwright.async_api import async_playwright

# 子类目配置
SUB_CATEGORIES = [
    {"name": "耳环", "text": "耳环"},
    {"name": "脚链", "text": "脚链"},
    {"name": "戒指", "text": "戒指"},
    {"name": "手环与手链", "text": "手环与手链"},
    {"name": "项链", "text": "项链"},
]

BASE_URL = "https://www.fastmoss.com/zh/e-commerce/search?page=1&l1_cid=8&l2_cid=905608&region=TH"

async def export_category(page, category_info, index, total):
    """导出单个类目的数据"""
    name = category_info["name"]
    text = category_info["text"]
    
    print(f"\n[{index}/{total}] 正在处理: {name}")
    
    try:
        # 先尝试直接点击（如果已经在展开状态）
        try:
            category_btn = await page.wait_for_selector(f'text={text}', timeout=3000)
            await category_btn.click()
            print(f"  点击子类目: {name}")
        except:
            # 如果没找到，尝试展开下拉菜单
            print(f"  尝试展开下拉菜单...")
            dropdown = await page.wait_for_selector('text=时尚配件', timeout=5000)
            await dropdown.click()
            await page.wait_for_timeout(1000)
            
            # 再次尝试点击子类目
            category_btn = await page.wait_for_selector(f'text={text}', timeout=5000)
            await category_btn.click()
            print(f"  点击子类目: {name}")
        
        # 等待页面数据刷新
        await page.wait_for_timeout(3000)
        
        # 查找并点击数据导出按钮
        print(f"  查找导出按钮...")
        export_btn = await page.wait_for_selector('text=数据导出', timeout=5000)
        
        if export_btn:
            print(f"  点击导出按钮...")
            await export_btn.click()
            await page.wait_for_timeout(5000)
            
            # 检查是否有确认对话框
            try:
                confirm_btn = await page.wait_for_selector('button:has-text("确定")', timeout=3000)
                if confirm_btn:
                    await confirm_btn.click()
                    print(f"  点击确认按钮")
                    await page.wait_for_timeout(3000)
            except:
                pass
            
            print(f"  ✅ {name} 导出完成")
        else:
            print(f"  ⚠️ 未找到导出按钮")
            
    except Exception as e:
        print(f"  ❌ {name} 导出失败: {e}")

async def main():
    """主函数"""
    print("=" * 60)
    print("FastMoss 数据导出工具 V2")
    print("=" * 60)
    
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=False)
        context = await browser.new_context()
        page = await context.new_page()
        
        print(f"\n访问页面...")
        await page.goto(BASE_URL, wait_until="networkidle")
        await page.wait_for_timeout(3000)
        
        print(f"\n开始导出数据...")
        print("=" * 60)
        
        total = len(SUB_CATEGORIES)
        for i, category in enumerate(SUB_CATEGORIES, 1):
            await export_category(page, category, i, total)
            if i < total:
                await page.wait_for_timeout(2000)
        
        print("\n" + "=" * 60)
        print("导出完成！请检查下载文件夹")
        print("=" * 60)
        
        await page.wait_for_timeout(3000)
        await browser.close()

if __name__ == "__main__":
    asyncio.run(main())
