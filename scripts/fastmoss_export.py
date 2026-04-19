#!/usr/bin/env python3
"""
FastMoss 数据导出脚本
用于导出泰国站点时尚配件类目下指定子类目的数据
"""

import asyncio
import time
from playwright.async_api import async_playwright
from pathlib import Path

# 子类目配置（从页面获取）
SUB_CATEGORIES = [
    {"name": "耳环", "selector": "text=耳环"},
    {"name": "脚链", "selector": "text=脚链"},
    {"name": "戒指", "selector": "text=戒指"},
    {"name": "手环与手链", "selector": "text=手环与手链"},
    {"name": "项链", "selector": "text=项链"},
]

BASE_URL = "https://www.fastmoss.com/zh/e-commerce/search?page=1&l1_cid=8&l2_cid=905608&region=TH"

async def export_category(page, category_info, index):
    """导出单个类目的数据"""
    name = category_info["name"]
    selector = category_info["selector"]
    
    print(f"\n[{index}/5] 正在处理: {name}")
    
    try:
        # 点击子类目
        print(f"  点击子类目: {name}")
        category_btn = await page.wait_for_selector(selector, timeout=5000)
        await category_btn.click()
        
        # 等待页面数据刷新
        await page.wait_for_timeout(3000)
        
        # 查找并点击数据导出按钮
        print(f"  查找导出按钮...")
        export_btn = await page.wait_for_selector('text=数据导出', timeout=5000)
        
        if export_btn:
            print(f"  点击导出按钮...")
            await export_btn.click()
            
            # 等待导出处理
            await page.wait_for_timeout(5000)
            
            # 检查是否有确认对话框或下载提示
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
    print("FastMoss 数据导出工具")
    print("=" * 60)
    print(f"目标: 泰国站点 - 时尚配件 - 5个子类目")
    print(f"导出方式: 直接下载 Excel/CSV")
    print("=" * 60)
    
    async with async_playwright() as p:
        # 启动浏览器（非 headless 模式，方便观察）
        browser = await p.chromium.launch(headless=False)
        
        # 创建新页面
        context = await browser.new_context()
        page = await context.new_page()
        
        # 访问初始页面
        print(f"\n访问初始页面...")
        await page.goto(BASE_URL, wait_until="networkidle")
        await page.wait_for_timeout(3000)
        
        print(f"\n页面已加载，开始导出数据...")
        print("=" * 60)
        
        # 依次导出各个类目
        for i, category in enumerate(SUB_CATEGORIES, 1):
            await export_category(page, category, i)
            # 每个类目之间等待一段时间
            if i < len(SUB_CATEGORIES):
                await page.wait_for_timeout(2000)
        
        print("\n" + "=" * 60)
        print("所有类目导出完成！")
        print("请检查下载文件夹中的文件")
        print("=" * 60)
        
        # 保持浏览器打开一会儿，方便查看
        await page.wait_for_timeout(5000)
        await browser.close()

if __name__ == "__main__":
    asyncio.run(main())
