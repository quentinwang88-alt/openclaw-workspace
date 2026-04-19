#!/usr/bin/env python3
"""批量LLM打标 - 分批处理"""

import subprocess
import sys
import time
from pathlib import Path

FEISHU_URL = "https://gcngopvfvo0q.feishu.cn/wiki/CtGxwJpTEifSh5kIVtgcM2vCnLf?table=tblKhPn64Q266tRz&view=vewmWdRUHq"
BATCH_SIZE = 500
TOTAL_RECORDS = 3000

def run_batch(batch_num):
    """运行单批次"""
    print(f"\n{'='*60}")
    print(f"🔄 处理批次 {batch_num}/{(TOTAL_RECORDS-1)//BATCH_SIZE + 1}")
    print(f"{'='*60}")

    cmd = [
        sys.executable, "run_pipeline.py",
        "--feishu-url", FEISHU_URL,
        "--overwrite-chinese-name",
        "--overwrite-subcategory",
        "--limit", str(BATCH_SIZE)
    ]

    result = subprocess.run(cmd, capture_output=True, text=True, cwd=Path(__file__).parent)

    # 解析输出
    output = result.stdout + result.stderr

    # 查找关键信息
    if '"updated_records":' in output:
        import json
        try:
            # 尝试解析JSON输出
            start = output.find('{')
            end = output.rfind('}') + 1
            if start >= 0 and end > start:
                data = json.loads(output[start:end])
                stats = data.get('data', {}).get('stats', {})
                updated = stats.get('updated_records', 0)
                skipped = stats.get('skipped_records', 0)
                print(f"✅ 批次完成: 更新 {updated} 条, 跳过 {skipped} 条")
                return updated, skipped
        except:
            pass

    print(f"⚠️ 批次完成 (输出解析失败)")
    print(f"STDOUT: {output[:500]}")
    return 0, 0

def main():
    total_updated = 0
    total_skipped = 0
    batches = (TOTAL_RECORDS - 1) // BATCH_SIZE + 1

    print("🚀 开始批量LLM打标")
    print(f"📊 总记录数: {TOTAL_RECORDS}")
    print(f"📦 批次大小: {BATCH_SIZE}")
    print(f"🔢 总批次数: {batches}")

    for i in range(1, batches + 1):
        updated, skipped = run_batch(i)
        total_updated += updated
        total_skipped += skipped

        print(f"📈 累计进度: 更新 {total_updated} 条, 跳过 {total_skipped} 条")

        # 批次间延迟，避免限流
        if i < batches:
            print("⏳ 等待5秒后继续...")
            time.sleep(5)

    print(f"\n{'='*60}")
    print("🎉 全部完成!")
    print(f"{'='*60}")
    print(f"📊 总计更新: {total_updated} 条")
    print(f"📊 总计跳过: {total_skipped} 条")

if __name__ == "__main__":
    main()
