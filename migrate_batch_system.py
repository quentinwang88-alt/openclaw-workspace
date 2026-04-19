#!/usr/bin/env python3
"""
批处理系统迁移工具
从旧的独立脚本迁移到新的统一批处理系统
"""
import json
from pathlib import Path


def extract_creators_from_old_script(script_path: Path):
    """从旧脚本中提取 CREATORS 数据"""
    with open(script_path, 'r', encoding='utf-8') as f:
        content = f.read()
    
    # 简单的提取逻辑（假设 CREATORS 是一个列表）
    # 实际使用时可能需要更复杂的解析
    import re
    match = re.search(r'CREATORS\s*=\s*(\[.*?\])', content, re.DOTALL)
    if match:
        creators_str = match.group(1)
        # 使用 eval 解析（仅用于迁移，生产环境不推荐）
        try:
            return eval(creators_str)
        except:
            print(f"⚠️  无法解析 {script_path} 中的 CREATORS 数据")
            return None
    return None


def migrate_all_batches():
    """迁移所有批次"""
    workspace = Path(__file__).parent
    
    # 旧脚本映射
    old_scripts = {
        'batch_5': 'process_5_creators.py',
        'batch_6_10': 'process_6_10_creators.py',
        'batch_11_15': 'process_11_15_creators.py',
        'batch_16_20': 'process_16_20_creators.py',
        'batch_21_25': 'process_21_25_creators.py',
        'batch_26_30': 'process_26_30_creators.py',
    }
    
    batch_data = {}
    
    for batch_name, script_name in old_scripts.items():
        script_path = workspace / script_name
        if not script_path.exists():
            print(f"⚠️  脚本不存在: {script_name}")
            continue
        
        print(f"📄 提取 {script_name} 中的数据...")
        creators = extract_creators_from_old_script(script_path)
        
        if creators:
            batch_data[batch_name] = creators
            print(f"  ✅ 提取到 {len(creators)} 个达人")
        else:
            print(f"  ❌ 提取失败")
    
    # 保存到 batch_data.json
    output_file = workspace / 'batch_data.json'
    with open(output_file, 'w', encoding='utf-8') as f:
        json.dump(batch_data, f, indent=2, ensure_ascii=False)
    
    print(f"\n✅ 迁移完成！数据已保存到: {output_file}")
    print(f"📊 共迁移 {len(batch_data)} 个批次")
    
    # 打印使用说明
    print("\n" + "="*70)
    print("使用新系统:")
    print("="*70)
    for batch_name in batch_data.keys():
        print(f"  python batch_runner.py {batch_name}")
    print("\n或运行所有批次:")
    print("  python batch_runner.py all")
    print("="*70)


def verify_migration():
    """验证迁移结果"""
    workspace = Path(__file__).parent
    batch_data_file = workspace / 'batch_data.json'
    
    if not batch_data_file.exists():
        print("❌ batch_data.json 不存在，请先运行迁移")
        return False
    
    with open(batch_data_file, 'r', encoding='utf-8') as f:
        batch_data = json.load(f)
    
    print("="*70)
    print("迁移验证")
    print("="*70)
    
    total_creators = 0
    for batch_name, creators in batch_data.items():
        creator_count = len(creators)
        total_creators += creator_count
        print(f"✅ {batch_name}: {creator_count} 个达人")
        
        # 验证数据完整性
        for i, creator in enumerate(creators, 1):
            if 'record_id' not in creator:
                print(f"  ⚠️  达人 {i} 缺少 record_id")
            if 'tk_handle' not in creator:
                print(f"  ⚠️  达人 {i} 缺少 tk_handle")
            if 'video_ids' not in creator:
                print(f"  ⚠️  达人 {i} 缺少 video_ids")
    
    print(f"\n📊 总计: {len(batch_data)} 个批次, {total_creators} 个达人")
    print("="*70)
    
    return True


if __name__ == "__main__":
    import sys
    
    if len(sys.argv) > 1 and sys.argv[1] == 'verify':
        verify_migration()
    else:
        print("批处理系统迁移工具")
        print("\n⚠️  注意: batch_data.json 已手动创建，无需运行此迁移工具")
        print("\n如需验证迁移结果:")
        print("  python migrate_batch_system.py verify")
        print("\n" + "="*70)
        
        response = input("\n是否验证现有配置? (Y/n): ")
        if response.lower() != 'n':
            verify_migration()
