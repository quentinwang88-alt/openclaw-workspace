# 批处理系统重构说明

## 概述

原有的 6 个批处理脚本（`process_5_creators.py` ~ `process_26_30_creators.py`）存在大量重复代码。现已重构为统一的批处理系统，消除代码重复，提高可维护性。

## 新架构

```
batch_processor_core.py    # 核心处理逻辑（通用函数）
batch_data.json            # 批次数据配置（JSON格式）
batch_runner.py            # 统一运行器（CLI入口）
```

## 使用方法

### 1. 运行单个批次

```bash
# 运行第5批（5个达人）
python batch_runner.py batch_5

# 运行第26-30批（5个达人）
python batch_runner.py batch_26_30
```

### 2. 运行所有批次

```bash
python batch_runner.py all
```

### 3. 查看可用批次

```bash
python batch_runner.py
```

## 添加新批次

编辑 [`batch_data.json`](batch_data.json)，添加新的批次数据：

```json
{
  "batch_31_35": [
    {
      "record_id": "recXXXXXXXXXXX",
      "tk_handle": "creator_handle",
      "video_ids": ["7615...", "7614..."]
    }
  ]
}
```

然后运行：

```bash
python batch_runner.py batch_31_35
```

## 优势

1. **消除重复代码**：核心逻辑只维护一份
2. **数据与逻辑分离**：批次数据存储在 JSON 文件中，易于管理
3. **统一入口**：所有批次通过同一个运行器执行
4. **易于扩展**：添加新批次只需修改 JSON 配置
5. **更好的错误处理**：统一的异常处理和日志记录

## 旧脚本迁移

旧的批处理脚本（`process_5_creators.py` 等）已被标记为 **DEPRECATED**，建议使用新系统。

如需继续使用旧脚本，它们仍然可以正常运行，但不再维护。

## 文件说明

- [`batch_processor_core.py`](batch_processor_core.py) - 核心处理模块，包含 `process_creator()` 和 `run_batch()` 函数
- [`batch_data.json`](batch_data.json) - 批次数据配置，包含所有批次的达人信息
- [`batch_runner.py`](batch_runner.py) - CLI 运行器，提供命令行接口
- `process_*_creators.py` - **已废弃**，保留用于向后兼容

## 示例输出

```
======================================================================
批量处理达人 (batch_5)
======================================================================
待处理达人数量: 5

[1/5] 开始处理...
======================================================================
处理达人: soe..moe..kyi
Record ID: recvdmcVxL2Q1m
视频数量: 12（最多取 24 个）
======================================================================
⏳ 步骤 1/3: 获取封面（共 12 个）...
  [1/12] 获取视频 7615214987761454354... ✅
  ...
  封面获取结果: 12/12
⏳ 步骤 2/3: 生成宫图...
  下载图片（宫图 1，共 12 张）...
  下载结果: 12/12
  创建宫格 1...
  ✅ 宫图 1 已生成: /Users/likeu3/.openclaw/workspace/output/grids/soe..moe..kyi_grid_1.jpg
⏳ 步骤 3/3: 上传到飞书（共 1 张宫图）...
  ✅ 上传成功: file_token_xxx (soe..moe..kyi_grid_1.jpg)

======================================================================
批处理完成
======================================================================
✅ 成功: 5
❌ 失败: 0
📊 总计: 5

📄 结果已保存: /Users/likeu3/.openclaw/workspace/output/grids/batch_batch_5_summary.json
```
