# 批处理核心模块改进说明

## 改进内容

### 1. 视频数量处理优化
**问题**：之前固定获取 24 个视频，如果达人视频不足 24 个会导致问题。

**解决方案**：
- 改为"有多少抓多少"的策略
- 最多抓取 24 个视频
- 如果不足 24 个，则抓取所有可用视频

```python
# 修改前
fetch_count = min(len(video_ids), MAX_VIDEOS_PER_CREATOR)  # 固定 24

# 修改后
fetch_count = min(len(video_ids), MAX_VIDEOS_PER_CREATOR)  # 有多少抓多少，最多 24
```

### 2. 宫图生成逻辑优化
**问题**：第二张宫图的生成条件过于严格，需要至少 `MIN_COVER_COUNT + 12` (24) 个封面。

**解决方案**：
- 第一张宫图：使用前 12 个封面
- 第二张宫图：使用第 13-24 个封面，只要有至少 `MIN_COVER_COUNT` (12) 个就生成

```python
# 修改前
batch2 = cover_urls[12:24] if len(cover_urls) >= MIN_COVER_COUNT + 12 else []

# 修改后
batch2_urls = cover_urls[12:24] if len(cover_urls) > 12 else []
# 在生成时检查是否满足最小数量要求
if batch_idx == 2 and len(batch_urls) < MIN_COVER_COUNT:
    print(f"  ⏭️ 宫图 {batch_idx}：封面不足，跳过")
    continue
```

### 3. 添加数据叠加功能
**问题**：封面上没有显示视频播放量和带货金额数据。

**解决方案**：
- 在 `creator_data` 中添加可选的 `video_data` 字段
- 从 `video_data` 中提取播放量（views）和带货金额（revenue）
- 将这些数据传递给 `create_canvas` 方法，在封面上叠加显示

```python
# 数据结构
creator_data = {
    'record_id': 'rec001',
    'tk_handle': 'example_user',
    'video_ids': ['7615214987761454354', ...],
    'video_data': [  # 可选字段
        {'views': 1500000, 'revenue': 25000.0},
        {'views': 800000, 'revenue': 15000.0},
        ...
    ]
}
```

## 使用方法

### 基本使用（仅视频 ID）
```python
from batch_processor_core import process_creator

creator_data = {
    'record_id': 'recvdmcVxL2Q1m',
    'tk_handle': 'soe..moe..kyi',
    'video_ids': [
        '7615214987761454354',
        '7615174225762012424',
        # ... 更多视频 ID（有多少提供多少，最多 24 个）
    ]
}

result = process_creator(creator_data)
```

### 高级使用（包含播放量和带货金额）
```python
from batch_processor_core import process_creator

creator_data = {
    'record_id': 'recvdmcVxL2Q1m',
    'tk_handle': 'soe..moe..kyi',
    'video_ids': [
        '7615214987761454354',
        '7615174225762012424',
        # ... 更多视频 ID
    ],
    'video_data': [  # 与 video_ids 一一对应
        {'views': 1500000, 'revenue': 25000.0},
        {'views': 800000, 'revenue': 15000.0},
        # ... 更多视频数据
    ]
}

result = process_creator(creator_data)
```

## 生成结果

### 宫图生成规则
1. **第一张宫图**：使用前 12 个视频封面（必须至少有 12 个）
2. **第二张宫图**：使用第 13-24 个视频封面（如果有至少 12 个则生成）

### 示例场景

#### 场景 1：有 24 个视频
- 生成 2 张宫图
- 宫图 1：视频 1-12
- 宫图 2：视频 13-24

#### 场景 2：有 18 个视频
- 生成 2 张宫图
- 宫图 1：视频 1-12
- 宫图 2：视频 13-18（6 个封面，不足 12 个，跳过）
- **结果**：只生成 1 张宫图

#### 场景 3：有 15 个视频
- 生成 2 张宫图
- 宫图 1：视频 1-12
- 宫图 2：视频 13-15（3 个封面，不足 12 个，跳过）
- **结果**：只生成 1 张宫图

#### 场景 4：有 10 个视频
- 封面不足 12 个，处理失败
- **结果**：不生成宫图

## 封面数据叠加

当提供 `video_data` 时，每个视频封面底部会显示：
- **播放量**：格式化显示（如 1.5M、800K）
- **带货金额**：格式化显示（如 25.0K、15.0K）

显示效果：
```
┌─────────────┐
│             │
│   视频封面   │
│             │
├─────────────┤
│ ▶ 1.5M      │  ← 播放量
│ $ 25.0K     │  ← 带货金额
└─────────────┘
```

## 配置说明

在 [`config.py`](config.py:1) 中可以配置：

```python
# 封面数量最低要求（生成宫图至少需要的封面数）
MIN_COVER_COUNT = 12

# 每个达人最多获取的视频数
MAX_VIDEOS_PER_CREATOR = 24  # 在 batch_processor_core.py 中定义
```

## 注意事项

1. **最少封面数**：至少需要 12 个成功下载的封面才能生成第一张宫图
2. **第二张宫图**：需要至少 12 个额外的封面（即总共至少 24 个）
3. **数据对应**：如果提供 `video_data`，确保其长度与 `video_ids` 一致
4. **数据格式**：`video_data` 中的每个元素应包含 `views` 和 `revenue` 字段

## 错误处理

### 封面不足
```python
{
    'status': 'failed',
    'reason': 'insufficient_covers',
    'tk_handle': 'example_user'
}
```

### 下载失败
```python
{
    'status': 'failed',
    'reason': 'all_grids_failed',
    'tk_handle': 'example_user'
}
```

### 成功
```python
{
    'status': 'success',
    'tk_handle': 'example_user',
    'record_id': 'rec001',
    'grid_paths': ['/path/to/grid_1.jpg', '/path/to/grid_2.jpg'],
    'file_tokens': ['file_token_1', 'file_token_2'],
    'grids_generated': 2
}
```

## 相关文件

- [`batch_processor_core.py`](batch_processor_core.py:1) - 核心处理逻辑
- [`batch_runner.py`](batch_runner.py:1) - 批处理运行器
- [`batch_data.json`](batch_data.json:1) - 批处理数据
- [`config.py`](config.py:1) - 配置文件
- [`skills/creator-crm/core/image_processor.py`](skills/creator-crm/core/image_processor.py:1) - 图片处理模块
- [`skills/creator-crm/core/data_fetchers.py`](skills/creator-crm/core/data_fetchers.py:1) - 数据获取模块
