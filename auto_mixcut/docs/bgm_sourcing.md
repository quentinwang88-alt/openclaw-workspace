# BGM Sourcing Guide

## 推荐路径

### 1. 最稳妥：付费商用音乐库

适合正式批量发布和广告投放。

- Epidemic Sound Business / Pro
- Artlist Business
- Uppbeat Business

优点：授权链更清楚，适合外部剪辑软件先把音乐嵌入视频，再上传 TikTok。

### 2. 免费试用：Pixabay Music

适合早期测试和低预算素材池。

使用要求：

- 只下载音乐分类里的曲目。
- 保留曲目页 URL。
- 保留文件名。
- 保留下载日期。
- 记录到 `assets/bgm/LICENSES.csv`。

注意：免费库可能存在 Content ID 或投稿人权属不清的风险，适合测试，不适合作为长期唯一来源。

### 3. TikTok Commercial Music Library

适合最终发布时在 TikTok 内选择商业音乐。

注意：

- CML 是给 TikTok 平台内使用的商业音乐库。
- 如果我们在本地 FFmpeg 渲染时直接嵌入外部下载音乐，要确保你另有离线同步授权。
- 如果只为了 TikTok 发布，可以输出静音/低音量版本，发布时在 TikTok 里添加 CML 音乐。

## 不建议

- 不要从热门视频里扒音频。
- 不要从 Spotify / YouTube / 抖音 / TikTok 普通音乐库下载后嵌入商品视频。
- 不要使用无法证明授权来源的 MP3。

## 导入流程

1. 下载或购买 BGM。
2. 把文件放入 `assets/bgm/`。
3. 在 `assets/bgm/LICENSES.csv` 新增一行授权记录。
4. 重新运行 render，系统会自动使用目录里的音频。

## TikTok 链接提取（仅作候选素材）

`BGM素材库` 支持填写 `TK来源链接`，再把 `提取状态` 设为 `待提取`。运行：

```bash
python3 scripts/ingest_tiktok_bgm.py
```

程序会优先下载视频关联的独立 TikTok 音乐资产并标准化为 M4A，再把文件写回
现有 `音频文件` 字段。用户原声或无法取得独立音乐、只能回退到整条视频音轨时，
会标记为 `需人工处理`；正式音乐资产标记为 `已完成`。

本机通过 `com.likeu3.auto-mixcut-tiktok-bgm-ingest` LaunchAgent 每 10 分钟巡检一次，
单轮最多处理 10 条；填写链接并设为 `待提取` 后无需手工运行命令。
巡检同时会把 `国家` 与 `是否优先使用` 同步进推荐数据库。

推荐时先读取产品目标国家，排序规则固定为：目标国家且勾选 `是否优先使用`、
目标国家的其他可用 BGM；只有目标国家没有任何可用 BGM 时，才回退到全库评分。
国家字段支持中文名称和 `VN`、`TH` 等国家代码，入库时会自动标准化。

授权字段继续保留用于素材记录，但不会参与推荐池过滤或推荐评分。新提取的 TikTok
音轨也不会再由程序自动写成 `授权状态=限制`、`状态=待授权确认`。
