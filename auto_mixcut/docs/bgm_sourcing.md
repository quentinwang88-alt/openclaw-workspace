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
