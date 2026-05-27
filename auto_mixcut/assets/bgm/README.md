# AutoMixcut BGM Library

把可商用 BGM 文件放到本目录或子目录，渲染时系统会优先使用这里的音频文件。

支持格式：

- `.mp3`
- `.wav`
- `.m4a`
- `.aac`

命名建议：

```text
market_category_mood_bpm_source_trackid.ext
vn_hair_accessories_light_100_pixabay_123456.mp3
```

授权要求：

1. 只放确认可商用的音乐。
2. 每首歌必须在 `LICENSES.csv` 登记来源、授权类型、下载链接和下载日期。
3. 不要直接放抖音、TikTok、Spotify、YouTube 上听到的热门歌。
4. 如果来自 TikTok Commercial Music Library，建议发布时在 TikTok 内添加，不建议离线下载后嵌入成片。
5. 如果来自 Epidemic Sound / Artlist / Uppbeat 等付费库，需要保留账号、套餐、授权截图或 license 文件。
6. 如果来自 Pixabay，保留曲目页 URL、文件名和 Pixabay Content License 证明。

当前没有正式 BGM 时，系统会自动生成 `test_soft_bgm_15s.m4a` 作为测试占位音轨。它只用于流程测试，不建议作为最终发布音乐。
