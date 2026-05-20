---
name: refresh-remake-product-images
description: |
  从原始脚本管理表指定任务复制最新产品图片，并刷新短视频复刻流程相关表的主图/产品图。
  用于用户说“把某个产品的主图按原始脚本表任务 N 刷新到最终提示词表/产品表/产品图片表”、
  “更新复刻流程主图”、“同步原始脚本表里的产品图片到复刻表”等场景。
---

# Refresh Remake Product Images

## 作用

把原始脚本管理表里某个 `产品ID + 任务编号` 的 `产品图片` 作为权威来源，重新上传并写入短视频复刻流程里的三张表：

- 最终提示词表：`产品图片`
- 产品表：`产品主图`
- 产品图片表：`图片`

脚本会先精确定位原始脚本表记录，要求 `产品ID` 和 `任务编号` 同时匹配且只命中一条，避免找错任务。

## 使用方式

先 dry-run 看命中数量：

```bash
python3 -u skills/refresh-remake-product-images/scripts/refresh_images.py \
  --product-id "1735248298952524853" \
  --task-no "118"
```

确认后执行：

```bash
python3 -u skills/refresh-remake-product-images/scripts/refresh_images.py \
  --product-id "1735248298952524853" \
  --task-no "118" \
  --apply
```

## 默认表

脚本默认使用当前短视频复刻链路的三张目标表：

- `final-prompt`: `https://gcngopvfvo0q.feishu.cn/base/W2NhbdB2Eafp55sjXjMcoCLpnxc?table=tblalZ9WBwXyILkt&view=vewo4XqxdM`
- `product`: `https://gcngopvfvo0q.feishu.cn/base/Sl95b3FqLaNIp8slR47c7GzxnMb?table=tblbFOq4V4mqfZkW&view=vewSoOE9Mk`
- `product-image`: `https://gcngopvfvo0q.feishu.cn/base/DfPfbMxVXaYH7XscidMcYF6pnvg?table=tbluowLhwKya557l&view=vewHhNHfGE`

如用户给了新的表链接，用脚本参数覆盖：

```bash
--final-prompt-url "..."
--product-url "..."
--product-image-url "..."
--script-url "..."
```

## 操作规则

- 默认只 dry-run，不改表。
- 真实写入必须加 `--apply`。
- 源表必须唯一命中 `产品ID + 任务编号`。
- 源表图片字段默认是 `产品图片`，可用 `--source-image-field` 覆盖。
- 每张目标表会重新上传附件，再写入目标字段，不直接复用源表 `file_token`。
- 如果目标表同一产品有多条记录，会全部刷新；这对最终提示词表的多路线/多条提示词记录是预期行为。

## 常用检查

如果只想刷新某些目标表，可用：

```bash
--targets final-prompt
--targets final-prompt,product
--targets all
```
