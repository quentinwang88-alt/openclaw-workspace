# Video Structure Router

薄结构调用层，只负责：读取 `sd_*`、归一化候选、按生产能力筛选、生成结构方向合同、校验合同、记录 `sr_*` 血缘。

- 不写入或修改任何 `sd_*` 表。
- `PROMPT_ONLY` 产生的镜头数始终标为 `UNAVAILABLE`。
- `S1-S4` 仅是原创流程的兼容输出槽位，不是固定内容方向。
- 每个母体脚本绑定一个结构合同；轻变体继承母体结构。
- 可复现口径：request + policy_version + random_seed + data_snapshot。

当前原创流程能力仍要求 4–6 个镜头，因此路由器会过滤明确不兼容的单镜到底/超多镜头结构。后续放宽原创脚本 Schema 后，只需修改调用方 capability，不需要改结构库。

只读预览：

```bash
PYTHONPATH=packages/video_structure_router python3 -m video_structure_router.cli \
  --country TH --category 配饰 --product-type 发饰 --count 4
```

只有显式增加 `--write` 才会写入 `sr_*`；命令永远不写 `sd_*`。
