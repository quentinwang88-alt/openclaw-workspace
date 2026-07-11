#!/bin/zsh
set -euo pipefail
cd /Users/likeu3/.openclaw/workspace/skills/creator-crm
export KALODATA_403_WAIT_SECONDS=0
python3 -u scripts/run_excel_outreach_pipeline.py \
  --excel /Users/likeu3/Downloads/501-2500.xlsx \
  --limit 100 \
  --batch-id CRM501_600_0615R2 \
  --max-videos 12 \
  --max-grids 1 \
  --no-rds \
  --include-manual \
  --write-feishu \
  --output-feishu-url 'https://gcngopvfvo0q.feishu.cn/wiki/GniZwFTlviMlTxk4EZCcGdQNnBX?table=tblz2BY19H7xki2a&view=vewWoKmn8G' \
  --product-name '本轮计划建联产品' \
  --product-category '轻上装' \
  --selling-points '日常好搭、适合低成本短视频展示'
