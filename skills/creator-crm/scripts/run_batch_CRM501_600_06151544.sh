#!/usr/bin/env bash
set -euo pipefail
cd /Users/likeu3/.openclaw/workspace/skills/creator-crm
export CREATOR_CRM_OSS_PROVIDER=local
export KALODATA_403_WAIT_SECONDS=0
unset HTTP_PROXY HTTPS_PROXY http_proxy https_proxy
echo "===== $(date '+%Y-%m-%d %H:%M:%S') batch CRM501_600_06151544 start no-rds ====="
python3 -u scripts/run_excel_outreach_pipeline.py \
  --excel /Users/likeu3/Downloads/501-2500.xlsx \
  --limit 100 \
  --batch-id CRM501_600_06151544 \
  --product-name "本轮计划建联产品" \
  --product-category "轻上装" \
  --selling-points "日常好搭、适合低成本短视频展示" \
  --output-feishu-url "https://gcngopvfvo0q.feishu.cn/wiki/GniZwFTlviMlTxk4EZCcGdQNnBX?table=tblz2BY19H7xki2a&view=vewWoKmn8G" \
  --write-feishu \
  --no-rds
echo "===== $(date '+%Y-%m-%d %H:%M:%S') batch CRM501_600_06151544 done ====="
