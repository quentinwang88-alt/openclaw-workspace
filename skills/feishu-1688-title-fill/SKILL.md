---
name: feishu-1688-title-fill
description: |
  Fill a Feishu bitable `产品标题` field from `采购链接` 1688 product pages by
  reusing the user's logged-in Google Chrome session and writing back in safe
  batches. Use this when the user asks to“抓1688标题”“回填产品标题”“补采购链接
  对应的商品名”“把飞书表里的 1688 标题补齐”, especially when direct curl,
  headless Playwright, or aggressive scraping is likely to trigger 1688 captcha
  or anti-bot checks.
---

# Feishu 1688 Title Fill

Use this skill when the user wants OpenClaw to read a Feishu table, fetch real
1688 product titles from `采购链接`, and write them back into `产品标题` without
manually running several shell commands.

This is the right skill for requests like:

- “帮我把这个飞书表里的 1688 标题补齐”
- “根据采购链接回填产品标题”
- “抓一下 1688 商品名并写回飞书”
- “把空着的产品标题都补上”

## What this skill does

1. Reads the target Feishu bitable from the URL the user provides
2. Counts rows whose `采购链接` contains `1688.com` and whose `产品标题` is blank
3. Uses the user's already logged-in `Google Chrome` to quietly open the 1688
   pages in background tabs
4. Extracts the real product title from each page
5. Writes titles back to Feishu in small safe batches
6. Re-checks Feishu after every batch and stops when pending rows reach `0`

## How to run

Use the bundled wrapper script:

`scripts/run_fill.py`

Required input:

- `--feishu-url "https://...feishu.cn/wiki/...?...table=..."`

Default behavior:

1. count pending rows whose `采购链接` contains `1688.com` and whose `产品标题` is blank
2. run the quiet Chrome worker in batches of `10`
3. re-check Feishu after every batch
4. stop when pending rows reach `0`

## Example

```bash
python3 /Users/likeu3/.openclaw/workspace/skills/feishu-1688-title-fill/scripts/run_fill.py \
  --feishu-url "https://gcngopvfvo0q.feishu.cn/wiki/XXX?table=tblYYY&view=vewZZZ"
```

Useful flags:

- `--batch-size 5`
- `--max-batches 2`
- `--sleep 0.5`
- `--chrome-wait-seconds 30`
- `--link-field 采购链接`
- `--title-field 产品标题`

## Operating rules

- Always prefer the quiet Chrome path for production writeback.
- Do not start with raw `curl`, headless browser scraping, or aggressive bulk
  requests when the user wants the real table updated.
- Assume the user may already be logged into 1688 in Chrome; verify with one
  sample row if the login state is uncertain.
- Keep Chrome quiet:
  do not activate or foreground the window unless the user explicitly asks.
- Process in small batches.
  Default to `10`; reduce to `5` if 1688 starts slowing down or showing
  captcha/login pages.
- Re-count pending Feishu rows after every batch instead of assuming updates
  succeeded.
- Stop immediately if one batch makes `0` progress, then inspect whether Chrome
  lost login state or 1688 started showing captcha.

## Recovery rules

- If titles suddenly come back as empty, test one row first.
- If Chrome shows `验证码拦截` or redirects to login, ask the user to complete the
  verification in Chrome, then rerun the wrapper.
- If only a few rows remain, it is acceptable to rerun with a smaller
  `--batch-size`.
- Always finish with a final Feishu count check and report:
  `with_title`, `missing`.

## Scripts

- `scripts/run_fill.py`
  Preferred entry point. Wraps the worker in safe batches and verifies progress.

## Notes

- The wrapper uses the existing worker at:
  `/Users/likeu3/.openclaw/workspace/skills/1688-title-fetcher/fill_feishu_1688_titles.py`
- Feishu access is resolved from:
  `/Users/likeu3/.openclaw/workspace/skills/hair-style-review/core/feishu.py`
- This skill is intentionally narrow:
  it is for 1688 product title backfill into Feishu tables, not for full
  supplier scraping or price/image enrichment.
