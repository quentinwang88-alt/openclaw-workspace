#!/bin/bash
# 试点 B 二进制下载 v2：只用 GitHub 直连（镜像两次被 sha256 拦下，弃用）。
# 断点续传仅限同一来源，跨源不混用；结果写 DOWNLOAD_STATUS.txt。
cd "$(dirname "$0")/../third_party/xiaohongshu-mcp" || exit 1
SHA_FILE="$PWD/expected_sha256.txt"   # 校验和存实验室目录，不放 /tmp
STATUS=DOWNLOAD_STATUS.txt
BASE="https://github.com/xpzouying/xiaohongshu-mcp/releases/download/v2.5.0"

for f in xiaohongshu-login-darwin-arm64 xiaohongshu-mcp-darwin-arm64; do
  expected=$(grep "^$f|" "$SHA_FILE" | cut -d'|' -f2 | sed 's/^sha256://')
  # 幂等：最终文件已在且哈希正确 → 跳过
  if [ -f "$f" ] && [ "$(shasum -a 256 "$f" | cut -d' ' -f1)" = "$expected" ]; then
    echo "✓ $f 已就位（历史校验通过，跳过）" >> "$STATUS"
    continue
  fi
  rm -f "$f.part"
  curl -sfL -C - --retry 3 --max-time 3000 -o "$f.part" "$BASE/$f"
  actual=$(shasum -a 256 "$f.part" 2>/dev/null | cut -d' ' -f1)
  if [ "$actual" = "$expected" ]; then
    mv "$f.part" "$f"; chmod +x "$f"
    xattr -d com.apple.quarantine "$f" 2>/dev/null
    echo "✓ $f 下载完成且 sha256 校验通过（源: github 直连）" >> "$STATUS"
  else
    echo "✗ $f 未完成或校验不符（保留 .part 供同源续传）" >> "$STATUS"
  fi
done
echo "== 完成时间 $(date '+%F %T') ==" >> "$STATUS"
