from __future__ import annotations

import re
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Optional

from .size_chart_detector import SizeChartDetectionError, SizeChartDetector


SIZE_TOKEN = re.compile(
    r"(?:^|\s|[\[【(（,/])(?:XS|S|M|L|XL|XXL|XXXL|[2-9]XL)(?:$|\s|[\]】)）,/])",
    re.IGNORECASE,
)
FIT_TERMS = ("均码", "尺码", "建议体重", "建议身高", "适穿", "斤", "Free size")
SPEC_TERMS = (
    "胸围",
    "衣长",
    "肩宽",
    "袖长",
    "腰围",
    "臀围",
    "裤长",
    "长(cm)",
    "宽(cm)",
    "高(cm)",
    "体积",
    "重量(g)",
)


def is_source_size_spec_text(text: str) -> bool:
    """Conservatively accept a source table that actually maps SKU sizes."""
    normalized = " ".join(str(text or "").split())
    if len(normalized) < 20 or len(normalized) > 6000:
        return False
    has_size_column = "尺码" in normalized
    has_size_value = bool(SIZE_TOKEN.search(normalized)) or "均码" in normalized
    has_fit_data = any(term.lower() in normalized.lower() for term in FIT_TERMS)
    has_spec_data = any(term.lower() in normalized.lower() for term in SPEC_TERMS)
    return has_size_column and has_size_value and has_fit_data and has_spec_data


@dataclass(frozen=True)
class SourceSizeChartCapture:
    path: str
    evidence: str
    cached: bool = False


class SourceSizeChartExtractor:
    """Capture 1688's structured size/spec table without disturbing Miaoshou."""

    def __init__(self, detector: Optional[SizeChartDetector] = None) -> None:
        self.detector = detector or SizeChartDetector()

    async def extract(
        self,
        miaoshou_page: Any,
        source_url: str,
        product_id: str,
        timeout_ms: int,
    ) -> SourceSizeChartCapture:
        if not source_url or "1688.com" not in source_url:
            raise SizeChartDetectionError("No usable 1688 source URL is available")
        safe_id = re.sub(r"[^0-9A-Za-z_-]", "_", product_id).strip("_")
        if not safe_id:
            raise SizeChartDetectionError("Source product ID is unavailable")
        output_dir = Path(__file__).resolve().parents[3] / "runtime" / "source_size_charts"
        output_dir.mkdir(parents=True, exist_ok=True)
        output_path = output_dir / f"{safe_id}.png"
        if output_path.is_file() and output_path.stat().st_size > 1000:
            return SourceSizeChartCapture(
                path=str(output_path),
                evidence="cached 1688 structured size/spec table",
                cached=True,
            )

        source_page = await miaoshou_page.context.new_page()
        try:
            await source_page.goto(
                source_url,
                wait_until="domcontentloaded",
                timeout=timeout_ms,
            )
            await source_page.wait_for_timeout(3000)
            final_url = str(source_page.url or "")
            if product_id and product_id not in final_url and product_id not in source_url:
                raise SizeChartDetectionError(
                    "1688 source page redirected to a different product"
                )
            candidate = await self._mark_best_candidate(source_page)
            if not candidate:
                raise SizeChartDetectionError(
                    "1688 source page has no structured table containing both a size column and size values"
                )
            locator = source_page.locator(
                '[data-miaoshou-source-size-chart="candidate"]'
            )
            await locator.scroll_into_view_if_needed(timeout=timeout_ms)
            await locator.screenshot(path=str(output_path), timeout=timeout_ms)
            selection = self.detector.detect([str(output_path)])
            return SourceSizeChartCapture(
                path=str(output_path),
                evidence=selection.evidence or "1688 structured size/spec table",
            )
        except SizeChartDetectionError:
            if output_path.exists():
                output_path.unlink()
            raise
        except Exception as exc:
            if output_path.exists():
                output_path.unlink()
            raise SizeChartDetectionError(
                f"1688 structured size-table capture failed: {exc}"
            ) from exc
        finally:
            await source_page.close()

    async def _mark_best_candidate(self, page: Any) -> bool:
        candidates = await page.locator("body *").evaluate_all(
            """
            elements => {
              const visible = element => {
                const style = window.getComputedStyle(element);
                const rect = element.getBoundingClientRect();
                return style.display !== 'none' && style.visibility !== 'hidden' &&
                  rect.width >= 300 && rect.height >= 80;
              };
              const results = [];
              for (const element of elements) {
                if (!visible(element)) continue;
                const text = (element.innerText || '').replace(/\\s+/g, ' ').trim();
                if (text.length < 20 || text.length > 6000) continue;
                if (!text.includes('尺码')) continue;
                const hasSize = /(^|[\\s【（(,\\/])(XS|S|M|L|XL|XXL|XXXL|[2-9]XL)($|[\\s】）),\\/])/i.test(text) || text.includes('均码');
                const hasSpec = /(胸围|衣长|肩宽|袖长|腰围|臀围|裤长|建议体重|建议身高|适穿|长\\(cm\\)|宽\\(cm\\)|高\\(cm\\)|体积|重量\\(g\\))/i.test(text);
                if (!hasSize || !hasSpec) continue;
                const rect = element.getBoundingClientRect();
                let score = 0;
                if (text.includes('商品件重尺')) score += 10;
                if (text.includes('包装信息')) score += 5;
                if (element.querySelector('table,[role="table"]')) score += 8;
                score += Math.min(10, (text.match(/\\b(?:XS|S|M|L|XL|XXL|XXXL|[2-9]XL)\\b/gi) || []).length);
                score -= Math.floor(text.length / 1000);
                results.push({element, score, area: rect.width * rect.height, text});
              }
              results.sort((a, b) => b.score - a.score || a.area - b.area);
              return results.slice(0, 12).map((row, index) => ({
                index,
                score: row.score,
                text: row.text
              }));
            }
            """
        )
        for row in candidates:
            if not is_source_size_spec_text(str(row.get("text") or "")):
                continue
            index = int(row["index"])
            marked = await page.locator("body *").evaluate_all(
                """
                (elements, wantedIndex) => {
                  const visible = element => {
                    const style = window.getComputedStyle(element);
                    const rect = element.getBoundingClientRect();
                    return style.display !== 'none' && style.visibility !== 'hidden' &&
                      rect.width >= 300 && rect.height >= 80;
                  };
                  const rows = [];
                  for (const element of elements) {
                    if (!visible(element)) continue;
                    const text = (element.innerText || '').replace(/\\s+/g, ' ').trim();
                    if (text.length < 20 || text.length > 6000 || !text.includes('尺码')) continue;
                    const hasSize = /(^|[\\s【（(,\\/])(XS|S|M|L|XL|XXL|XXXL|[2-9]XL)($|[\\s】）),\\/])/i.test(text) || text.includes('均码');
                    const hasSpec = /(胸围|衣长|肩宽|袖长|腰围|臀围|裤长|建议体重|建议身高|适穿|长\\(cm\\)|宽\\(cm\\)|高\\(cm\\)|体积|重量\\(g\\))/i.test(text);
                    if (!hasSize || !hasSpec) continue;
                    const rect = element.getBoundingClientRect();
                    let score = 0;
                    if (text.includes('商品件重尺')) score += 10;
                    if (text.includes('包装信息')) score += 5;
                    if (element.querySelector('table,[role="table"]')) score += 8;
                    score += Math.min(10, (text.match(/\\b(?:XS|S|M|L|XL|XXL|XXXL|[2-9]XL)\\b/gi) || []).length);
                    score -= Math.floor(text.length / 1000);
                    rows.push({element, score, area: rect.width * rect.height});
                  }
                  rows.sort((a, b) => b.score - a.score || a.area - b.area);
                  const selected = rows[wantedIndex];
                  if (!selected) return false;
                  selected.element.setAttribute('data-miaoshou-source-size-chart', 'candidate');
                  return true;
                }
                """,
                index,
            )
            if marked:
                return True
        return False
