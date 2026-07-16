from __future__ import annotations

import html
from pathlib import Path
from typing import Any

from .database import LightTryonDB


def _value(value: Any) -> str:
    if value is None:
        return ""
    return html.escape(str(value))


def export_review_html(db: LightTryonDB, output_path: str | Path, *, product_id: str | None = None) -> dict[str, Any]:
    jobs = db.list_jobs(product_id=product_id, generation_status="success")
    cards: list[str] = []
    for job in jobs:
        context = db.get_job_context(job["job_id"])
        video = Path(str(job.get("output_video_path") or "")).expanduser()
        video_uri = video.resolve().as_uri() if video.is_file() else ""
        qc = job.get("qc_result") or {}
        total = qc.get("total_score", "-")
        subtitle = (job.get("prompt_payload") or {}).get("subtitle_plan") or {}
        subtitle_text = " / ".join(str(cue.get("text") or "") for cue in subtitle.get("cues") or [])
        player = f'<video controls preload="metadata" src="{html.escape(video_uri)}"></video>' if video_uri else '<div class="missing">视频文件不存在</div>'
        cards.append(
            f"""
            <article class="card">
              {player}
              <div class="meta">
                <h2>{_value(job['job_id'])}</h2>
                <p><b>商品</b> {_value(context['product'].get('product_name'))}</p>
                <p><b>组合</b> {_value(job['scene_id'])} · {_value(job['action_id'])} · {_value(job['styling_id'])}</p>
                <p><b>字幕</b> {_value(subtitle_text)}</p>
                <p><b>QC</b> <span class="status {_value(job['qc_status'])}">{_value(job['qc_status'])}</span> · {_value(total)} 分</p>
                <p><b>人工操作</b> 使用 CLI：<code>review-set --job-id {_value(job['job_id'])} --decision passed|failed</code></p>
              </div>
            </article>
            """
        )
    page = f"""<!doctype html>
<html lang="zh-CN"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<title>AI 轻量试穿视频复核台</title>
<style>
body{{margin:0;background:#f5f4ef;color:#24231f;font:15px/1.55 -apple-system,BlinkMacSystemFont,"PingFang SC",sans-serif}}
header{{position:sticky;top:0;background:#fff9;padding:18px 28px;backdrop-filter:blur(12px);border-bottom:1px solid #dedbd0;z-index:2}}
h1{{margin:0;font-size:22px}} main{{display:grid;grid-template-columns:repeat(auto-fill,minmax(320px,1fr));gap:20px;padding:24px}}
.card{{background:white;border:1px solid #dedbd0;border-radius:16px;overflow:hidden;box-shadow:0 8px 26px #35321f12}}
video,.missing{{display:block;width:100%;aspect-ratio:9/16;max-height:520px;background:#171717;object-fit:contain}}
.missing{{display:grid;place-items:center;color:#aaa}} .meta{{padding:16px}} h2{{font-size:15px;word-break:break-all;margin:0 0 12px}}
p{{margin:7px 0}} b{{display:inline-block;min-width:52px;color:#666}} code{{font-size:11px;word-break:break-all}}
.status{{padding:2px 8px;border-radius:999px;background:#eee}} .passed{{background:#dcf6df;color:#176727}} .failed{{background:#ffe0dc;color:#8a2016}} .manual_review{{background:#fff0c8;color:#765300}}
</style></head><body><header><h1>AI 轻量试穿视频复核台</h1><div>{len(jobs)} 条已生成视频</div></header><main>{''.join(cards) or '<p>暂无已生成视频</p>'}</main></body></html>"""
    path = Path(output_path).expanduser().resolve()
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(page, encoding="utf-8")
    return {"output_path": str(path), "jobs": len(jobs)}


def set_manual_review(db: LightTryonDB, job_id: str, decision: str, note: str = "") -> dict[str, Any]:
    if decision not in {"passed", "failed"}:
        raise ValueError("人工复核 decision 必须是 passed 或 failed")
    job = db.get_job(job_id)
    if not job:
        raise KeyError(f"找不到任务: {job_id}")
    current = job.get("qc_result") or {}
    current["manual_review"] = {"decision": decision, "note": note}
    current["decision"] = decision
    db.apply_qc(job_id, decision, current)
    return {"job_id": job_id, "qc_status": decision, "note": note}
