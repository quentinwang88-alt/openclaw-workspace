#!/usr/bin/env python3
"""Publish structured market-insight reports to Feishu docs and notifications."""

from __future__ import annotations

from typing import Any, Dict

from src.feishu import FeishuAPIError, FeishuDocClient


class MarketInsightReportPublisher(object):
    def __init__(self, doc_client=None):
        self.doc_client = doc_client or FeishuDocClient()

    def publish(
        self,
        report_markdown: str,
        report_payload: Dict[str, Any],
        country: str,
        category: str,
        batch_date: str,
        report_output: Dict[str, Any],
    ) -> Dict[str, Any]:
        output = dict(report_output or {})
        if not bool(output.get("enabled")):
            return self._result(status="skipped", message="report_output.enabled 未开启")

        doc_title = "市场洞察报告-{country}-{category}-{batch_date}".format(
            country=country,
            category=category,
            batch_date=str(batch_date or "").replace("-", ""),
        )
        folder_token = str(output.get("parent_folder_token") or "").strip()
        domain = str(output.get("feishu_domain") or "gcngopvfvo0q.feishu.cn").strip()
        webhook_url = str(output.get("notify_webhook_url") or "").strip()
        webhook_secret = str(output.get("notify_webhook_secret") or "").strip()
        try:
            doc_info = self.doc_client.create_document(title=doc_title, folder_token=folder_token)
            self.doc_client.append_markdown(document_id=doc_info["document_id"], content=report_markdown)
            doc_url = "https://{domain}/docx/{token}".format(domain=domain, token=doc_info["doc_token"])
            notify_status = "skipped"
            notify_message = "未配置通知目标"
            target_label = ""
            if webhook_url:
                self.doc_client.send_webhook_text(
                    webhook_url=webhook_url,
                    text=self._build_notification_text(report_payload, country, category, batch_date, doc_url),
                    secret=webhook_secret,
                )
                notify_status = "sent"
                notify_message = "已通过 webhook 推送"
                target_label = "webhook"
            elif str(output.get("notify_receive_id") or "").strip() and str(output.get("notify_receive_id_type") or "").strip():
                self.doc_client.send_text_message(
                    receive_id_type=str(output.get("notify_receive_id_type") or "").strip(),
                    receive_id=str(output.get("notify_receive_id") or "").strip(),
                    text=self._build_notification_text(report_payload, country, category, batch_date, doc_url),
                )
                notify_status = "sent"
                notify_message = "已通过 IM 消息推送"
                target_label = "{type}:{target}".format(
                    type=str(output.get("notify_receive_id_type") or "").strip(),
                    target=str(output.get("notify_receive_id") or "").strip(),
                )
            return self._result(
                status="published",
                message="飞书文档已创建",
                feishu_doc_token=doc_info["doc_token"],
                feishu_doc_url=doc_url,
                notification_status=notify_status,
                notification_message=notify_message,
                notification_target=target_label,
            )
        except FeishuAPIError as exc:
            if webhook_url:
                try:
                    self.doc_client.send_webhook_text(
                        webhook_url=webhook_url,
                        text=self._build_webhook_fallback_text(report_payload, country, category, batch_date, str(exc)),
                        secret=webhook_secret,
                    )
                    return self._result(
                        status="partial",
                        message=str(exc),
                        notification_status="sent",
                        notification_message="飞书文档创建失败，已通过 webhook 推送报告摘要",
                        notification_target="webhook",
                    )
                except Exception as webhook_exc:
                    return self._result(
                        status="failed",
                        message=str(exc),
                        notification_status="failed",
                        notification_message=str(webhook_exc),
                    )
            return self._result(status="failed", message=str(exc), notification_status="failed", notification_message=str(exc))
        except Exception as exc:
            return self._result(status="failed", message=str(exc), notification_status="failed", notification_message=str(exc))

    def _build_notification_text(
        self,
        report_payload: Dict[str, Any],
        country: str,
        category: str,
        batch_date: str,
        doc_url: str,
    ) -> str:
        summary = report_payload.get("decision_summary", {}) or {}
        enter = self._summary_line(summary, "enter")
        watch = self._summary_line(summary, "watch")
        avoid = self._summary_line(summary, "avoid")
        return (
            "本批次市场洞察报告已生成\n\n"
            "国家 / 类目：{country} / {category}\n"
            "批次日期：{batch_date}\n\n"
            "决策摘要：\n"
            "值得立即进入：{enter}\n"
            "建议观察：{watch}\n"
            "建议避开：{avoid}\n\n"
            "查看完整报告：{doc_url}"
        ).format(
            country=country,
            category=category,
            batch_date=batch_date,
            enter=enter,
            watch=watch,
            avoid=avoid,
            doc_url=doc_url,
        )

    def _summary_line(self, summary: Dict[str, Any], bucket: str) -> str:
        bucket_payload = dict(summary.get(bucket) or {})
        names = list(bucket_payload.get("display_names") or [])
        total_count = int(bucket_payload.get("total_count") or 0)
        overflow = int(bucket_payload.get("overflow_count") or 0)
        if not names:
            return "无"
        text = "、".join(names)
        if overflow > 0:
            text = "{text} 等 {total} 个方向".format(text=text, total=total_count)
        return text

    def _build_webhook_fallback_text(
        self,
        report_payload: Dict[str, Any],
        country: str,
        category: str,
        batch_date: str,
        error_message: str,
    ) -> str:
        summary = report_payload.get("decision_summary", {}) or {}
        enter = self._summary_line(summary, "enter")
        watch = self._summary_line(summary, "watch")
        avoid = self._summary_line(summary, "avoid")
        return (
            "本批次市场洞察报告已生成（文档创建失败，先推送摘要）\n\n"
            "国家 / 类目：{country} / {category}\n"
            "批次日期：{batch_date}\n\n"
            "决策摘要：\n"
            "值得立即进入：{enter}\n"
            "建议观察：{watch}\n"
            "建议避开：{avoid}\n\n"
            "文档创建失败原因：{error_message}"
        ).format(
            country=country,
            category=category,
            batch_date=batch_date,
            enter=enter,
            watch=watch,
            avoid=avoid,
            error_message=error_message,
        )

    def _result(self, status: str, message: str, **kwargs) -> Dict[str, Any]:
        result = {
            "status": status,
            "message": message,
            "feishu_doc_token": "",
            "feishu_doc_url": "",
            "notification_status": "skipped",
            "notification_message": "",
            "notification_target": "",
        }
        result.update(kwargs)
        return result
