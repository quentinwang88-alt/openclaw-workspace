#!/usr/bin/env python3
"""LLM-backed tagging for Market Insight v1."""

from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Dict, Iterable, Iterator, List, Tuple

from src.feishu import build_bitable_client
from src.hermes_analyzer import HermesAnalyzer
from src.market_insight_models import MarketInsightProductTag, ProductRankingSnapshot
from src.models import HermesOutputError
from src.table_adapter import TableAdapter


class MarketInsightAnalyzer(object):
    def __init__(self, skill_dir, hermes_bin=None, timeout_seconds=None, command_runner=None):
        self.base = HermesAnalyzer(
            skill_dir=skill_dir,
            hermes_bin=hermes_bin,
            timeout_seconds=timeout_seconds,
            command_runner=command_runner,
        )
        self.shared = TableAdapter()
        self._lazy_image_clients = {}

    def tag_product(self, snapshot: ProductRankingSnapshot, taxonomy: Dict[str, object]) -> MarketInsightProductTag:
        prepared_images = self._prepare_product_images(snapshot)
        if not prepared_images:
            return self._invalid_sample("缺少可读图片，按无效样本处理")
        payload = {
            "category": snapshot.category,
            "product_name": snapshot.product_name,
            "shop_name": snapshot.shop_name,
            "price_min": snapshot.price_min,
            "price_max": snapshot.price_max,
            "price_mid": snapshot.price_mid,
            "raw_category": snapshot.raw_category,
            "images": prepared_images,
            "taxonomy": taxonomy,
        }
        try:
            response_payload = self.base._run_prompt(
                prompt_name="market_insight_product_tagging_prompt_v1.txt",
                payload=payload,
                product_images=prepared_images,
            )
            return self.validate_tagging_result(response_payload, taxonomy=taxonomy)
        except HermesOutputError as exc:
            if self._looks_like_image_preparation_error(exc):
                return self._invalid_sample("图片不可读，按无效样本处理")
            raise

    def tag_products(
        self,
        snapshots: Iterable[ProductRankingSnapshot],
        taxonomy: Dict[str, object],
        max_workers: int = 1,
    ) -> List[MarketInsightProductTag]:
        items = list(snapshots)
        results: List[MarketInsightProductTag] = [None] * len(items)  # type: ignore[assignment]
        for index, tag in self.iter_tag_products(items, taxonomy=taxonomy, max_workers=max_workers):
            results[index] = tag
        return results

    def iter_tag_products(
        self,
        snapshots: Iterable[ProductRankingSnapshot],
        taxonomy: Dict[str, object],
        max_workers: int = 1,
    ) -> Iterator[Tuple[int, MarketInsightProductTag]]:
        items = list(snapshots)
        if max(1, max_workers) <= 1:
            for index, snapshot in enumerate(items):
                yield index, self._tag_product_safely(snapshot, taxonomy)
            return

        with ThreadPoolExecutor(max_workers=max(1, max_workers)) as executor:
            futures = {
                executor.submit(self._tag_product_safely, snapshot, taxonomy): index
                for index, snapshot in enumerate(items)
            }
            for future in as_completed(futures):
                yield futures[future], future.result()

    def validate_tagging_result(self, payload: Dict[str, object], taxonomy: Dict[str, object]) -> MarketInsightProductTag:
        if not isinstance(payload, dict):
            raise HermesOutputError("单商品打标结果必须是 JSON object")
        is_valid_sample = payload.get("is_valid_sample")
        if not isinstance(is_valid_sample, bool):
            raise HermesOutputError("is_valid_sample 必须是布尔值")

        style_options = {str(item) for item in (taxonomy.get("style_cluster") or taxonomy.get("style_tag_main") or [])}
        form_options = {str(item) for item in (taxonomy.get("product_form") or taxonomy.get("product_form_or_result") or [])}
        element_options = {str(item) for item in taxonomy.get("element_tags") or []}
        value_options = {str(item) for item in (taxonomy.get("value_points") or taxonomy.get("buying_motives") or [])}
        scene_options = {str(item) for item in taxonomy.get("scene_tags") or []}
        length_options = {str(item) for item in (taxonomy.get("length_form") or ["other"])}

        style_cluster = self._require_string(payload, "style_cluster", fallback_key="style_tag_main")
        if style_cluster not in style_options:
            raise HermesOutputError("style_cluster 不在 taxonomy 中: {value}".format(value=style_cluster))

        product_form = self._require_string(payload, "product_form", fallback_key="product_form_or_result")
        if product_form not in form_options:
            raise HermesOutputError("product_form 不在 taxonomy 中: {value}".format(value=product_form))
        length_form = self._require_optional_string(payload, "length_form", default="other")
        if length_form not in length_options:
            raise HermesOutputError("length_form 不在 taxonomy 中: {value}".format(value=length_form))

        style_tags_secondary = self._validate_string_array(payload.get("style_tags_secondary"), style_options, "style_tags_secondary", max_items=2)
        element_tags = self._validate_string_array(payload.get("element_tags"), element_options, "element_tags", max_items=4)
        value_points = self._validate_string_array(payload.get("value_points", payload.get("buying_motives")), value_options, "value_points", max_items=3)
        scene_tags = self._validate_string_array(payload.get("scene_tags"), scene_options, "scene_tags", max_items=3)
        reason_short = self._require_string(payload, "reason_short")
        if len(reason_short) > 40:
            raise HermesOutputError("reason_short 长度超过 40")

        return MarketInsightProductTag(
            is_valid_sample=is_valid_sample,
            style_cluster=style_cluster,
            style_tags_secondary=style_tags_secondary,
            product_form=product_form,
            length_form=length_form,
            element_tags=element_tags,
            value_points=value_points,
            scene_tags=scene_tags,
            reason_short=reason_short,
        )

    def _validate_string_array(self, value, allowed_values, field_name: str, max_items: int) -> List[str]:
        if value is None:
            return []
        if not isinstance(value, list):
            raise HermesOutputError("{field} 必须是数组".format(field=field_name))
        results = []
        seen = set()
        for item in value:
            if not isinstance(item, str):
                raise HermesOutputError("{field} 内元素必须是字符串".format(field=field_name))
            text = item.strip()
            if not text:
                continue
            if text not in allowed_values:
                raise HermesOutputError("{field} 不在 taxonomy 中: {value}".format(field=field_name, value=text))
            if text in seen:
                continue
            seen.add(text)
            results.append(text)
            if len(results) >= max_items:
                break
        return results

    def _require_string(self, payload: Dict[str, object], key: str, fallback_key: str = "") -> str:
        value = payload.get(key)
        if value is None and fallback_key:
            value = payload.get(fallback_key)
        if not isinstance(value, str):
            raise HermesOutputError("{key} 必须是字符串".format(key=key))
        text = value.strip()
        if not text:
            raise HermesOutputError("{key} 不能为空".format(key=key))
        return text

    def _require_optional_string(self, payload: Dict[str, object], key: str, default: str = "") -> str:
        value = payload.get(key)
        if value is None:
            return str(default or "").strip()
        if not isinstance(value, str):
            raise HermesOutputError("{key} 必须是字符串".format(key=key))
        text = value.strip()
        return text or str(default or "").strip()

    def _tag_product_safely(self, snapshot: ProductRankingSnapshot, taxonomy: Dict[str, object]) -> MarketInsightProductTag:
        try:
            return self.tag_product(snapshot, taxonomy)
        except Exception:
            return self._invalid_sample("打标异常，按无效样本处理")

    def _invalid_sample(self, reason_short: str) -> MarketInsightProductTag:
        return MarketInsightProductTag(
            is_valid_sample=False,
            style_cluster="other",
            style_tags_secondary=[],
            product_form="other",
            length_form="other",
            element_tags=["other"],
            value_points=["other"],
            scene_tags=["other"],
            reason_short=reason_short[:40],
        )

    def _looks_like_image_preparation_error(self, exc: Exception) -> bool:
        message = str(exc)
        return "图片字段存在，但未能准备 Hermes 可读图片" in message or "缺少产品图片" in message

    def _prepare_product_images(self, snapshot: ProductRankingSnapshot) -> List[str]:
        existing = [str(item or "").strip() for item in snapshot.product_images if str(item or "").strip()]
        if existing and not any(self._needs_lazy_resolution(item) for item in existing):
            return existing

        raw_value = snapshot.raw_product_images
        if raw_value is None:
            return [item for item in existing if str(item or "").strip()]

        client = self._get_lazy_image_client(snapshot)
        resolved = self.shared._normalize_images(raw_value, client=client)
        return [item for item in resolved if str(item or "").strip()]

    def _get_lazy_image_client(self, snapshot: ProductRankingSnapshot):
        source_feishu_url = str(snapshot.source_feishu_url or "").strip()
        source_app_token = str(snapshot.source_app_token or "").strip()
        source_table_id = str(snapshot.source_table_id or "").strip()
        if not source_feishu_url and not (source_app_token and source_table_id):
            return None
        cache_key = (source_feishu_url, source_app_token, source_table_id)
        if cache_key not in self._lazy_image_clients:
            self._lazy_image_clients[cache_key] = build_bitable_client(
                feishu_url=source_feishu_url,
                app_token=source_app_token,
                bitable_table_id=source_table_id,
            )
        return self._lazy_image_clients[cache_key]

    def _needs_lazy_resolution(self, image_ref: str) -> bool:
        text = str(image_ref or "").strip()
        return text.startswith("feishu-file-token:") or "/open-apis/drive/v1/medias/batch_get_tmp_download_url" in text
