#!/usr/bin/env python3
"""Production composition root for the V1 Lite wig replication skill.

This module owns the infrastructure adapters. Importing/building it performs no
external writes and never applies database migrations automatically.
"""

from __future__ import annotations

import base64
import io
import json
import mimetypes
import os
import sys
from concurrent.futures import ThreadPoolExecutor, as_completed
from contextlib import contextmanager
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Any
from urllib.parse import parse_qs, unquote, urlparse

import httpx
import pymysql
import requests
from openai import OpenAI
from pymysql.cursors import DictCursor


WORKSPACE_ROOT = Path(__file__).resolve().parents[3]
PACKAGE_ROOT = WORKSPACE_ROOT / "packages" / "wig_success_replication"
for value in (WORKSPACE_ROOT, PACKAGE_ROOT):
    if str(value) not in sys.path:
        sys.path.insert(0, str(value))

from workspace_support import load_repo_env  # noqa: E402

load_repo_env()

from wig_success_replication.application import WigReplicationApplication  # noqa: E402
from wig_success_replication.feishu_outbox import FeishuOutboxService  # noqa: E402
from wig_success_replication.hashing import deterministic_id, product_fact_hash  # noqa: E402
from wig_success_replication.model_run_logger import ModelRunLogger  # noqa: E402
from wig_success_replication.models import (  # noqa: E402
    Appearance,
    BatchStatus,
    MotherStatus,
    ProductFactCard,
    ProductFactStatus,
)
from wig_success_replication.mother_service import MotherInput, MotherTemplateService  # noqa: E402
from wig_success_replication.replication_service import ReplicationBatchService  # noqa: E402
from wig_success_replication.repository import (  # noqa: E402
    MySQLRepository,
    ProductFactRecord,
)
from wig_success_replication.structured_llm import StructuredResponsesClient  # noqa: E402


API_ROOT = "https://open.feishu.cn/open-apis"
DEFAULT_APP_TOKEN = "FVX3bW6Nwa18NCshPVBcohjnnRd"
DEFAULT_MOTHER_TABLE = "tblrxlscyh8DwgNo"
DEFAULT_PRODUCT_TABLE = "tblknKtWA89Ug5y1"
DEFAULT_PROMPT_TABLE = "tblGUAnE7zGLClfW"
CODEX_BASE_URL = "https://chatgpt.com/backend-api/codex"


class RuntimeConfigurationError(RuntimeError):
    pass


def _read_json(path: Path) -> dict[str, Any]:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return {}
    return value if isinstance(value, dict) else {}


def _feishu_credentials() -> tuple[str, str]:
    config_path = Path(os.environ.get("OPENCLAW_CONFIG", "~/.openclaw/openclaw.json")).expanduser()
    channel = (_read_json(config_path).get("channels") or {}).get("feishu") or {}
    app_id = str(channel.get("appId") or "").strip()
    secret = str(channel.get("appSecret") or "").strip()
    if not app_id or not secret:
        raise RuntimeConfigurationError(f"Feishu credentials are missing in {config_path}")
    return app_id, secret


def _codex_token() -> str:
    candidates = (
        Path.home() / ".codex" / "auth.json",
        Path.home() / ".openclaw" / "agents" / "main" / "agent" / "auth-profiles.json",
        Path.home() / ".hermes" / "auth.json",
    )
    for path in candidates:
        payload = _read_json(path)
        tokens = payload.get("tokens") or {}
        token = str(tokens.get("access_token") or "").strip() if isinstance(tokens, dict) else ""
        if token:
            return token
        profiles = payload.get("profiles") or {}
        if isinstance(profiles, dict):
            preferred = profiles.get("openai-codex:default") or {}
            token = str(preferred.get("access") or "").strip() if isinstance(preferred, dict) else ""
            if token:
                return token
            for profile in profiles.values():
                if isinstance(profile, dict) and str(profile.get("access") or "").strip():
                    return str(profile["access"]).strip()
        providers = payload.get("providers") or {}
        provider = providers.get("openai-codex") or {} if isinstance(providers, dict) else {}
        provider_tokens = provider.get("tokens") or {} if isinstance(provider, dict) else {}
        token = str(provider_tokens.get("access_token") or "").strip() if isinstance(provider_tokens, dict) else ""
        if token:
            return token
    raise RuntimeConfigurationError("local openai-codex OAuth access token was not found")


def _database_url() -> str:
    value = (
        os.environ.get("WIG_REPLICATION_DATABASE_URL")
        or os.environ.get("LIKEU_AI_DATABASE_URL")
        or ""
    ).strip()
    if not value:
        raise RuntimeConfigurationError("WIG_REPLICATION_DATABASE_URL or LIKEU_AI_DATABASE_URL is required")
    return value


def _connection_factory(database_url: str):
    parsed = urlparse(database_url)
    if parsed.scheme not in {"mysql", "mysql+pymysql"}:
        raise RuntimeConfigurationError("wig replication requires a mysql+pymysql database URL")
    query = parse_qs(parsed.query)
    charset = (query.get("charset") or ["utf8mb4"])[0]

    def connect():
        return pymysql.connect(
            host=parsed.hostname or "localhost",
            port=parsed.port or 3306,
            user=unquote(parsed.username or ""),
            password=unquote(parsed.password or ""),
            database=unquote(parsed.path.lstrip("/")),
            charset=charset,
            cursorclass=DictCursor,
            autocommit=False,
            connect_timeout=10,
            read_timeout=60,
            write_timeout=60,
        )

    return connect


def _proxy_url() -> str:
    explicit = os.environ.get("WIG_REPLICATION_PROXY_URL", "").strip()
    if explicit:
        return explicit
    config = _read_json(Path(os.environ.get("OPENCLAW_CONFIG", "~/.openclaw/openclaw.json")).expanduser())
    providers = ((config.get("models") or {}).get("providers") or {}) if isinstance(config, dict) else {}
    codex = providers.get("openai-codex") or {} if isinstance(providers, dict) else {}
    return str(codex.get("proxy") or "").strip() if isinstance(codex, dict) else ""


def _openai_client() -> OpenAI:
    kwargs: dict[str, Any] = {
        "api_key": _codex_token(),
        "base_url": os.environ.get("WIG_REPLICATION_CODEX_BASE_URL", CODEX_BASE_URL).rstrip("/"),
        "timeout": float(os.environ.get("WIG_REPLICATION_MODEL_TIMEOUT_SECONDS", "300")),
        "max_retries": 0,
    }
    proxy = _proxy_url()
    if proxy:
        kwargs["http_client"] = httpx.Client(proxy=proxy, timeout=kwargs["timeout"], trust_env=False)
    return OpenAI(**kwargs)


@dataclass
class FeishuClient:
    app_id: str
    app_secret: str
    access_token: str = ""
    _media_cache: dict[str, str] = field(default_factory=dict, init=False, repr=False)

    def token(self) -> str:
        if self.access_token:
            return self.access_token
        response = requests.post(
            f"{API_ROOT}/auth/v3/tenant_access_token/internal",
            json={"app_id": self.app_id, "app_secret": self.app_secret},
            timeout=30,
        )
        payload = response.json()
        if response.status_code >= 400 or payload.get("code") not in (0, "0"):
            raise RuntimeError(f"Feishu authentication failed: {payload.get('msg') or response.status_code}")
        self.access_token = str(payload["tenant_access_token"])
        return self.access_token

    def request(
        self,
        method: str,
        path: str,
        *,
        params: Any = None,
        body: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        response = requests.request(
            method,
            f"{API_ROOT}{path}",
            headers={"Authorization": f"Bearer {self.token()}", "Content-Type": "application/json"},
            params=params,
            json=body,
            timeout=60,
        )
        try:
            payload = response.json()
        except ValueError as exc:
            raise RuntimeError(f"Feishu returned non-JSON ({response.status_code})") from exc
        if response.status_code >= 400 or payload.get("code") not in (0, "0", None):
            raise RuntimeError(f"Feishu {method} {path} failed: {payload.get('code')} {payload.get('msg')}")
        return payload.get("data") or {}

    def list_records(self, app_token: str, table_id: str, *, page_size: int = 100) -> list[dict[str, Any]]:
        result: list[dict[str, Any]] = []
        page_token = ""
        while True:
            params = {"page_size": page_size}
            if page_token:
                params["page_token"] = page_token
            data = self.request(
                "GET",
                f"/bitable/v1/apps/{app_token}/tables/{table_id}/records",
                params=params,
            )
            result.extend(data.get("items") or [])
            if not data.get("has_more"):
                return result
            page_token = str(data.get("page_token") or "")

    def get_record(self, app_token: str, table_id: str, record_id: str) -> dict[str, Any]:
        data = self.request(
            "GET", f"/bitable/v1/apps/{app_token}/tables/{table_id}/records/{record_id}"
        )
        return data.get("record") or data

    def update_record(self, app_token: str, table_id: str, record_id: str, fields: dict[str, Any]) -> None:
        self.request(
            "PUT",
            f"/bitable/v1/apps/{app_token}/tables/{table_id}/records/{record_id}",
            body={"fields": fields},
        )

    def create_record(self, app_token: str, table_id: str, fields: dict[str, Any]) -> dict[str, Any]:
        return self.request(
            "POST",
            f"/bitable/v1/apps/{app_token}/tables/{table_id}/records",
            body={"fields": fields},
        )

    def attachment_data_urls(self, attachments: Any, *, limit: int = 4) -> list[str]:
        values = attachments if isinstance(attachments, list) else []
        direct: dict[str, str] = {}
        names: dict[str, str] = {}
        tokens: list[str] = []
        for item in values[:limit]:
            if not isinstance(item, dict):
                continue
            token = str(item.get("file_token") or item.get("token") or "").strip()
            url = str(item.get("tmp_download_url") or item.get("tmp_url") or item.get("url") or "").strip()
            if token:
                tokens.append(token)
                names[token] = str(item.get("name") or "")
                if url:
                    direct[token] = url
        missing_tokens = [token for token in tokens if token not in self._media_cache]
        if missing_tokens:
            params = [("file_tokens", token) for token in missing_tokens]
            try:
                data = self.request("GET", "/drive/v1/medias/batch_get_tmp_download_url", params=params)
                for item in data.get("tmp_download_urls") or []:
                    if isinstance(item, dict) and item.get("file_token") and item.get("tmp_download_url"):
                        direct[str(item["file_token"])] = str(item["tmp_download_url"])
            except Exception:
                if not direct:
                    raise
        result: list[str] = []
        for token in tokens:
            cached = self._media_cache.get(token)
            if cached:
                result.append(cached)
                continue
            url = direct.get(token)
            if not url:
                continue
            response = requests.get(
                url,
                headers={"Authorization": f"Bearer {self.token()}"},
                timeout=60,
            )
            response.raise_for_status()
            if len(response.content) > 12 * 1024 * 1024:
                raise ValueError(f"product image exceeds 12 MiB: {names.get(token) or token}")
            raw = response.content
            mime = response.headers.get("Content-Type", "").split(";", 1)[0].strip()
            try:
                from PIL import Image, ImageOps

                image = ImageOps.exif_transpose(Image.open(io.BytesIO(raw))).convert("RGB")
                image.thumbnail((1024, 1024))
                output = io.BytesIO()
                image.save(output, format="JPEG", quality=85, optimize=True)
                raw = output.getvalue()
                mime = "image/jpeg"
            except Exception:
                if not mime.startswith("image/"):
                    mime = mimetypes.guess_type(names.get(token, ""))[0] or "image/jpeg"
            data_url = f"data:{mime};base64,{base64.b64encode(raw).decode('ascii')}"
            self._media_cache[token] = data_url
            result.append(data_url)
        return result


def _relation_ids(value: Any) -> list[str]:
    if isinstance(value, str):
        return [value] if value.startswith("rec") else []
    if isinstance(value, dict):
        raw = value.get("record_ids") or value.get("link_record_ids") or []
        return _relation_ids(raw)
    if not isinstance(value, list):
        return []
    result: list[str] = []
    for item in value:
        if isinstance(item, str) and item.startswith("rec"):
            result.append(item)
        elif isinstance(item, dict):
            nested = item.get("record_ids") or item.get("link_record_ids") or []
            result.extend(_relation_ids(nested))
            candidate = str(item.get("record_id") or item.get("recordId") or item.get("id") or "")
            if candidate.startswith("rec"):
                result.append(candidate)
    return list(dict.fromkeys(result))


def _text(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, str):
        return value.strip()
    if isinstance(value, list):
        parts = []
        for item in value:
            if isinstance(item, dict):
                parts.append(str(item.get("text") or item.get("name") or ""))
            else:
                parts.append(str(item))
        return "".join(parts).strip()
    if isinstance(value, dict):
        return str(value.get("text") or value.get("name") or "").strip()
    return str(value).strip()


def _optional_count(value: Any) -> int | None:
    if value in (None, ""):
        return None
    if isinstance(value, bool):
        raise ValueError("每产品生成数（累计目标）must be an integer between 1 and 20")
    try:
        number = float(value)
    except (TypeError, ValueError) as exc:
        raise ValueError("每产品生成数（累计目标）must be an integer between 1 and 20") from exc
    if not number.is_integer() or not 1 <= number <= 20:
        raise ValueError("每产品生成数（累计目标）must be an integer between 1 and 20")
    return int(number)


def _notes_parts(notes: str) -> tuple[list[str], list[str], list[str]]:
    selling_points: list[str] = []
    proof_actions: list[str] = []
    forbidden: list[str] = []
    for raw in notes.replace("；", "\n").splitlines():
        line = raw.strip()
        if not line:
            continue
        label, separator, value = line.replace("：", ":", 1).partition(":")
        if not separator or not value.strip():
            continue
        normalized = label.strip().lower()
        items = [item.strip() for item in value.replace("，", ",").split(",") if item.strip()]
        if normalized in {"卖点", "已确认卖点", "selling points"}:
            selling_points.extend(items)
        elif normalized in {"证明动作", "展示动作", "proof actions"}:
            proof_actions.extend(items)
        elif normalized in {"禁用", "禁止", "禁用说法", "forbidden"}:
            forbidden.extend(items)
    return selling_points, proof_actions, forbidden


VARIANT_LABELS = {
    "high_fidelity_h1": "高保真·H1基准",
    "high_fidelity_h2": "高保真·H2外壳轻变",
    "high_fidelity_h3": "高保真·H3钩子轻变",
    "general_hook_rebuild": "一般复刻·G1钩子重构",
    "general_reveal_rebuild": "一般复刻·G2揭晓重构",
    "general_proof_rebuild": "一般复刻·G3证明重构",
    "general_cta_rebuild": "一般复刻·G4场景CTA重构",
    "general_pacing_rebuild": "一般复刻·G5节奏重构",
    "high_fidelity": "高还原",
    "shell_variant": "外壳变体",
    "hook_variant": "钩子变体",
    "reveal_variant": "揭示动作变体",
    "proof_variant": "证明动作变体",
    "cross_high_inheritance": "跨产品高继承",
    "cross_adaptation": "跨产品适配",
}


@dataclass
class FeishuPromptWriter:
    client: FeishuClient
    app_token: str
    table_id: str

    def apply(self, operation: str, payload: dict[str, Any]) -> None:
        if operation != "create_prompt_row":
            raise ValueError(f"unsupported Feishu outbox operation: {operation}")
        fields = {
            "复刻产品": str(payload["product_id"]),
            "版本类型": VARIANT_LABELS.get(str(payload["variant_type"]), str(payload["variant_type"])),
            "本条改动": str(payload["change_summary"]),
            "完整提示词": str(payload["full_prompt"]),
            "状态": str(payload.get("status") or "待审核"),
        }
        # The Lite table intentionally has no hidden prompt ID field. Exact field
        # comparison makes an ambiguous network retry idempotent without widening
        # the user-facing schema.
        for row in self.client.list_records(self.app_token, self.table_id):
            existing = row.get("fields") or {}
            if all(_text(existing.get(key)) == _text(value) for key, value in fields.items()):
                return
        self.client.create_record(self.app_token, self.table_id, fields)


class FeishuTaskRunner:
    ACTIONS = {
        "开始生成": "one-click",
        "处理母版": "process-mother",
        "确认母版": "confirm-mother",
        "生成复刻": "generate",
    }

    def __init__(
        self,
        *,
        client: FeishuClient,
        app_token: str,
        mother_table: str,
        product_table: str,
        repository: MySQLRepository,
        mother_service: MotherTemplateService,
        replication_service: ReplicationBatchService,
        outbox_service: FeishuOutboxService,
    ):
        self.client = client
        self.app_token = app_token
        self.mother_table = mother_table
        self.product_table = product_table
        self.repository = repository
        self.mother_service = mother_service
        self.replication_service = replication_service
        self.outbox_service = outbox_service

    @staticmethod
    def mother_id(record_id: str) -> str:
        return deterministic_id("mx_wig_mother", record_id)

    def preview_pending(
        self,
        action: str = "all",
        record_id: str | None = None,
        limit: int = 20,
        dry_run: bool = True,
    ) -> dict[str, Any]:
        rows = [self._mother_row(record_id)] if record_id else self.client.list_records(self.app_token, self.mother_table)
        eligible: list[dict[str, Any]] = []
        for row in rows:
            fields = row.get("fields") or {}
            requested = self.ACTIONS.get(_text(fields.get("动作请求")), "")
            selected_action = action if action != "all" else requested
            if not selected_action:
                continue
            if action != "all" or requested == selected_action:
                eligible.append({
                    "record_id": row.get("record_id"),
                    "mother_name": _text(fields.get("母版名称")),
                    "planned_action": selected_action,
                    "mother_status": _text(fields.get("母版状态")),
                    "replication_status": _text(fields.get("复刻状态")),
                    "selected_product_count": len(_relation_ids(fields.get("待复刻产品"))),
                })
            if len(eligible) >= limit:
                break
        return {"dry_run": bool(dry_run), "count": len(eligible), "tasks": eligible}

    def preview_outbox(self, limit: int = 100, dry_run: bool = True) -> dict[str, Any]:
        rows = self.repository.pending_outbox(limit)
        return {
            "dry_run": bool(dry_run),
            "count": len(rows),
            "outbox_ids": [row.outbox_id for row in rows],
            "operations": [row.operation for row in rows],
        }

    def run_pending(self, action: str, record_id: str | None = None, limit: int = 20) -> dict[str, Any]:
        if action != "all":
            if not record_id:
                raise ValueError(f"{action} requires a Feishu record ID")
            return self._run_one(action, record_id)
        preview = self.preview_pending(limit=limit)
        tasks = preview["tasks"]
        if not tasks:
            return {"processed": 0, "results": []}
        results: list[dict[str, Any]] = []
        # Each completed task drains the shared Feishu outbox, so task rows
        # stay serial. Independent products are parallelized within the batch.
        max_workers = min(int(os.environ.get("WIG_REPLICATION_TASK_CONCURRENCY", "1")), len(tasks))
        with ThreadPoolExecutor(max_workers=max(1, max_workers)) as executor:
            futures = {
                executor.submit(self._run_one, item["planned_action"], item["record_id"]): item
                for item in tasks
            }
            for future in as_completed(futures):
                item = futures[future]
                try:
                    results.append(future.result())
                except Exception as exc:
                    results.append({
                        "record_id": item["record_id"],
                        "status": "failed",
                        "error": f"{type(exc).__name__}: {exc}",
                    })
        return {"processed": len(results), "results": results}

    def _run_one(self, action: str, record_id: str) -> dict[str, Any]:
        if action == "one-click":
            return self._one_click(record_id)
        if action == "process-mother":
            return self._process_mother(record_id)
        if action == "confirm-mother":
            return self._confirm_mother(record_id)
        if action == "generate":
            return self._generate(record_id)
        raise ValueError(f"unsupported action: {action}")

    @contextmanager
    def _task_lock(self, record_id: str):
        factory = getattr(self.repository, "connection_factory", None)
        if not callable(factory):
            yield True
            return
        connection = factory()
        lock_name = f"wsr:{record_id}"[:64]
        acquired = False
        try:
            with connection.cursor() as cursor:
                cursor.execute("SELECT GET_LOCK(%s, 0) AS acquired", (lock_name,))
                acquired = bool((cursor.fetchone() or {}).get("acquired"))
            yield acquired
        finally:
            if acquired:
                try:
                    with connection.cursor() as cursor:
                        cursor.execute("SELECT RELEASE_LOCK(%s)", (lock_name,))
                except Exception:
                    pass
            connection.close()

    def _progress(self, record_id: str, status: str, message: str, **extra: Any) -> None:
        now = datetime.now().strftime("%H:%M:%S")
        fields: dict[str, Any] = {
            "复刻状态": status,
            "结果摘要": f"[{now}] {message}",
        }
        fields.update(extra)
        self.client.update_record(self.app_token, self.mother_table, record_id, fields)

    def _one_click(self, record_id: str) -> dict[str, Any]:
        with self._task_lock(record_id) as acquired:
            if not acquired:
                return {"record_id": record_id, "status": "skipped_locked"}
            try:
                self._progress(record_id, "校验资料", "正在校验母版、产品、人物图片和生成数量")
                row = self._mother_row(record_id)
                fields = row.get("fields") or {}
                name = _text(fields.get("母版名称"))
                script = _text(fields.get("成功脚本"))
                validation = _text(fields.get("验证说明"))
                source_ids = _relation_ids(fields.get("来源产品"))
                target_ids = _relation_ids(fields.get("待复刻产品"))
                if not name or not script or not validation or len(source_ids) != 1:
                    raise ValueError("请填写母版名称、成功脚本、验证说明，并选择唯一来源产品")
                if not target_ids:
                    raise ValueError("请至少选择一个待复刻产品")
                if not (fields.get("人物脸部参考图") or []):
                    raise ValueError("请上传人物脸部参考图")
                _optional_count(fields.get("每产品生成数"))

                self._progress(
                    record_id,
                    "处理母版",
                    "正在复用或生成母版，并执行独立审查",
                    **{"母版状态": "处理中"},
                )
                source, image_urls = self._prepare_product(self._product_row(source_ids[0]))
                result = self.mother_service.process(MotherInput(
                    mother_id=self.mother_id(record_id),
                    feishu_record_id=record_id,
                    name=name,
                    source_product_id=source.product_id,
                    source_script=script,
                    validation_notes=validation,
                    source_product_fact=source.fact,
                    source_image_urls=tuple(image_urls),
                    human_notes=_text(fields.get("特殊要求")),
                ))
                mother_record = result.record
                if mother_record.status != MotherStatus.CONFIRMED:
                    mother_record = self.mother_service.confirm(self.mother_id(record_id))
                self.client.update_record(self.app_token, self.mother_table, record_id, {
                    "母版状态": "已确认",
                    "母版摘要": mother_record.contract.human_summary,
                })
                self._progress(
                    record_id,
                    "生成提示词",
                    f"母版V{mother_record.version}已就绪，正在为{len(target_ids)}个产品生成提示词",
                )
                return self._generate(record_id)
            except Exception as exc:
                self.client.update_record(self.app_token, self.mother_table, record_id, {
                    "复刻状态": "失败",
                    "结果摘要": f"{type(exc).__name__}: {str(exc)[:1200]}",
                    "动作请求": None,
                })
                raise

    def _mother_row(self, record_id: str | None) -> dict[str, Any]:
        if not record_id:
            raise ValueError("record_id is required")
        return self.client.get_record(self.app_token, self.mother_table, record_id)

    def _product_row(self, record_id: str) -> dict[str, Any]:
        return self.client.get_record(self.app_token, self.product_table, record_id)

    def _prepare_product(self, row: dict[str, Any]) -> tuple[ProductFactRecord, list[str]]:
        fields = row.get("fields") or {}
        product_id = _text(fields.get("产品ID"))
        if not product_id:
            raise ValueError("selected product is missing 产品ID")
        if _text(fields.get("产品资料状态")) != "已确认":
            raise ValueError(f"product {product_id} is not human-confirmed")
        attachments = fields.get("产品图片") or []
        image_refs = [
            str(item.get("file_token") or item.get("token") or item.get("name") or "")
            for item in attachments if isinstance(item, dict)
        ]
        if not image_refs:
            raise ValueError(f"product {product_id} has no image")
        image_urls = self.client.attachment_data_urls(attachments)
        if not image_urls:
            raise ValueError(f"product {product_id} images could not be downloaded")
        name = _text(fields.get("产品名称"))
        if not name:
            raise ValueError(f"product {product_id} is missing 产品名称")
        notes = _text(fields.get("产品补充说明"))
        selling_points, proof_actions, forbidden = _notes_parts(notes)
        digest = product_fact_hash(product_id, image_refs, notes)
        current = self.repository.latest_product_fact(product_id)
        if current and current.source_hash == digest:
            if current.status != ProductFactStatus.CONFIRMED:
                self.repository.set_product_status(product_id, current.version, ProductFactStatus.CONFIRMED)
                current.status = ProductFactStatus.CONFIRMED
                current.fact.human_confirmed = True
            return current, image_urls
        fact = ProductFactCard(
            product_id=product_id,
            market=["MX"],
            appearance=Appearance(
                length="unknown", texture="unknown", color="unknown", bangs="unknown",
                layers="unknown", face_framing="unknown",
            ),
            confirmed_selling_points=selling_points,
            visual_proof_actions=proof_actions,
            forbidden_claims=forbidden,
            uncertain_points=["外观细节由复刻编译阶段直接读取已确认产品图片，不凭空写入事实卡"],
            evidence_notes=[value for value in (f"产品名称：{name}" if name else "", notes) if value],
            human_confirmed=True,
        )
        record = ProductFactRecord(
            product_id=product_id,
            version=1 if current is None else current.version + 1,
            source_hash=digest,
            fact=fact,
            human_summary=_text(fields.get("产品资料摘要")) or notes or name or product_id,
            status=ProductFactStatus.CONFIRMED,
        )
        self.repository.save_product_fact(record)
        return record, image_urls

    def _process_mother(self, record_id: str) -> dict[str, Any]:
        row = self._mother_row(record_id)
        fields = row.get("fields") or {}
        name = _text(fields.get("母版名称"))
        script = _text(fields.get("成功脚本"))
        validation_notes = _text(fields.get("验证说明"))
        relation = _relation_ids(fields.get("来源产品"))
        if not name or not script or not validation_notes or len(relation) != 1:
            raise ValueError("母版名称、成功脚本、验证说明和唯一来源产品均为必填")
        self.client.update_record(self.app_token, self.mother_table, record_id, {"母版状态": "处理中"})
        try:
            source, image_urls = self._prepare_product(self._product_row(relation[0]))
            result = self.mother_service.process(MotherInput(
                mother_id=self.mother_id(record_id),
                feishu_record_id=record_id,
                name=name,
                source_product_id=source.product_id,
                source_script=script,
                validation_notes=validation_notes,
                source_product_fact=source.fact,
                source_image_urls=tuple(image_urls),
                human_notes=_text(fields.get("特殊要求")),
            ))
            self.client.update_record(self.app_token, self.mother_table, record_id, {
                "母版状态": "待确认",
                "母版摘要": result.record.contract.human_summary,
                "动作请求": None,
            })
            return {
                "record_id": record_id,
                "mother_id": result.record.mother_id,
                "mother_version": result.record.version,
                "created": result.created,
                "status": "待确认",
            }
        except Exception as exc:
            self.client.update_record(self.app_token, self.mother_table, record_id, {
                "母版状态": "失败", "母版摘要": f"{type(exc).__name__}: {str(exc)[:900]}"
            })
            raise

    def _confirm_mother(self, record_id: str) -> dict[str, Any]:
        record = self.mother_service.confirm(self.mother_id(record_id))
        self.client.update_record(self.app_token, self.mother_table, record_id, {
            "母版状态": "已确认", "动作请求": None,
        })
        return {
            "record_id": record_id,
            "mother_id": record.mother_id,
            "mother_version": record.version,
            "status": "已确认",
        }

    def _generate(self, record_id: str) -> dict[str, Any]:
        row = self._mother_row(record_id)
        fields = row.get("fields") or {}
        if _text(fields.get("母版状态")) != "已确认":
            raise ValueError("mother must be human-confirmed in Feishu")
        product_record_ids = _relation_ids(fields.get("待复刻产品"))
        if not product_record_ids:
            raise ValueError("待复刻产品 is empty")
        face_attachments = fields.get("人物脸部参考图") or []
        face_reference_images = self.client.attachment_data_urls(face_attachments, limit=2)
        if not face_reference_images:
            raise ValueError("人物脸部参考图 is required before generation")
        per_product_count = _optional_count(fields.get("每产品生成数"))
        self._progress(record_id, "生成提示词", "正在读取目标产品并生成完整提示词")
        try:
            products: list[str] = []
            images: dict[str, list[str]] = {}
            preparation_errors: list[str] = []
            for product_record_id in product_record_ids:
                try:
                    product, image_urls = self._prepare_product(self._product_row(product_record_id))
                    products.append(product.product_id)
                    images[product.product_id] = image_urls
                except Exception as exc:
                    preparation_errors.append(f"{product_record_id}: {exc}")
            if not products:
                raise ValueError("no selected product has confirmed usable data: " + "; ".join(preparation_errors))
            result = self.replication_service.generate(
                mother_id=self.mother_id(record_id),
                product_ids=products,
                special_requirements=_text(fields.get("特殊要求")),
                product_image_urls=images,
                face_reference_image_urls=face_reference_images,
                per_product_count=per_product_count,
            )
            self._progress(record_id, "写入结果", f"本轮新增{result.saved_prompts}条，正在写入复刻提示词表")
            delivery = self.outbox_service.retry(limit=200)
            errors = [*preparation_errors, *result.errors]
            if delivery["failed"]:
                errors.append(f"Feishu待重试写回={delivery['failed']}")
            if result.batch.status == BatchStatus.COMPLETED and not errors:
                feishu_status = "已完成"
            elif result.saved_prompts or not result.created:
                feishu_status = "部分完成" if errors else "已完成"
            else:
                feishu_status = "失败"
            summary = (
                f"批次 {result.batch.batch_id}；本轮新增 {result.saved_prompts} 条；"
                f"失败产品 {len(result.failed_products) + len(preparation_errors)} 个；"
                # Another per-minute worker may drain part of the shared
                # outbox while this task is finishing. The completed count is
                # therefore invocation-local; only pending failures are a
                # stable operator-facing metric.
                f"飞书待重试 {delivery['failed']} 条"
            )
            if errors:
                summary += "；原因：" + " | ".join(errors)[:1200]
            self.client.update_record(self.app_token, self.mother_table, record_id, {
                "复刻状态": feishu_status,
                "结果摘要": summary,
                "动作请求": None,
            })
            return {
                "record_id": record_id,
                "batch_id": result.batch.batch_id,
                "created": result.created,
                "status": feishu_status,
                "saved_prompts": result.saved_prompts,
                "failed_products": list(result.failed_products),
                "preparation_errors": preparation_errors,
                "outbox_delivery": delivery,
            }
        except Exception as exc:
            self.client.update_record(self.app_token, self.mother_table, record_id, {
                "复刻状态": "失败", "结果摘要": f"{type(exc).__name__}: {str(exc)[:1200]}"
            })
            raise


class ProductionApplication(WigReplicationApplication):
    def __init__(self, *, runner: FeishuTaskRunner, outbox: FeishuOutboxService, connection_factory: Any):
        super().__init__(task_runner=runner, outbox_service=outbox)
        self._runner = runner
        self._connection_factory = connection_factory

    def preview_pending(self, **kwargs: Any) -> dict[str, Any]:
        return self._runner.preview_pending(**kwargs)

    def preview_feishu_outbox(self, **kwargs: Any) -> dict[str, Any]:
        return self._runner.preview_outbox(**kwargs)

    def check_runtime(self) -> dict[str, Any]:
        checks: dict[str, Any] = {
            "model": "gpt-5.6-sol",
            "reasoning_effort": "high",
            "feishu_credentials": False,
            "codex_oauth": False,
            "database": False,
            "required_tables": False,
            "replication_v12_columns": False,
        }
        errors: list[str] = []
        try:
            _feishu_credentials()
            checks["feishu_credentials"] = True
        except Exception as exc:
            errors.append(f"Feishu: {exc}")
        try:
            _codex_token()
            checks["codex_oauth"] = True
        except Exception as exc:
            errors.append(f"Codex OAuth: {exc}")
        try:
            with self._connection_factory() as connection, connection.cursor() as cursor:
                cursor.execute("SELECT 1 AS ok")
                checks["database"] = bool((cursor.fetchone() or {}).get("ok"))
                cursor.execute(
                    "SELECT COUNT(*) AS count FROM information_schema.tables "
                    "WHERE table_schema=DATABASE() AND table_name LIKE 'wsr\\_%'"
                )
                checks["required_tables"] = int((cursor.fetchone() or {}).get("count") or 0) >= 8
                cursor.execute(
                    "SELECT column_name FROM information_schema.columns "
                    "WHERE table_schema=DATABASE() AND table_name='wsr_replication_prompt' "
                    "AND column_name IN ('sequence_no','replication_mode','creative_route',"
                    "'creative_signature_json','planner_version')"
                )
                checks["replication_v12_columns"] = len(cursor.fetchall()) == 5
        except Exception as exc:
            errors.append(f"RDS: {exc}")
        checks["ok"] = all(
            checks[key]
            for key in (
                "feishu_credentials",
                "codex_oauth",
                "database",
                "required_tables",
                "replication_v12_columns",
            )
        )
        checks["errors"] = errors
        return checks


def build_application() -> ProductionApplication:
    app_id, app_secret = _feishu_credentials()
    connection_factory = _connection_factory(_database_url())
    repository = MySQLRepository(connection_factory)
    llm = StructuredResponsesClient.from_openai_client(
        _openai_client(), logger=ModelRunLogger(repository), max_retries=1
    )
    feishu = FeishuClient(app_id, app_secret)
    app_token = os.environ.get("WIG_REPLICATION_APP_TOKEN", DEFAULT_APP_TOKEN).strip() or DEFAULT_APP_TOKEN
    writer = FeishuPromptWriter(
        feishu,
        app_token,
        os.environ.get("WIG_REPLICATION_PROMPT_TABLE_ID", DEFAULT_PROMPT_TABLE).strip() or DEFAULT_PROMPT_TABLE,
    )
    outbox = FeishuOutboxService(repository, writer)
    runner = FeishuTaskRunner(
        client=feishu,
        app_token=app_token,
        mother_table=os.environ.get("WIG_REPLICATION_MOTHER_TABLE_ID", DEFAULT_MOTHER_TABLE).strip() or DEFAULT_MOTHER_TABLE,
        product_table=os.environ.get("WIG_REPLICATION_PRODUCT_TABLE_ID", DEFAULT_PRODUCT_TABLE).strip() or DEFAULT_PRODUCT_TABLE,
        repository=repository,
        mother_service=MotherTemplateService(repository, llm),
        replication_service=ReplicationBatchService(repository, llm, max_product_workers=2),
        outbox_service=outbox,
    )
    return ProductionApplication(runner=runner, outbox=outbox, connection_factory=connection_factory)
