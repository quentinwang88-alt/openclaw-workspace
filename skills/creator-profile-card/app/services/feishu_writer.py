"""
飞书回写服务 — 读取/写入达人画像卡业务表。
"""
import json
import logging
from pathlib import Path
from typing import Any, Dict, List, Optional

import requests

from ..config import (
    FEISHU_APP_ID,
    FEISHU_APP_SECRET,
    FEISHU_APP_TOKEN,
    FEISHU_TABLE_ID,
)

logger = logging.getLogger(__name__)


def _resolve_feishu_credentials() -> tuple:
    """解析飞书凭证，优先环境变量，回退到 openclaw.json。"""
    app_id = FEISHU_APP_ID
    app_secret = FEISHU_APP_SECRET

    if not app_id or not app_secret:
        config_path = Path.home() / ".openclaw" / "openclaw.json"
        if config_path.exists():
            cfg = json.loads(config_path.read_text())
            ch = cfg.get("channels", {}).get("feishu", {})
            app_id = app_id or ch.get("appId", "")
            app_secret = app_secret or ch.get("appSecret", "")

    return app_id, app_secret


def _get_tenant_token() -> str:
    """获取 tenant access token。"""
    app_id, app_secret = _resolve_feishu_credentials()
    if not app_id or not app_secret:
        raise RuntimeError("未配置飞书 app_id / app_secret")

    resp = requests.post(
        "https://open.feishu.cn/open-apis/auth/v3/tenant_access_token/internal",
        json={"app_id": app_id, "app_secret": app_secret},
    )
    data = resp.json()
    if data.get("code") != 0:
        raise RuntimeError(f"获取飞书 token 失败: {data}")
    return data["tenant_access_token"]


def _headers(content_type: str = "application/json") -> Dict[str, str]:
    return {
        "Authorization": f"Bearer {_get_tenant_token()}",
        "Content-Type": content_type,
    }


def upload_attachment(
    file_path: str,
    app_token: str,
    file_name: Optional[str] = None,
) -> Dict[str, Any]:
    """上传附件到飞书，返回 file_token 等信息。

    Args:
        file_path: 本地文件路径。
        app_token: 飞书 app_token（作为 parent_node）。
        file_name: 上传后文件名（默认用原文件名）。

    Returns:
        {"file_token": "...", "name": "...", "size": ..., "type": "..."}
    """
    p = Path(file_path)
    if not p.exists():
        raise FileNotFoundError(f"文件不存在: {file_path}")

    file_size = p.stat().st_size
    name = file_name or p.name
    ext = p.suffix.lower()
    mime_map = {".jpg": "image/jpeg", ".jpeg": "image/jpeg", ".png": "image/png", ".webp": "image/webp"}
    content_type = mime_map.get(ext, "image/jpeg")

    url = "https://open.feishu.cn/open-apis/drive/v1/medias/upload_all"
    token = _get_tenant_token()

    with open(p, "rb") as fh:
        resp = requests.post(
            url,
            headers={"Authorization": f"Bearer {token}"},
            data={
                "file_name": name,
                "parent_type": "bitable_image",
                "parent_node": app_token,
                "size": str(file_size),
            },
            files={"file": (name, fh, content_type)},
        )

    data = resp.json()
    if data.get("code") != 0:
        raise RuntimeError(f"上传附件失败: {data}")
    return data.get("data", {})


def resolve_wiki_token(wiki_token: str) -> str:
    """解析 wiki token 为底层 app_token。"""
    resp = requests.get(
        "https://open.feishu.cn/open-apis/wiki/v2/spaces/get_node",
        headers=_headers(),
        params={"token": wiki_token},
    )
    data = resp.json()
    if data.get("code") != 0:
        raise RuntimeError(f"解析 wiki token 失败: {data}")
    return data["data"]["node"]["obj_token"]


def list_fields(app_token: str, table_id: str) -> List[Dict[str, Any]]:
    """列出表中所有字段。"""
    resp = requests.get(
        f"https://open.feishu.cn/open-apis/bitable/v1/apps/{app_token}/tables/{table_id}/fields",
        headers=_headers(),
        params={"page_size": 100},
    )
    data = resp.json()
    if data.get("code") != 0:
        raise RuntimeError(f"列出字段失败: {data}")
    return data.get("data", {}).get("items", [])


def create_record(
    app_token: str,
    table_id: str,
    fields: Dict[str, Any],
) -> Dict[str, Any]:
    """创建单条记录。"""
    resp = requests.post(
        f"https://open.feishu.cn/open-apis/bitable/v1/apps/{app_token}/tables/{table_id}/records",
        headers=_headers(),
        json={"fields": fields},
    )
    data = resp.json()
    if data.get("code") != 0:
        raise RuntimeError(f"创建记录失败: {data}")
    return data.get("data", {}).get("record", {})


def batch_create_records(
    app_token: str,
    table_id: str,
    records: List[Dict[str, Any]],
) -> List[Dict[str, Any]]:
    """批量创建记录。"""
    resp = requests.post(
        f"https://open.feishu.cn/open-apis/bitable/v1/apps/{app_token}/tables/{table_id}/records/batch_create",
        headers=_headers(),
        json={"records": records},
    )
    data = resp.json()
    if data.get("code") != 0:
        raise RuntimeError(f"批量创建记录失败: {data}")
    return data.get("data", {}).get("records", [])


def update_record(
    app_token: str,
    table_id: str,
    record_id: str,
    fields: Dict[str, Any],
) -> Dict[str, Any]:
    """更新单条记录。"""
    resp = requests.put(
        f"https://open.feishu.cn/open-apis/bitable/v1/apps/{app_token}/tables/{table_id}/records/{record_id}",
        headers=_headers(),
        json={"fields": fields},
    )
    data = resp.json()
    if data.get("code") != 0:
        raise RuntimeError(f"更新记录失败: {data}")
    return data.get("data", {}).get("record", {})


def _extract_creator_handle(url: str) -> str:
    """从 TikTok URL 提取 @handle。"""
    import re
    m = re.search(r'@([^/?]+)', url)
    return m.group(1).lower() if m else url.lower()


def find_record_by_url(
    app_token: str,
    table_id: str,
    creator_url: str,
) -> Optional[Dict[str, Any]]:
    """根据达人链接查找已有记录（按 @handle 模糊匹配）。"""
    target_handle = _extract_creator_handle(creator_url)

    # 拉全部记录在 Python 侧匹配（表数据量不大）
    resp = requests.get(
        f"https://open.feishu.cn/open-apis/bitable/v1/apps/{app_token}/tables/{table_id}/records",
        headers=_headers(),
        params={"page_size": 500},
    )
    data = resp.json()
    if data.get("code") != 0:
        logger.warning("查询记录失败: %s", data)
        return None
    for item in data.get("data", {}).get("items", []):
        flds = item.get("fields", {})
        url_field = flds.get("达人链接", {})
        if isinstance(url_field, dict):
            stored_url = url_field.get("link", "")
        else:
            stored_url = str(url_field)
        stored_handle = _extract_creator_handle(stored_url)
        if stored_handle == target_handle:
            return item
    return None


def write_profile_to_feishu(
    creator_url: str,
    history_relation: str,
    writable_fields: Dict[str, Any],
    cover_collage_images: Optional[List[str]] = None,
    app_token: Optional[str] = None,
    table_id: Optional[str] = None,
) -> Dict[str, Any]:
    """将画像写入飞书业务表。

    - 如果达人链接已存在，更新记录（不覆盖历史关系、当前动作）
    - 如果不存在，创建新记录
    - 上传封面拼图到记录附件字段

    Args:
        creator_url: 达人主页链接。
        history_relation: 历史关系（人工输入）。
        writable_fields: 通过校验的可写入字段。
        cover_collage_images: 封面拼图本地路径列表。
        app_token: 飞书 app_token。
        table_id: 表格 table_id。

    Returns:
        {"action": "create"/"update", "record_id": "...", ...}
    """
    app_token = app_token or FEISHU_APP_TOKEN
    table_id = table_id or FEISHU_TABLE_ID

    if not app_token or not table_id:
        raise RuntimeError("未配置 CREATOR_PROFILE_FEISHU_APP_TOKEN / CREATOR_PROFILE_FEISHU_TABLE_ID")

    # 上传封面拼图
    attachments = []
    if cover_collage_images:
        for img_path in cover_collage_images:
            try:
                att = upload_attachment(img_path, app_token)
                attachments.append(att)
            except Exception as e:
                logger.warning("封面拼图上传失败: %s → %s", img_path, e)

    # 构建待写入字段（AI 可自动写入的字段）
    fields_to_write = {}
    for key in ["活跃度", "内容类型", "画面风格", "推荐商品/品类", "沟通切入点"]:
        if key in writable_fields and writable_fields[key]:
            fields_to_write[key] = writable_fields[key]

    # 适配类目（多选，传入 list）
    if "适配类目" in writable_fields and writable_fields["适配类目"]:
        fields_to_write["适配类目"] = writable_fields["适配类目"]

    # 封面拼图附件
    if attachments:
        fields_to_write["封面拼图"] = attachments

    # 查找已有记录
    existing = find_record_by_url(app_token, table_id, creator_url)

    if existing:
        record_id = existing["record_id"]
        update_record(app_token, table_id, record_id, fields_to_write)
        return {
            "action": "update",
            "record_id": record_id,
            "fields_written": list(fields_to_write.keys()),
            "attachments_uploaded": len(attachments),
        }
    else:
        # 创建新记录，包含达人链接和历史关系
        fields_to_write["达人链接"] = {"link": creator_url, "text": creator_url}
        fields_to_write["历史关系"] = history_relation
        record = create_record(app_token, table_id, fields_to_write)
        return {
            "action": "create",
            "record_id": record.get("record_id", ""),
            "fields_written": list(fields_to_write.keys()),
            "attachments_uploaded": len(attachments),
        }
