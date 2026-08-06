#!/usr/bin/env python3
"""Create the two Feishu workbench schemas idempotently."""
from __future__ import annotations

import argparse
import sys
from pathlib import Path

SKILL_ROOT = Path(__file__).resolve().parents[1]
if str(SKILL_ROOT) not in sys.path:
    sys.path.insert(0, str(SKILL_ROOT))

from core.bitable import FeishuBitableClient, resolve_wiki_bitable_app_token  # noqa: E402
from core.feishu_url_parser import parse_feishu_bitable_url  # noqa: E402
from core.production_script_feishu import (  # noqa: E402
    OPERATION_TASK_FIELD_RENAMES,
    OPERATION_TASK_FIELDS,
    OPERATION_TASK_STATUS_OPTIONS,
    PRODUCT_TYPE_OPTIONS,
    PRODUCTION_SCRIPT_FIELDS,
    TEST_PHASE_OPTIONS,
    TOP_CATEGORY_OPTIONS,
    ensure_fields,
    ensure_single_select_options,
    rename_known_fields,
)

DEFAULT_OPERATION_URL = (
    "https://gcngopvfvo0q.feishu.cn/wiki/"
    "RJxHw0uAkiJPkSkXvMvcq3hXn5B?table=tblr8C7uvGIPBQar&view=vewWEsmd4q"
)
DEFAULT_SCRIPT_URL = (
    "https://gcngopvfvo0q.feishu.cn/wiki/"
    "KsX7w8Y8ZiJfnsk2Mtvc7xLun1f?table=tblIvHJ0nsn9WCwi&view=vewKfXc8lj"
)


def _client(url: str) -> FeishuBitableClient:
    info = parse_feishu_bitable_url(url)
    if not info:
        raise ValueError(f"无法解析飞书链接: {url}")
    app_token = info.app_token
    if "/wiki/" in url:
        app_token = resolve_wiki_bitable_app_token(app_token)
    return FeishuBitableClient(app_token, info.table_id)


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--operation-url", default=DEFAULT_OPERATION_URL)
    parser.add_argument("--script-url", default=DEFAULT_SCRIPT_URL)
    args = parser.parse_args()

    operation_client = _client(args.operation_url)
    script_client = _client(args.script_url)
    renamed = rename_known_fields(operation_client, OPERATION_TASK_FIELD_RENAMES)
    enum_updated = ensure_single_select_options(
        operation_client,
        {
            "一级类目（需填写）": TOP_CATEGORY_OPTIONS,
            "产品类型（需填写）": PRODUCT_TYPE_OPTIONS,
            "测试阶段（可选，默认初测）": TEST_PHASE_OPTIONS,
            "任务状态（需填写，仅选择待执行）": OPERATION_TASK_STATUS_OPTIONS,
        },
    )
    operation_fields = ensure_fields(
        operation_client,
        primary_field_name="任务ID",
        specs=OPERATION_TASK_FIELDS,
    )
    if renamed:
        print("运营任务表字段已重命名: " + ", ".join(renamed))
    if enum_updated:
        print("运营任务表单选枚举已更新: " + ", ".join(enum_updated))
    script_fields = ensure_fields(
        script_client,
        primary_field_name="脚本ID",
        specs=PRODUCTION_SCRIPT_FIELDS,
    )
    print(f"短视频运营任务表字段数: {len(operation_fields)}")
    print(f"原创视频生产脚本字段数: {len(script_fields)}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
