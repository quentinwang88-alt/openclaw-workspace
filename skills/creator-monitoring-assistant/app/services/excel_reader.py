#!/usr/bin/env python3
"""Excel 读取。"""

from __future__ import annotations

from pathlib import Path
from typing import Dict, List

import pandas as pd


REQUIRED_COLUMNS = [
    "达人名称",
    "联盟归因 GMV",
    "退款金额",
    "归因订单数",
    "联盟归因成交件数",
    "已退款的商品件数",
    "平均订单金额",
    "日均商品成交件数",
    "视频数",
    "直播数",
    "预计佣金",
    "已发货样品数",
]


def read_creator_weekly_excel(source_file_path: str) -> List[Dict[str, object]]:
    path = Path(source_file_path)
    if not path.exists():
        raise FileNotFoundError(f"Excel 文件不存在: {path}")

    if path.suffix.lower() == ".csv":
        df = pd.read_csv(path)
    else:
        df = pd.read_excel(path)

    df.columns = [str(col).strip() for col in df.columns]
    missing = [col for col in REQUIRED_COLUMNS if col not in df.columns]
    if missing:
        raise ValueError(f"Excel 缺少必需列: {', '.join(missing)}")

    df = df.fillna("")
    return df.to_dict(orient="records")

