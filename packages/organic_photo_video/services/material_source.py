"""素材只读适配器（自动图文供稿 Phase 1，方案 §五）。

以只读方式访问小红书素材实验室的 SQLite 素材库与图片目录，
对外暴露「统一素材包」，屏蔽实验室内部表结构。本模块：

- 不修改素材库（mode=ro 连接）；
- 不 import 实验室代码；
- 不做任何网络/模型调用。

红线：所有素材 ``authorization`` 均为 ``reference_only``（仅作生成参考，
水印原图或轻度改图不得进入发布产物，见预研报告红线与方案 §十四）。
"""
from __future__ import annotations

import hashlib
import os
import sqlite3
from dataclasses import dataclass, field
from pathlib import Path
from typing import List, Optional

DEFAULT_LIBRARY_DB = (
    "/Users/likeu3/.openclaw/workspace/labs/xhs-material-lab/var/material_library.sqlite3"
)
DEFAULT_IMAGES_ROOT = "/Users/likeu3/.openclaw/workspace/labs/xhs-material-lab/out"

_IMAGE_SUFFIXES = {".jpg", ".jpeg", ".png", ".webp"}


class MaterialSourceError(RuntimeError):
    pass


@dataclass
class MaterialImage:
    seq: int
    path: Path
    sha256: str
    exists: bool = True

    @property
    def is_image_format(self) -> bool:
        return self.path.suffix.lower() in _IMAGE_SUFFIXES


@dataclass
class MaterialPackage:
    """统一素材包：一篇笔记的完整可引用形态。"""

    note_id: str
    source_url: str
    origin: str                      # manual / search
    theme: str                       # 采集分类
    title: str
    description: str
    author_nickname: str
    like_count: Optional[int]
    collected_count: Optional[int]
    review_status: str               # selected / pending_review / rejected
    authorization: str               # 目前恒为 reference_only
    fetch_status: str
    expected_image_count: int        # 原始下载地址数（未知为 0）
    images: List[MaterialImage] = field(default_factory=list)

    @property
    def complete(self) -> bool:
        """下载完整性：抓取成功、有图、全部文件在盘且数量对齐。"""
        if self.fetch_status != "fetched" or not self.images:
            return False
        if any(not img.exists or not img.is_image_format for img in self.images):
            return False
        if self.expected_image_count and len(self.images) != self.expected_image_count:
            return False
        return True

    @property
    def version_fingerprint(self) -> str:
        """素材版本指纹：note_id + 有序图片 sha256；图片集合变化即失效缓存。"""
        payload = self.note_id + "|" + "|".join(
            f"{img.seq}:{img.sha256}" for img in self.images
        )
        return hashlib.sha256(payload.encode("utf-8")).hexdigest()


class MaterialSource:
    """素材库只读访问层。"""

    def __init__(self, db_path: Optional[str] = None, images_root: Optional[str] = None):
        self.db_path = Path(
            db_path or os.environ.get("OPV_MATERIAL_LIBRARY_DB") or DEFAULT_LIBRARY_DB
        )
        self.images_root = Path(
            images_root or os.environ.get("OPV_MATERIAL_IMAGES_ROOT") or DEFAULT_IMAGES_ROOT
        )
        if not self.db_path.exists():
            raise MaterialSourceError(f"素材库不存在：{self.db_path}")

    def _connect(self) -> sqlite3.Connection:
        conn = sqlite3.connect(f"file:{self.db_path}?mode=ro", uri=True)
        conn.row_factory = sqlite3.Row
        return conn

    def _load_images(self, conn: sqlite3.Connection, note_pk: int) -> List[MaterialImage]:
        rows = conn.execute(
            "SELECT seq, file_path, sha256 FROM note_images WHERE note_pk = ? ORDER BY seq",
            (note_pk,),
        ).fetchall()
        images: List[MaterialImage] = []
        for row in rows:
            path = self.images_root / row["file_path"]
            images.append(
                MaterialImage(
                    seq=int(row["seq"]),
                    path=path,
                    sha256=str(row["sha256"] or ""),
                    exists=path.is_file(),
                )
            )
        return images

    def _row_to_package(self, conn: sqlite3.Connection, row: sqlite3.Row) -> MaterialPackage:
        package = MaterialPackage(
            note_id=str(row["note_id"]),
            source_url=str(row["source_url"] or ""),
            origin=str(row["origin"] or ""),
            theme=str(row["theme"] or ""),
            title=str(row["title"] or ""),
            description=str(row["description"] or ""),
            author_nickname=str(row["author_nickname"] or ""),
            like_count=row["like_count"],
            collected_count=row["collected_count"],
            review_status=str(row["status"] or "pending_review"),
            authorization=str(row["authorization"] or "reference_only"),
            fetch_status=str(row["fetch_status"] or ""),
            expected_image_count=int(row["expected"] or 0),
        )
        package.images = self._load_images(conn, int(row["pk"]))
        return package

    def get(self, note_id: str) -> Optional[MaterialPackage]:
        conn = self._connect()
        try:
            row = conn.execute(
                "SELECT n.*, n.id AS pk, 0 AS expected FROM notes n WHERE n.note_id = ?",
                (note_id,),
            ).fetchone()
            return self._row_to_package(conn, row) if row else None
        finally:
            conn.close()

    def list_packages(
        self,
        *,
        theme: Optional[str] = None,
        themes: Optional[List[str]] = None,
        origins: Optional[List[str]] = None,
        include_pending: bool = True,
        include_selected: bool = True,
        include_rejected: bool = False,
        require_complete: bool = True,
        limit: Optional[int] = None,
    ) -> List[MaterialPackage]:
        """按消费口径列出素材包。

        人工拒绝默认排除；人工已选与未处理默认都允许（人工审选不再是生产前提）。
        ``require_complete`` 只保留下载完整的素材。
        """
        clauses, params = ["1=1"], []
        if theme:
            clauses.append("n.theme = ?")
            params.append(theme)
        if themes:
            clauses.append(f"n.theme IN ({','.join('?' * len(themes))})")
            params.extend(themes)
        if origins:
            clauses.append(f"n.origin IN ({','.join('?' * len(origins))})")
            params.extend(origins)
        statuses = []
        if include_selected:
            statuses.append("selected")
        if include_pending:
            statuses.append("pending_review")
        if include_rejected:
            statuses.append("rejected")
        if statuses:
            clauses.append(f"n.status IN ({','.join('?' * len(statuses))})")
            params.extend(statuses)
        if require_complete:
            clauses.append("n.fetch_status = 'fetched'")
            clauses.append("n.image_count > 0")
        sql = (
            "SELECT n.*, n.id AS pk, 0 AS expected FROM notes n WHERE "
            + " AND ".join(clauses)
            + " ORDER BY n.id"
        )
        if limit:
            sql += f" LIMIT {int(limit)}"
        conn = self._connect()
        try:
            rows = conn.execute(sql, params).fetchall()
            packages = [self._row_to_package(conn, row) for row in rows]
        finally:
            conn.close()
        if require_complete:
            packages = [p for p in packages if p.complete]
        return packages
