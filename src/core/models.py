"""本地 ORM 模型。

定义 TiebaScraper 内部使用的辅助数据库模型。
"""

from __future__ import annotations

from datetime import datetime  # noqa: TC003

from sqlalchemy import BigInteger, Boolean, DateTime, String, func
from sqlalchemy.orm import Mapped, mapped_column
from tiebameow.models.orm import Base


class PendingThreadScan(Base):
    """标记全量扫描尚未完成的 Thread。

    程序重启时可读取本表恢复未完成的扫描任务。

    Attributes:
        tid: 主题贴 tid（主键）
        fid: 贴吧 fid（恢复任务时可用于日志）
        fname: 贴吧名（恢复任务时可用于日志）
        backfill: 是否为回溯任务
        created_at: 记录创建时间
    """

    __tablename__ = "pending_thread_scan"

    tid: Mapped[int] = mapped_column(BigInteger, primary_key=True)
    fid: Mapped[int] = mapped_column(BigInteger, nullable=False)
    fname: Mapped[str] = mapped_column(String(255), nullable=False)
    backfill: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False, server_default=func.now())

    def to_dict(self) -> dict:
        result = {}
        for c in self.__table__.columns:
            value = getattr(self, c.name)
            if value is not None:
                result[c.name] = value
        return result


class PendingCommentScan(Base):
    """标记楼中楼扫描尚未完成的 Post。

    用于恢复 FullScanCommentsTask / IncrementalScanCommentsTask。

    Attributes:
        tid: 主题贴 tid
        pid: 回复 pid
        backfill: 是否为回溯任务
        task_kind: 任务类型（"full" 或 "incremental"）
        created_at: 记录创建时间
    """

    __tablename__ = "pending_comment_scan"

    tid: Mapped[int] = mapped_column(BigInteger, primary_key=True)
    pid: Mapped[int] = mapped_column(BigInteger, primary_key=True)
    backfill: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False)
    task_kind: Mapped[str] = mapped_column(String(16), nullable=False)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False, server_default=func.now())

    def to_dict(self) -> dict:
        result = {}
        for c in self.__table__.columns:
            value = getattr(self, c.name)
            if value is not None:
                result[c.name] = value
        return result
