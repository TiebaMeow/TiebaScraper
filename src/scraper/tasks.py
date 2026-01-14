"""任务定义模块。

该模块定义了爬虫系统中使用的各种任务类型和优先级。
任务通过优先队列进行调度，支持不同优先级的任务处理。
"""

from __future__ import annotations

import dataclasses
from enum import IntEnum
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from datetime import datetime

    from tiebameow.models.dto import ThreadDTO


class Priority(IntEnum):
    """任务优先级，数值越小，优先级越高。

    定义了四种优先级：
    - HIGH: 首页实时扫描和回复增量扫描任务
    - MEDIUM: 回复全量扫描和楼中楼增量扫描任务
    - LOW: 楼中楼全量扫描和分区维护任务
    - BACKFILL: 历史数据回溯任务
    """

    HIGH = 1  # ScanThreads, IncrementalScanPosts
    MEDIUM = 2  # FullScanPosts, IncrementalScanComments
    LOW = 3  # FullScanComments
    BACKFILL = 4  # BackfillTasks


@dataclasses.dataclass(order=True, slots=True, frozen=True)
class Task:
    """放入优先级队列的任务对象。

    Attributes:
        priority: 任务优先级
        content: 任务内容对象
    """

    priority: Priority
    content: (
        ScanThreadsTask
        | FullScanPostsTask
        | IncrementalScanPostsTask
        | FullScanCommentsTask
        | IncrementalScanCommentsTask
        | DeepScanTask
    ) = dataclasses.field(compare=False)

    @property
    def unique_key(self) -> tuple[int, str, object]:
        """获取任务的唯一标识键。"""
        content = self.content
        return (self.priority.value, type(content).__name__, content.unique_key)


@dataclasses.dataclass(slots=True, frozen=True)
class ScanThreadsTask:
    """扫描指定贴吧的任务。

    Attributes:
        fid: 贴吧fid
        fname: 贴吧名
        pn: 页码
        rn: 每页条目数量，默认为30
        is_good: 是否为精华分区，默认为False
        backfill: 是否为回溯任务，默认为False
        max_pages: 最大扫描页数，默认为100
        force: 是否强制扫描（忽略去重和更新检查），默认为False
    """

    fid: int
    fname: str
    pn: int
    rn: int = 30
    is_good: bool = False
    backfill: bool = False
    max_pages: int = 100
    force: bool = False

    @property
    def unique_key(self) -> tuple[int, int, bool]:
        return (self.fid, self.pn, self.is_good)


@dataclasses.dataclass(slots=True, frozen=True)
class FullScanPostsTask:
    """全量扫描指定主题贴的所有回复和楼中楼。

    Attributes:
        tid: 主题贴tid
        rn: 每页条目数量，默认为30
        backfill: 是否为回溯任务，默认为False
        thread_dto: 主题贴元数据（用于任务完成后更新DB）
    """

    tid: int
    rn: int = 30
    backfill: bool = False
    thread_dto: ThreadDTO | None = None

    @property
    def unique_key(self) -> int:
        return self.tid


@dataclasses.dataclass(slots=True, frozen=True)
class IncrementalScanPostsTask:
    """增量扫描指定主题贴的回复和楼中楼。

    Attributes:
        tid: 主题贴tid
        last_time: 上次扫描的最后回复时间戳
        last_floor: 上次扫描的最后楼层，默认为1
        rn: 每页条目数量，默认为30
        backfill: 是否为回溯任务，默认为False
        target_last_time: 期望更新到的最后回复时间（用于任务完成后更新DB）
        target_reply_num: 期望更新到的回复数（用于任务完成后更新DB）
    """

    tid: int
    last_time: datetime
    last_floor: int = 1
    rn: int = 30
    backfill: bool = False
    target_last_time: datetime | None = None
    target_reply_num: int | None = None

    @property
    def unique_key(self) -> int:
        return self.tid


@dataclasses.dataclass(slots=True, frozen=True)
class FullScanCommentsTask:
    """扫描单个回复下所有楼中楼的任务。

    Attributes:
        tid: 主题贴tid
        pid: 回复pid
        backfill: 是否为回溯任务，默认为False
    """

    tid: int
    pid: int
    backfill: bool = False

    @property
    def unique_key(self) -> tuple[int, int]:
        return (self.tid, self.pid)


@dataclasses.dataclass(slots=True, frozen=True)
class IncrementalScanCommentsTask:
    """增量扫描单个回复下的楼中楼。

    Attributes:
        tid: 主题贴tid
        pid: 回复pid
        backfill: 是否为回溯任务，默认为False
    """

    tid: int
    pid: int
    backfill: bool = False

    @property
    def unique_key(self) -> tuple[int, int]:
        return (self.tid, self.pid)


@dataclasses.dataclass(slots=True, frozen=True)
class DeepScanTask:
    """深度扫描单个贴子下的楼中楼。

    Attributes:
        tid: 主题贴tid
        pid: 回复pid
        depth: 页码扫描深度，默认为10
    """

    tid: int
    pid: int
    depth: int = 10

    @property
    def unique_key(self) -> tuple[int, int]:
        return (self.tid, self.pid)
