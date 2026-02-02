"""任务定义模块。

该模块定义了爬虫系统中使用的各种任务类型和优先级。
任务通过优先队列进行调度，支持不同优先级的任务处理。
"""

from __future__ import annotations

import dataclasses
import itertools
import time
from enum import IntEnum
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from datetime import datetime

    from tiebameow.models.dto import ThreadDTO


class Priority(IntEnum):
    """任务优先级，数值越小，优先级越高。

    定义了六种优先级，细化控制实时性与吞吐量的平衡：
    - REALTIME: 首页实时扫描任务（最高优先级）
    - HIGH: 回复增量扫描任务
    - MEDIUM: 回复全量扫描和楼中楼增量扫描任务
    - LOW: 楼中楼全量扫描任务
    - BACKGROUND: DeepScan 深度扫描任务
    - BACKFILL: 历史数据回溯任务（最低优先级）
    """

    REALTIME = 0  # ScanThreads（首页实时扫描）
    HIGH = 1  # IncrementalScanPosts
    MEDIUM = 2  # FullScanPosts, IncrementalScanComments
    LOW = 3  # FullScanComments
    BACKGROUND = 4  # DeepScan
    BACKFILL_THREADS = 5  # ScanThreads (Backfill)
    BACKFILL_POSTS = 6  # BackfillTasks (Posts/Comments)


# 全局序列号生成器
_sequence_counter = itertools.count()


@dataclasses.dataclass(slots=True, frozen=True)
class Task:
    """放入优先级队列的任务对象。

    任务按以下顺序排序：
    1. 优先级（数值越小优先级越高）
    2. 创建时间（越早创建越先出队）
    3. 序列号（确保相同优先级和创建时间的任务有唯一排序）

    Attributes:
        priority: 任务优先级
        content: 任务内容对象
        timestamp: 任务创建时间戳（自动设置）
        sequence_number: 全局递增序列号（自动设置）
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
    timestamp: float = dataclasses.field(init=False)
    sequence_number: int = dataclasses.field(init=False)

    def __post_init__(self):
        """自动初始化创建时间和序列号。"""
        object.__setattr__(self, "timestamp", time.time())
        object.__setattr__(self, "sequence_number", next(_sequence_counter))

    def __lt__(self, other):
        if not isinstance(other, Task):
            return NotImplemented
        return (self.priority.value, self.timestamp, self.sequence_number) < (
            other.priority.value,
            other.timestamp,
            other.sequence_number,
        )

    def __le__(self, other):
        if not isinstance(other, Task):
            return NotImplemented
        return (self.priority.value, self.timestamp, self.sequence_number) <= (
            other.priority.value,
            other.timestamp,
            other.sequence_number,
        )

    def __eq__(self, other):
        if not isinstance(other, Task):
            return NotImplemented
        return (self.priority.value, self.timestamp, self.sequence_number) == (
            other.priority.value,
            other.timestamp,
            other.sequence_number,
        )

    def __gt__(self, other):
        if not isinstance(other, Task):
            return NotImplemented
        return (self.priority.value, self.timestamp, self.sequence_number) > (
            other.priority.value,
            other.timestamp,
            other.sequence_number,
        )

    def __ge__(self, other):
        if not isinstance(other, Task):
            return NotImplemented
        return (self.priority.value, self.timestamp, self.sequence_number) >= (
            other.priority.value,
            other.timestamp,
            other.sequence_number,
        )

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
        stored_last_time: 数据库中存储的最后回复时间戳（用于判断停止扫描）
        stored_reply_num: 数据库中存储的回复数（用于计算 DeepScan 期望值）
        rn: 每页条目数量，默认为30
        backfill: 是否为回溯任务，默认为False
        target_last_time: 期望更新到的最后回复时间（用于任务完成后更新DB）
        target_reply_num: 期望更新到的回复数（用于任务完成后更新DB）
    """

    tid: int
    stored_last_time: datetime
    stored_reply_num: int = 0
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
    """深度扫描主题贴内深处的楼中楼。

    扫描主题贴的前 depth 页和后 depth 页回复，检查这些回复的楼中楼更新。
    用于补偿增量扫描无法覆盖的"深处"楼中楼更新。

    Attributes:
        tid: 主题贴tid
        total_pages: 主题贴总页数
        depth: 页码扫描深度，扫描前 n 页 + 后 n 页回复，默认为3
        expected_new_comments: 期望找到的新楼中楼数量（至少为1）
    """

    tid: int
    total_pages: int
    depth: int = 3
    expected_new_comments: int = 1

    @property
    def unique_key(self) -> int:
        return self.tid
