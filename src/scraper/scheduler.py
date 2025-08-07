"""任务调度器模块。

该模块实现了灵活的任务调度系统，支持两种调度模式：
1. 周期性调度：持续监控论坛首页，定期生成扫描任务
2. 历史回溯调度：批量生成历史数据抓取任务

使用策略模式设计，便于扩展新的调度策略。
"""

import asyncio
import logging
from abc import ABC, abstractmethod
from asyncio import PriorityQueue
from typing import Literal

from ..core import Container
from ..models import Forum
from .tasks import Priority, ScanThreadsTask, Task

log = logging.getLogger(__name__)


class SchedulingStrategy(ABC):
    """调度策略抽象基类。

    定义了调度策略的通用接口，具体的调度逻辑由子类实现。
    每个策略都可以访问任务队列和依赖容器。

    Attributes:
        queue: 任务优先队列，用于存放生成的任务。
        container: 依赖注入容器，提供配置和资源访问。
        log: 日志记录器。
    """

    def __init__(self, queue: PriorityQueue, container: Container):
        self.queue = queue
        self.container = container
        self.log = logging.getLogger(f"{self.__class__.__name__}")

    @abstractmethod
    async def run(self):
        """执行调度策略"""
        ...


class PeriodicSchedulingStrategy(SchedulingStrategy):
    """周期性调度策略。

    实现持续的周期性任务调度，定期为配置的贴吧生成首页扫描任务。
    """

    async def run(self):
        """周期性调度逻辑"""
        self.log.info(f"Starting PERIODIC mode. Interval: {self.container.config.scheduler_interval_seconds}s")
        forums = self.container.forums
        interval = self.container.config.scheduler_interval_seconds

        if not forums:
            self.log.warning("No forums configured in MONITORED_FORUMS. Scheduler will be idle.")
            return

        while True:
            self.log.info(f"Scheduler tick: Generating homepage scan tasks for forums: {forums}")
            await self._schedule_homepage_scans(forums)

            self.log.info(f"All homepage tasks scheduled. Sleeping for {interval} seconds.")
            await asyncio.sleep(interval)

    async def _schedule_homepage_scans(self, forums: list[Forum]):
        """调度首页扫描任务"""
        for forum in forums:
            task_content = ScanThreadsTask(fid=forum.fid, fname=forum.fname, pn=1)
            task = Task(priority=Priority.HIGH, content=task_content)
            await self.queue.put(task)
            self.log.info(f"Scheduled homepage scan for [{forum.fname}吧] with priority={Priority.HIGH.name}")


class BackfillSchedulingStrategy(SchedulingStrategy):
    """历史回溯调度策略。

    实现历史数据的批量抓取调度，为指定的贴吧生成大量的历史页面扫描任务。
    该策略会在生成完所有任务后退出，不会持续运行。

    Attributes:
        max_pages: 每个贴吧要扫描的最大页数。
    """

    def __init__(self, queue: PriorityQueue, container: Container, max_pages: int):
        super().__init__(queue, container)
        self.max_pages = max_pages

    async def run(self):
        """历史回溯调度逻辑"""
        self.log.info(f"Starting BACKFILL mode, pages 1 to {self.max_pages}.")
        forums = self.container.forums

        if not forums:
            self.log.warning("No forums configured in MONITORED_FORUMS. Scheduler will be idle.")
            return

        for forum in forums:
            await self._schedule_backfill_tasks(forum)

        self.log.info("Backfill task generation completed. Scheduler is exiting.")

    async def _schedule_backfill_tasks(self, forum: Forum):
        """为单个贴吧调度回溯任务"""
        for page_num in range(1, self.max_pages + 1):
            task_content = ScanThreadsTask(fid=forum.fid, fname=forum.fname, pn=page_num)
            task = Task(priority=Priority.LOW, content=task_content)
            await self.queue.put(task)

            # 每生成10个任务短暂yield，防止长时间阻塞事件循环
            if page_num % 10 == 0:
                self.log.info(f"Scheduled backfill tasks up to page {page_num}/{self.max_pages} for [{forum.fname}吧]")
                await asyncio.sleep(0)


class Scheduler:
    """任务调度器主类。

    提供统一的调度接口，根据指定的模式创建相应的调度策略。
    支持周期性模式和历史回溯模式的任务调度。

    Attributes:
        queue: 任务优先队列。
        container: 依赖注入容器。
        log: 日志记录器。
    """

    def __init__(self, queue: PriorityQueue, container: Container):
        self.queue = queue
        self.container = container
        self.log = logging.getLogger(self.__class__.__name__)

    def _create_strategy(self, mode: Literal["periodic", "backfill"]) -> SchedulingStrategy:
        """根据模式创建调度策略"""
        if mode == "periodic":
            return PeriodicSchedulingStrategy(self.queue, self.container)
        elif mode == "backfill":
            max_pages = self.container.config.max_backfill_pages
            return BackfillSchedulingStrategy(self.queue, self.container, max_pages)
        else:
            raise ValueError(f"Unknown scheduler mode: {mode}")

    async def run(self, mode: Literal["periodic", "backfill"] = "periodic"):
        """
        根据不同模式生成任务并放入队列。

        Args:
            mode: 'periodic' 为周期性扫描模式, 'backfill' 为历史回溯模式。
        """
        strategy = self._create_strategy(mode)
        await strategy.run()
