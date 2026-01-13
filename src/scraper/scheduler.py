"""任务调度器模块。

该模块实现了灵活的任务调度系统，支持两种调度模式：
1. 周期性调度：持续监控论坛首页，定期生成扫描任务
2. 历史回溯调度：仅投递回溯首页任务，由 Worker 递推后续页

使用单一类实现，内部通过不同的方法承载不同模式的逻辑，便于维护与扩展。
"""

from __future__ import annotations

import asyncio
from asyncio import PriorityQueue
from typing import TYPE_CHECKING, Literal

from tiebameow.utils.logger import logger

from .tasks import Priority, ScanThreadsTask, Task

if TYPE_CHECKING:
    from collections.abc import Callable

    from tiebameow.models.orm import Forum

    from ..core import Container


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
        self.log = logger.bind(name="Scheduler")

    async def run(self, mode: Literal["periodic", "backfill"] = "periodic"):
        """根据不同模式生成任务并放入队列。

        Args:
            mode: 调度模式，支持 "periodic" 和 "backfill"。
        """
        if mode == "periodic":
            await self._run_periodic()
        elif mode == "backfill":
            await self._run_backfill()
        else:
            raise ValueError(f"Unknown scheduler mode: {mode}")

    async def _run_periodic(self):
        """
        周期性调度任务生成器。

        根据配置的调度间隔，持续生成首页扫描任务，支持分组调度和默认调度。
        """
        log_interval = self.container.config.scheduler_interval_seconds
        logger.info("Starting PERIODIC mode. Default Interval: {}s", log_interval)

        tasks_to_schedule = []
        grouped_forum_names = set()
        available_forums_map = {f.fname: f for f in (self.container.forums or [])}

        for group in self.container.config.groups:
            group_forums = [available_forums_map[name] for name in group.forums if name in available_forums_map]
            grouped_forum_names.update(group.forums)

            interval = group.interval_seconds or self.container.config.scheduler_interval_seconds

            if group_forums:
                logger.info(
                    "Starting Group '{}' loop. Interval: {}s. Forums: {}",
                    group.name,
                    interval,
                    [f.fname for f in group_forums],
                )
                tasks_to_schedule.append(
                    self._run_loop(
                        lambda f=group_forums: f,
                        interval,
                        f"Group-{group.name}",
                    )
                )
            elif group.forums:
                logger.warning(
                    "Group '{}' has forums configured {} but none were initialized/found.", group.name, group.forums
                )

        def get_default_forums() -> list[Forum]:
            """获取未分组的默认贴吧列表。"""
            all_forums = self.container.forums or []
            return [f for f in all_forums if f.fname not in grouped_forum_names]

        logger.info("Starting Default loop for non-grouped/dynamic forums.")
        tasks_to_schedule.append(
            self._run_loop(get_default_forums, self.container.config.scheduler_interval_seconds, "Default")
        )

        if not tasks_to_schedule:
            logger.warning("No loops scheduled. check configuration.")
            return

        await asyncio.gather(*tasks_to_schedule)

    async def _run_loop(self, forums_getter: Callable[[], list[Forum]], interval: int, task_name: str):
        """通用调度循环"""
        good_every = self.container.config.good_page_every_ticks
        tick = 0
        while True:
            forums = forums_getter()
            if not forums:
                if task_name == "Default":
                    logger.debug("[{}] No forums to scan. Sleeping.", task_name)
                await asyncio.sleep(interval)
                continue

            logger.debug(
                "[{}] tick #{}: Generating homepage scan tasks for {} forums.",
                task_name,
                tick,
                len(forums),
            )

            await self._schedule_homepage_scans(forums, is_good=False)

            if tick % good_every == 0:
                logger.debug("[{}] Extra GOOD-section scheduling.", task_name)
                await self._schedule_homepage_scans(forums, is_good=True)

            await asyncio.sleep(interval)
            tick += 1

    async def _run_backfill(self):
        """历史回溯调度任务生成器。"""
        max_pages = self.container.config.max_backfill_pages
        logger.info("Starting BACKFILL mode (homepage kickoff, max_pages={}).", max_pages)
        forums = self.container.forums or []

        if not forums:
            logger.warning("No forums configured in tieba.forums. Scheduler will be idle.")
            return

        for forum in forums:
            await self._schedule_backfill_homepage(forum, max_pages, is_good=False)
            await self._schedule_backfill_homepage(forum, max_pages, is_good=True)

        logger.info("Backfill homepage tasks scheduled. Scheduler is exiting.")

    async def _schedule_homepage_scans(self, forums: list[Forum], *, is_good: bool = False):
        """调度首页扫描任务（周期模式）。

        Args:
            forums: 需要扫描的贴吧列表。
        """
        for forum in forums:
            task_content = ScanThreadsTask(fid=forum.fid, fname=forum.fname, pn=1, is_good=is_good)
            task = Task(priority=Priority.HIGH, content=task_content)
            await self.queue.put(task)
            section = "GOOD" if is_good else "NORMAL"
            logger.debug("Scheduled {} homepage scan for [{}吧] with priority=HIGH", section, forum.fname)

    async def _schedule_backfill_homepage(self, forum: Forum, max_pages: int, *, is_good: bool = False):
        """为单个贴吧调度回溯任务的起点页面，由 Worker 递推后续页。

        在 hybrid 模式下，从第 2 页开始，非 hybrid 模式则从第 1 页开始。

        Args:
            forum: 需要扫描的贴吧。
            max_pages: 最大扫描页数。
        """
        start_pn = 2 if self.container.config.mode == "hybrid" else 1

        task_content = ScanThreadsTask(
            fid=forum.fid,
            fname=forum.fname,
            pn=start_pn,
            backfill=True,
            max_pages=max_pages,
            is_good=is_good,
        )
        task = Task(priority=Priority.BACKFILL, content=task_content)
        await self.queue.put(task)
        section = "GOOD" if is_good else "NORMAL"
        logger.debug(
            "Scheduled BACKFILL [{}] start pn={} for [{}吧] with priority=BACKFILL (max_pages={}).",
            section,
            start_pn,
            forum.fname,
            max_pages,
        )
