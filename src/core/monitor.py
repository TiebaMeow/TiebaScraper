"""系统监控模块。

该模块负责定期采集系统级指标，包括：
1. 事件循环延迟 (Event Loop Lag)
2. 数据库连接池状态 (DB Pool Stats)
"""

from __future__ import annotations

import asyncio
from typing import TYPE_CHECKING, Any, cast

from tiebameow.utils.logger import logger

from .metrics import DB_POOL_STATS, EVENT_LOOP_LAG

if TYPE_CHECKING:
    from collections.abc import Callable

    from sqlalchemy.pool.impl import AsyncAdaptedQueuePool

    from .container import Container


class SystemMonitor:
    """系统监控器。

    在后台运行，定期采集指标。

    Attributes:
        container: 依赖注入容器，用于访问数据库引擎。
        interval: 采集间隔（秒）。
    """

    def __init__(self, container: Container, interval: float = 1.0):
        if interval <= 0:
            raise ValueError("interval must be greater than 0")

        self.container = container
        self.interval = interval
        self._db_pool_error_logged = False

    def _log_db_pool_warning_once(self, error: Exception) -> None:
        if not self._db_pool_error_logged:
            logger.warning("Failed to collect DB pool stats: {}", error)
            self._db_pool_error_logged = True

    def _read_pool_metric(self, metric_reader: Callable[[], Any]) -> float | None:
        value = metric_reader()

        if isinstance(value, bool):
            return None
        if isinstance(value, int | float):
            return float(max(0, value))

        return None

    def _resolve_db_pool(self) -> AsyncAdaptedQueuePool | None:
        if not self.container.db_engine:
            return None

        db_engine = self.container.db_engine

        try:
            return cast("AsyncAdaptedQueuePool", db_engine.pool)
        except AttributeError:
            return None

    def _collect_db_pool_stats(self) -> None:
        pool = self._resolve_db_pool()
        if pool is None:
            self._db_pool_error_logged = False
            return

        metric_mapping: tuple[tuple[str, Callable[[], int]], ...] = (
            ("capacity", lambda: pool.size()),
            ("available", lambda: pool.checkedin()),
            ("acquired", lambda: pool.checkedout()),
            ("overflow", lambda: pool.overflow()),
        )

        has_error = False
        for state, metric_reader in metric_mapping:
            try:
                value = self._read_pool_metric(metric_reader)
            except Exception as e:
                has_error = True
                self._log_db_pool_warning_once(e)
                continue

            if value is None:
                continue

            DB_POOL_STATS.labels(state=state).set(value)

        if not has_error:
            self._db_pool_error_logged = False

    async def run(self) -> None:
        """运行监控循环。"""
        logger.info("System Monitor started.")
        loop = asyncio.get_running_loop()
        expected_wake_time = loop.time() + self.interval

        try:
            while True:
                sleep_for = max(0.0, expected_wake_time - loop.time())
                await asyncio.sleep(sleep_for)

                try:
                    real_wake_time = loop.time()
                    EVENT_LOOP_LAG.observe(max(0.0, real_wake_time - expected_wake_time))
                    expected_wake_time = real_wake_time + self.interval

                    self._collect_db_pool_stats()
                except Exception as e:
                    logger.exception("Unexpected error in System Monitor loop: {}", e)

        except asyncio.CancelledError:
            logger.info("System Monitor stopped.")
