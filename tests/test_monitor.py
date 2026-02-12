"""系统监控模块测试。"""

import asyncio
from types import SimpleNamespace
from typing import Any, cast

import pytest

from src.core.monitor import SystemMonitor


class _DummyPool:
    def size(self):
        return 10

    def checkedin(self):
        return 7

    def checkedout(self):
        return 3

    def overflow(self):
        return 1


def test_monitor_interval_must_be_positive():
    """interval 必须为正数。"""
    container = cast("Any", SimpleNamespace(db_engine=None))

    with pytest.raises(ValueError, match="interval must be greater than 0"):
        SystemMonitor(container=container, interval=0)


def test_collect_db_pool_stats_sets_metrics(monkeypatch):
    """应正确采集 DB 连接池指标。"""
    calls: list[tuple[str, float]] = []

    class _MetricHandle:
        def __init__(self, state: str):
            self.state = state

        def set(self, value: float):
            calls.append((self.state, value))

    class _Metric:
        def labels(self, *, state: str):
            return _MetricHandle(state)

    monitor_module = __import__("src.core.monitor", fromlist=["DB_POOL_STATS"])
    monkeypatch.setattr(monitor_module, "DB_POOL_STATS", _Metric())

    container = cast("Any", SimpleNamespace(db_engine=SimpleNamespace(pool=_DummyPool())))
    monitor = SystemMonitor(container=container, interval=1.0)

    monitor._collect_db_pool_stats()

    assert ("capacity", 10.0) in calls
    assert ("available", 7.0) in calls
    assert ("acquired", 3.0) in calls
    assert ("overflow", 1.0) in calls


def test_collect_db_pool_stats_logs_warning_once(monkeypatch):
    """采集异常时只记录一次 warning，避免日志洪泛。"""
    warning_calls: list[str] = []

    class _BrokenPool:
        def size(self):
            raise RuntimeError("boom")

    class _Logger:
        @staticmethod
        def warning(msg, *_):
            warning_calls.append(msg)

    monitor_module = __import__("src.core.monitor", fromlist=["logger"])
    monkeypatch.setattr(monitor_module, "logger", _Logger())

    container = cast("Any", SimpleNamespace(db_engine=SimpleNamespace(pool=_BrokenPool())))
    monitor = SystemMonitor(container=container, interval=1.0)

    monitor._collect_db_pool_stats()
    monitor._collect_db_pool_stats()

    assert len(warning_calls) == 1


def test_collect_db_pool_stats_partial_failure_does_not_block_other_metrics(monkeypatch):
    """单个指标采集失败不应阻断其他指标采集，且 warning 只记录一次。"""
    warning_calls: list[str] = []
    calls: list[tuple[str, float]] = []

    class _PartiallyBrokenPool:
        def size(self):
            raise RuntimeError("boom")

        def checkedin(self):
            return 7

        def checkedout(self):
            return 3

        def overflow(self):
            return 1

    class _MetricHandle:
        def __init__(self, state: str):
            self.state = state

        def set(self, value: float):
            calls.append((self.state, value))

    class _Metric:
        def labels(self, *, state: str):
            return _MetricHandle(state)

    class _Logger:
        @staticmethod
        def warning(msg, *_):
            warning_calls.append(msg)

    monitor_module = __import__("src.core.monitor", fromlist=["DB_POOL_STATS", "logger"])
    monkeypatch.setattr(monitor_module, "DB_POOL_STATS", _Metric())
    monkeypatch.setattr(monitor_module, "logger", _Logger())

    container = cast("Any", SimpleNamespace(db_engine=SimpleNamespace(pool=_PartiallyBrokenPool())))
    monitor = SystemMonitor(container=container, interval=1.0)

    monitor._collect_db_pool_stats()
    monitor._collect_db_pool_stats()

    assert len(warning_calls) == 1
    assert ("available", 7.0) in calls
    assert ("acquired", 3.0) in calls
    assert ("overflow", 1.0) in calls


@pytest.mark.asyncio
async def test_monitor_run_can_be_cancelled(monkeypatch):
    """run 循环应能被取消并正常退出。"""
    lag_values: list[float] = []

    class _LagMetric:
        @staticmethod
        def observe(value: float):
            lag_values.append(value)

    monitor_module = __import__("src.core.monitor", fromlist=["EVENT_LOOP_LAG"])
    monkeypatch.setattr(monitor_module, "EVENT_LOOP_LAG", _LagMetric())

    container = cast("Any", SimpleNamespace(db_engine=None))
    monitor = SystemMonitor(container=container, interval=0.01)

    task = asyncio.create_task(monitor.run())
    await asyncio.sleep(0.03)
    task.cancel()

    await task
    assert task.done()

    assert lag_values, "Expected EVENT_LOOP_LAG to be observed at least once"
