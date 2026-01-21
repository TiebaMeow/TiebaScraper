"""Tieba爬虫应用程序主入口模块。

该模块提供三种运行模式：
1. 周期性模式(periodic): 持续监控指定论坛的最新内容
2. 回溯模式(backfill): 抓取指定论坛的历史数据（仅投递首页，由 Worker 递推）
3. 混合模式(hybrid): 周期监控首页 + 一次性触发回溯首页

统一入口 main(mode) 根据模式分支启动相应组件。
"""

import asyncio
import platform
from typing import Literal

from tiebameow.utils.logger import init_logger, logger

from src.core import initialize_application
from src.scraper import Scheduler, Worker

init_logger(
    service_name="TiebaScraper",
    enable_error_filelog=True,
    diagnose=False,
)

GRACEFUL_SHUTDOWN_TIMEOUT = 60


async def _run_periodic_mode(scheduler: Scheduler, workers: list[Worker], task_queue) -> None:
    """运行周期模式，支持优雅退出。

    优雅退出流程：
    1. 收到取消信号后，停止调度器（不再生成新任务）
    2. 等待任务队列清空（带超时）
    3. 取消所有 Worker
    """
    tasks: list[asyncio.Task] = []

    scheduler_task = asyncio.create_task(scheduler.run(mode="periodic"), name="scheduler")
    tasks.append(scheduler_task)
    tasks.extend(asyncio.create_task(w.run(), name=f"worker-{i}") for i, w in enumerate(workers))

    try:
        await asyncio.gather(*tasks)
    except asyncio.CancelledError:
        logger.info("Received shutdown signal. Starting graceful shutdown...")

        scheduler.stop()

        if not scheduler_task.done():
            try:
                await asyncio.wait_for(scheduler_task, timeout=5)
            except TimeoutError:
                scheduler_task.cancel()
                try:
                    await scheduler_task
                except asyncio.CancelledError:
                    pass

        queue_size = task_queue.qsize()
        if queue_size > 0:
            logger.info(
                "Waiting for {} tasks in queue to complete (timeout={}s)...", queue_size, GRACEFUL_SHUTDOWN_TIMEOUT
            )
            try:
                await asyncio.wait_for(task_queue.join(), timeout=GRACEFUL_SHUTDOWN_TIMEOUT)
                logger.info("All queued tasks completed.")
            except TimeoutError:
                remaining = task_queue.qsize()
                logger.warning("Graceful shutdown timeout. {} tasks remaining in queue.", remaining)

        worker_tasks = [t for t in tasks if t.get_name().startswith("worker-")]
        for t in worker_tasks:
            if not t.done():
                t.cancel()
        if worker_tasks:
            await asyncio.gather(*worker_tasks, return_exceptions=True)

        logger.info("Graceful shutdown completed.")
        raise


async def _run_hybrid_mode(scheduler: Scheduler, workers: list[Worker], task_queue) -> None:
    """运行混合模式，支持优雅退出。

    混合模式同时运行周期调度器和回溯调度器。
    优雅退出流程：
    1. 收到取消信号后，停止周期调度器（不再生成新任务）
    2. 等待回溯调度器完成（如果还在运行）
    3. 等待任务队列清空（带超时）
    4. 取消所有 Worker
    """
    tasks: list[asyncio.Task] = []

    periodic_task = asyncio.create_task(scheduler.run(mode="periodic"), name="scheduler-periodic")
    tasks.append(periodic_task)

    backfill_task = asyncio.create_task(scheduler.run(mode="backfill"), name="scheduler-backfill")
    tasks.append(backfill_task)

    tasks.extend(asyncio.create_task(w.run(), name=f"worker-{i}") for i, w in enumerate(workers))

    try:
        await asyncio.gather(*tasks)
    except asyncio.CancelledError:
        logger.info("Received shutdown signal in hybrid mode. Starting graceful shutdown...")

        scheduler.stop()

        if not periodic_task.done():
            try:
                await asyncio.wait_for(periodic_task, timeout=5)
            except TimeoutError:
                periodic_task.cancel()
                try:
                    await periodic_task
                except asyncio.CancelledError:
                    pass

        if not backfill_task.done():
            logger.info("Waiting for backfill scheduler to complete...")
            try:
                await asyncio.wait_for(backfill_task, timeout=10)
            except TimeoutError:
                logger.warning("Backfill scheduler timeout, cancelling...")
                backfill_task.cancel()
                try:
                    await backfill_task
                except asyncio.CancelledError:
                    pass

        queue_size = task_queue.qsize()
        if queue_size > 0:
            logger.info(
                "Waiting for {} tasks in queue to complete (timeout={}s)...", queue_size, GRACEFUL_SHUTDOWN_TIMEOUT
            )
            try:
                await asyncio.wait_for(task_queue.join(), timeout=GRACEFUL_SHUTDOWN_TIMEOUT)
                logger.info("All queued tasks completed.")
            except TimeoutError:
                remaining = task_queue.qsize()
                logger.warning("Graceful shutdown timeout. {} tasks remaining in queue.", remaining)

        worker_tasks = [t for t in tasks if t.get_name().startswith("worker-")]
        for t in worker_tasks:
            if not t.done():
                t.cancel()
        if worker_tasks:
            await asyncio.gather(*worker_tasks, return_exceptions=True)

        logger.info("Graceful shutdown completed.")
        raise


async def main(mode: Literal["periodic", "backfill", "hybrid"] = "periodic"):
    """统一入口，根据模式启动应用。

    - periodic: 周期性调度器 + 多 worker，常驻运行，支持优雅退出。
    - backfill: 一次性回溯调度器 + 多 worker；等待队列清空后优雅退出。
    - hybrid: 周期调度器常驻 + 回溯调度器跑一轮 + 多 worker。
    """
    container, task_queue = await initialize_application(mode=mode)

    tasks: list[asyncio.Task] = []
    scheduler = None
    try:
        logger.info("Starting application in {} mode.", mode)

        scheduler = Scheduler(queue=task_queue, container=container)
        worker_count = 3 if mode == "periodic" else 5
        workers = [Worker(i, task_queue, container) for i in range(worker_count)]

        if mode == "periodic":
            await _run_periodic_mode(scheduler, workers, task_queue)

        elif mode == "backfill":
            scheduler_task = asyncio.create_task(scheduler.run(mode="backfill"), name="scheduler")
            worker_tasks = [asyncio.create_task(w.run(), name=f"worker-{i}") for i, w in enumerate(workers)]
            tasks.append(scheduler_task)
            tasks.extend(worker_tasks)

            await scheduler_task
            await task_queue.join()

        else:  # hybrid
            await _run_hybrid_mode(scheduler, workers, task_queue)

    except asyncio.CancelledError:
        logger.info("Received cancellation in {} mode.", mode)
        raise

    except Exception as e:
        logger.exception("Application failed to start or run: {}", e)
    finally:
        for t in tasks:
            if not t.done():
                t.cancel()
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)

        logger.info("Shutting down application...")

        try:
            await Worker.close_datastore()
        except Exception:
            pass

        await container.teardown()


def get_loop_factory():
    if platform.system() != "Windows":
        try:
            import uvloop  # type: ignore

            return uvloop.new_event_loop

        except ImportError:
            logger.warning("uvloop not installed; using default asyncio event loop.")

        except Exception as e:
            logger.warning("Failed to set up uvloop; using default asyncio event loop. Error: {}", e)

    else:
        logger.info("Running on Windows, using the default ProactorEventLoop.")

    return None


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Tieba Scraper Application")
    parser.add_argument(
        "--mode",
        choices=["periodic", "backfill", "hybrid"],
        default="periodic",
        help=("Running mode: 'periodic' for continuous monitoring, 'backfill' for historical data, 'hybrid' for both."),
    )
    args = parser.parse_args()

    loop_factory = get_loop_factory()

    try:
        if loop_factory is not None:
            with asyncio.Runner(loop_factory=loop_factory) as runner:
                runner.run(main(args.mode))
        else:
            asyncio.run(main(args.mode))
    except KeyboardInterrupt:
        logger.info("Application stopped by user.")
