"""Tieba爬虫应用程序主入口模块。

该模块提供了两种运行模式的实现：
1. 周期性模式(periodic): 持续监控指定论坛的最新内容
2. 回溯模式(backfill): 抓取指定论坛的历史数据

使用面向对象的架构设计，通过调度器和工作器的配合来实现高效的异步爬虫系统。
"""

import asyncio
import logging
import platform

from src.core import initialize_application
from src.scraper import Scheduler, Worker

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s")
log = logging.getLogger(__name__)


async def main_periodic():
    """运行周期性监控模式的主函数。

    该函数创建并启动一个完整的爬虫系统，包括：
    - 依赖注入容器的初始化
    - 任务队列的设置
    - 调度器实例用于生成扫描任务
    - 多个工作器实例用于并发处理任务

    运行模式为周期性模式，会持续监控配置中指定的论坛。

    Raises:
        Exception: 当应用程序启动或运行过程中发生错误时抛出。
    """
    container, task_queue = await initialize_application(mode="periodic")

    try:
        log.info("Starting application in periodic mode.")

        scheduler_instance = Scheduler(queue=task_queue, container=container)
        worker_instances = [Worker(i, task_queue, container) for i in range(3)]

        scheduler_task = asyncio.create_task(scheduler_instance.run(mode="periodic"))

        worker_tasks = [asyncio.create_task(worker.run()) for worker in worker_instances]

        await asyncio.gather(scheduler_task, *worker_tasks)

    except Exception as e:
        log.exception(f"Application failed to start or run: {e}")
    finally:
        log.info("Shutting down application...")
        await container.teardown()


async def main_backfill():
    """使用回溯模式的主函数。

    该函数运行历史数据回溯模式，用于抓取指定论坛的历史数据。
    与周期性模式不同，回溯模式会在完成所有历史数据抓取后自动结束。

    运行流程：
    1. 创建调度器生成历史页面扫描任务
    2. 启动多个工作器处理任务
    3. 等待调度器完成任务生成
    4. 等待所有任务处理完成
    5. 优雅关闭工作器

    Raises:
        Exception: 当应用程序启动或运行过程中发生错误时抛出。
    """
    container, task_queue = await initialize_application(mode="backfill")

    try:
        log.info("Starting application with OOP interface in backfill mode.")

        scheduler_instance = Scheduler(queue=task_queue, container=container)
        worker_instances = [Worker(i, task_queue, container) for i in range(5)]

        scheduler_task = asyncio.create_task(scheduler_instance.run(mode="backfill"))

        worker_tasks = [asyncio.create_task(worker.run()) for worker in worker_instances]

        await scheduler_task

        await task_queue.join()

        for task in worker_tasks:
            task.cancel()

        await asyncio.gather(*worker_tasks, return_exceptions=True)

    except Exception as e:
        log.exception(f"Application failed to start or run: {e}")
    finally:
        log.info("Shutting down application...")
        await container.teardown()


def setup_event_loop():
    if platform.system() != "Windows":
        try:
            import asyncio

            import uvloop  # type: ignore

            asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())

        except ImportError:
            log.exception("uvloop is not installed, falling back to the default asyncio event loop.")

        except Exception:
            log.exception("Failed to set up uvloop, falling back to the default asyncio event loop.")

    else:
        log.info("Running on Windows, using the default ProactorEventLoop.")


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Tieba Scraper Application")
    parser.add_argument(
        "--mode",
        choices=["periodic", "backfill"],
        default="periodic",
        help="Running mode: 'periodic' for continuous monitoring, 'backfill' for historical data.",
    )
    args = parser.parse_args()

    setup_event_loop()

    try:
        if args.mode == "periodic":
            asyncio.run(main_periodic())
        else:
            asyncio.run(main_backfill())
    except KeyboardInterrupt:
        log.info("Application stopped by user.")
