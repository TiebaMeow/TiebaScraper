"""Tieba爬虫应用程序主入口模块。

该模块提供三种运行模式：
1. 周期性模式(periodic): 持续监控指定论坛的最新内容
2. 回溯模式(backfill): 抓取指定论坛的历史数据（仅投递首页，由 Worker 递推）
3. 混合模式(hybrid): 周期监控首页 + 一次性触发回溯首页

统一入口 main(mode) 根据模式分支启动相应组件。
"""

import asyncio
import logging
import platform
from typing import Literal

from src.core import initialize_application
from src.scraper import Scheduler, Worker
from src.utils import setup_logging

# 统一日志配置（可用环境变量 LOG_LEVEL 覆盖级别）
setup_logging()
log = logging.getLogger("main")


async def main(mode: Literal["periodic", "backfill", "hybrid"] = "periodic"):
    """统一入口，根据模式启动应用。

    - periodic: 周期性调度器 + 多 worker，常驻运行。
    - backfill: 一次性回溯调度器 + 多 worker；等待队列清空后优雅退出。
    - hybrid: 周期调度器常驻 + 回溯调度器跑一轮 + 多 worker。
    """
    container, task_queue = await initialize_application(mode=mode)

    tasks: list[asyncio.Task] = []
    try:
        log.info(f"Starting application in {mode} mode.")

        scheduler = Scheduler(queue=task_queue, container=container)
        worker_count = 3 if mode == "periodic" else 5
        workers = [Worker(i, task_queue, container) for i in range(worker_count)]

        if mode == "periodic":
            tasks.append(asyncio.create_task(scheduler.run(mode="periodic"), name="scheduler"))
            tasks.extend(asyncio.create_task(w.run(), name=f"worker-{i}") for i, w in enumerate(workers))
            await asyncio.gather(*tasks)

        elif mode == "backfill":
            scheduler_task = asyncio.create_task(scheduler.run(mode="backfill"), name="scheduler")
            worker_tasks = [asyncio.create_task(w.run(), name=f"worker-{i}") for i, w in enumerate(workers)]
            tasks.append(scheduler_task)
            tasks.extend(worker_tasks)

            await scheduler_task
            await task_queue.join()

        else:  # hybrid
            periodic_task = asyncio.create_task(scheduler.run(mode="periodic"), name="scheduler-periodic")
            tasks.append(periodic_task)

            backfill_task = asyncio.create_task(scheduler.run(mode="backfill"), name="scheduler-backfill")
            tasks.append(backfill_task)

            tasks.extend(asyncio.create_task(w.run(), name=f"worker-{i}") for i, w in enumerate(workers))

            await asyncio.gather(*tasks)

    except asyncio.CancelledError:
        log.info(f"Received cancellation in {mode} mode.")
        raise

    except Exception as e:
        log.exception(f"Application failed to start or run: {e}")

    finally:
        for t in tasks:
            if not t.done():
                t.cancel()
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)

        log.info("Shutting down application...")

        try:
            await Worker.close_datastore()
        except Exception:
            pass

        await container.teardown()


def setup_event_loop():
    if platform.system() != "Windows":
        try:
            import asyncio

            import uvloop  # type: ignore

            asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())

        except ImportError:
            # 非关键依赖，降低为 warning，避免冗长堆栈
            log.warning("uvloop not installed; using default asyncio event loop.")

        except Exception as e:
            log.warning(f"Failed to set up uvloop; using default asyncio event loop. Error: {e}")

    else:
        log.info("Running on Windows, using the default ProactorEventLoop.")


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

    setup_event_loop()

    try:
        asyncio.run(main(args.mode))
    except KeyboardInterrupt:
        log.info("Application stopped by user.")
