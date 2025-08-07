"""项目初始化模块。

该模块包含应用程序启动时需要执行的初始化任务，
例如初始化数据库中的监控贴吧列表等。
"""

import asyncio
import logging
from typing import Literal

from sqlalchemy import select

from ..config import Config
from ..models import Forum
from .container import Container

log = logging.getLogger(__name__)


async def initialize_application(mode: Literal["periodic", "backfill"]) -> tuple[Container, asyncio.PriorityQueue]:
    """初始化整个应用程序。

    该函数封装了应用启动所需的所有核心初始化步骤：
    1. 根据运行模式加载配置。
    2. 创建并设置依赖注入容器。
    3. 初始化数据库中的贴吧列表。
    4. 创建主任务队列。

    Args:
        mode: 应用程序的运行模式 ("periodic" 或 "backfill")。

    Returns:
        一个元组，包含初始化完成的容器实例和任务队列。
    """
    log.info(f"Initializing application in '{mode}' mode...")

    app_config = Config(mode=mode)

    container = Container(config=app_config)
    await container.setup()

    await initialize_forums(container)

    task_queue = asyncio.PriorityQueue()

    log.info("Application initialized successfully.")
    return container, task_queue


async def initialize_forums(container: Container) -> None:
    """初始化或同步数据库中的贴吧列表。

    从配置文件中读取要监控的贴吧名称，使用aiotieba客户端获取它们的fid，
    然后将这些信息存入数据库。同时，将获取到的fid列表更新到容器中。

    Args:
        container: 依赖注入容器实例。
    """
    log.info("Initializing forum list...")
    if not container.tb_client or not container.async_sessionmaker:
        raise RuntimeError("Container is not set up properly.")

    try:
        async with container.async_sessionmaker() as session:
            existing_forums = await session.execute(select(Forum))
            existing_forums = existing_forums.scalars().all()

            forum_list: list[Forum] = []
            for forum_name in container.config.forums:
                if forum := next((f for f in existing_forums if f.fname == forum_name), None):
                    forum_list.append(forum)
                    continue

                async with container.limiter:  # type: ignore
                    fid = await container.tb_client.get_fid(forum_name)
                if not fid:
                    log.warning(f"Forum [{forum_name}吧] not found, skipping.")
                    continue
                new_forum = Forum(fid=fid, fname=forum_name)
                session.add(new_forum)
                forum_list.append(new_forum)
                log.info(f"Added new forum: {forum_name} (fid={fid})")

            await session.commit()

    except Exception as e:
        log.error(f"Failed to initialize forums: {e}")
        await session.rollback()
        raise
    container.forums = forum_list
    log.info(f"Forums initialized: {[f.fname for f in forum_list]}")
