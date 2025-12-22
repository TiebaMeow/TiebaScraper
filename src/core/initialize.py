"""项目初始化模块。

该模块包含应用程序启动时需要执行的初始化任务，
例如初始化数据库中的监控贴吧列表等。
"""

from __future__ import annotations

import asyncio
from typing import Literal

from sqlalchemy import select, text
from tiebameow.models.orm import Base, Forum
from tiebameow.utils.logger import logger

from .config import Config
from .container import Container


async def initialize_application(
    mode: Literal["periodic", "backfill", "hybrid"],
) -> tuple[Container, asyncio.PriorityQueue]:
    """初始化整个应用程序。

    该函数封装了应用启动所需的所有核心初始化步骤：
    1. 根据运行模式加载配置。
    2. 创建并设置依赖注入容器。
    3. 创建数据库表并注册分区。
    4. 初始化数据库中的贴吧列表。
    5. 创建主任务队列。

    Args:
        mode: 应用程序的运行模式 ("periodic"、"backfill" 或 "hybrid")。

    Returns:
        一个元组，包含初始化完成的容器实例和任务队列。
    """
    logger.info("Initializing application in '{}' mode...", mode)

    app_config = Config(mode=mode)

    container = Container(config=app_config)
    await container.setup()

    await create_tables(container)

    await initialize_forums(container)

    task_queue = asyncio.PriorityQueue(maxsize=10000)

    logger.info("Application initialized successfully.")
    return container, task_queue


async def create_tables(container: Container) -> None:
    """创建数据库表。

    使用SQLAlchemy的Base.metadata.create_all方法在数据库中创建所有定义的模型表。

    Args:
        container: 依赖注入容器实例，提供数据库会话工厂。
    """
    logger.info("Initializing database tables...")

    if not container.tb_client or not container.async_sessionmaker:
        raise RuntimeError("Container is not set up properly.")

    try:
        async with container.db_engine.begin() as conn:  # type: ignore
            await conn.run_sync(Base.metadata.create_all)
            if container.config.partition_enabled:
                await conn.execute(text("CREATE EXTENSION IF NOT EXISTS timescaledb;"))
                for table in ("thread", "post", "comment"):
                    try:
                        await conn.execute(
                            text(
                                f"SELECT create_hypertable('public.{table}', 'create_time', "
                                f"chunk_time_interval => INTERVAL '{container.config.p_interval}', "
                                "if_not_exists => TRUE);"
                            )
                        )
                        logger.info("Converted {} to hypertable.", table)
                    except Exception as e:
                        logger.exception("Failed to convert {} to hypertable: {}", table, e)
                        raise

    except Exception as e:
        logger.exception("Failed to create database tables: {}", e)
        raise

    logger.info("Database tables created successfully.")


async def initialize_forums(container: Container) -> None:
    """初始化或同步数据库中的贴吧列表。

    从配置文件中读取要监控的贴吧名称，使用aiotieba客户端获取它们的fid，
    然后将这些信息存入数据库。同时，将获取到的fid列表更新到容器中。

    Args:
        container: 依赖注入容器实例。
    """
    logger.info("Initializing forum list...")
    if not container.tb_client or not container.async_sessionmaker:
        raise RuntimeError("Container is not set up properly.")

    forum_list: list[Forum] = []
    try:
        async with container.async_sessionmaker() as session:
            try:
                existing_forums = await session.execute(select(Forum))
                existing_forums = existing_forums.scalars().all()

                for forum_name in container.config.forums:
                    if forum := next((f for f in existing_forums if f.fname == forum_name), None):
                        forum_list.append(forum)
                        continue

                    fid = await container.tb_client.get_fid(forum_name)
                    if not fid:
                        logger.warning("Forum [{}吧] not found, skipping.", forum_name)
                        continue
                    new_forum = Forum(fid=fid, fname=forum_name)
                    session.add(new_forum)
                    forum_list.append(new_forum)
                    logger.info("Added new forum: {} (fid={})", forum_name, fid)

                await session.commit()

            except Exception:
                await session.rollback()
                raise

    except Exception as e:
        logger.exception("Failed to initialize forums: {}", e)
        raise

    container.forums = forum_list
    logger.info("Forums initialized: {}", [f.fname for f in forum_list])
