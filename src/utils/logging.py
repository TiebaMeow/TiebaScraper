"""统一日志配置模块。

提供 setup_logging() 以在应用启动时一次性配置全局日志。
可通过环境变量 LOG_LEVEL 设置日志级别（默认 INFO）。
"""

from __future__ import annotations

import logging
import os
import sys


def _str_to_level(level: str) -> int:
    mapping = {
        "CRITICAL": logging.CRITICAL,
        "ERROR": logging.ERROR,
        "WARNING": logging.WARNING,
        "INFO": logging.INFO,
        "DEBUG": logging.DEBUG,
        "NOTSET": logging.NOTSET,
    }
    return mapping.get(level.upper(), logging.INFO)


def setup_logging(level: int | str | None = None) -> None:
    """配置全局日志输出。

    Args:
        level: 日志级别，int 或名称。若未提供，则读取环境变量 LOG_LEVEL，默认 INFO。
    """
    if level is None:
        env_level = os.getenv("LOG_LEVEL", "INFO")
        resolved_level = _str_to_level(env_level)
    elif isinstance(level, str):
        resolved_level = _str_to_level(level)
    else:
        resolved_level = level

    root_logger = logging.getLogger()

    for h in list(root_logger.handlers):
        root_logger.removeHandler(h)

    root_logger.setLevel(resolved_level)

    formatter = logging.Formatter(
        fmt="%(asctime)s [%(levelname)s] %(name)s:%(lineno)d | %(message)s",
        datefmt="%m-%d %H:%M:%S",
    )

    stream_handler = logging.StreamHandler(stream=sys.stderr)
    stream_handler.setFormatter(formatter)
    stream_handler.setLevel(resolved_level)
    root_logger.addHandler(stream_handler)

    logging.getLogger("aiotieba").setLevel(logging.WARNING)
