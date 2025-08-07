"""爬虫模块包。

包含爬虫系统的核心执行组件：
- Scheduler: 任务调度器，负责生成爬取任务
- Worker: 工作器，负责执行具体的爬取任务
- 各种任务定义和处理器
"""

from .scheduler import Scheduler
from .worker import Worker
