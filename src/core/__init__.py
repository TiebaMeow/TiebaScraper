"""核心模块包。

包含爬虫系统的核心组件：
- Container: 依赖注入容器，管理所有外部资源
- DataStore: 数据存储层，提供统一的数据访问接口
- initialize: 应用初始化逻辑
"""

from .container import Container
from .datastore import DataStore
from .initialize import initialize_application

__all__ = ["Container", "DataStore", "initialize_application"]
