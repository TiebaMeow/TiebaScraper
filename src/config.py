"""应用程序配置管理模块。

该模块负责从TOML配置文件中加载应用程序的各项配置，
包括数据库连接、Redis连接、贴吧认证信息、爬虫限制等。
支持周期性和回溯两种运行模式的配置。
"""

import tomllib
from pathlib import Path
from typing import ClassVar, Literal
from urllib.parse import quote_plus

from pydantic import (
    BaseModel,
    Field,
    PostgresDsn,
    RedisDsn,
    ValidationError,
    computed_field,
)


class DatabaseConfig(BaseModel):
    """数据库配置模型"""

    host: str = "localhost"
    port: int = 5432
    username: str = "admin"
    password: str = "123456"
    db_name: str = "tieba_data"
    p_interval: str = "1 month"
    p_premake: int = 4


class RedisConfig(BaseModel):
    """Redis配置模型"""

    host: str = "localhost"
    port: int = 6379
    username: str = ""
    password: str = ""
    db: int = 0


class TiebaConfig(BaseModel):
    """贴吧相关配置模型"""

    BDUSS: str = Field(...)
    forums: list[str] = Field(..., min_length=1)
    max_backfill_pages: int = Field(100, gt=0)


class RateLimitConfig(BaseModel):
    """请求频率限制配置模型"""

    rps: int = Field(10, gt=0)
    concurrency: int = Field(8, gt=0)


class SchedulerConfig(BaseModel):
    """调度器配置模型"""

    interval_seconds: int = Field(60, gt=0)


class PydanticConfig(BaseModel):
    """Pydantic总配置模型"""

    database: DatabaseConfig = Field(default_factory=DatabaseConfig)
    redis: RedisConfig = Field(default_factory=RedisConfig)
    tieba: TiebaConfig
    rate_limit: RateLimitConfig = Field(default_factory=lambda: RateLimitConfig(rps=10, concurrency=8))
    scheduler: SchedulerConfig = Field(default_factory=lambda: SchedulerConfig(interval_seconds=60))

    @computed_field
    @property
    def database_url(self) -> PostgresDsn:
        """生成PostgreSQL数据库连接URL"""
        return PostgresDsn(
            f"postgresql+asyncpg://{quote_plus(self.database.username)}:{quote_plus(self.database.password)}"
            f"@{self.database.host}:{self.database.port}/{self.database.db_name}"
        )

    @computed_field
    @property
    def redis_url(self) -> RedisDsn:
        """生成Redis连接URL"""
        if self.redis.username and self.redis.password:
            return RedisDsn(
                f"redis://{quote_plus(self.redis.username)}:{quote_plus(self.redis.password)}"
                f"@{self.redis.host}:{self.redis.port}/{self.redis.db}"
            )
        if self.redis.password:
            return RedisDsn(
                f"redis://:{quote_plus(self.redis.password)}@{self.redis.host}:{self.redis.port}/{self.redis.db}"
            )
        return RedisDsn(f"redis://{self.redis.host}:{self.redis.port}/{self.redis.db}")


class Config:
    """应用程序配置类。

    负责加载和管理应用程序的所有配置项，包括：
    - 数据库连接配置
    - Redis缓存配置
    - 贴吧API认证配置
    - 爬虫限制和调度配置

    Attributes:
        BASE_DIR (Path): 项目根目录路径
        CONFIG_FILE (Path): 配置文件路径
        pydantic_config (PydanticConfig): Pydantic应用配置模型
        mode (str): 运行模式('periodic'或'backfill')
    """

    BASE_DIR: ClassVar[Path] = Path(__file__).resolve().parent.parent
    CONFIG_FILE: ClassVar[Path] = BASE_DIR / "config.toml"

    pydantic_config: PydanticConfig
    mode: Literal["periodic", "backfill", "hybrid"]

    def __init__(self, mode: Literal["periodic", "backfill", "hybrid"] = "periodic"):
        """初始化配置对象。

        从配置文件中加载各项配置，构建数据库和Redis连接URL，
        并设置爬虫相关的限制参数。

        Args:
            mode: 运行模式，可选值为'periodic'(周期性)、'backfill'(回溯)或'hybrid'(混合)。
                默认为'periodic'。
        """
        config_data = self._load_toml()

        try:
            self.pydantic_config = PydanticConfig.model_validate(config_data)
        except ValidationError as e:
            raise ValueError(f"配置文件验证失败: {e}") from e

        self.mode = mode

    @classmethod
    def _load_toml(cls) -> dict:
        """从配置文件加载配置数据。

        Returns:
            dict: 配置数据字典，如果加载失败则引发异常。
        """
        if not cls.CONFIG_FILE.exists():
            raise FileNotFoundError(f"错误: 配置文件 {cls.CONFIG_FILE} 未找到。")
        with cls.CONFIG_FILE.open("rb") as f:
            return tomllib.load(f)

    @property
    def database_url(self) -> str:
        return str(self.pydantic_config.database_url)

    @property
    def p_interval(self) -> str:
        return self.pydantic_config.database.p_interval

    @property
    def p_premake(self) -> int:
        return self.pydantic_config.database.p_premake

    @property
    def redis_url(self) -> str:
        return str(self.pydantic_config.redis_url)

    @property
    def BDUSS(self) -> str:  # noqa: N802
        return self.pydantic_config.tieba.BDUSS

    @property
    def forums(self) -> list[str]:
        return self.pydantic_config.tieba.forums

    @property
    def max_backfill_pages(self) -> int:
        return self.pydantic_config.tieba.max_backfill_pages

    @property
    def rps_limit(self) -> int:
        return self.pydantic_config.rate_limit.rps

    @property
    def concurrency_limit(self) -> int:
        return self.pydantic_config.rate_limit.concurrency

    @property
    def scheduler_interval_seconds(self) -> int:
        return self.pydantic_config.scheduler.interval_seconds

    def __repr__(self) -> str:
        """返回配置对象的字符串表示。

        Returns:
            str: 包含主要配置项的字符串表示。
        """
        return (
            f"Config(database_url={self.database_url}, p_interval={self.p_interval}, p_premake={self.p_premake}, "
            f"redis_url={self.redis_url}, "
            f"BDUSS={self.BDUSS}, forums={self.forums}, "
            f"rps_limit={self.rps_limit}, concurrency_limit={self.concurrency_limit}, "
            f"scheduler_interval_seconds={self.scheduler_interval_seconds}, "
            f"mode={self.mode})"
        )
