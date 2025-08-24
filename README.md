<div align="center">

# TiebaScraper

_基于 aiotieba 的高性能百度贴吧异步爬虫工具，支持实时监控和历史数据回溯。_

</div>

## 功能特性

- **实时监控模式**: 持续监控指定贴吧首页的最新内容并推送到 Redis 队列
- **历史数据回溯**: 抓取指定贴吧的历史数据
- **混合模式**: 同时运行实时监控和历史数据回溯
- **异步高性能**: 基于 asyncio 的全异步架构
- **数据持久化**: 支持 PostgreSQL 数据库分区存储
- **缓存支持**: 使用 Redis 进行缓存优化
- **限流保护**: 同时支持速率限制和并发控制
- **可扩展性**: 通过 Redis 队列支持高度可自定义的实时内容审查

## 环境要求

- [uv](https://docs.astral.sh/uv/)
- [PostgreSQL](https://www.postgresql.org/) 14+
- （可选）[pg_partman](https://github.com/pgpartman/pg_partman) 5.2.4
- [Redis](https://redis.io/) 6.0+

## 安装依赖

```shell
uv sync
```

`uv` 会自动下载合适的 Python 版本并安装所有依赖。对于 Linux 系统，将自动同时安装并在运行时尝试使用 `uvloop`。

## 参数配置

1. 复制配置文件模板

    ```bash
    cp config.example.toml config.toml
    ```

2. 编辑 `config.toml` 文件，根据注释填写相应的配置项

3. 为 `TiebaScraper` 创建一个单独的数据库（名称可以自定义）：

    ```sql
    CREATE DATABASE tieba_data;
    ```

4. 推荐单独为 `TiebaScraper` 创建一个用户：

    ```sql
    CREATE USER your_username WITH PASSWORD 'your_password';
    GRANT ALL PRIVILEGES ON DATABASE tieba_data TO your_username;
    ```

### 数据库分区配置（可选）

如果你的数据量不大（单表量级在百万行以下），可以不启用分区。请注意，我们将爬取到的所有数据存到了 `thread`、`post`、`comment` 三张表中。

如果你希望对数据进行分区存储，以避免单表数据量过大，可以使用 `pg_partman` 插件来管理 PostgreSQL 数据库中的分区表。

首先，请确保配置文件中的 `partition_enabled` 选项设置为 `true`：

```toml
partition_enabled = true
```

并根据你的数据量合理设置 p_interval 和 p_premake 的值，以确保单个分区不会太大，以及默认分区不会有太多数据。

然后请确保 PostgreSQL 数据库与 `pg_partman` 插件已安装。Linux 用户可以参考 `pg_partman` 官方文档，使用 `make install` 命令安装插件。Windows 用户可以使用 WSL 或 Docker，如果一定要使用 Windows 原生 PostgreSQL，请参考[Windows用户配置](#windows用户数据库分区配置)

#### Windows用户数据库分区配置

1. 安装 [MSYS2](https://www.msys2.org/) 或类似环境
2. clone `pg_partman` 仓库到本地
3. 在 `pg_partman` 目录下创建并执行以下脚本：

    ```bash
    #!/bin/bash
    EXTENSION=pg_partman
    VERSION=$(grep default_version $EXTENSION.control | \
            sed -e "s/default_version[[:space:]]*=[[:space:]]*'\([^']*\)'/\1/")

    cat sql/types/*.sql > "${EXTENSION}--${VERSION}.sql"
    cat sql/tables/*.sql >> "${EXTENSION}--${VERSION}.sql"
    cat sql/functions/*.sql >> "${EXTENSION}--${VERSION}.sql"
    cat sql/procedures/*.sql >> "${EXTENSION}--${VERSION}.sql"
    ```

    你也可以将上面的脚本使用 AI 翻译为 PowerShell 脚本，这样就无需 MSYS2 等类Unix环境。

4. 将生成的 `pg_partman--<version>.sql` 和 `pg_partman.control` 文件复制到 `C:\Program Files\PostgreSQL\<version>\share\extension` 目录下

## 使用方法

### 实时监控模式

实时监控模式下，程序将定期扫描指定贴吧的首页，并根据配置将获取到的新内容推送到消费者队列中。

```bash
uv run python main.py
```

如果你开启了分区，实时监控模式将会自动定期维护分区，为默认分区（未命中当前分区时间范围的数据）中的数据创建合适的分区，并将其迁移到相应的分区中。

### 历史数据回溯模式

历史数据回溯模式下，程序将根据用户定义的深度抓取指定贴吧的历史数据。

```bash
uv run python main.py --mode backfill
```

如果你开启了分区，在历史数据回溯模式下，你需要手动维护分区：

```sql
CALL partman.partition_data_proc('public.{table}');
CALL partman.run_maintenance_proc();
VACUUM ANALYZE public.{table};
```

将 `{table}` 替换为 `thread`, `post`, `comment` 分别执行。

### 混合模式

混合模式下，程序将同时运行实时监控和历史数据回溯。

```bash
uv run python main.py --mode hybrid
```

如果你开启了分区，混合模式下，程序也将自动定期维护分区。

## 项目结构

```text
src/
├── core/           # 核心模块（依赖注入、数据存储等）
├── models/         # 数据模型定义
├── scraper/        # 爬虫核心（调度器、任务、工作器）
└── utils/          # 工具类
```
