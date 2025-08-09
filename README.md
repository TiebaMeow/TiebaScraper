<div align="center">

# TiebaScraper

_基于 aiotieba 的高性能百度贴吧异步爬虫工具，支持实时监控和历史数据回溯。_

</div>

## 功能特性

- **实时监控模式**: 持续监控指定贴吧首页的最新内容并推送到 Redis 队列
- **历史数据回溯**: 抓取指定贴吧的历史数据
- **异步高性能**: 基于 asyncio 的全异步架构
- **数据持久化**: 支持 PostgreSQL 数据库存储
- **缓存支持**: 使用 Redis 进行缓存优化
- **限流保护**: 同时支持速率限制和并发控制

## 环境要求

- [uv](https://docs.astral.sh/uv/)
- [PostgreSQL](https://www.postgresql.org/) 14+
- [pg_partman](https://github.com/pgpartman/pg_partman) 5.2.4
- [Redis](https://redis.io/) 6.0+

## 数据库配置

为了支持数据分区存储，避免单表数据量过大，我们将使用 `pg_partman` 插件来管理 PostgreSQL 数据库中的分区表。

请确保 PostgreSQL 数据库与 `pg_partman` 插件已安装。Linux 用户可以参考 `pg_partman` 官方文档，使用 `make install` 命令安装插件。Windows 用户可以使用 WSL 或 Docker，也可以参考以下步骤：

> 1. 安装 [MSYS2](https://www.msys2.org/) 或类似环境
> 2. clone `pg_partman` 仓库到本地
> 3. 在 `pg_partman` 目录下创建并执行以下脚本：

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

> 4. 将生成的 `pg_partman--<version>.sql` 和 `pg_partman.control` 文件复制到 `C:\Program Files\PostgreSQL\<version>\share\extension` 目录下

请为 `TiebaScraper` 创建一个单独的数据库（名称可以自定义）：

```sql
CREATE DATABASE tieba_data;
```

推荐单独为 `TiebaScraper` 创建一个用户：

```sql
CREATE USER your_username WITH PASSWORD 'your_password';
GRANT ALL PRIVILEGES ON DATABASE tieba_data TO your_username;
```

## 安装依赖

```shell
uv sync --extra speedup
```

`uv` 会自动下载合适的 Python 版本并安装所有依赖。对于 Linux 系统，将自动同时安装并在运行时尝试使用 `uvloop`。

## 参数配置

1. 复制配置文件模板

    ```bash
    cp config.example.toml config.toml
    ```

2. 编辑 `config.toml` 文件，填入以下信息：

- `forums`: 要监控的贴吧列表，末尾不用带“吧”字
- `database`: PostgreSQL 数据库连接信息和分区配置
  - `host`: 数据库主机地址
  - `port`: 数据库端口（一般保持默认）
  - `username`: 数据库用户名
  - `password`: 数据库密码
  - `db_name`: 数据库名称
  - `p_interval`: 分区间隔
  - `p_premake`: 分区预创建数量
- `redis`: Redis 缓存服务器信息
  - `host`: Redis 主机地址
  - `port`: Redis 端口（一般保持默认）
  - `username`: Redis 用户名，没有可以留空为 `""`
  - `password`: Redis 密码，没有可以留空为 `""`

## 使用方法

### 实时监控模式

```bash
uv run python main.py
```

### 历史数据回溯模式

```bash
uv run python main.py --mode backfill
```

## 项目结构

```text
src/
├── core/           # 核心模块（依赖注入、数据存储等）
├── models/         # 数据模型定义
├── scraper/        # 爬虫核心（调度器、任务、工作器）
└── utils/          # 工具类
```
