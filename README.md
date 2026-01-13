<div align="center">

# TiebaScraper

_基于 aiotieba 的高性能百度贴吧异步爬虫工具，支持实时监控和历史数据回溯。_

</div>

## 功能特性

- **三种运行模式可选**：
  - **实时监控模式**: 持续监控指定贴吧首页的最新内容
  - **历史数据回溯模式**: 抓取指定贴吧的历史数据
  - **混合模式**: 同时运行实时监控和历史数据回溯
- **高性能、高可靠**：
  - 基于 asyncio/uvloop 的全异步架构
  - 同时支持速率限制和并发控制
  - 内置重试与防 429 雪崩保护机制
  - 支持 PostgreSQL 数据库分区存储
  - 支持使用 Redis 进行缓存优化
- **可扩展性**：
  - 合理的表结构设计，支持高效存储与查询大量数据
  - 通过 WebSocket/Redis 支持高度可自定义的实时内容审查
  - 实时监控模式支持通过 WebSocket 动态添加或删除监控的贴吧
  - 与 TiebaMeow 工具集无缝集成

## 环境要求

- [uv](https://docs.astral.sh/uv/)
- [PostgreSQL](https://www.postgresql.org/) 14+
- （可选）[TimescaleDB](https://github.com/timescale/timescaledb)
- （可选）[Redis](https://redis.io/) 6.0+

## 安装依赖

```shell
uv sync
```

`uv` 会自动下载合适的 Python 版本并安装所有依赖。对于 Linux 系统，将自动同时安装并在运行时尝试使用 `uvloop`。

## 参数配置

1. 复制配置文件模板

    ```bash
    # Linux / macOS
    cp config.example.toml config.toml
    # Windows (PowerShell)
    Copy-Item config.example.toml config.toml
    ```

2. 编辑 `config.toml` 文件，根据注释填写相应的配置项

3. （可选）使用分组配置，可以为不同的分组设置不同的调度周期

4. 为 `TiebaScraper` 创建一个单独的数据库（名称可以自定义）：

    ```sql
    CREATE DATABASE tieba_data;
    ```

5. 推荐单独为 `TiebaScraper` 创建一个用户：

    ```sql
    CREATE USER your_username WITH PASSWORD 'your_password';
    GRANT ALL PRIVILEGES ON DATABASE tieba_data TO your_username;
    \c tieba_data
    GRANT ALL PRIVILEGES ON SCHEMA public TO your_username;
    GRANT CREATE ON SCHEMA public TO your_username;
    ```

### 数据库分区配置（可选）

如果你的数据量不大（单表量级在百万行以下），可以不启用分区。请注意，我们将爬取到的所有数据存到了 `thread`、`post`、`comment` 三张表中。

如果你希望对数据进行分区存储，以避免单表数据量过大，可以使用 `TimescaleDB` 扩展来管理 PostgreSQL 数据库中的分区表。

首先，请确保配置文件中的 `partition_enabled` 选项设置为 `true`：

```toml
partition_enabled = true
```

并根据你的数据量合理设置 p_interval 的值（如 "3 months"），以确保单个分区不会太大。

然后请确保 PostgreSQL 数据库已安装 `TimescaleDB` 扩展。可以参考[官方文档](https://www.tigerdata.com/docs/self-hosted/latest/install)进行安装，推荐直接使用官方 Docker 镜像 `timescale/timescaledb:latest-pg17`。

## 使用方法

### 三种运行模式

你可以在以下三种模式中选择适合你场景的模式运行：

#### 实时监控模式

实时监控模式下，程序将定期扫描指定贴吧的首页，并根据配置将获取到的新内容推送到消费者队列中。

```bash
uv run main.py
```

#### 历史数据回溯模式

历史数据回溯模式下，程序将根据用户定义的深度抓取指定贴吧的历史数据。

```bash
uv run main.py --mode backfill
```

#### 混合模式

混合模式下，程序将同时运行实时监控和历史数据回溯。混合模式的历史数据回溯任务将从第2页开始抓取。

```bash
uv run main.py --mode hybrid
```

### 内容审查

当程序运行在实时监控模式或混合模式时，你可以在 `config.toml` 中配置通过 WebSocket/Redis 将内容推送到审查服务进行处理。你可以在 [examples](./examples) 目录下找到消费者示例和反序列化示例。

你可以选择两种消息格式：

- `id` 模式仅推送内容类型与 tid/pid，需要内容审查端通过回表查询或手动 fetch 获取完整对象，不过你可以获得更小的序列化/反序列化开销与更小的消息体。
- `object` 模式会推送完整序列化后的对象，你可以直接获得完整数据，并可通过 [tiebameow](https://github.com/TiebaMeow/tiebameow/blob/main/src/tiebameow/serializer/serializer.py) 提供的反序列化函数反序列化得到完整的 `tiebameow` DTO 对象，但会带来更大的消息体与更大的序列化/反序列化开销。DTO 对象的定义请参考 [tiebameow/models/dto.py](https://github.com/TiebaMeow/tiebameow/blob/main/src/tiebameow/models/dto.py)。

如果你希望 `TiebaScraper` -> `内容审查服务` 的消息推送足够可靠，或者说你不希望漏掉任何一条内容，推荐使用 Redis + object 模式。object 模式下的 Redis Streams 可以保证消息不丢失，并且可以通过消费者组来实现多实例水平扩展。详细原理可参考 [Redis Streams 官方文档](https://redis.io/docs/latest/develop/data-types/streams/) 或 [中文教程](https://redis.com.cn/redis-stream.html)。

TiebaMeow 提供了一个基于 NoneBot2 的 QQ 机器人 [TiebaManageBot](https://github.com/TiebaMeow/TiebaManageBot)，你可以使用它提供的无缝集成来快速配置简单的内容审查服务。你也可以使用任何你喜欢的技术栈来实现审查服务，只要它能够连接到 WebSocket/Redis 并处理消息即可。

### 动态添加/删除贴吧

运行于实时监控模式时，`TiebaScraper` 支持动态添加或删除监控的贴吧。你可以通过 WebSocket 连接发送以下格式的消息来实现：

- 添加贴吧：

    ```json
    {
        "type": "add_forum",
        "fname": "贴吧名"
    }
    ```

- 删除贴吧：

    ```json
    {
        "type": "remove_forum",
        "fname": "贴吧名"
    }
    ```

动态添加的贴吧将会使用全局默认的调度周期运行。

## Docker 部署

### 使用 docker-compose

1. **准备配置文件**:

    复制 Docker 环境配置文件模板：

    ```bash
    # Linux / macOS
    cp config.docker.example.toml config.toml
    # Windows (PowerShell)
    Copy-Item config.docker.example.toml config.toml
    ```

    然后，根据你的需求编辑 `config.toml` 文件。**请注意**，在 Docker 环境中，应用容器需要通过服务名（`postgres` 和 `redis`）来访问数据库和 Redis，因此请确保主机名配置正确。`config.docker.example.toml` 已预设了正确的主机名。

    你也可以使用环境变量来传递配置参数，环境变量将会优先于配置文件生效，详情请参考 `.env.example` 中的注释。

2. **启动服务**:

    在项目根目录下运行以下命令：

    ```bash
    docker compose up -d
    ```

    该命令会从 Docker Hub 拉取 `tiebameow/tiebascraper` 和 `timescale/timescaledb:latest-pg17` 镜像，并启动应用、PostgreSQL（已预装 `TimescaleDB`）和 Redis 服务。

3. **查看日志**:

    ```bash
    docker compose logs -f app
    ```

4. **切换运行模式**:

    默认情况下，应用以 `periodic`（实时监控）模式运行。如果你需要切换模式，可以运行以下命令：

    ```bash
    docker compose run --rm app --mode backfill
    ```

5. **停止服务**:

    ```bash
    docker compose down
    ```

### 其他注意事项

- 可以根据需要使用已有的 PostgreSQL 和 Redis 实例，修改 `config.toml` 中的连接信息并删除 `docker-compose.yml` 中对应的服务定义即可。
- 若需外部访问 WebSocket，请在配置中将 `websocket.host` 设置为 `0.0.0.0` 并在 `docker-compose.yml` 中映射端口。

## 项目结构

```text
.
├── examples/               # 示例代码（消费者实现等）
├── src/
│   ├── core/               # 核心架构模块
│   │   ├── config.py       # 配置加载与 Pydantic 模型定义
│   │   ├── container.py    # 依赖注入容器 (IoC)，管理 DB/Redis/Client 生命周期
│   │   ├── datastore.py    # 数据持久化逻辑 (PostgreSQL/TimescaleDB)
│   │   ├── initialize.py   # 应用初始化流程
│   │   ├── publisher.py    # 消息推送 (Redis/WebSocket)
│   │   └── ws_server.py    # WebSocket 服务端实现
│   ├── scraper/            # 爬虫业务逻辑
│   │   ├── scheduler.py    # 任务调度器 (周期性/回溯模式)
│   │   ├── tasks.py        # 任务定义 (Task 数据类)
│   │   └── worker.py       # 任务执行器 (Worker)
│   └── utils/              # 通用工具函数
├── tests/                  # 测试用例
├── config.toml             # 配置文件
├── main.py                 # 程序入口
└── pyproject.toml          # 项目依赖与构建配置
```
