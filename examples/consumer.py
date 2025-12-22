import asyncio
import json
from typing import Any

from redis.asyncio import Redis
from redis.exceptions import ResponseError
from tiebameow.serializer import deserialize


async def main() -> None:
    """示例：从单一 Redis Stream 消费对象事件，并通过 item_type 进行区分。"""

    # 创建异步 Redis 客户端并开启 decode_responses，避免应用层手动 decode
    redis = Redis(host="localhost", port=6379, db=0, decode_responses=True)

    # 指定 Stream 名称
    # stream key 构造：f"{stream_prefix}:{fid}:{object_type}"
    stream_key = "scraper:tieba:events:123456789:thread"
    # 指定消费者组和消费者名称
    # 不同消费者组可以同时获取同一条消息（广播）
    # 但同一消费者组中只有一个消费者会被分配到该条消息（负载均衡）
    group = "mygroup"
    consumer = "consumer1"

    # 创建消费者组，若已存在则忽略 BUSYGROUP 错误
    try:
        await redis.xgroup_create(stream_key, group, id="0", mkstream=True)
    except ResponseError as e:
        if "BUSYGROUP" not in str(e):
            raise

    try:
        while True:
            # 阻塞读取：最多等待 10 秒。使用 ">" 仅拉取未分配过的新消息。
            # NOACK=True：不把消息加入 PEL，无需 XACK。
            messages: list[tuple[str, list[tuple[str, dict[str, Any]]]]] | None = await redis.xreadgroup(
                group,
                consumer,
                streams={stream_key: ">"},
                count=1,
                block=10_000,  # 阻塞时间（毫秒），超时返回 None
                noack=True,
            )

            if not messages:
                continue

            # 消息结构示例：
            # [
            #     (
            #         "scraper:tieba:events:123456789:thread",
            #         [
            #             (
            #                 "1714389534321-0",
            #                 {"data": '{"object_type": "thread", "object_id": 12345, "payload": {...}}'},
            #             ),
            #         ],
            #     ),
            # ]
            try:
                _stream, entries = messages[0]
                msg_id, fields = entries[0]

                # 业务负载约定在字段 data 中，内容为 JSON 字符串
                raw = fields.get("data")
                if raw is None:
                    # 字段缺失：记录并跳过
                    print(f"[warn] message {msg_id} missing 'data' field: {fields}")
                    continue

                data = json.loads(raw)
                object_type = data.get("object_type")
                object_id = data.get("object_id")
                payload = data.get("payload")
                print(f"[info] received message {msg_id} of type {object_type} with object id {object_id}")
            except (IndexError, ValueError, TypeError, json.JSONDecodeError) as e:
                print(f"[error] failed to parse message: {messages!r} ({e})")
                continue

            # 反序列化为领域对象（Thread）并处理
            try:
                obj = deserialize(object_type, payload)
            except Exception as e:
                print(f"[error] deserialize failed for message {msg_id}: {e}")
                continue

            # 处理流程
            # ...
            print(obj)
            await asyncio.sleep(1)

            # 如果没有设定 noack=True，则需要在成功处理后使用 XACK 确认
            # await redis.xack(stream_key, group, msg_id)

    except asyncio.CancelledError:
        # 任务取消时允许向上传播以触发 finally
        raise
    except KeyboardInterrupt:
        # Ctrl+C
        print("[info] shutting down consumer...")
    finally:
        await redis.close()


if __name__ == "__main__":
    asyncio.run(main())
