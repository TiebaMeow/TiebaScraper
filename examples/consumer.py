import asyncio
import json

from redis.asyncio import Redis

from .deserialization import deserialize


async def main():
    redis = Redis(host="localhost", port=6379, db=0)
    try:
        await redis.xgroup_create("events:thread", "mygroup", id="0", mkstream=True)
    except Exception:
        pass
    while True:
        message = await redis.xreadgroup("mygroup", "consumer1", streams={"events:thread": ">"}, count=1, block=0)
        thread = json.loads(message[0][1][0][1][b"data"].decode("utf-8"))["payload"]
        thread_obj = deserialize("thread", thread)
        print(thread_obj)


if __name__ == "__main__":
    asyncio.run(main())
