import redis.asyncio as redis
import json
import asyncio

class RedisClient:
    def __init__(self, host="127.0.0.1", port=6379):
        self.host = host
        self.port = port
        self.client = None

    async def connect(self):
        if not self.client:
            self.client = redis.Redis(host=self.host, port=self.port, decode_responses=True)
        return self.client

    async def publish(self, stream, payload):
        try:
            await self.client.xadd(stream, {"data": json.dumps(payload)})
        except Exception as e:
            print(f"Redis Publish Error [{stream}]: {e}")

    async def read_stream(self, streams_dict, block=100, count=10):
        try:
            return await self.client.xread(streams_dict, block=block, count=count)
        except Exception as e:
            print(f"Redis Read Error: {e}")
            return []
