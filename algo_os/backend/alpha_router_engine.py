import asyncio
import json

from services.redis_stream import RedisStream


class AlphaRouterEngine:

    def __init__(self):

        self.redis = RedisStream()

        self.input_stream = "strategy_signals"
        self.output_stream = "portfolio_orders"

        self.last_id = "0-0"

    async def start(self):

        print("Alpha Router Engine started")

        while True:

            streams = self.redis.read(self.input_stream, self.last_id)

            for stream, entries in streams:

                for msg_id, payload in entries:

                    self.last_id = msg_id

                    signal = json.loads(payload["data"])

                    if signal["score"] > 0.65:

                        await self.redis.publish(
                            self.output_stream,
                            signal
                        )

            await asyncio.sleep(0.05)