import asyncio
import json

from services.redis_stream import RedisStream


class PortfolioAllocatorEngine:

    def __init__(self):

        self.redis = RedisStream()

        self.input_stream = "portfolio_orders"

        self.last_id = "0-0"

        self.strategy_capital = {

            "OrderFlowAlpha": 0.30,
            "LiquiditySweepAlpha": 0.25,
            "VolatilityBreakoutAlpha": 0.25,
            "InstitutionalAccumulationAlpha": 0.20

        }

    async def start(self):

        print("Portfolio Allocator started")

        while True:

            streams = self.redis.read(self.input_stream, self.last_id)

            for stream, entries in streams:

                for msg_id, payload in entries:

                    self.last_id = msg_id

                    order = json.loads(payload["data"])

                    strategy = order["strategy"]

                    order["capital_weight"] = \
                        self.strategy_capital.get(strategy, 0.1)

                    await self.redis.publish("execution_orders", order)

            await asyncio.sleep(0.05)