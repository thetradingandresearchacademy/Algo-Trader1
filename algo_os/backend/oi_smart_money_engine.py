import asyncio
import json
import time
from backend.redis_client import RedisClient

class OISmartMoneyEngine:
    """
    Analyzes Option Chain for Walls and Traps.
    Updates every 20s or on chain snapshot.
    """
    def __init__(self):
        self.redis = RedisClient()
        self.last_bias = "NEUTRAL"

    async def start(self):
        await self.redis.connect()
        print("OI Smart Money Engine Started")
        
        await self.listen_option_chain()

    async def listen_option_chain(self):
        last_id = "$"
        while True:
            # option_chain stream expected to contain snapshot list
            results = await self.redis.read_stream({"option_chain_snapshot": last_id}, block=2000)
            if not results: continue
            for _, messages in results:
                for msg_id, payload in messages:
                    last_id = msg_id
                    data = json.loads(payload["data"])
                    await self.analyze_chain(data)

    async def analyze_chain(self, data):
        chain = data.get("chain", [])
        price = data.get("underlying_price", 0)
        if not chain or price == 0: return

        # 1. Wall Detection
        call_wall_item = max(chain, key=lambda x: x["CE_OI"])
        put_wall_item = max(chain, key=lambda x: x["PE_OI"])
        
        call_wall = call_wall_item["strike"]
        put_wall = put_wall_item["strike"]

        # 2. Trap Detection
        call_trap = (price > call_wall and call_wall_item.get("CE_OI_change", 0) > 0)
        put_trap = (price < put_wall and put_wall_item.get("PE_OI_change", 0) > 0)

        bias = "NEUTRAL"
        if call_trap: bias = "BULLISH"
        elif put_trap: bias = "BEARISH"
        
        # 3. Smart Money Score
        score = 0
        if call_trap or put_trap: score += 30
        if price > call_wall or price < put_wall: score += 20
        
        result = {
            "symbol": data.get("symbol", "NIFTY"),
            "call_wall": call_wall,
            "put_wall": put_wall,
            "call_trap": call_trap,
            "put_trap": put_trap,
            "bias": bias,
            "score": score,
            "timestamp": time.time()
        }
        await self.redis.publish("oi_signals", result)
