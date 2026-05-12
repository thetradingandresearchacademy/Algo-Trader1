import asyncio
import json
import time
from backend.redis_client import RedisClient

class GammaEngineV2:
    """
    Detects Strike Zone escapes with high velocity and acceleration.
    Captures 'gamma explosion' events for options trading.
    """
    def __init__(self):
        self.redis = RedisClient()
        self.state = {}
        self.index_data = {"strength": 0}

    async def start(self):
        await self.redis.connect()
        print("Gamma Engine V2 Started")
        
        await asyncio.gather(
            self.listen_ticks(),
            self.listen_index()
        )

    async def listen_index(self):
        last_id = "$"
        while True:
            results = await self.redis.read_stream({"index_signals": last_id}, block=1000)
            if not results: continue
            for _, messages in results:
                for msg_id, payload in messages:
                    last_id = msg_id
                    self.index_data = json.loads(payload["data"])

    async def listen_ticks(self):
        last_id = "$"
        while True:
            results = await self.redis.read_stream({"micro_ticks": last_id}, block=500, count=100)
            if not results: continue
            for _, messages in results:
                for msg_id, payload in messages:
                    last_id = msg_id
                    data = json.loads(payload["data"])
                    await self.process_tick(data)

    async def process_tick(self, tick):
        symbol = tick["symbol"]
        if symbol not in ["NIFTY", "BANKNIFTY"]: return
        if self.index_data.get("strength", 0) < 65: return

        price = tick["price"]
        step = 50 if symbol == "NIFTY" else 100
        strike = round(price / step) * step

        s = self.state.setdefault(symbol, {
            "last_price": price, "last_velocity": 0, "strike": strike,
            "at_strike": False, "left_strike": False, "last_signal_time": 0
        })

        velocity = price - s["last_price"]
        acceleration = velocity - s["last_velocity"]
        
        at_strike = abs(price - strike) < (step * 0.1)
        if at_strike:
            s["at_strike"] = True
            s["strike"] = strike
            s["left_strike"] = False

        if s["at_strike"] and abs(price - s["strike"]) > step * 0.2:
            s["left_strike"] = True

        now = time.time()
        if s["left_strike"] and abs(velocity) > (step * 0.05) and acceleration > 0:
            if now - s["last_signal_time"] > 120:
                s["last_signal_time"] = now
                signal = {
                    "symbol": symbol,
                    "signal": "CALL_BUY" if velocity > 0 else "PUT_BUY",
                    "trigger": "GAMMA_EXPLOSION",
                    "price": price,
                    "velocity": velocity,
                    "acceleration": acceleration,
                    "timestamp": now
                }
                await self.redis.publish("gamma_signals", signal)

        s["last_price"] = price
        s["last_velocity"] = velocity
