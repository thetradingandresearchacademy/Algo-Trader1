import asyncio
import json
import time
from backend.redis_client import RedisClient

class OptionsExecutionEngine:
    """
    Translates Stock/Gamma signals into Option trades.
    Handles Strike Selection, Premium Filters, and Initial SL/Target set.
    """
    def __init__(self):
        self.redis = RedisClient()
        self.oi_bias = "NEUTRAL"
        self.oi_data = {}
        self.latest_chain = {}

    async def start(self):
        await self.redis.connect()
        print("Options Execution Engine Started")
        
        await asyncio.gather(
            self.listen_signals(),
            self.listen_oi(),
            self.listen_chain_updates()
        )

    async def listen_oi(self):
        last_id = "$"
        while True:
            results = await self.redis.read_stream({"oi_signals": last_id}, block=1000)
            if not results: continue
            for _, messages in results:
                for msg_id, payload in messages:
                    last_id = msg_id
                    self.oi_data = json.loads(payload["data"])
                    self.oi_bias = self.oi_data.get("bias", "NEUTRAL")

    async def listen_chain_updates(self):
        # Listen for real-time option LTP updates
        last_id = "$"
        while True:
            results = await self.redis.read_stream({"option_ticks": last_id}, block=500, count=100)
            if not results: continue
            for _, messages in results:
                for msg_id, payload in messages:
                    last_id = msg_id
                    data = json.loads(payload["data"])
                    self.latest_chain[data["symbol"]] = data["ltp"]

    async def listen_signals(self):
        last_id = "$"
        while True:
            results = await self.redis.read_stream(["stock_signals", "gamma_signals"], last_id, block=500)
            if not results: continue
            for stream, messages in results:
                for msg_id, payload in messages:
                    last_id = msg_id
                    signal = json.loads(payload["data"])
                    await self.process_signal(signal)

    async def process_signal(self, signal):
        # 1. Alignment Filter
        if self.oi_bias != "NEUTRAL" and self.oi_bias not in signal["signal"]:
            return # Block if not aligned with smart money bias

        symbol = signal["symbol"]
        price = signal["price"]
        direction = "CE" if "CALL" in signal["signal"] else "PE"
        velocity = signal.get("velocity", 0)
        trigger = signal.get("trigger", "NORMAL")

        # 2. Strike Selection
        step = 50 if symbol == "NIFTY" else 100
        atm = round(price / step) * step
        
        # PRO Logic: Use ATM+1 for fast moves
        if abs(velocity) > (step * 0.1):
            strike = atm + step if direction == "CE" else atm - step
        else:
            strike = atm

        option_symbol = f"{symbol}_{strike}_{direction}"
        premium = self.latest_chain.get(option_symbol, 0)

        # 3. Premium Filter (₹10 - ₹250)
        if premium < 10 or premium > 250: return

        # 4. SL / Target Set
        sl_pct = 0.6 if trigger == "GAMMA_EXPLOSION" else 0.7 # 40% vs 30% SL
        
        trade = {
            "symbol": symbol,
            "option_symbol": option_symbol,
            "entry": premium,
            "sl": premium * sl_pct,
            "t1": premium * 1.3,
            "t2": premium * 1.6,
            "t3": premium * 2.0,
            "trigger": trigger,
            "velocity": velocity,
            "timestamp": time.time()
        }
        await self.redis.publish("option_trades", trade)
        print(f"🎯 Trade Dispatched: {option_symbol} @ {premium}")
