import asyncio
import json
from datetime import datetime

from services.redis_stream import RedisStream

class LiquidityTrapEngine:
    def __init__(self):
        self.redis = RedisStream()
        self.input_stream = "micro_ticks"
        self.output_stream = "strategy_signals"
        self.last_id = self.redis.get_latest_id(self.input_stream)
        
        self.open_prices = {}
        
    async def start(self):
        print("Liquidity Trap Engine started")
        while True:
            try:
                streams = self.redis.read(self.input_stream, self.last_id)
                if not streams:
                    await asyncio.sleep(0.05)
                    continue
                    
                for stream, entries in streams:
                    for msg_id, payload in entries:
                        self.last_id = msg_id
                        raw = payload.get("data")
                        if not raw: continue
                        tick = json.loads(raw)
                        await self.evaluate_trap(tick)
                await asyncio.sleep(0.001)
            except Exception as e:
                print(f"LiquidityTrapEngine Error: {e}")
                await asyncio.sleep(1)

    async def evaluate_trap(self, tick):
        symbol = tick.get("symbol")
        price = tick.get("ltp", 0)
        
        if not symbol or price <= 0: return

        # Time window 9:15 to 10:00 AM
        now = datetime.now()
        if now.hour > 10: return
        
        if symbol not in self.open_prices:
            self.open_prices[symbol] = price
            
        open_price = self.open_prices[symbol]
        
        # If stock is down > 2% from its own open price within the first 45 mins after a gap up
        # We assume it's a Trap reversal.
        if price < open_price * 0.98:
            signal = {
                "symbol": symbol,
                "signal": "SELL",
                "price": price,
                "score": 90,
                "strategy": "LIQUIDITY_TRAP_SHORT",
                "timestamp": datetime.utcnow().isoformat(),
            }
            print(f"🪤 LIQUIDITY TRAP | Selling reversed gap: {symbol} @ {price}")
            await self.redis.publish(self.output_stream, signal)
            await asyncio.sleep(5) # debounce
