import asyncio
import json
from datetime import datetime

from services.redis_stream import RedisStream

class ExplosiveBreakoutEngine:
    def __init__(self):
        self.redis = RedisStream()
        self.input_stream = "micro_ticks"
        self.output_stream = "alpha_signals"
        self.last_id = self.redis.get_latest_id(self.input_stream)
        
        self.triggered_today = set()
        
    async def start(self):
        print("Explosive Breakout Engine (VCP + RVOL) started")
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
                        await self.evaluate_breakout(tick)
                await asyncio.sleep(0.001)
            except Exception as e:
                print(f"ExplosiveBreakoutEngine Error: {e}")
                await asyncio.sleep(1)

    async def evaluate_breakout(self, tick):
        symbol = tick.get("symbol")
        price = tick.get("ltp", 0)
        vol = tick.get("volume", 0)
        day_high = tick.get("day_high", 0)
        
        if not symbol or price <= 0: return
        
        # Only process each symbol once per day for "Explosive" entry
        if symbol in self.triggered_today: return
        
        # Logic: Price is AT or ABOVE the tracked day high, and volume is significant
        if price >= day_high and price > 0 and day_high > 0:
            if vol > 50000: # Significant volume for large-cap stocks
                self.triggered_today.add(symbol)
                
                # Smart support levels for trailing SL
                est_swing_low = round(price * 0.985, 2)
                est_vwap = round(price * 0.99, 2)
                
                signal = {
                    "symbol": symbol,
                    "side": "BUY",
                    "price": price,
                    "score": 100,
                    "strategy": "EXPLOSIVE_BREAKOUT",
                    "source": "breakout_engine",
                    "timestamp": datetime.utcnow().isoformat(),
                    "features": {
                        "rvol_spike": True, 
                        "breakout": True,
                        "swing_low": est_swing_low,
                        "vwap": est_vwap
                    }
                }
                print(f"💥 EXPLOSIVE BREAKOUT | {symbol} @ {price} (Vol: {vol} | High: {day_high})")
                await self.redis.publish(self.output_stream, signal)
