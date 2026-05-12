import asyncio
import json
from datetime import datetime, timedelta, timezone
from collections import deque

from services.redis_stream import RedisStream

class GammaSqueezeEngine:
    def __init__(self):
        self.redis = RedisStream()
        self.input_stream = "micro_ticks"
        self.output_stream = "alpha_signals"
        self.last_id = self.redis.get_latest_id(self.input_stream)
        self.windows = {}
        self.volume_windows = {}
        
    async def start(self):
        print("Gamma Squeeze Engine started")
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
                        await self.evaluate_gamma(tick)
                await asyncio.sleep(0.001)
            except Exception as e:
                print(f"GammaSqueezeEngine Error: {e}")
                await asyncio.sleep(1)

    async def evaluate_gamma(self, tick):
        symbol = tick.get("symbol")
        price = tick.get("ltp") or tick.get("price", 0)
        volume = tick.get("volume", 0)
        
        if not symbol or price <= 0: return

        # ─── SaaS Quality: Price Sanity ───
        if symbol == "BANKNIFTY" and (price < 45000 or price > 60000):
            return
        if symbol == "NIFTY" and (price < 18000 or price > 26000):
            return
        
        ist = timezone(timedelta(hours=5, minutes=30))
        now = datetime.now(ist)
        hour = now.hour
        minute = now.minute
        weekday = now.weekday() # 0 = Mon, 1 = Tue, 2 = Wed, 3 = Thu, 4 = Fri
        
        # User Defined Expiry Rules:
        is_nifty_expiry = (symbol == "NIFTY" and weekday == 3) # Thursday
        is_banknifty_expiry = (symbol == "BANKNIFTY" and weekday == 2) # Wednesday
        
        if not (is_nifty_expiry or is_banknifty_expiry):
            return
            
        # ─── 3. Rolling Window for Velocity ───
        if symbol not in self.windows:
            self.windows[symbol] = deque(maxlen=20)
            self.volume_windows[symbol] = deque(maxlen=20)
            
        self.windows[symbol].append(price)
        self.volume_windows[symbol].append(volume)
        
        if len(self.windows[symbol]) < 10:
            return

        # ─── 4. Velocity Check (Momentum) ───
        # Price must have moved at least 0.1% in the last 10 ticks
        move_pct = abs(price - self.windows[symbol][0]) / self.windows[symbol][0]
        if move_pct < 0.001:
            return

        # ─── 5. Strike Proximity ───
        step = 100 if "BANK" in symbol else 50
        dist_to_strike = abs(price % step)
        if dist_to_strike > 10: # Tightened from 5 to avoid noise, but allowed more range
            return
            
        # ─── 6. Volume Spike (SaaS Grade) ───
        avg_vol = sum(self.volume_windows[symbol]) / len(self.volume_windows[symbol])
        if volume < avg_vol * 1.5:
            return

        # ─── 7. Signal Generation ───
        side = "BUY" if price > self.windows[symbol][0] else "SELL"
        
        signal = {
            "symbol": symbol,
            "side": side,
            "price": price,
            "score": 85,
            "strategy": "GAMMA_SQUEEZE",
            "timestamp": datetime.utcnow().isoformat(),
            "features": {
                "velocity": round(move_pct * 100, 3),
                "strike_dist": round(dist_to_strike, 2),
                "vol_multiplier": round(volume / (avg_vol + 1), 2)
            }
        }
        
        print(f"☢️ GAMMA SQUEEZE | {symbol} @ {price} | Velocity: {move_pct*100:.2f}% | Side: {side}")
        await self.redis.publish(self.output_stream, signal)
        
        # Debounce: No repeat signals for 2 minutes
        await asyncio.sleep(120)
