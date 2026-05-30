import json
import asyncio
from datetime import datetime
from collections import defaultdict, deque
from services.redis_stream import RedisStream

class StockScoringEngine:
    """
    VWAP-based scoring system for stock intraday trades.
    Consumes: micro_ticks or feature stream
    Publishes to: alpha_signals
    
    Rules (Max 100):
    - Weekly VWAP cross: +30
    - Monthly VWAP breakout: +50
    - Monthly VWAP zone (±5%): +30
    - Above swing low: +10
    - Below monthly VWAP zone (3-5% edge): +10
    - Volume contraction: +10
    
    Trigger at Score >= 70.
    """

    def __init__(self):
        self.redis = RedisStream()
        self.input_stream = "micro_ticks"
        self.output_stream = "alpha_signals"
        self.last_id = self.redis.get_latest_id(self.input_stream)
        
        # Rolling windows for real feature computation
        self.price_windows = defaultdict(lambda: deque(maxlen=100))
        self.volume_windows = defaultdict(lambda: deque(maxlen=50))
        self.stock_states = {}
        self.last_compute = {}

    async def start(self):
        print("Stock Scoring Engine started")
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
                        if not raw:
                            continue
                        tick = json.loads(raw)
                        self.process_tick(tick)
                await asyncio.sleep(0.001)
            except Exception as e:
                print("StockScoringEngine error:", e)
                await asyncio.sleep(1)

    def process_tick(self, tick):
        symbol = tick.get("symbol")
        price = tick.get("ltp") or tick.get("price")
        
        if not price:
            return
        
        from datetime import datetime, time as dtime, timedelta, timezone
        ist = timezone(timedelta(hours=5, minutes=30))
        current_time = datetime.now(ist).time()
        if current_time >= dtime(15, 20) or current_time < dtime(9, 15):
            return
            
        # Always update rolling windows (even if we skip scoring due to cooldown)
        self.price_windows[symbol].append(price)
        vol = tick.get("volume", 0) or tick.get("trade_volume", 0)
        if vol > 0:
            self.volume_windows[symbol].append(vol)
            
        now = datetime.now()
        if symbol in self.last_compute and (now - self.last_compute[symbol]).seconds < 5:
            return
            
        self.last_compute[symbol] = now
            
        # Extract real features from tick data or scanner-enriched features
        features = tick.get("features", {})
        
        # Use real VWAP data if available (from scanner/alpha signals), else compute from price
        weekly_vwap = features.get("weekly_vwap") or features.get("vwap")
        monthly_vwap = features.get("monthly_vwap")
        swing_low = features.get("swing_low")
        
        # If VWAPs not in features, derive from rolling price history
        if not weekly_vwap:
            prices = [p for p in self.price_windows.get(symbol, [price]) if p > 0]
            weekly_vwap = sum(prices) / len(prices) if prices else price * 0.99
        if not monthly_vwap:
            monthly_vwap = weekly_vwap * 0.99  # Approximate from weekly if missing
        if not swing_low:
            prices = list(self.price_windows.get(symbol, [price]))
            swing_low = min(prices) if prices else price * 0.95
        
        # Volume contraction: compare current tick volume to rolling average
        vol = tick.get("volume", 0) or tick.get("trade_volume", 0)
        vol_window = self.volume_windows.get(symbol, [vol])
        avg_vol = sum(vol_window) / len(vol_window) if vol_window else 1
        volume_contraction = vol < avg_vol * 0.8 if avg_vol > 0 else False
        
        score = 0
        
        # 1. Weekly VWAP cross (+30)
        if price > weekly_vwap:
            score += 30
            
        # 2. Monthly VWAP Position (+50 for above, +20 for Breakout Zone)
        monthly_dist = (price - monthly_vwap) / monthly_vwap
        if monthly_dist > 0: # Above Monthly VWAP
            score += 50
            if monthly_dist <= 0.02: # Breakout Zone (within 2%)
                score += 20
        elif abs(monthly_dist) <= 0.03: # Retest/Near Zone below
            score += 20
            
        # 3. Structure Hold (+10)
        if price > swing_low:
            score += 10
            
        # 4. Position below monthly VWAP (+10)
        if price < monthly_vwap and 0.03 <= monthly_dist <= 0.05:
            score += 10
            
        # 5. Volume contraction (+10)
        if volume_contraction:
            score += 10
            
        # Threshold Logic
        if score >= 70:
            signal_type = "STRONG BUY" if score >= 85 else "BUY"
            
            # Simple state tracking to avoid duplicate signals (rate-limiting at source)
            if symbol not in self.stock_states or (datetime.now() - self.stock_states[symbol]).total_seconds() > 600:
                self.stock_states[symbol] = datetime.now()
                self._publish_signal(symbol, signal_type, price, score, swing_low, monthly_vwap)
                
    def _publish_signal(self, symbol, side, price, score, swing_low, vwap):
        signal = {
            "symbol": symbol,
            "signal": side,
            "price": price,
            "score": score,
            "source": "stock_scoring",
            "timestamp": datetime.utcnow().isoformat(),
            "features": {
                "swing_low": round(swing_low, 2) if swing_low else None,
                "vwap": round(vwap, 2) if vwap else None
            }
        }
        
        print(f"STOCK SIGNAL | {side} | {symbol} | SCORE: {score}")
        asyncio.create_task(self.redis.publish(self.output_stream, signal))

