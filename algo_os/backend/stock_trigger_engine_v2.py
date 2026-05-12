import asyncio
import json
import time
from datetime import datetime
from backend.redis_client import RedisClient
from backend.shared_state import StockState, SECTOR_MAP

class StockTriggerEngineV2:
    def __init__(self):
        self.redis = RedisClient()
        self.stocks = {}
        self.sector_data = {"all_scores": {}, "top_sectors": []}
        self.index_data = {"bias": "NEUTRAL", "strength": 0}
        self.trend_regime = False

    async def start(self):
        await self.redis.connect()
        print("Stock Trigger Engine V2 Started")
        
        await asyncio.gather(
            self.listen_ticks(),
            self.listen_sectors(),
            self.listen_index()
        )

    async def listen_ticks(self):
        last_id = "$"
        while True:
            results = await self.redis.read_stream({"micro_ticks": last_id}, block=500, count=100)
            if not results: continue
            for stream, messages in results:
                for msg_id, payload in messages:
                    last_id = msg_id
                    data = json.loads(payload["data"])
                    await self.process_tick(data)

    async def listen_sectors(self):
        last_id = "$"
        while True:
            results = await self.redis.read_stream({"sector_signals": last_id}, block=1000)
            if not results: continue
            for stream, messages in results:
                for msg_id, payload in messages:
                    last_id = msg_id
                    self.sector_data = json.loads(payload["data"])
                    
                    # Update Trend Regime
                    strong_sectors = sum(1 for s, score in self.sector_data["all_scores"].items() if score > 65)
                    self.trend_regime = (self.index_data["strength"] > 70 and strong_sectors >= 2)

    async def listen_index(self):
        last_id = "$"
        while True:
            results = await self.redis.read_stream({"index_signals": last_id}, block=1000)
            if not results: continue
            for stream, messages in results:
                for msg_id, payload in messages:
                    last_id = msg_id
                    self.index_data = json.loads(payload["data"])

    async def process_tick(self, data):
        symbol = data["symbol"]
        if symbol not in self.stocks:
            self.stocks[symbol] = StockState(symbol=symbol)
        
        stock = self.stocks[symbol]
        prev_below_vwap = stock.price < stock.vwap if stock.vwap > 0 else False
        
        stock.update(data["price"], data["volume"], data["prev_close"])
        
        trigger = None
        # 1. VWAP RECLAIM
        if prev_below_vwap and stock.price > stock.vwap:
            trigger = "VWAP_RECLAIM"
        # 2. BREAKOUT
        elif stock.price >= stock.day_high:
            trigger = "BREAKOUT"
        # 3. MOMENTUM
        elif stock.price > stock.vwap * 1.003:
            sector = SECTOR_MAP.get(symbol)
            if sector in self.sector_data["top_sectors"]:
                trigger = "MOMENTUM"

        if trigger:
            await self.fire_signal(stock, trigger)

    async def fire_signal(self, stock, trigger):
        now = time.time()
        if now - stock.last_signal_time < 20: return

        score = 0
        if self.index_data["bias"] == "BULLISH": score += 20
        sector = SECTOR_MAP.get(stock.symbol)
        if self.sector_data["all_scores"].get(sector, 0) > 60: score += 20
        if stock.pct_change > 1.5: score += 20 # leader
        score += 20 # structure/trigger
        
        threshold = 55 if self.trend_regime else 60
        if score >= threshold:
            stock.last_signal_time = now
            signal = {
                "symbol": stock.symbol,
                "signal": "CALL_BUY",
                "trigger": trigger,
                "score": score,
                "price": stock.price,
                "timestamp": now
            }
            await self.redis.publish("stock_signals", signal)
