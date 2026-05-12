import asyncio
import json
from datetime import datetime
from services.redis_stream import RedisStream


# --- Inline State Classes ---
class StockState:
    def __init__(self, symbol):
        self.symbol = symbol
        self.price = 0.0
        self.volume = 0
        self.prev_close = 0.0
        self.vwap = 0.0
        self.pct_change = 0.0
        self._cum_vol = 0
        self._cum_pv = 0.0

    def update(self, price, volume, prev_close=None):
        self.price = price
        self.volume = volume
        if prev_close:
            self.prev_close = prev_close
        if self.prev_close > 0:
            self.pct_change = ((price - self.prev_close) / self.prev_close) * 100
        self._cum_vol += volume
        self._cum_pv += price * volume
        self.vwap = self._cum_pv / self._cum_vol if self._cum_vol > 0 else price


class SectorState:
    def __init__(self, name, stocks):
        self.name = name
        self.stocks = stocks
        self.rel_strength = 0.0
        self.breadth = 0.0
        self.momentum_density = 0.0
        self.score = 0.0


# Sector → stock mapping (Nifty 100 classification)
REVERSE_SECTOR_MAP = {
    "BANKING": ["HDFCBANK", "ICICIBANK", "SBIN", "KOTAKBANK", "AXISBANK", "INDUSINDBK", "AUBANK", "CANBK"],
    "IT": ["INFY", "TCS", "HCLTECH", "WIPRO", "TECHM", "LTIM", "PERSISTENT", "COFORGE", "MPHASIS"],
    "ENERGY": ["RELIANCE", "NTPC", "ONGC", "BPCL", "GAIL", "HINDPETRO", "IOC", "POWERGRID", "PFC", "REC"],
    "PHARMA": ["SUNPHARMA", "DRREDDY", "CIPLA", "DIVISLAB", "AUROPHARMA", "LUPIN", "IPCALAB"],
    "AUTO": ["TATAMOTORS", "M&M", "MARUTI", "BAJAJ-AUTO", "EICHERMOT", "TVSMOTOR", "HEROMOTOCO"],
    "METALS": ["TATASTEEL", "JSWSTEEL", "HINDALCO", "VEDL", "NMDC"],
    "FMCG": ["ITC", "HINDUNILVR", "NESTLEIND", "TATACONSUM", "PIDILITIND"],
    "INFRA": ["LT", "ADANIENT", "ADANIPORTS", "GRASIM", "ULTRACEMCO", "DLF", "GMRINFRA"],
    "FINANCIAL": ["BAJFINANCE", "BAJAJFINSV", "CHOLAFIN", "HDFCLIFE", "SBILIFE", "ICICIPRULI", "SBICARD"],
}


class SectorEngine:
    """
    Computes Sectoral Strength, Breadth, and Momentum.
    Updates rolling state per tick, computes scores every 20s.
    """
    def __init__(self):
        self.redis = RedisStream()
        self.stocks = {}
        self.sectors = {}
        self.index_return = 0.0
        
        for sector_name in REVERSE_SECTOR_MAP:
            self.sectors[sector_name] = SectorState(name=sector_name, stocks=REVERSE_SECTOR_MAP[sector_name])

    async def start(self):
        print("Sector Engine Started")
        
        await asyncio.gather(
            self.listen_ticks(),
            self.process_batch_loop()
        )

    async def listen_ticks(self):
        last_id = self.redis.get_latest_id("micro_ticks")
        while True:
            results = self.redis.read("micro_ticks", last_id)
            if not results:
                await asyncio.sleep(0.1)
                continue
            
            for stream, messages in results:
                for msg_id, payload in messages:
                    last_id = msg_id
                    data = json.loads(payload.get("data", "{}"))
                    symbol = data.get("symbol")
                    if not symbol:
                        continue
                    
                    if symbol not in self.stocks:
                        self.stocks[symbol] = StockState(symbol=symbol)
                    
                    self.stocks[symbol].update(
                        data.get("price", data.get("ltp", 0)),
                        data.get("volume", 0),
                        data.get("prev_close")
                    )
                    
                    if symbol == "NIFTY":
                        self.index_return = self.stocks[symbol].pct_change

    async def process_batch_loop(self):
        while True:
            await asyncio.sleep(20) # 20 sec batch
            await self.compute_sectors()

    async def compute_sectors(self):
        scores = {}
        top_sectors = []
        
        for name, sector in self.sectors.items():
            valid_stocks = [self.stocks[s] for s in sector.stocks if s in self.stocks]
            if not valid_stocks: continue
            
            # 1. Relative Strength
            avg_ret = sum(s.pct_change for s in valid_stocks) / len(valid_stocks)
            sector.rel_strength = avg_ret - self.index_return
            
            # 2. Breadth
            sector.breadth = sum(1 for s in valid_stocks if s.price > s.vwap) / len(valid_stocks)
            
            # 3. Momentum
            sector.momentum_density = sum(1 for s in valid_stocks if s.pct_change > 2.0) / len(valid_stocks)
            
            # Score logic: rel_strength*40 + breadth*25 + vol*20 + momentum*15
            # Simplified vol as 1.0 for now
            sector.score = (
                max(0, min(1, (sector.rel_strength + 2) / 4)) * 40 +
                sector.breadth * 25 +
                20 + # Base volume score
                sector.momentum_density * 15
            )
            scores[name] = sector.score
            
        # Top 2 best sectors
        sorted_sectors = sorted(scores.items(), key=lambda x: x[1], reverse=True)
        top_sectors = [s[0] for s in sorted_sectors[:2] if s[1] > 60]
        
        output = {
            "top_sectors": top_sectors,
            "all_scores": scores,
            "timestamp": datetime.utcnow().isoformat()
        }
        await self.redis.publish("sector_signals", output)
