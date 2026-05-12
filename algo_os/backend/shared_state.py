from dataclasses import dataclass, field
from typing import Dict, List, Optional
import time

class StockState:
    __slots__ = [
        'symbol', 'price', 'volume', 'prev_close', 'day_high', 'day_low',
        'sum_pv', 'sum_v', 'vwap', 'prev_below_vwap', 'last_signal_time'
    ]
    
    def __init__(self, symbol: str):
        self.symbol = symbol
        self.price = 0.0
        self.volume = 0.0
        self.prev_close = 0.0
        self.day_high = -float('inf')
        self.day_low = float('inf')
        self.sum_pv = 0.0
        self.sum_v = 0.0
        self.vwap = 0.0
        self.prev_below_vwap = False
        self.last_signal_time = 0.0
    
    @property
    def pct_change(self) -> float:
        if self.prev_close == 0: return 0.0
        return (self.price - self.prev_close) / self.prev_close * 100

    def update(self, price: float, volume: float, prev_close: float):
        self.price = price
        self.prev_close = prev_close
        self.volume = volume # This should be cumulative day volume from tick
        
        if price > self.day_high: self.day_high = price
        if price < self.day_low: self.day_low = price
        
        # VWAP calculation (Assuming volume in tick is cumulative or delta?)
        # Standard micro_ticks usually have cumulative volume. 
        # If it's delta, we add to sum_v. If cumulative, we use it directly for sum_v.
        # Contract says volume, price. We'll treat it as cumulative for VWAP efficiency.
        # But wait, VWAP = sum(p*v_delta) / sum(v_delta).
        # We'll use a simplified approximation or assume we get delta if needed.
        # Most high-perf systems use cumulative: sum_pv and sum_v.
        
        # Approximation for rolling:
        self.sum_pv += price * (volume - self.sum_v if self.sum_v > 0 else volume)
        self.sum_v = volume
        if self.sum_v > 0:
            self.vwap = self.sum_pv / self.sum_v

@dataclass
class SectorState:
    name: str
    stocks: List[str] = field(default_factory=list)
    score: float = 0.0
    leaders: List[str] = field(default_factory=list)
    
    # Components
    rel_strength: float = 0.0
    breadth: float = 0.0
    vol_score: float = 0.0
    momentum_density: float = 0.0

# STATIC SECTOR MAP
SECTOR_MAP = {
    "RELIANCE": "ENERGY", "ONGC": "ENERGY", "BPCL": "ENERGY",
    "TCS": "IT", "INFY": "IT", "HCLTECH": "IT", "WIPRO": "IT",
    "HDFCBANK": "BANKING", "ICICIBANK": "BANKING", "SBIN": "BANKING", "KOTAKBANK": "BANKING", "AXISBANK": "BANKING",
    "BHARTIARTL": "TELECOM",
    "ITC": "FMCG", "HINDUNILVR": "FMCG", "NESTLEIND": "FMCG",
    "LT": "INFRA",
    "M&M": "AUTO", "TATAMOTORS": "AUTO", "MARUTI": "AUTO",
    "SUNPHARMA": "PHARMA", "DRREDDY": "PHARMA", "CIPLA": "PHARMA"
}

REVERSE_SECTOR_MAP = {}
for stock, sector in SECTOR_MAP.items():
    if sector not in REVERSE_SECTOR_MAP: REVERSE_SECTOR_MAP[sector] = []
    REVERSE_SECTOR_MAP[sector].append(stock)
