"""
Regime Engine — Multi-Timeframe Market Regime Classifier

Classifies market regime across 3 timeframes:
  • Intraday  (from tick data, updated every 30s)
  • Daily     (from daily candles, updated every scan cycle)
  • Weekly    (from weekly aggregates)

Publishes: regime_state stream → consumed by MDE for directional bias
"""

import json
import asyncio
import time
from datetime import datetime, timedelta, timezone
from collections import deque, defaultdict

from services.redis_stream import RedisStream


class RegimeEngine:

    def __init__(self):
        self.redis = RedisStream()
        self.tick_stream = "micro_ticks"
        self.output_stream = "regime_state"
        self.last_tick_id = "$"  # CRITICAL: Use $ to read only NEW ticks, not replay old data

        # ─── Intraday State (per symbol) ─────────────────────
        self.price_windows = defaultdict(lambda: deque(maxlen=120))  # ~2 min of ticks
        self.volume_windows = defaultdict(lambda: deque(maxlen=120))
        self.vwap_state = defaultdict(lambda: {"sum_pv": 0.0, "sum_v": 0.0, "vwap": 0.0})
        self.vwma_window = defaultdict(lambda: deque(maxlen=20))  # 20-period VWMA

        # ─── Regime State ────────────────────────────────────
        self.intraday_regime = "NEUTRAL"   # TRENDING_UP, TRENDING_DOWN, RANGE_BOUND, VOLATILE
        self.daily_regime = "NEUTRAL"       # BULL_TREND, BEAR_TREND, CONSOLIDATION
        self.weekly_regime = "NEUTRAL"      # BULL_MARKET, BEAR_MARKET, TRANSITION

        # ─── Dual-Index Direction (Convergence/Divergence) ───
        # BULLISH_CONVERGENCE  = both indices trending up → buy dips
        # BEARISH_CONVERGENCE  = both indices trending down → sell rises
        # DIVERGENCE           = indices disagree → sit out / reduce size
        # NEUTRAL              = insufficient data
        self.index_direction = "NEUTRAL"

        # ─── VIX (Real India VIX from tick feed, token 26017) ───
        self.vix_proxy = 0.0   # Real India VIX value (typically 10-30)
        self.vix_history = deque(maxlen=60)  # track for trend
        self._real_vix_received = False  # Flag: True once we get real VIX tick
        self._min_ticks_received = 0     # Gate: don't act on data until enough ticks

        # Per-symbol regime
        self.symbol_regimes = {}

        self.last_publish = 0

    async def start(self):
        print("🧭 Regime Engine started | Multi-Timeframe Classification")

        asyncio.create_task(self._intraday_classifier())
        asyncio.create_task(self._periodic_publish())
        asyncio.create_task(self._heartbeat())

        while True:
            await asyncio.sleep(60)

    async def _heartbeat(self):
        """Terminal heartbeat every 60s."""
        while True:
            await asyncio.sleep(60)
            vix_label = "REAL" if self._real_vix_received else "PROXY"
            syms = list(self.symbol_regimes.keys())
            print(f"💓 [REGIME HEARTBEAT] Intraday={self.intraday_regime} | Direction={self.index_direction} | "
                  f"VIX={self.vix_proxy} [{vix_label}] | Symbols: {syms} | Ticks: {self._min_ticks_received}")

    # ─── Intraday Regime (Tick-Based) ────────────────────────

    async def _intraday_classifier(self):
        """Classify intraday regime from real-time tick data."""
        while True:
            try:
                streams = self.redis.read(self.tick_stream, self.last_tick_id)
                if not streams:
                    await asyncio.sleep(0.05)
                    continue

                for stream, entries in streams:
                    for msg_id, payload in entries:
                        self.last_tick_id = msg_id
                        raw = payload.get("data")
                        if not raw:
                            continue
                        tick = json.loads(raw)
                        self._process_tick(tick)

            except Exception as e:
                print(f"Regime tick error: {e}")
                await asyncio.sleep(1)

    def _process_tick(self, tick):
        symbol = tick.get("symbol")
        price = tick.get("ltp") or tick.get("price", 0)
        volume = tick.get("volume", 0)

        if not symbol or price <= 0:
            return

        self._min_ticks_received += 1

        # ─── REAL India VIX — direct from tick feed ──────────
        if symbol == "INDIAVIX":
            if price > 0 and price < 100:  # Sanity: real VIX is 5-80 range
                self.vix_proxy = round(price, 1)
                self._real_vix_received = True
                self.vix_history.append(price)
            return  # VIX is not a tradeable index, skip price/vwap processing

        # Update price window
        pw = self.price_windows[symbol]
        pw.append(price)

        # Update volume window
        vw = self.volume_windows[symbol]
        vw.append(volume)

        # ─── VWAP Calculation ────────────────────────────────
        vs = self.vwap_state[symbol]
        vol_delta = max(0, volume - vs.get("last_vol", 0))
        vs["last_vol"] = volume
        if vol_delta > 0:
            vs["sum_pv"] += price * vol_delta
            vs["sum_v"] += vol_delta
            if vs["sum_v"] > 0:
                vs["vwap"] = vs["sum_pv"] / vs["sum_v"]

        # ─── 20-period VWMA ──────────────────────────────────
        vwma_w = self.vwma_window[symbol]
        vwma_w.append({"price": price, "vol": vol_delta if vol_delta > 0 else 1})

        vwma = self._calc_vwma(vwma_w)

        # ─── Classify (only for index symbols) ───────────────
        if symbol in ("NIFTY", "BANKNIFTY") and len(pw) >= 30:
            regime = self._classify_intraday(symbol, price, pw, vs["vwap"], vwma)
            self.symbol_regimes[symbol] = {
                "regime": regime,
                "vwap": round(vs["vwap"], 2),
                "vwma_20": round(vwma, 2),
                "price": round(price, 2),
                "updated": datetime.utcnow().isoformat()
            }

            # Set global intraday regime from NIFTY
            if symbol == "NIFTY":
                self.intraday_regime = regime

            # Compute dual-index direction after both are classified
            self._compute_index_direction()

    def _calc_vwma(self, window):
        """Volume-Weighted Moving Average."""
        if not window:
            return 0
        total_pv = sum(d["price"] * d["vol"] for d in window)
        total_v = sum(d["vol"] for d in window)
        return total_pv / total_v if total_v > 0 else 0

    def _classify_intraday(self, symbol, price, prices, vwap, vwma):
        """
        Classify intraday regime based on price action relative to VWAP/VWMA.
        """
        prices_list = list(prices)
        if len(prices_list) < 20:
            return "NEUTRAL"

        recent = prices_list[-20:]
        older = prices_list[-40:-20] if len(prices_list) >= 40 else prices_list[:20]

        # Metrics
        above_vwap_count = sum(1 for p in recent if p > vwap)
        above_vwap_pct = above_vwap_count / len(recent)

        # Price range (volatility proxy)
        price_range = max(recent) - min(recent)
        avg_price = sum(recent) / len(recent)
        range_pct = price_range / avg_price if avg_price > 0 else 0

        # Higher highs / lower lows
        mid = len(recent) // 2
        first_half_high = max(recent[:mid]) if mid > 0 else 0
        second_half_high = max(recent[mid:])
        first_half_low = min(recent[:mid]) if mid > 0 else float('inf')
        second_half_low = min(recent[mid:])

        # VWMA slope
        vwma_slope = "flat"
        if vwma > 0 and avg_price > 0:
            if price > vwma * 1.001:
                vwma_slope = "up"
            elif price < vwma * 0.999:
                vwma_slope = "down"

        # ─── Classification Logic ────────────────────────────
        # VOLATILE: High range + frequent direction changes
        if range_pct > 0.012:  # Increased from 0.008 to allow stronger trends without being called volatile
            return "VOLATILE"

        # TRENDING_UP: Consistently above VWAP + higher highs
        if above_vwap_pct > 0.6 and second_half_high >= first_half_high and (vwma_slope == "up" or price > vwap * 1.002):
            return "TRENDING_UP"

        # TRENDING_DOWN: Consistently below VWAP + lower lows
        if above_vwap_pct < 0.4 and second_half_low <= first_half_low and (vwma_slope == "down" or price < vwap * 0.998):
            return "TRENDING_DOWN"

        # RANGE_BOUND: Oscillating around VWAP
        if 0.4 <= above_vwap_pct <= 0.6 and range_pct < 0.006:
            return "RANGE_BOUND"

        return "NEUTRAL"

    def _compute_index_direction(self):
        """Compare NIFTY and BANKNIFTY to determine convergence/divergence.
        
        Fund Manager Logic:
        - Both falling → BEARISH_CONVERGENCE → sell on rise, no fresh longs
        - Both rising  → BULLISH_CONVERGENCE → buy on dip, no fresh shorts
        - One up, one down → DIVERGENCE → reduce size, be selective
        - Either neutral → NEUTRAL → normal operation
        
        Also computes VIX proxy from recent price range volatility.
        """
        nifty = self.symbol_regimes.get("NIFTY", {})
        bnf = self.symbol_regimes.get("BANKNIFTY", {})

        nifty_regime = nifty.get("regime", "NEUTRAL")
        bnf_regime = bnf.get("regime", "NEUTRAL")

        bullish_set = {"TRENDING_UP"}
        bearish_set = {"TRENDING_DOWN"}

        old_direction = self.index_direction

        if nifty_regime in bearish_set and bnf_regime in bearish_set:
            self.index_direction = "BEARISH_CONVERGENCE"
        elif nifty_regime in bullish_set and bnf_regime in bullish_set:
            self.index_direction = "BULLISH_CONVERGENCE"
        elif (nifty_regime in bullish_set and bnf_regime in bearish_set) or \
             (nifty_regime in bearish_set and bnf_regime in bullish_set):
            self.index_direction = "DIVERGENCE"
        elif nifty_regime == "VOLATILE" or bnf_regime == "VOLATILE":
            self.index_direction = "VOLATILE"
        else:
            self.index_direction = "NEUTRAL"

        # ─── VIX: Use real India VIX if available, else fallback proxy ─
        if not self._real_vix_received:
            # Fallback proxy only if real VIX feed is missing
            nifty_prices = self.price_windows.get("NIFTY")
            if nifty_prices and len(nifty_prices) >= 60:
                prices_list = list(nifty_prices)[-60:]
                high = max(prices_list)
                low = min(prices_list)
                avg = sum(prices_list) / len(prices_list) if prices_list else 1
                range_pct = (high - low) / avg if avg > 0 else 0
                raw_vix = min(50, range_pct * 2500)  # Scaled to realistic VIX range (0-50)
                self.vix_history.append(raw_vix)
                if len(self.vix_history) >= 5:
                    self.vix_proxy = round(sum(list(self.vix_history)[-5:]) / 5, 1)
                else:
                    self.vix_proxy = round(raw_vix, 1)

        # ─── Log direction changes (only after enough data) ──
        if old_direction != self.index_direction and self._min_ticks_received >= 60:
            nifty_vwap_pos = "above" if nifty.get("price", 0) > nifty.get("vwap", 0) else "below"
            bnf_vwap_pos = "above" if bnf.get("price", 0) > bnf.get("vwap", 0) else "below"
            vix_label = "REAL" if self._real_vix_received else "PROXY"
            print(f"🧭 INDEX DIRECTION: {self.index_direction} | "
                  f"NIFTY={nifty_regime} (VWAP:{nifty_vwap_pos}) | "
                  f"BANKNIFTY={bnf_regime} (VWAP:{bnf_vwap_pos}) | "
                  f"VIX={self.vix_proxy} [{vix_label}]")

    # ─── Periodic Publish ────────────────────────────────────

    async def _periodic_publish(self):
        """Publish regime state every 10 seconds."""
        while True:
            try:
                # Add VWAP position for each symbol
                symbols_with_vwap = {}
                for sym, data in self.symbol_regimes.items():
                    symbols_with_vwap[sym] = {
                        **data,
                        "vwap_position": "above" if data.get("price", 0) > data.get("vwap", 0) else "below",
                    }

                state = {
                    "intraday": self.intraday_regime,
                    "daily": self.daily_regime,
                    "weekly": self.weekly_regime,
                    "index_direction": self.index_direction,
                    "vix_proxy": self.vix_proxy,
                    "symbols": symbols_with_vwap,
                    "timestamp": datetime.utcnow().isoformat()
                }
                await self.redis.publish(self.output_stream, state)
            except Exception as e:
                print(f"Regime publish error: {e}")
            await asyncio.sleep(10)
