"""
Index Alpha Engine v2 — VWAP + 20-VWMA Rejection Trading

Smart intraday signal generation for NIFTY/BANKNIFTY:
  • VWAP bounce/rejection trades on both sides
  • 20-period VWMA trend filter
  • Volume confirmation (1.5x spike)
  • 3-minute cooldown between signals per symbol
  • Regime-aware scoring
"""

import json
import asyncio
import os
import time
from collections import deque, defaultdict
from datetime import datetime

from services.redis_stream import RedisStream


class IndexAlphaEngine:

    def __init__(self):
        self.redis = RedisStream()
        self.input_stream = "volatility_expansion_signals"
        self.output_stream = "alpha_signals"
        self.regime_stream = "regime_state"
        self.last_id = self.redis.get_latest_id(self.input_stream)
        self.last_regime_id = self.redis.get_latest_id(self.regime_stream)

        # Rolling data per symbol
        self.price_window = defaultdict(lambda: deque(maxlen=60))
        self.volume_window = defaultdict(lambda: deque(maxlen=60))
        self.pressure_window = defaultdict(lambda: deque(maxlen=20))

        # VWAP state per symbol (intraday)
        self.vwap_state = defaultdict(lambda: {"sum_pv": 0.0, "sum_v": 0.0, "vwap": 0.0, "last_vol": 0})

        # VWMA 20-period window
        self.vwma_window = defaultdict(lambda: deque(maxlen=20))

        # Signal cooldown (3 min per symbol)
        self.last_signal_time = {}
        self.COOLDOWN_SECONDS = 300

        # Regime state
        self.current_regime = "NEUTRAL"
        self.index_direction = "NEUTRAL"  # from RegimeEngine dual-index analysis

        # Feature weights
        self.weights = self.load_weights()

    def load_weights(self):
        try:
            path = os.path.join("config", "feature_weights.json")
            with open(path) as f:
                return json.load(f)
        except Exception:
            return {
                "footprint": 0.25, "vacuum": 0.15, "sweep": 0.15,
                "accumulation": 0.15, "volatility": 0.15, "iceberg": 0.05,
                "vwap_rejection": 0.10
            }

    async def start(self):
        print("Index Alpha Engine v2 started | VWAP + VWMA Rejection Mode")

        asyncio.create_task(self._monitor_regime())
        asyncio.create_task(self._heartbeat())

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
                        if raw is None:
                            continue
                        features = json.loads(raw)
                        signal = self.generate_signal(features)
                        if signal:
                            payload_out = {
                                "symbol": features.get("symbol"),
                                "side": signal["side"],
                                "price": features.get("price"),
                                "score": signal.get("score", 75),
                                "timestamp": features.get("timestamp"),
                                "features": {**features, **signal.get("extra_features", {})}
                            }
                            side_emoji = "🟢" if signal["side"] == "BUY" else "🔴"
                            print(f"ALPHA {side_emoji} | {signal['side']} {features.get('symbol')} | "
                                  f"Score: {signal.get('score', 0)} | VWAP: {signal.get('vwap_tag', '')} | "
                                  f"Regime: {self.current_regime}")
                            await self.redis.publish(self.output_stream, payload_out)
                await asyncio.sleep(0.001)

            except Exception as e:
                print("IndexAlphaEngine error:", e)
                await asyncio.sleep(1)

    async def _monitor_regime(self):
        """Subscribe to regime_state for directional bias + index direction."""
        while True:
            try:
                streams = self.redis.read(self.regime_stream, self.last_regime_id)
                if streams:
                    for _, entries in streams:
                        for msg_id, payload in entries:
                            self.last_regime_id = msg_id
                            raw = payload.get("data")
                            if raw:
                                state = json.loads(raw)
                                self.current_regime = state.get("intraday", "NEUTRAL")
                                self.index_direction = state.get("index_direction", "NEUTRAL")
            except Exception:
                pass
            await asyncio.sleep(2)

    async def _heartbeat(self):
        """Print signal generation heartbeat."""
        while True:
            await asyncio.sleep(60)
            symbols_with_data = [s for s, w in self.price_window.items() if len(w) >= 10]
            print(f"💓 [ALPHA HEARTBEAT] Regime={self.current_regime} | Direction={self.index_direction} | "
                  f"Symbols with data: {len(symbols_with_data)} | Cooldowns active: {len(self.last_signal_time)}")

    # ─── Feature Score ───────────────────────────────────────

    def compute_feature_score(self, f):
        score = 0
        # Advanced features
        if f.get("footprint_score", 0) > 60:
            score += self.weights.get("footprint", 0.25)
        if f.get("liquidity_gap", 0) > 0.5:
            score += self.weights.get("vacuum", 0.15)
        if f.get("sweep_signal"):
            score += self.weights.get("sweep", 0.15)
        if f.get("accumulation_signal"):
            score += self.weights.get("accumulation", 0.15)
        if f.get("compression_ratio", 1) < 0.4:
            score += self.weights.get("volatility", 0.15)
                
        return score

    # ─── Smart Signal Generation ─────────────────────────────

    def generate_signal(self, f):
        symbol = f.get("symbol")
        price = f.get("price", 0)
        volume = f.get("trade_volume", 0) or f.get("volume", 0)

        if not symbol or not price:
            return None

        from datetime import datetime, time as dtime, timedelta, timezone
        ist = timezone(timedelta(hours=5, minutes=30))
        current_time = datetime.now(ist).time()
        if current_time >= dtime(15, 20) or current_time < dtime(9, 15):
            return None

        # ─── Cooldown check (3 min) ──────────────────────────
        now = time.time()
        if symbol in self.last_signal_time:
            if now - self.last_signal_time[symbol] < self.COOLDOWN_SECONDS:
                return None

        # ─── Update rolling windows ──────────────────────────
        pw = self.price_window[symbol]
        pw.append(price)
        vw = self.volume_window[symbol]
        vw.append(volume)

        if len(pw) < 10:
            return None

        # ─── Calculate VWAP ──────────────────────────────────
        vs = self.vwap_state[symbol]
        vol_delta = max(0, volume - vs["last_vol"]) if volume > vs["last_vol"] else max(volume, 1)
        vs["last_vol"] = volume
        vs["sum_pv"] += price * vol_delta
        vs["sum_v"] += vol_delta
        vwap = vs["sum_pv"] / vs["sum_v"] if vs["sum_v"] > 0 else price

        # ─── Calculate 20-VWMA ───────────────────────────────
        vwma_w = self.vwma_window[symbol]
        vwma_w.append({"p": price, "v": vol_delta})
        total_pv = sum(d["p"] * d["v"] for d in vwma_w)
        total_v = sum(d["v"] for d in vwma_w)
        vwma = total_pv / total_v if total_v > 0 else price

        # ─── VWMA Slope ──────────────────────────────────────
        vwma_slope = 0
        if len(vwma_w) >= 5:
            old_pv = sum(d["p"] * d["v"] for d in list(vwma_w)[:5])
            old_v = sum(d["v"] for d in list(vwma_w)[:5])
            old_vwma = old_pv / old_v if old_v > 0 else vwma
            vwma_slope = (vwma - old_vwma) / old_vwma if old_vwma > 0 else 0

        # ─── Volume Spike Check ──────────────────────────────
        avg_vol = sum(vw) / len(vw) if vw else 1
        vol_spike = vol_delta > (avg_vol * 1.5) if avg_vol > 0 else False

        # ─── Pressure Score ──────────────────────────────────
        imbalance = f.get("bid_ask_imbalance", 0)
        momentum = f.get("momentum_ignition", 0)
        pressure = abs(imbalance) * momentum

        pressure_w = self.pressure_window[symbol]
        pressure_w.append(pressure)
        avg_pressure = sum(pressure_w) / len(pressure_w) if pressure_w else 0

        # ─── Feature-based score ─────────────────────────────
        feature_score = self.compute_feature_score(f)
        normalized_pressure = min(avg_pressure / 100, 1)
        total_score = normalized_pressure + feature_score

        # ─── VWAP Rejection Logic ────────────────────────────
        price_vs_vwap = (price - vwap) / vwap if vwap > 0 else 0
        vwap_rejection_buy = False
        vwap_rejection_sell = False

        # BUY: Price touches VWAP from above and bounces
        if -0.002 <= price_vs_vwap <= 0.003 and price > vwap:
            # Confirm with recent price action (was below, now recovering)
            recent_below = sum(1 for p in list(pw)[-5:] if p < vwap)
            if recent_below >= 1 and price > vwap:
                vwap_rejection_buy = True
                total_score += self.weights.get("vwap_rejection", 0.10)

        # SELL: Price tests VWAP from below and gets rejected
        if -0.003 <= price_vs_vwap <= 0.002 and price < vwap:
            recent_above = sum(1 for p in list(pw)[-5:] if p > vwap)
            if recent_above >= 1 and price < vwap:
                vwap_rejection_sell = True
                total_score += self.weights.get("vwap_rejection", 0.10)

        # ─── Regime Filter ───────────────────────────────────
        regime_bonus = 0
        if self.current_regime == "TRENDING_UP" and imbalance > 0:
            regime_bonus = 0.05
        elif self.current_regime == "TRENDING_DOWN" and imbalance < 0:
            regime_bonus = 0.05
        elif self.current_regime == "VOLATILE":
            total_score *= 0.8  # Reduce confidence in volatile regime
        total_score += regime_bonus

        # ─── Trend Continuation Logic (For BIG Trending Days) ───
        is_trending_buy = (self.current_regime == "TRENDING_UP" and price > vwma and vwma_slope > 0.0001)
        is_trending_sell = (self.current_regime == "TRENDING_DOWN" and price < vwma and vwma_slope < -0.0001)

        # Determine side
        side = None
        vwap_tag = ""

        # BUY conditions: VWAP bounce OR strong imbalance OR trend continuation
        if vwap_rejection_buy or imbalance > 0.5 or is_trending_buy:
            # Block BUY in bearish regime/convergence
            if self.current_regime == "TRENDING_DOWN" or self.index_direction == "BEARISH_CONVERGENCE":
                if not (is_trending_buy and total_score > 0.80): # Reduced from 0.85
                    return None
            
            side = "BUY"
            if is_trending_buy: vwap_tag = "TREND_CONT"
            elif vwap_rejection_buy: vwap_tag = "VWAP_BOUNCE"
            else: vwap_tag = "MOMENTUM"
            
            if price > vwma: total_score += 0.15 # Stronger weight for being above VWMA

        # SELL conditions: VWAP rejection OR strong imbalance OR trend continuation
        elif vwap_rejection_sell or imbalance < -0.5 or is_trending_sell:
            # Block SELL in bullish regime/convergence
            if self.current_regime == "TRENDING_UP" or self.index_direction == "BULLISH_CONVERGENCE":
                if not (is_trending_sell and total_score > 0.80): # Reduced from 0.85
                    return None
            
            side = "SELL"
            if is_trending_sell: vwap_tag = "TREND_CONT"
            elif vwap_rejection_sell: vwap_tag = "VWAP_REJECT"
            else: vwap_tag = "MOMENTUM"
            
            if price < vwma: total_score += 0.15

        if side is None:
            return None

        # ─── Final Signal Decision (Now evaluated after VWMA bonus) ───
        # Regime-aware quality gate: NEUTRAL/VOLATILE days need higher conviction
        # to prevent signal factory behavior (17k+ signals on flat days)
        if self.current_regime in ("TRENDING_UP", "TRENDING_DOWN"):
            MIN_SCORE = 0.72  # Trending: aligned with MDE threshold
        elif self.current_regime == "VOLATILE":
            MIN_SCORE = 0.78  # Volatile: cautious
        else:
            MIN_SCORE = 0.80  # NEUTRAL: only high-conviction signals

        if total_score < MIN_SCORE:
            return None

        # Volume confirmation bonus
        final_score = int(total_score * 100)
        if vol_spike:
            final_score = min(100, final_score + 10)

        # Record signal time for cooldown
        self.last_signal_time[symbol] = now

        return {
            "side": side,
            "score": final_score,
            "vwap_tag": vwap_tag,
            "extra_features": {
                "vwap": round(vwap, 2),
                "vwma_20": round(vwma, 2),
                "vwma_slope": round(vwma_slope, 6),
                "vwap_rejection": vwap_tag,
                "vol_spike": vol_spike,
                "regime": self.current_regime,
                "index_direction": self.index_direction,
            }
        }