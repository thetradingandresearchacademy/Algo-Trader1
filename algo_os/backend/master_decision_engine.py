import json
import asyncio
from collections import defaultdict
from datetime import datetime

from services.redis_stream import RedisStream


class MasterDecisionEngine:

    def __init__(self):

        self.redis = RedisStream()

        self.alpha_stream = "alpha_signals"
        self.stock_stream = "stock_scanner"

        self.output_stream = "validated_signals"
        self.risk_stream = "risk_state"
        self.regime_stream = "regime_state"

        self.last_ids = defaultdict(lambda: self.redis.get_today_id())

        self.watchlist = set()
        self.market_bias = "NEUTRAL"
        self.in_drawdown = False

        # Regime state
        self.intraday_regime = "NEUTRAL"
        self.daily_regime = "NEUTRAL"
        self.weekly_regime = "NEUTRAL"

        # Dual-Index Direction (from RegimeEngine)
        self.index_direction = "NEUTRAL"  # BULLISH_CONVERGENCE, BEARISH_CONVERGENCE, DIVERGENCE, VOLATILE
        self.vix_proxy = 0.0              # 0-100 scale VIX proxy

        # Portfolio state
        self.capital = 1000000  # configurable
        self.index_allocation = 0.5
        self.stock_allocation = 0.5

        self.active_positions = {}

        # Cooldown: track recently closed symbols to allow re-entry after cooldown
        self.closed_cooldown = {}  # symbol → datetime of closure
        self.COOLDOWN_SECONDS = 120  # 2 min cooldown before re-entry into same symbol

        # Position limits
        self.MAX_POSITIONS = 10  # Raised from 5 — allows both index + stock positions

    async def start(self):

        print("MDE Hedge Mode Started")

        asyncio.create_task(self.load_watchlist())
        asyncio.create_task(self.monitor_risk())
        asyncio.create_task(self.monitor_regime())
        asyncio.create_task(self.cleanup_positions())
        asyncio.create_task(self._heartbeat())

        while True:

            streams = self.redis.read(self.alpha_stream, self.last_ids[self.alpha_stream])

            if not streams:
                await asyncio.sleep(0.05)
                continue

            for stream, entries in streams:

                for msg_id, payload in entries:

                    self.last_ids[self.alpha_stream] = msg_id

                    signal = json.loads(payload.get("data"))

                    decision = self.evaluate(signal)

                    if decision:
                        await self.redis.publish(self.output_stream, decision)

    async def load_watchlist(self):

        while True:

            streams = self.redis.read(self.stock_stream, self.last_ids[self.stock_stream])

            if streams:
                for stream, entries in streams:
                    for msg_id, payload in entries:

                        self.last_ids[self.stock_stream] = msg_id

                        data = json.loads(payload.get("data"))

                        # symbols is a list of string names
                        raw_symbols = data.get("symbols", [])
                        # Ensure we get strings (handle both list-of-str and list-of-dict)
                        symbol_names = []
                        for s in raw_symbols:
                            if isinstance(s, dict):
                                symbol_names.append(s.get("symbol", ""))
                            else:
                                symbol_names.append(str(s))
                        self.watchlist = set(symbol_names)
                        self.market_bias = data.get("bias", "NEUTRAL")
                        print(f"MDE: Watchlist updated with {len(self.watchlist)} symbols | Bias: {self.market_bias}")

            await asyncio.sleep(5)

    async def monitor_risk(self):
        while True:
            streams = self.redis.read(self.risk_stream, self.last_ids[self.risk_stream])
            if streams:
                for stream, entries in streams:
                    for msg_id, payload in entries:
                        self.last_ids[self.risk_stream] = msg_id
                        state = json.loads(payload.get("data", "{}"))
                        self.in_drawdown = not state.get("enabled", True)
            await asyncio.sleep(2)

    async def monitor_regime(self):
        """Subscribe to regime_state for directional bias + index direction."""
        while True:
            try:
                streams = self.redis.read(self.regime_stream, self.last_ids[self.regime_stream])
                if streams:
                    for _, entries in streams:
                        for msg_id, payload in entries:
                            self.last_ids[self.regime_stream] = msg_id
                            raw = payload.get("data")
                            if raw:
                                state = json.loads(raw)
                                self.intraday_regime = state.get("intraday", "NEUTRAL")
                                self.daily_regime = state.get("daily", "NEUTRAL")
                                self.weekly_regime = state.get("weekly", "NEUTRAL")
                                self.index_direction = state.get("index_direction", "NEUTRAL")
                                self.vix_proxy = state.get("vix_proxy", 0.0)
            except Exception:
                pass
            await asyncio.sleep(3)

    async def cleanup_positions(self):
        """Remove closed/expired trades from active_positions.
        
        CRITICAL FIX: Uses "0-0" instead of "$" so we catch ALL close events,
        """
        trade_last_id = "$"  # Fixed to $ to prevent action replay of all old trades
        while True:
            try:
                # Read trade_results to remove closed positions
                streams = self.redis.read("trade_results", trade_last_id)
                if streams:
                    for stream, entries in streams:
                        for msg_id, payload in entries:
                            trade_last_id = msg_id
                            raw = payload.get("data")
                            if raw:
                                trade = json.loads(raw)
                                symbol = trade.get("symbol")
                                if symbol and symbol in self.active_positions:
                                    del self.active_positions[symbol]
                                    self.closed_cooldown[symbol] = datetime.utcnow()
                                    print(f"MDE: Position closed — {symbol} ({len(self.active_positions)} active)")

                # Expire stale positions (older than 10 min — reduced from 30 min for scalping)
                now = datetime.utcnow()
                expired = []
                for sym, pos in list(self.active_positions.items()):
                    ts = pos.get("timestamp", "")
                    try:
                        pos_time = datetime.fromisoformat(ts)
                        if (now - pos_time).total_seconds() > 600:  # 10 min (was 30 min)
                            expired.append(sym)
                    except:
                        expired.append(sym)  # invalid timestamp = stale
                for sym in expired:
                    del self.active_positions[sym]
                    print(f"MDE: Expired stale position — {sym} ({len(self.active_positions)} active)")

                # Clean up old cooldowns (older than cooldown period)
                expired_cooldowns = [
                    s for s, t in self.closed_cooldown.items()
                    if (now - t).total_seconds() > self.COOLDOWN_SECONDS
                ]
                for s in expired_cooldowns:
                    del self.closed_cooldown[s]

            except Exception as e:
                print(f"MDE cleanup error: {e}")
            await asyncio.sleep(5)

    async def _heartbeat(self):
        """Terminal heartbeat every 60s."""
        while True:
            await asyncio.sleep(60)
            print(f"💓 [MDE HEARTBEAT] Regime={self.intraday_regime} | Direction={self.index_direction} | "
                  f"VIX={self.vix_proxy} | Active={len(self.active_positions)}/{self.MAX_POSITIONS} | "
                  f"Watchlist={len(self.watchlist)} | Drawdown={self.in_drawdown}")

    # ---------------------------------------------------------
    # CORE DECISION
    # ---------------------------------------------------------

    def evaluate(self, signal):

        symbol = signal.get("symbol")
        features = signal.get("features", {})
        
        from datetime import datetime, time as dtime, timedelta, timezone
        ist = timezone(timedelta(hours=5, minutes=30))
        current_time = datetime.now(ist).time()

        # Hard close: no entries after 3:20 PM or before 9:15 AM
        if current_time >= dtime(15, 20) or current_time < dtime(9, 15):
            return None

        is_index = symbol in ["NIFTY", "BANKNIFTY", "SENSEX"]
        is_scanner_signal = signal.get("source") == "apex_scanner"
        is_vwap_retest = signal.get("source") == "vwap_retest"
        is_breakout = signal.get("strategy") == "EXPLOSIVE_BREAKOUT"

        # --------- FILTER ---------
        # Bypass watchlist for scanner, VWAP retest, and breakout signals
        if not is_index and not is_scanner_signal and not is_vwap_retest and not is_breakout:
            if symbol not in self.watchlist:
                return None

        # --------- SCORE ---------
        footprint = features.get("footprint_score", 0)
        imbalance = features.get("bid_ask_imbalance", 0)
        momentum = features.get("momentum_ignition", 0)

        score = (
            (footprint > 60) * 0.25 +
            (abs(imbalance) > 0.5) * 0.25 +
            (momentum > 50) * 0.15
        )

        # VWAP/VWMA feature bonuses
        if features.get("vwap_rejection"):
            score += 0.15
        if features.get("vol_spike"):
            score += 0.10
        if features.get("regime") in ("TRENDING_UP", "TRENDING_DOWN"):
            score += 0.05

        # Override score if signal already gives it (stock engine / scanner / vwap_retest)
        if "score" in signal:
            score = signal["score"] / 100.0

        # ─── Normalize Side ──────────────────────────────
        side = signal.get("side") or signal.get("signal")
        if not side:
            # Fallback to imbalance if side is missing
            side = "BUY" if imbalance > 0 else "SELL"

        # ━━━ TRENDING DAY AGGRESSIVE BYPASS ━━━━━━━━━━━━━━━━━━
        # On clear trending days with index convergence, lower the bar significantly
        # This is the "Golden Opportunity" mode for trending markets
        is_trending_convergence = (
            self.intraday_regime in ("TRENDING_UP", "TRENDING_DOWN") and
            self.index_direction in ("BULLISH_CONVERGENCE", "BEARISH_CONVERGENCE")
        )
        
        if is_trending_convergence:
            # Auto-align side with trend direction
            trend_side = "BUY" if self.intraday_regime == "TRENDING_UP" else "SELL"
            if side == trend_side:
                # Dramatically lower the bar for trend-aligned signals
                min_score = 0.45 if is_index else 0.50
                if score >= min_score:
                    print(f"🔥 TRENDING CONVERGENCE BYPASS | {symbol} {side} | score={score:.2f} >= {min_score}")
                    # Skip all other regime filters — go straight to portfolio check
                    return self._build_decision(symbol, side, score, signal, is_index, features)

        # Threshold: 0.60 for index, 0.55 for stock (lowered from 0.70/0.65)
        min_score = 0.60 if is_index else 0.55
        if score < min_score:
            return None

        # ─── Counter-trend filter (soft — only block index counter-trend) ─
        if is_index:
            if self.intraday_regime == "TRENDING_DOWN" and side == "BUY":
                return None
            if self.intraday_regime == "TRENDING_UP" and side == "SELL":
                return None

        # ─── DUAL-INDEX DIRECTION FILTER (Relaxed for trading) ──
        if self.index_direction == "BEARISH_CONVERGENCE" and side == "BUY":
            if is_index or score < 0.65:
                return None

        if self.index_direction == "BULLISH_CONVERGENCE" and side == "SELL":
            if is_index or score < 0.65:
                return None

        if self.index_direction == "DIVERGENCE":
            if is_index and score < 0.75:
                return None

        return self._build_decision(symbol, side, score, signal, is_index, features)

    def _build_decision(self, symbol, side, score, signal, is_index, features):
        """Build the final decision dict with capital allocation and portfolio checks."""
        # ─────── CAPITAL ALLOCATION ─────────
        if self.intraday_regime in ("TRENDING_UP", "TRENDING_DOWN"):
            self.index_allocation = 0.6
            self.stock_allocation = 0.4
        elif self.intraday_regime == "VOLATILE":
            self.index_allocation = 0.3
            self.stock_allocation = 0.3
        else:
            self.index_allocation = 0.4
            self.stock_allocation = 0.6

        if is_index:
            capital = self.capital * self.index_allocation
        else:
            capital = self.capital * self.stock_allocation

        if self.in_drawdown:
            capital *= 0.5
        if self.intraday_regime == "VOLATILE":
            capital *= 0.7

        if self.vix_proxy > 40:
            capital *= 0.5
        elif self.vix_proxy > 25:
            capital *= 0.7

        if self.index_direction == "DIVERGENCE":
            capital *= 0.6

        # ─── SMART POSITION SIZING ───
        risk_per_trade = 0.01
        if self.intraday_regime in ("TRENDING_UP", "TRENDING_DOWN") and self.index_direction in ("BULLISH_CONVERGENCE", "BEARISH_CONVERGENCE"):
            risk_per_trade = 0.02
            print(f"🔥 ALL THROTTLE IN | Trending Day ({self.index_direction}) | Doubling size for {symbol}")

        position_size = capital * risk_per_trade
        qty = max(10, int(position_size / signal.get("price", 1)))

        # --------- PORTFOLIO CHECK ---------
        if len(self.active_positions) >= self.MAX_POSITIONS:
            return None
        if symbol in self.active_positions:
            return None
        if symbol in self.closed_cooldown:
            elapsed = (datetime.utcnow() - self.closed_cooldown[symbol]).total_seconds()
            if elapsed < self.COOLDOWN_SECONDS:
                return None

        strategy = signal.get("strategy", "INDEX" if is_index else "STOCK")

        decision = {
            "symbol": symbol,
            "signal": side,
            "confidence": score,
            "qty": qty,
            "price": signal.get("price"),
            "strategy": strategy,
            "source": signal.get("source", ""),
            "timestamp": datetime.utcnow().isoformat(),
            "features": features
        }

        self.active_positions[symbol] = decision

        regime_tag = f" | Regime: {self.intraday_regime}" if self.intraday_regime != "NEUTRAL" else ""
        print(f"🚀 TRADE APPROVED | {symbol} | {side} | {strategy} | score={score:.2f} | "
              f"size={qty} | active={len(self.active_positions)}/{self.MAX_POSITIONS}{regime_tag}")

        return decision