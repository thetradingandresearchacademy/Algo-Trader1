import json
import asyncio
from collections import defaultdict
from datetime import datetime

from services.redis_stream import RedisStream

# Enterprise-grade typed models
from models.signal import Signal
from models.market_state import MarketState


class MasterDecisionEngine:

    def __init__(self):

        self.redis = RedisStream()

        self.alpha_stream = "alpha_signals"
        self.stock_stream = "stock_scanner"

        self.output_stream = "validated_signals"
        self.risk_stream = "risk_state"
        self.regime_stream = "regime_state"

        # CRITICAL FIX: Use get_latest_id for alpha_signals to prevent
        # replaying entire day's signals on reboot (was causing burst-then-silence)
        self.last_ids = defaultdict(lambda: self.redis.get_latest_id(self.alpha_stream))
        # Pre-set alpha_signals cursor to latest to skip historical backlog
        self.last_ids[self.alpha_stream] = self.redis.get_latest_id(self.alpha_stream)

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
        self.COOLDOWN_SECONDS = 1800  # 30 min cooldown to prevent whipsawing

        # Position limits
        self.MAX_POSITIONS = 5  # Reduced to 5 high-conviction trades
        
        # Daily Limits to prevent churn
        self.daily_trades_count = defaultdict(int)
        self.MAX_TRADES_PER_SYMBOL = 2 

    async def start(self):

        print("MDE Hedge Mode Started")

        asyncio.create_task(self.load_watchlist())
        asyncio.create_task(self.monitor_risk())
        asyncio.create_task(self.monitor_regime())
        asyncio.create_task(self.cleanup_positions())
        asyncio.create_task(self._heartbeat())
        asyncio.create_task(self._midnight_reset_loop())

        while True:

            streams = self.redis.read(self.alpha_stream, self.last_ids[self.alpha_stream])

            if not streams:
                await asyncio.sleep(0.05)
                continue

            for stream, entries in streams:

                for msg_id, payload in entries:

                    self.last_ids[self.alpha_stream] = msg_id

                    signal_dict = json.loads(payload.get("data"))
                    signal = Signal.from_dict(signal_dict)
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

                now = datetime.utcnow()

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

    async def _midnight_reset_loop(self):
        """Reset daily trade counts at midnight."""
        while True:
            try:
                from datetime import datetime, timedelta
                now = datetime.now()
                tomorrow = now.date() + timedelta(days=1)
                midnight = datetime.combine(tomorrow, datetime.min.time())
                sleep_seconds = (midnight - now).total_seconds()
                await asyncio.sleep(min(sleep_seconds, 3600))
                if datetime.now().date() == tomorrow:
                    print("⏰ MIDNIGHT RESET | Clearing daily_trades_count")
                    self.daily_trades_count.clear()
            except Exception as e:
                print(f"Error in midnight reset loop: {e}")
                await asyncio.sleep(60)

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
        if not is_index and not is_scanner_signal and not is_vwap_retest and not is_breakout:
            if symbol not in self.watchlist:
                return None
                
        # 1. ANTI-CHURN: Stop-Trading if daily limit reached
        if self.daily_trades_count[symbol] >= self.MAX_TRADES_PER_SYMBOL:
            return None

        # 2. VOLATILITY FILTER: Handled dynamically in position sizing (_build_decision)
        # instead of starving the funnel by blocking it.

        # 3. CONVICTION SCORING
        score = signal.get("score", 0) / 100.0 if "score" in signal else 0.0
        if not score:
            footprint = features.get("footprint_score", 0)
            imbalance = features.get("bid_ask_imbalance", 0)
            momentum = features.get("momentum_ignition", 0)
            score = ((footprint > 60) * 0.25 + (abs(imbalance) > 0.5) * 0.25 + (momentum > 50) * 0.15)
            if features.get("vwap_rejection"): score += 0.15
            if features.get("vol_spike"): score += 0.10

        side = signal.get("side") or signal.get("signal", "BUY")

        # Block shorting for cash equity symbols (ends with -EQ)
        if (side == "SELL" or side == "SHORT") and symbol.endswith("-EQ"):
            print(f"🚫 MDE BLOCK | Shorting cash equity is disabled: {symbol}")
            return None

        # 4. STATE-OF-THE-ART REGIME ALIGNMENT
        is_convergent = (self.intraday_regime in ("TRENDING_UP", "TRENDING_DOWN") and 
                         self.index_direction in ("BULLISH_CONVERGENCE", "BEARISH_CONVERGENCE"))

        # Tightened threshold to solve your 3k profit/40 trade issue
        min_score = 0.72 if is_convergent else 0.85 # High bar for autonomous mode
        
        if score < min_score:
            return None

        # 5. SYMBOL-LEVEL PNL COOLDOWN
        # Wait longer if the last trade was a loser to prevent revenge trading
        if symbol in self.closed_cooldown:
            # Assuming you pass pnl in closed_cooldown dictionary in future, fallback to 15m if not
            cooldown_time = 900 # 15 mins default
            if isinstance(self.closed_cooldown[symbol], dict):
                last_exit_pnl = self.closed_cooldown[symbol].get("pnl", 0)
                if last_exit_pnl < 0:
                    cooldown_time = 3600 # 1 hour penalty for a losing setup
                
            # If closed_cooldown stores datetime directly
            close_time = self.closed_cooldown[symbol]
            if isinstance(close_time, dict): close_time = close_time.get('time', datetime.utcnow())
                
            elapsed = (datetime.utcnow() - close_time).total_seconds()
            if elapsed < cooldown_time:
                return None

        return self._build_decision(symbol, side, score, signal, is_index, features)

    def _build_decision(self, symbol, side, score, signal, is_index, features):
        """Build the final decision dict with capital allocation and portfolio checks."""
        # Retrieve current capital dynamically from Redis if available to ensure sync with RiskEngine
        try:
            stored_cap = self.redis.get_hashall("system_config").get("current_capital")
            if stored_cap:
                self.capital = float(stored_cap)
        except Exception:
            pass

        # ─────── CAPITAL ALLOCATION ─────────
        # Allocate capital per position based on MAX_POSITIONS (was risk percentage-of-capital, causing tiny trades)
        base_position_size = self.capital / max(1, self.MAX_POSITIONS)
        
        position_size = base_position_size
        if self.in_drawdown:
            position_size *= 0.5
        if self.intraday_regime == "VOLATILE":
            position_size *= 0.7

        if self.vix_proxy > 40:
            position_size *= 0.5
        elif self.vix_proxy > 25:
            position_size *= 0.7

        if self.index_direction == "DIVERGENCE":
            position_size *= 0.6

        # Dynamic Volatility Wall Scaling
        atr_ratio = features.get("atr_ratio", 1.0)
        if atr_ratio < 0.8 and self.intraday_regime in ("NEUTRAL", "RANGE", "RANGE_BOUND"):
            position_size *= 0.5
            print(f"📉 VOLATILITY WALL SCALING | Regime: {self.intraday_regime} | atr_ratio: {atr_ratio} < 0.8 | Scaling position size by 0.5")

        # Enforce a minimum position size of Rs 50,000 to cover transactional charges (Rs 200+)
        position_size = max(50000.0, position_size)
        qty = int(position_size / signal.get("price", 1))
        qty = max(10, qty)

        # --------- PORTFOLIO CHECK ---------
        if len(self.active_positions) >= self.MAX_POSITIONS:
            return None
        if symbol in self.active_positions:
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
            "features": features.to_dict() if hasattr(features, "to_dict") else features
        }

        self.daily_trades_count[symbol] += 1
        self.active_positions[symbol] = decision

        regime_tag = f" | Regime: {self.intraday_regime}" if self.intraday_regime != "NEUTRAL" else ""
        print(f"🚀 TRADE APPROVED | {symbol} | {side} | {strategy} | score={score:.2f} | "
              f"size={qty} | active={len(self.active_positions)}/{self.MAX_POSITIONS}{regime_tag}")

        return decision