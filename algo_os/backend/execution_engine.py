import json
import asyncio
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path
import importlib.util

from services.redis_stream import RedisStream
from services.broker_api import BrokerAPI
from services.cache_manager import CacheManager

from config import settings as sys_config
from datetime import time as dtime

# Enterprise-grade typed models
from models.signal import Signal
from models.position import Position as TypedPosition
from models.market_state import MarketState

class ExecutionEngine:
    # Both LIVE_TRADING and CONFIRM_REAL_TRADING must be True in tara_config.py
    _is_live = getattr(sys_config, "LIVE_TRADING", False)
    _is_paper = getattr(sys_config, "PAPER_TRADING", False)
    CONFIRM_REAL_TRADING = _is_live and not _is_paper
    DEFAULT_QTY = 50

    # ==========================================
    #  SCALP & TRAILING SL CONFIGURATION (3-Stage Progressive)
    # ==========================================
    # Stage 1: Breakeven — lock entry price early
    BREAKEVEN_TRIGGER_PCT = 0.012  # +1.2% → move SL to entry (whipsaw protection)
    # Stage 2: Tight trail — protect developing profits
    TRAIL_STAGE2_TRIGGER = 0.012   # +1.2% → trail at 0.6% below high (Was 0.7%)
    TRAIL_STAGE2_PCT = 0.006       # 0.6% trailing distance
    # Stage 3: Lock profit — maximize signature near target
    TRAIL_STAGE3_TRIGGER = 0.02    # +2.0% → trail at 0.4% below high (Was 1.5%)
    TRAIL_STAGE3_PCT = 0.004       # 0.4% trailing distance (tight)
    TRAILING_SL_PCT = 0.0035       # Default trail (Stage 2 fallback)
    SCALP_TARGET_PCT = 0.01        # 1% quick scalp target
    SCALP_TIME_LIMIT = 300         # 5-minute scalp window (seconds)

    # Index Point-based Trailing (New)
    INDEX_TRAIL_TRIGGER_PTS = 20   # +20 points → auto trail
    INDEX_TRAIL_STEP_PTS = 15
    TRADE_COOLDOWN_SECONDS = 300   # 5-minute cooldown per symbol to prevent re-entry

    def __init__(self):

        self.redis = RedisStream()
        self.broker = BrokerAPI()  # Singleton — real broker gateway
        self.cache_manager = CacheManager()

        # Streams
        self.signal_stream = "portfolio_orders"
        self.price_stream = "micro_ticks"
        self.risk_stream = "risk_state"
        self.active_positions_stream = "active_positions"
        self.regime_stream = "market_regime"
        self.command_stream = "control_commands"

        # Internal state (MUST be instance-level, not class-level)
        self._order_timestamps = []
        self._modify_timestamps = []
        self._current_regime = "NEUTRAL"
        self._index_direction = "NEUTRAL"
        self._vix = 15.0
        self._failure_count = 0
        self._force_paper_mode = False  # Instance-level: resets on restart

        # Stream cursors
        # Using today_id ensures we catch signals missed during restarts
        # but we'll add logic to skip truly stale ones.
        self.last_signal_id = self.redis.get_today_id()
        self.last_price_id = self.redis.get_latest_id(self.price_stream)
        self.last_risk_id = "0-0"
        self.last_cmd_id = self.redis.get_latest_id(self.command_stream)

        # Active trades
        self.positions = {}
        self.symbol_cooldowns = {} # symbol -> last_close_time
        self.active_trades_key = "active_trades"

        # Asynchronous Priority Queue for SEBI-compliant execution scheduling
        self.order_queue = asyncio.PriorityQueue()

        # Load Scrip Master (manages downloads/refreshes cleanly)
        self._load_scrip_master()

        # Risk state
        self.trading_enabled = True

    # ---------------------------------------------------------
    # ENGINE START
    # ---------------------------------------------------------

    async def _monitor_regime(self):
        """Monitor regime state to adjust trailing logic in real-time."""
        last_id = self.redis.get_latest_id(self.regime_stream)
        while True:
            try:
                streams = self.redis.read(self.regime_stream, last_id)
                if streams:
                    for _, entries in streams:
                        for msg_id, payload in entries:
                            last_id = msg_id
                            data = json.loads(payload.get("data", "{}"))
                            state = MarketState.from_dict(data)
                            self._current_regime = state.intraday_regime
                            self._index_direction = state.index_direction
                            self._vix = state.vix_proxy
                await asyncio.sleep(5)
            except Exception:
                await asyncio.sleep(10)

    async def start(self):

        mode = "🔴 LIVE TRADING" if self.CONFIRM_REAL_TRADING else "📝 PAPER TRADING"
        print(f"Execution Engine started | Mode: {mode}")
        print(f"  Config: LIVE_TRADING={self._is_live} | PAPER_TRADING={self._is_paper} | CONFIRM={self.CONFIRM_REAL_TRADING}")

        # Pre-validate broker connectivity on startup
        if self.CONFIRM_REAL_TRADING:
            try:
                auth_ok = self.broker.authenticate(force=True)  # Force fresh auth on boot
                if not auth_ok:
                    print("⚠️ BROKER AUTH FAILED ON STARTUP — Falling back to PAPER MODE")
                    self._force_paper_mode = True
                else:
                    print(f"✅ Broker authenticated for LIVE trading | Session: {self.broker._authenticated} | API: {self.broker._smart_connect is not None}")
            except Exception as e:
                print(f"⚠️ BROKER AUTH EXCEPTION: {e} — Falling back to PAPER MODE")
                self._force_paper_mode = True
        
        # Log final resolved mode after auth check
        resolved_mode = "🔴 LIVE" if (self.CONFIRM_REAL_TRADING and not self._force_paper_mode) else "📝 PAPER"
        print(f"  ▸ Resolved execution mode: {resolved_mode}")

        asyncio.create_task(self._monitor_regime())
        asyncio.create_task(self.order_dispatcher())

        # Load persisted positions from Redis
        self._load_positions()

        asyncio.create_task(self.monitor_prices())
        asyncio.create_task(self.monitor_risk())
        asyncio.create_task(self.monitor_scanner_prices())  # Check stocks via scanner updates
        asyncio.create_task(self._poll_stock_ltp())  # Periodic LTP poll for stock positions
        asyncio.create_task(self.periodic_position_broadcast())
        asyncio.create_task(self.intraday_squareoff())
        asyncio.create_task(self.scalp_monitor())  # NEW: scalp exit monitor
        asyncio.create_task(self.monitor_commands())  # Handle paper/live toggle
        asyncio.create_task(self._heartbeat())  # Terminal visibility

        while True:

            try:

                # Read validated signals
                alpha_streams = self.redis.read(self.signal_stream, self.last_signal_id)

                if not alpha_streams:
                    await asyncio.sleep(0.05)
                    continue

                # Process validated signals
                for stream, entries in alpha_streams:
                    for msg_id, payload in entries:
                        self.last_signal_id = msg_id
                        raw = payload.get("data")
                        if raw is None:
                            continue
                        signal_dict = json.loads(raw)
                        signal = Signal.from_dict(signal_dict)
                        await self.process_signal(signal)

            except Exception as e:

                print("ExecutionEngine signal error:", e)
                await asyncio.sleep(1)

    async def order_dispatcher(self):
        """Worker task to process broker orders sequentially from the priority queue."""
        while True:
            try:
                priority, order_fn, args, kwargs, future = await self.order_queue.get()
                try:
                    res = await order_fn(*args, **kwargs)
                    if not future.done():
                        future.set_result(res)
                except Exception as e:
                    if not future.done():
                        future.set_exception(e)
                finally:
                    self.order_queue.task_done()
            except asyncio.CancelledError:
                break
            except Exception as e:
                print("Error in order dispatcher:", e)
                await asyncio.sleep(0.1)

    async def queue_order_execution(self, priority, fn, *args, **kwargs):
        """Helper to queue an order execution coroutine and await its result."""
        future = asyncio.get_event_loop().create_future()
        await self.order_queue.put((priority, fn, args, kwargs, future))
        return await future

    async def _heartbeat(self):
        """Print heartbeat every 60s for terminal visibility."""
        while True:
            await asyncio.sleep(60)
            mode = "LIVE" if (self.CONFIRM_REAL_TRADING and not self._force_paper_mode) else "PAPER"
            if self._force_paper_mode:
                mode += " (FALLBACK)"
            broker_status = "AUTH_OK" if self.broker._authenticated else "AUTH_FAIL"
            print(f"💓 [EXEC HEARTBEAT] Mode={mode} | Broker={broker_status} | "
                  f"Positions={len(self.positions)} | "
                  f"Trading={'ON' if self.trading_enabled else 'OFF'} | "
                  f"Failures={self._failure_count} | "
                  f"Regime={self._current_regime} | Direction={self._index_direction}")

    async def process_signal(self, signal):
        symbol = signal.get("symbol")
        side = signal.get("signal")
        price = signal.get("price")
        ts_str = signal.get("timestamp", "")

        if not symbol or not side or price is None:
            return

        # --- STALE SIGNAL CHECK ---
        # Don't execute signals older than 15 minutes (protects from catch-up replay)
        try:
            if ts_str:
                # Remove 'Z' or offset if present
                clean_ts = ts_str.split('+')[0].split('Z')[0]
                sig_time = datetime.fromisoformat(clean_ts)
                # Use timezone-aware UTC comparison
                from datetime import timezone as _tz
                now_utc = datetime.now(_tz.utc).replace(tzinfo=None)
                signal_age = (now_utc - sig_time).total_seconds()
                if signal_age > 900: # 15 minutes
                    return
        except Exception as e:
            print(f"Timestamp parse error: {e}")

        print(f"📩 SIGNAL RECEIVED | {symbol} {side} @ {price} | Strat: {signal.get('strategy')}")

        if not self.trading_enabled:
            print(f"🚫 BLOCKED | Trading disabled (Risk/Circuit Breaker) | {symbol}")
            return

        ist = timezone(timedelta(hours=5, minutes=30))
        current_time = datetime.now(ist).time()

        # Hard close: no entries after 3:20 PM or before 9:15 AM
        if current_time >= dtime(15, 20) or current_time < dtime(9, 15):
            print(f"⚠️ Market Closed. Rejecting {symbol} signal. Current IST: {current_time}")
            return

        # F&O / Index cutoff: 3:25 PM — no fresh index/options entries
        is_index = symbol in ("NIFTY", "BANKNIFTY", "SENSEX")
        strategy = signal.get("strategy", "")
        is_fno = is_index or strategy.startswith("INDEX") or "CE" in symbol or "PE" in symbol

        if is_fno and current_time >= dtime(15, 25):
            print(f"⚠️ F&O Cutoff 3:25 PM | Rejecting {symbol} | IST: {current_time}")
            return

        # Stocks after 3:25 PM: only BTST/Swing with score >= 75
        if current_time >= dtime(15, 25):
            score = signal.get("score", signal.get("confidence", 0))
            if isinstance(score, float) and score < 1:
                score = int(score * 100)  # normalize 0-1 to 0-100
            if score < 75:
                print(f"⚠️ Post 3:25 PM stock rejected | {symbol} | score={score} < 75")
                return
            print(f"📊 BTST/Swing Entry Allowed | {symbol} | score={score} | post 3:25 PM")

        if symbol in self.positions:
            return

        # --- SCORE DEFENSE GATE ---
        sig_score = signal.get("score", signal.get("confidence", 0))
        if isinstance(sig_score, float) and sig_score < 1:
            sig_score = int(sig_score * 100)
        if sig_score < 65:
            return

        # --- MAX POSITIONS ---
        if len(self.positions) >= 5:
            return

        # --- Check Cooldown ---
        last_close = self.symbol_cooldowns.get(symbol, 0)
        if time.time() - last_close < self.TRADE_COOLDOWN_SECONDS:
            return

        await self.open_trade(symbol, side, price, signal)

    # ---------------------------------------------------------
    # TRADE ENTRY
    # ---------------------------------------------------------

    async def open_trade(self, symbol, side, price, signal):
        """Create a trade entry respecting config, throttling and safety rules.

        * qty – from config (default 50) and forced to a multiple of 10
        * target – 1‑3 % profit or flat ₹20‑30 for low/high price symbols
        * stop – below swing low if provided, otherwise 1 % below entry (BUY) or above (SELL)
        * order throttling – max 3 orders/sec (circuit‑breaker after 3 failures)
        """
        # ---- Block Cash Equity Shorting ----
        if (side == "SELL" or side == "SHORT") and symbol.endswith("-EQ"):
            print(f"🚫 EXECUTION BLOCK | Shorting cash equity is disabled: {symbol}")
            return

        # ---- Quantity (MDE risk-adjusted) ----
        qty = signal.get("qty", self.DEFAULT_QTY)

        # ---- Minimum Notional Check ----
        notional = qty * price
        if notional < 5000:
            print(f"🚫 DUST TRADE BLOCKED | {symbol} qty={qty} × ₹{price:.0f} = ₹{notional:.0f} < ₹5000 min")
            return

        # ---- Target & Stop‑Loss ----
        if price < 100 or price > 2000:
            # flat rupee targets for cheap/expensive stocks
            target = price + 30 if side == "BUY" else price - 30
            stop = price - 20 if side == "BUY" else price + 20
        else:
            # percentage based target (2 % default within 1‑3 % range)
            target_pct = 0.02
            target = price * (1 + target_pct) if side == "BUY" else price * (1 - target_pct)
            
            # --- SMART Initial Stop-Loss ---
            swing_low = signal.get("features", {}).get("swing_low")
            vwap = signal.get("features", {}).get("vwap")
            is_index = symbol in ("NIFTY", "BANKNIFTY", "SENSEX", "FINNIFTY", "MIDCPNIFTY")
            
            if is_index:
                # Dynamic point-based fallback for indices instead of 1%
                default_stop_pts = 120 if "BANKNIFTY" in symbol else (80 if "SENSEX" in symbol else 50)
                if side == "BUY":
                    stop = swing_low if swing_low else (vwap if vwap else price - default_stop_pts)
                else:
                    stop = price + (price - swing_low) if swing_low else (price + (price - vwap) if vwap else price + default_stop_pts)
            else:
                # Percentage-based fallback for stocks
                if side == "BUY":
                    if swing_low is not None:
                        stop = swing_low
                    elif vwap is not None:
                        stop = vwap
                    else:
                        stop = price * 0.99
                else: # SELL
                    stop = price * 1.01 # default fallback
                    if swing_low is not None and swing_low < price:
                        stop = price + (price - swing_low)
                    elif vwap is not None and vwap < price:
                        stop = price + (price - vwap)

        # ---- Throttling (max 3 orders/sec Angel One SEBI compliance) ----
        now = time.time()
        self._order_timestamps = [t for t in self._order_timestamps if now - t < 1]
        if len(self._order_timestamps) >= 3:
            print("⚠️ SEBI Order rate limit reached – delaying trade")
            await asyncio.sleep(0.4)  # Non-blocking back-off
        self._order_timestamps.append(time.time())

        # ---- Build trade object ----
        trade = TypedPosition(
            symbol=symbol,
            side=side,
            entry_price=round(price, 2),
            stop=round(stop, 2),
            initial_stop=round(stop, 2),  # preserve original SL for reference
            target=round(target, 2),
            qty=qty,
            entry_time=datetime.now(timezone(timedelta(hours=5, minutes=30))).strftime("%Y-%m-%dT%H:%M:%S"),
            features=signal.features,
            # --- Trailing SL state ---
            highest_price=round(price, 2),   # tracks highest since entry (BUY)
            lowest_price=round(price, 2),     # tracks lowest since entry (SELL)
            trailing_active=False,            # trailing kicks in after breakeven
            trail_stage=0,
            partial_booked=False,
            scalp_checked=False,
            mode="LIVE" if (self.CONFIRM_REAL_TRADING and not self._force_paper_mode) else "PAPER",
        )

        print(f"📋 TRADE SETUP | {symbol} {side} | Mode: {trade.mode} | ForcePaper: {self._force_paper_mode} | BrokerAuth: {self.broker._authenticated}")

        # ---- Track position immediately to prevent race conditions ----
        self.positions[symbol] = trade
        
        # ---- Place order (paper or real) ----
        use_live = self.CONFIRM_REAL_TRADING and not self._force_paper_mode
        # Entry priority: 100 - score
        score = int(signal.score)
        priority = max(1, 100 - score)
        
        if use_live:
            success = await self.queue_order_execution(priority, self._place_real_order, trade)
        else:
            trade.mode = "PAPER"
            success = self._place_paper_order(trade)

        if not success:
            # retry once
            print("Retrying order placement...")
            if use_live:
                success = await self.queue_order_execution(priority, self._place_real_order, trade)
            else:
                success = self._place_paper_order(trade)

        if not success and use_live:
            # LIVE failed twice — fallback to PAPER instead of circuit-breaking
            print("⚠️ LIVE order failed twice — auto-falling back to PAPER for this trade")
            self._failure_count += 1
            trade.mode = "PAPER"
            self._save_positions(symbol)
            success = self._place_paper_order(trade)
            if self._failure_count >= 5:
                self._force_paper_mode = True
                print("⚠️ BROKER UNREACHABLE: 5 consecutive failures — switching to PAPER MODE globally")
                print("💡 System will auto-retry LIVE mode in 5 minutes")
                asyncio.create_task(self._retry_live_mode())
                self._failure_count = 0  # Reset counter since we're in paper now

        if not success:
            self._failure_count += 1
            print(f"❌ Order failed after retry – failure count {self._failure_count}")
            self.positions.pop(symbol, None) # Remove if failed
            return

        # reset failure counter on success
        self._failure_count = 0
        self._save_positions() # Persist to Redis
        print(f"TRADE OPEN | {side} {symbol} @ {price:.2f} qty={qty} target={trade['target']} stop={trade['stop']}")
        
        # Publish active positions update
        asyncio.create_task(self.broadcast_positions())


    def _place_paper_order(self, trade):
        print(f"📝 PAPER TRADE PLACED: {trade['side']} {trade['qty']} {trade['symbol']} at {trade['entry_price']}")
        return True

    def _resolve_symbol_token(self, trade):
        symbol = trade.get("symbol", "")
        features = trade.get("features", {})
        
        # 1. Option mapped symbol handling
        if trade.get("is_options") or features.get("is_options") or "_CE" in symbol or "_PE" in symbol:
            base_sym = trade.get("original_symbol") or features.get("original_symbol") or symbol.split('_')[0]
            strike = trade.get("strike") or features.get("strike")
            opt_type = trade.get("opt_type") or features.get("opt_type") or symbol.split('_')[-1]
            
            if base_sym and strike and opt_type:
                candidates = []
                target_str1 = f"{float(strike):.1f}" if strike else ""
                target_str2 = str(int(strike)) if strike else ""
                
                # Fetch NFO items grouped by base symbol (O(1) filter, very fast)
                nfo_items = self.cache_manager.get_nfo_by_name(base_sym)
                for item in nfo_items:
                    s_str = str(item.get("strike", ""))
                    if s_str == target_str1 or s_str.startswith(target_str2 + ".") or s_str == target_str2 or target_str2 in s_str:
                        if item.get("symbol", "").upper().endswith(opt_type.upper()):
                            candidates.append(item)
                                
                if candidates:
                    def parse_expiry(item):
                        exp_str = item.get("expiry", "")
                        try:
                            from datetime import datetime
                            return datetime.strptime(exp_str, "%d%b%Y")
                        except:
                            from datetime import datetime
                            return datetime.max
                    candidates.sort(key=parse_expiry)
                    matched = candidates[0]
                    trade["symbol"] = matched.get("symbol")
                    self._apply_smart_lot_size(trade, matched)
                    return matched.get("token", ""), "NFO"

        # 2. Equity/Index resolution
        # Try exact match first (O(1))
        scrip = self.cache_manager.get_scrip_by_symbol(symbol)
        if scrip:
            self._apply_smart_lot_size(trade, scrip)
            return scrip.get("token", ""), scrip.get("exch_seg", "NSE")
            
        # Try symbol + '-EQ' for NSE Equities (O(1))
        eq_sym = f"{symbol}-EQ"
        scrip = self.cache_manager.get_scrip_by_symbol(eq_sym)
        if scrip:
            trade["symbol"] = eq_sym
            self._apply_smart_lot_size(trade, scrip)
            return scrip.get("token", ""), "NSE"
            
        # Try prefix/substring lookup on NSE only
        nse_candidates = self.cache_manager.get_nse_scrips_starting_with(symbol)
        if nse_candidates:
            matched = nse_candidates[0]
            trade["symbol"] = matched.get("symbol")
            self._apply_smart_lot_size(trade, matched)
            return matched.get("token", ""), "NSE"
                
        return "", "NSE"

    def _apply_smart_lot_size(self, trade, item):
        """Validates and adjusts trade quantity based on broker's exact lot size."""
        try:
            if not item: return
            lotsize_str = item.get("lotsize", "1")
            lotsize = int(lotsize_str) if str(lotsize_str).isdigit() else 1
            target_qty = int(trade.get("qty", self.DEFAULT_QTY))
            
            if lotsize > 1:
                lots = max(1, round(target_qty / lotsize))
                new_qty = lots * lotsize
            else:
                new_qty = max(1, target_qty)
                
            if new_qty != target_qty:
                print(f"🔄 SMART LOT SYNC: {trade['symbol']} | App Qty: {target_qty} → Broker Lot: {lotsize} | Adjusted Qty: {new_qty}")
                trade["qty"] = new_qty
        except Exception as e:
            print(f"⚠️ Error in smart lot size logic for {trade.get('symbol', 'unknown')}: {e}")

    async def _place_real_order(self, trade):
        """Place a REAL order via Angel One SmartConnect API."""
        try:
            # CRITICAL: Re-verify authentication every time if failure count is rising
            if self._failure_count > 0:
                self.broker.authenticate(force=True)

            # Resolve symbol token dynamically to ensure 100% acceptance
            if not trade.get("symboltoken"):
                token, exch = self._resolve_symbol_token(trade)
                
                # GAP FIX: Explicit Token Validation
                if not token or token == "":
                    print(f"❌ EXECUTION BLOCK | Token missing for {trade['symbol']}. Update scrip_master.")
                    return False
                    
                trade["symboltoken"] = token
                trade["exchange"] = exch

            # AUTO-RETRY LOGIC with rate limit compliance
            result = await self.broker.place_order(
                symbol=trade["symbol"],
                token=trade.get("symboltoken", ""),
                qty=trade["qty"],
                side=trade["side"],
                exchange=trade.get("exchange", "NSE"),
                order_type="MARKET",
                product_type="INTRADAY",
                price=trade["entry_price"]
            )
            
            if result.get("success"):
                trade["order_id"] = result.get("order_id", "")
                self._failure_count = 0
                print(f"⚡ LIVE ORDER CONFIRMED | {trade['side']} {trade['qty']} {trade['symbol']} | OrderID: {trade['order_id']}")
                return True
            else:
                print(f"⚠️ BROKER REJECTED | {trade['symbol']} | {result.get('error')}")
                return False
                
        except Exception as e:
            self._failure_count += 1
            print(f"❌ LIVE EXECUTION EXCEPTION | {trade['symbol']} | {e}")
            return False

    async def _reset_circuit_breaker(self):
        """Auto-recovers the engine from a temporary block to avoid complete stalls."""
        await asyncio.sleep(60)
        if self._failure_count >= 5:
            self._failure_count = 0
            self.trading_enabled = True
            print("🔄 Circuit breaker auto-reset complete - Trading Re-enabled")

    async def _retry_live_mode(self):
        """Periodically retry broker auth to restore LIVE trading from PAPER fallback."""
        await asyncio.sleep(300)  # Wait 5 minutes before retrying
        try:
            auth_ok = self.broker.authenticate(force=True)
            if auth_ok:
                self._force_paper_mode = False
                self._failure_count = 0
                print("✅ BROKER RECONNECTED — Restored LIVE TRADING MODE")
            else:
                print("⚠️ Broker still unreachable — staying in PAPER MODE (retry in 5m)")
                asyncio.create_task(self._retry_live_mode())
        except Exception as e:
            print(f"⚠️ Broker retry failed: {e} — staying in PAPER MODE (retry in 5m)")
            asyncio.create_task(self._retry_live_mode())

    # ---------------------------------------------------------
    # PERSISTENCE & HELPERS
    # ---------------------------------------------------------

    def _load_scrip_master(self):
        """Load symbol-to-token mapping from scrip_master.json.
        Auto-refreshes weekly (Monday) or if file is >7 days old."""
        try:
            import os
            path = Path(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))) / "data" / "scrip_master.json"

            # Weekly refresh check (Monday or stale >7 days)
            needs_refresh = not path.exists()
            if not needs_refresh:
                file_age_days = (time.time() - os.path.getmtime(path)) / 86400
                is_monday = datetime.now().weekday() == 0
                if file_age_days > 7 or (is_monday and file_age_days > 1):
                    needs_refresh = True

            if needs_refresh:
                self._download_scrip_master(path)

            # Let CacheManager initialize the filtered JSON into memory
            self.cache_manager.get_scrip_master(force_reload=needs_refresh)
            print("✅ Scrip master initialized via CacheManager singleton.")
        except Exception as e:
            print(f"Error loading scrip master: {e}")

    def _download_scrip_master(self, path):
        """Download fresh scrip master from Angel One, filter for NSE/NFO to optimize memory, and delete temp download."""
        temp_path = path.with_suffix('.temp')
        try:
            import urllib.request
            import json
            import os
            url = "https://margincalculator.angelbroking.com/OpenAPI_File/files/OpenAPIScripMaster.json"
            print("🔄 WEEKLY SCRIP MASTER REFRESH | Downloading from Angel One...")
            path.parent.mkdir(parents=True, exist_ok=True)
            urllib.request.urlretrieve(url, str(temp_path))
            print("📊 Processing download: filtering NSE/NFO segments to save memory...")
            
            with open(temp_path, 'r', encoding='utf-8') as f:
                raw_data = json.load(f)
                
            allowed_segments = {"NSE", "NFO"}
            allowed_keys = {"token", "symbol", "name", "expiry", "strike", "lotsize", "instrumenttype", "exch_seg"}
            
            filtered_data = []
            for item in raw_data:
                if item.get("exch_seg") in allowed_segments:
                    cleaned_item = {k: item[k] for k in allowed_keys if k in item}
                    filtered_data.append(cleaned_item)
                    
            with open(path, 'w', encoding='utf-8') as f:
                json.dump(filtered_data, f)
                
            print(f"✅ Filtered scrip master saved | {len(filtered_data)} scrips (temp file cleaned)")
        except Exception as e:
            print(f"⚠️ Scrip master download failed: {e} — using existing file")
        finally:
            if temp_path.exists():
                try:
                    os.remove(temp_path)
                except Exception as ex:
                    print(f"Error removing temp file: {ex}")

    def _save_positions(self, symbol=None):
        """Sync internal positions to Redis hash. If symbol provided, sync only that one."""
        try:
            if symbol:
                if symbol in self.positions:
                    trade = self.positions[symbol]
                    trade_data = trade.to_dict() if hasattr(trade, "to_dict") else trade
                    self.redis.set_hash(self.active_trades_key, symbol, trade_data)
            else:
                for sym, trade in self.positions.items():
                    trade_data = trade.to_dict() if hasattr(trade, "to_dict") else trade
                    self.redis.set_hash(self.active_trades_key, sym, trade_data)
        except Exception as e:
            print(f"Error saving positions to Redis: {e}")

    def _load_positions(self):
        """Restore positions from Redis on startup, filtering out stale trades."""
        try:
            persisted = self.redis.get_hashall(self.active_trades_key)
            if persisted:
                ist = timezone(timedelta(hours=5, minutes=30))
                today_str = datetime.now(ist).strftime("%Y-%m-%d")
                
                valid_positions = {}
                stale_count = 0
                for sym, trade in persisted.items():
                    # Check if trade is from today (entry_time is now IST)
                    entry_time = trade.get("timestamp") or trade.get("entry_time", "")
                    if today_str in entry_time:
                        valid_positions[sym] = TypedPosition.from_dict(trade)
                    else:
                        stale_count += 1
                        # Remove stale trade from Redis hash
                        self.redis.delete_hash(self.active_trades_key, sym)
                
                self.positions = valid_positions
                if stale_count > 0:
                     print(f"🧹 Purged {stale_count} stale positions from previous days.")
                if self.positions:
                     print(f"✅ Restored {len(self.positions)} active positions: {list(self.positions.keys())}")
        except Exception as e:
             print(f"Error restoring positions: {e}")


    # ---------------------------------------------------------
    # PRICE MONITOR
    # ---------------------------------------------------------

    async def monitor_prices(self):

        while True:

            try:

                streams = self.redis.read(self.price_stream, self.last_price_id)

                if not streams:
                    await asyncio.sleep(0.05)
                    continue

                for stream, entries in streams:

                    for msg_id, payload in entries:

                        self.last_price_id = msg_id

                        raw = payload.get("data")

                        if raw is None:
                            continue

                        tick = json.loads(raw)

                        await self.update_positions(tick)

            except Exception as e:

                print("ExecutionEngine price error:", e)
                await asyncio.sleep(1)

    # ---------------------------------------------------------
    # POSITION MANAGEMENT — With Proper Trailing SL
    # ---------------------------------------------------------

    async def update_positions(self, tick):

        symbol = tick.get("symbol")
        price = tick.get("ltp") or tick.get("price")
        
        # Safely ignore ticks with missing or zero LTP (prevents instant SL triggers)
        if not price or price <= 0:
            return

        if symbol not in self.positions:
            return

        trade = self.positions[symbol]

        if trade["side"] == "BUY":
            await self._manage_buy_position(symbol, trade, price)
        else:
            await self._manage_sell_position(symbol, trade, price)

    async def _manage_buy_position(self, symbol, trade, price):
        """SMART 3-Stage Progressive Trailing SL for BUY positions. Split strictly by Index vs Stock."""
        entry = trade["entry_price"]

        if price > trade["highest_price"]:
            trade["highest_price"] = price

        highest = trade["highest_price"]
        gain_pct = (highest - entry) / entry if entry > 0 else 0

        # Smart Trailing SL once price is above entry price (protects gains early)
        if price > entry:
            initial_risk = entry - trade["initial_stop"]
            if initial_risk > 0:
                new_stop = round(highest - initial_risk, 2)
                if new_stop > trade["stop"]:
                    trade["stop"] = new_stop
                    self._save_positions(symbol)
                    print(f"🛡️ SMART TRAIL (BUY) | {symbol} | SL → {trade['stop']:.2f}")

        is_index = symbol in ("NIFTY", "BANKNIFTY", "SENSEX", "FINNIFTY", "MIDCPNIFTY")

        if is_index:
            # --- INDEX LOGIC (Point-based) ---
            # Dynamic multiplier based on regime (Trending = Wide, Volatile = Tight)
            regime_mult = 1.0
            if self._index_direction == "BULLISH_CONVERGENCE":
                regime_mult = 1.5  # Wider trail to capture big trend
            elif self._index_direction == "VOLATILE":
                regime_mult = 0.7  # Tighter trail to protect gains

            multiplier = (2.5 if ("BANKNIFTY" in symbol or "SENSEX" in symbol) else 1.0) * regime_mult
            bk_pts = 15 * multiplier
            trail_trig_pts = self.INDEX_TRAIL_TRIGGER_PTS * multiplier
            trail_step_pts = self.INDEX_TRAIL_STEP_PTS * multiplier
            
            pts_gain = price - entry
            
            # Stage 1: Breakeven Lock
            if not trade.get("trail_stage") and pts_gain >= bk_pts:
                trade["trail_stage"] = 1
                trade["trailing_active"] = True
                new_stop = entry + 2 # Cover brokerage
                trade["stop"] = max(trade["stop"], new_stop)
                self._save_positions(symbol)
                print(f"🔒 STAGE 1 BREAKEVEN (INDEX) | {symbol} | SL → {new_stop:.2f} | RegimeMult: {regime_mult}")

            # Stage 2: Trail below highest
            elif trade.get("trail_stage", 0) >= 1 and pts_gain >= trail_trig_pts:
                index_stop = round(highest - trail_step_pts, 2)
                if index_stop > trade["stop"]:
                    trade["stop"] = index_stop
                    self._save_positions(symbol)
                    print(f" 🎯 INDEX TRAIL | {symbol} | SL → {trade['stop']:.2f}")

        else:
            # --- STOCK LOGIC (Percentage-based) ---
            # Stage 1: Breakeven Lock (+0.5%)
            if not trade.get("trail_stage") and price >= entry * (1 + self.BREAKEVEN_TRIGGER_PCT):
                trade["trail_stage"] = 1
                trade["trailing_active"] = True
                new_stop = round(entry * 1.001, 2)
                trade["stop"] = max(trade["stop"], new_stop)
                self._save_positions(symbol)
                print(f"🔒 STAGE 1 BREAKEVEN (STOCK) | {symbol} | SL → {new_stop:.2f}")

            # Stage 2: Tight Trail
            if trade.get("trail_stage", 0) >= 1 and gain_pct >= self.TRAIL_STAGE2_TRIGGER:
                if trade.get("trail_stage") < 2:
                    trade["trail_stage"] = 2
                    print(f"📈 STAGE 2 TRAIL | {symbol} | +{gain_pct*100:.1f}% | trailing 0.35% below high")
                new_stop = round(highest * (1 - self.TRAIL_STAGE2_PCT), 2)
                if new_stop > trade["stop"]:
                    trade["stop"] = new_stop
                    self._save_positions(symbol)

            # Stage 3: Lock Profit
            if trade.get("trail_stage", 0) >= 2 and gain_pct >= self.TRAIL_STAGE3_TRIGGER:
                if trade.get("trail_stage") < 3:
                    trade["trail_stage"] = 3
                    print(f"💰 STAGE 3 LOCK | {symbol} | +{gain_pct*100:.1f}% | trailing 0.25% below high")
                new_stop = round(highest * (1 - self.TRAIL_STAGE3_PCT), 2)
                if new_stop > trade["stop"]:
                    trade["stop"] = new_stop
                    self._save_positions(symbol)

            # Stage 1 fallback: basic trail for early stage
            elif trade.get("trailing_active") and trade.get("trail_stage", 0) == 1:
                new_stop = round(highest * (1 - self.TRAILING_SL_PCT), 2)
                if new_stop > trade["stop"]:
                    trade["stop"] = new_stop
                    self._save_positions(symbol)

        # --- Common Exit & Partial Logic ---
        vwap = trade["features"].get("vwap")
        if vwap and vwap > trade["stop"] and vwap < price:
            trade["stop"] = vwap
            self._save_positions(symbol)

        half_target = entry + (trade["target"] - entry) * 0.5
        if price >= half_target and not trade.get("partial_booked"):
            trade["partial_booked"] = True
            trade["stop"] = max(trade["stop"], entry)
            self._save_positions(symbol)

        if price >= trade["target"]:
            pnl = (trade["target"] - entry) * trade["qty"]
            await self.close_trade(symbol, trade, trade["target"], pnl)

        elif price <= trade["stop"]:
            pnl = (trade["stop"] - entry) * trade["qty"]
            await self.close_trade(symbol, trade, trade["stop"], pnl)

    async def _manage_sell_position(self, symbol, trade, price):
        """SMART 3-Stage Progressive Trailing SL for SELL positions."""
        entry = trade["entry_price"]

        if price < trade["lowest_price"]:
            trade["lowest_price"] = price

        lowest = trade["lowest_price"]
        gain_pct = (entry - lowest) / entry if entry > 0 else 0

        # Smart Trailing SL once price is below entry price (protects gains early for short)
        if price < entry:
            initial_risk = trade["initial_stop"] - entry
            if initial_risk > 0:
                new_stop = round(lowest + initial_risk, 2)
                if new_stop < trade["stop"]:
                    trade["stop"] = new_stop
                    self._save_positions(symbol)
                    print(f"🛡️ SMART TRAIL (SELL) | {symbol} | SL → {trade['stop']:.2f}")

        is_index = symbol in ("NIFTY", "BANKNIFTY", "SENSEX", "FINNIFTY", "MIDCPNIFTY")

        if is_index:
            # --- INDEX LOGIC (Point-based) ---
            multiplier = 2.5 if ("BANKNIFTY" in symbol or "SENSEX" in symbol) else 1.0
            bk_pts = 15 * multiplier
            trail_trig_pts = self.INDEX_TRAIL_TRIGGER_PTS * multiplier
            trail_step_pts = self.INDEX_TRAIL_STEP_PTS * multiplier
            
            low_pts_gain = entry - lowest
            
            # Stage 1: Breakeven Lock
            if not trade.get("trail_stage") and low_pts_gain >= bk_pts:
                trade["trail_stage"] = 1
                trade["trailing_active"] = True
                new_stop = entry - 2 # Cover brokerage
                trade["stop"] = min(trade["stop"], new_stop)
                self._save_positions(symbol)
                print(f"🔒 STAGE 1 BREAKEVEN (INDEX) | {symbol} | SL → {new_stop:.2f}")

            # Stage 2: Trail above lowest
            elif trade.get("trail_stage", 0) >= 1 and low_pts_gain >= trail_trig_pts:
                index_stop = round(lowest + trail_step_pts, 2)
                if index_stop < trade["stop"]:
                    trade["stop"] = index_stop
                    self._save_positions(symbol)
                    print(f" 🎯 INDEX TRAIL | {symbol} | SL → {trade['stop']:.2f}")

        else:
            # --- STOCK LOGIC (Percentage-based) ---
            # Stage 1: Breakeven Lock
            if not trade.get("trail_stage") and price <= entry * (1 - self.BREAKEVEN_TRIGGER_PCT):
                trade["trail_stage"] = 1
                trade["trailing_active"] = True
                new_stop = round(entry * 0.999, 2)
                trade["stop"] = min(trade["stop"], new_stop)
                self._save_positions(symbol)
                print(f"🔒 STAGE 1 BREAKEVEN (STOCK) | {symbol} | SL → {new_stop:.2f}")

            # Stage 2: Tight Trail
            if trade.get("trail_stage", 0) >= 1 and gain_pct >= self.TRAIL_STAGE2_TRIGGER:
                if trade.get("trail_stage") < 2:
                    trade["trail_stage"] = 2
                    print(f"📉 STAGE 2 TRAIL | {symbol} | +{gain_pct*100:.1f}% | trailing 0.35% above low")
                new_stop = round(lowest * (1 + self.TRAIL_STAGE2_PCT), 2)
                if new_stop < trade["stop"]:
                    trade["stop"] = new_stop
                    self._save_positions(symbol)

            # Stage 3: Lock Profit
            if trade.get("trail_stage", 0) >= 2 and gain_pct >= self.TRAIL_STAGE3_TRIGGER:
                if trade.get("trail_stage") < 3:
                    trade["trail_stage"] = 3
                    print(f"💰 STAGE 3 LOCK | {symbol} | +{gain_pct*100:.1f}% | trailing 0.25% above low")
                new_stop = round(lowest * (1 + self.TRAIL_STAGE3_PCT), 2)
                if new_stop < trade["stop"]:
                    trade["stop"] = new_stop
                    self._save_positions(symbol)

            # Stage 1 fallback: basic trail
            elif trade.get("trailing_active") and trade.get("trail_stage", 0) == 1:
                new_stop = round(lowest * (1 + self.TRAILING_SL_PCT), 2)
                if new_stop < trade["stop"]:
                    trade["stop"] = new_stop
                    self._save_positions(symbol)

        # --- Common Exit & Partial Logic ---
        vwap = trade["features"].get("vwap")
        if vwap and vwap < trade["stop"] and vwap > price:
            trade["stop"] = vwap
            self._save_positions(symbol)

        half_target = entry - (entry - trade["target"]) * 0.5
        if price <= half_target and not trade.get("partial_booked"):
            trade["partial_booked"] = True
            trade["stop"] = min(trade["stop"], entry)
            self._save_positions(symbol)

        if price <= trade["target"]:
            pnl = (entry - trade["target"]) * trade["qty"]
            await self.close_trade(symbol, trade, trade["target"], pnl)

        elif price >= trade["stop"]:
            pnl = (entry - trade["stop"]) * trade["qty"]
            await self.close_trade(symbol, trade, trade["stop"], pnl)

    # ---------------------------------------------------------
    # SCALP MONITOR — Quick exits for fast-moving trades
    # ---------------------------------------------------------

    async def scalp_monitor(self):
        """Autonomous Scalp Monitor: Exit on 1% or Time Decay."""
        while True:
            try:
                # Use IST consistently — entry_time is stored in IST format
                ist = timezone(timedelta(hours=5, minutes=30))
                now = datetime.now(ist).replace(tzinfo=None)
                for symbol in list(self.positions.keys()):
                    trade = self.positions.get(symbol)
                    if not trade or trade.get("scalp_checked"):
                        continue

                    try:
                        entry_time = datetime.fromisoformat(trade["entry_time"]).replace(tzinfo=None)
                    except:
                        continue

                    elapsed_seconds = (now - entry_time).total_seconds()
                    entry = trade["entry_price"]

                    # Calculate current gain safely
                    if trade["side"] == "BUY":
                        current_best = trade.get("highest_price", entry)
                        gain_pct = (current_best - entry) / entry if entry > 0 else 0
                    else:
                        current_best = trade.get("lowest_price", entry)
                        gain_pct = (entry - current_best) / entry if entry > 0 else 0

                    # 1. TIME-DECAY EXIT: Scalping signals die after 7 minutes (420s)
                    if elapsed_seconds > 420:
                        print(f"⏱️ TIME DECAY EXIT | {symbol} | Closing stale scalp.")
                        trade["scalp_checked"] = True
                        pnl = (current_best - entry) * trade["qty"] if trade["side"] == "BUY" else (entry - current_best) * trade["qty"]
                        await self.close_trade(symbol, trade, current_best, pnl)
                        continue

                    # 2. PROFIT PROTECTION: The "Instant Win" 1% Scalp
                    if gain_pct >= self.SCALP_TARGET_PCT: # 1% Scalp Target
                        trade["scalp_checked"] = True
                        pnl = gain_pct * entry * trade["qty"]
                        print(f"⚡ SCALP EXIT | {symbol} | +{gain_pct*100:.1f}% in {elapsed_seconds:.0f}s")
                        await self.close_trade(symbol, trade, current_best, pnl)

            except Exception as e:
                print(f"Scalp monitor error: {e}")
            await asyncio.sleep(1) # High-frequency check

    # ---------------------------------------------------------
    # TRADE EXIT
    # ---------------------------------------------------------

    async def close_trade(self, symbol, trade, exit_price, pnl):
        """Finalize a trade, publish result and handle failures."""
        if symbol not in self.positions:
            return

        # Place EXIT order via broker if LIVE
        if trade.get("mode") == "LIVE" and self.CONFIRM_REAL_TRADING and not self._force_paper_mode:
            try:
                # Resolve token if missing (can happen for restored positions)
                exit_token = trade.get("symboltoken", "")
                exit_exchange = trade.get("exchange", "NSE")
                exit_symbol = trade.get("symbol", symbol)
                if not exit_token:
                    exit_token, exit_exchange = self._resolve_symbol_token(trade)
                    exit_symbol = trade.get("symbol", symbol)
                
                if not exit_token:
                    print(f"⚠️ LIVE EXIT SKIP | {symbol} | No token resolved — closing on paper")
                else:
                    # Ensure broker is authenticated before exit
                    if not self.broker._authenticated:
                        self.broker.authenticate(force=True)
                    
                    # Queue exit with absolute priority = 0
                    exit_result = await self.queue_order_execution(
                        0,
                        self.broker.exit_position,
                        symbol=exit_symbol,
                        token=exit_token,
                        qty=trade["qty"],
                        side=trade["side"],
                        exchange=exit_exchange,
                        price=exit_price
                    )
                    if exit_result.get("success"):
                        print(f"⚡ LIVE EXIT ORDER | {symbol} | OrderID: {exit_result.get('order_id')}")
                    else:
                        print(f"⚠️ LIVE EXIT FAILED | {symbol} | {exit_result.get('error')} — closing on paper")
            except Exception as e:
                print(f"⚠️ LIVE EXIT EXCEPTION | {symbol} | {e}")

        result = {
            "symbol": symbol,
            "side": trade["side"],
            "entry_price": trade["entry_price"],
            "exit_price": round(exit_price, 2),
            "pnl": round(pnl, 2),
            "qty": trade["qty"],
            "entry_time": trade["entry_time"],
            "exit_time": datetime.now(timezone(timedelta(hours=5, minutes=30))).strftime("%Y-%m-%dT%H:%M:%S"),
            "mode": trade.get("mode", "PAPER"),
            "strategy": trade.get("strategy", "INDEX"),
            "features": trade["features"].to_dict() if hasattr(trade["features"], "to_dict") else trade["features"],
        }

        self.positions.pop(symbol, None)
        self.symbol_cooldowns[symbol] = time.time() # Start 5-min cooldown
        self.redis.delete_hash(self.active_trades_key, symbol) # Remove from persistence

        async def _attempt_publish():
            for attempt in range(2):
                try:
                    await self.redis.publish("trade_results", result)
                    return
                except Exception as e:
                    print(f"Publish error (attempt {attempt+1}): {e}")

        asyncio.create_task(_attempt_publish())
        pnl_emoji = "💰" if pnl > 0 else "🔴"
        print(f"{pnl_emoji} TRADE CLOSED | {trade.get('mode','PAPER')} | {symbol} | PnL ₹{pnl:.2f} | Active: {len(self.positions)}")
        asyncio.create_task(self.broadcast_positions())

    async def broadcast_positions(self):
        """Send current open positions to the UI."""
        try:
            payload = {
                "positions": [pos.to_dict() if hasattr(pos, "to_dict") else pos for pos in self.positions.values()],
                "timestamp": time.time()
            }
            await self.redis.publish(self.active_positions_stream, payload)
        except Exception as e:
            print(f"Error broadcasting positions: {e}")


    # ---------------------------------------------------------
    # RISK MONITOR
    # ---------------------------------------------------------

    async def monitor_risk(self):
        """Watch the ``risk_state`` stream and handle disablement reasons."""
        while True:
            try:
                streams = self.redis.read(self.risk_stream, self.last_risk_id)
                if not streams:
                    await asyncio.sleep(0.1)
                    continue
                for _, entries in streams:
                    for msg_id, payload in entries:
                        self.last_risk_id = msg_id
                        raw = payload.get("data")
                        if not raw: continue
                        state = json.loads(raw)
                        
                        # Logic: System is enabled only if RiskEngine says so AND circuit breaker is not triggered
                        risk_enabled = state.get("enabled", True)
                        cb_enabled = self._failure_count < 5
                        
                        new_enabled = risk_enabled and cb_enabled
                        if self.trading_enabled and not new_enabled:
                            reason = state.get("reason") or "Circuit Breaker Triggered"
                            print(f"⚠️ TRADING DISABLED | Reason: {reason}")
                        elif not self.trading_enabled and new_enabled:
                            print("✅ TRADING RE-ENABLED")
                            self._failure_count = 0
                            
                        self.trading_enabled = new_enabled
            except Exception as e:
                print("ExecutionEngine risk error:", e)
                await asyncio.sleep(1)

    # ---------------------------------------------------------
    # COMMAND MONITOR (Paper / Live Toggle)
    # ---------------------------------------------------------

    async def monitor_commands(self):
        while True:
            try:
                streams = self.redis.read(self.command_stream, self.last_cmd_id)
                if not streams:
                    await asyncio.sleep(1)
                    continue
                for _, entries in streams:
                    for msg_id, payload in entries:
                        self.last_cmd_id = msg_id
                        cmd = json.loads(payload.get("data", "{}"))
                        if cmd.get("command") == "SET_LIVE_TRADING":
                            self.CONFIRM_REAL_TRADING = cmd.get("mode") == "LIVE"
                            mode = "🔴 LIVE" if self.CONFIRM_REAL_TRADING else "📝 PAPER"
                            print(f"🔄 ExecutionEngine switched to {mode} TRADING MODE via Dashboard.")

                        elif cmd.get("command") == "START_TRADING":
                            self._failure_count = 0
                            self.trading_enabled = True
                            print("▶️ TRADING STARTED MANUALLY VIA DASHBOARD")

                        elif cmd.get("command") == "STOP_TRADING":
                            self.trading_enabled = False
                            print("⏸️ TRADING STOPPED MANUALLY VIA DASHBOARD")

                        elif cmd.get("command") == "SHUTDOWN_SYSTEM":
                            print("🛑 SHUTDOWN INITIATED... Squaring off all positions.")
                            asyncio.create_task(self._systematic_shutdown())

                        elif cmd.get("command") == "SET_FORCE_PAPER":
                            self._force_paper_mode = cmd.get("enabled", False)
                            state = "ON (Paper Fallback)" if self._force_paper_mode else "OFF (Normal)"
                            print(f"🔄 Force Paper Mode: {state}")
            except Exception as e:
                print(f"ExecutionEngine command error: {e}")
                await asyncio.sleep(1)

    async def _systematic_shutdown(self):
        """Squares off all positions at market price and shuts down the python process."""
        self.trading_enabled = False # Block new signals
        
        if self.positions:
            symbols = list(self.positions.keys())
            for symbol in symbols:
                trade = self.positions.get(symbol)
                if trade:
                    if trade["side"] == "BUY":
                        exit_price = trade.get("highest_price", trade["entry_price"])
                        pnl = (exit_price - trade["entry_price"]) * trade["qty"]
                    else:
                        exit_price = trade.get("lowest_price", trade["entry_price"])
                        pnl = (trade["entry_price"] - exit_price) * trade["qty"]
                    print(f"🛑 SYSTEM SHUTDOWN SQUAREOFF | {symbol} @ {exit_price:.2f}")
                    await self.close_trade(symbol, trade, exit_price, pnl)
        
        print("✅ System successfully squared off. All engines entering IDLE state.")
        
        # Clean up temporary files on shutdown
        try:
            from services.cleanup import cleanup_temp_files
            cleanup_temp_files()
        except Exception as e:
            print(f"⚠️ Error during systematic shutdown cleanup: {e}")
            
        await asyncio.sleep(3)
        # Instead of os._exit(0), we keep the process alive so the dashboard remains visible.
        # The engines are already blocked from new signals by self.trading_enabled = False.

    # ---------------------------------------------------------
    # SCANNER PRICE MONITOR (for stock positions)
    # ---------------------------------------------------------

    async def monitor_scanner_prices(self):
        """Monitor stock positions using scanner price updates.
        
        Since the WebSocket only subscribes to index tokens, stock
        positions never get price updates via micro_ticks. This task
        reads fresh alpha_signals from each scan cycle to check
        target/stop for open stock positions.
        """
        scanner_last_id = self.redis.get_latest_id("alpha_signals")
        while True:
            try:
                streams = self.redis.read("alpha_signals", scanner_last_id)
                if not streams:
                    await asyncio.sleep(1)
                    continue
                for stream, entries in streams:
                    for msg_id, payload in entries:
                        scanner_last_id = msg_id
                        raw = payload.get("data")
                        if not raw:
                            continue
                        sig = json.loads(raw)
                        symbol = sig.get("symbol")
                        price = sig.get("price")
                        if symbol and price and symbol in self.positions:
                            tick = {"symbol": symbol, "ltp": price}
                            await self.update_positions(tick)
            except Exception as e:
                print(f"Scanner price monitor error: {e}")
                await asyncio.sleep(2)

    async def _poll_stock_ltp(self):
        """Periodically fetch LTP from broker API for stock positions missing tick data.
        
        Index positions (NIFTY, BANKNIFTY, etc.) get real-time ticks via WebSocket.
        Stock positions rely on scanner signals which are infrequent. This task
        polls the broker LTP endpoint every 10 seconds for any stock position.
        """
        INDEX_SYMBOLS = {"NIFTY", "BANKNIFTY", "SENSEX", "FINNIFTY", "MIDCPNIFTY", "INDIAVIX"}
        while True:
            try:
                stock_positions = [
                    (sym, t) for sym, t in self.positions.items()
                    if sym not in INDEX_SYMBOLS
                ]
                if stock_positions and self.broker._authenticated:
                    for sym, trade in stock_positions:
                        try:
                            scrip = self.cache_manager.get_scrip_by_symbol(sym) or {}
                            token = trade.get("symboltoken") or scrip.get("token", "")
                            exchange = trade.get("exchange") or scrip.get("exch_seg", "NSE")
                            if not token:
                                continue
                                
                            # Strict WAF rate limit protection (max 2 req/sec)
                            await asyncio.sleep(0.5)
                            
                            ltp_resp = self.broker.api.ltpData(exchange, sym, token)
                            if ltp_resp and ltp_resp.get("data"):
                                ltp = ltp_resp["data"].get("ltp")
                                if ltp and ltp > 0:
                                    await self.update_positions({"symbol": sym, "ltp": float(ltp)})
                        except Exception as e:
                            # Hide excessive server JSON parse WAF errors to clean up terminal
                            if "JSON response" not in str(e):
                                print(f"LTP fetch error for {sym}: {e}")
                            pass
            except Exception as e:
                print(f"LTP poll error: {e}")
            await asyncio.sleep(10)

    # ---------------------------------------------------------
    # PERIODIC POSITION BROADCAST
    # ---------------------------------------------------------

    async def periodic_position_broadcast(self):
        """Re-broadcast positions every 5s so dashboard stays in sync."""
        while True:
            try:
                if self.positions:
                    await self.broadcast_positions()
            except Exception:
                pass
            await asyncio.sleep(5)

    # ---------------------------------------------------------
    # INTRADAY SQUARE-OFF (3:20 PM IST)
    # ---------------------------------------------------------

    async def intraday_squareoff(self):
        """Auto-close all open positions at 3:20 PM IST for intraday compliance."""
        while True:
            try:
                ist = timezone(timedelta(hours=5, minutes=30))
                now = datetime.now(ist)
                # Use a specific window (3:20 PM to 3:25 PM) to avoid triggering on old dates or slight drifts
                squareoff_start = now.replace(hour=15, minute=20, second=0, microsecond=0)
                squareoff_end = now.replace(hour=15, minute=25, second=0, microsecond=0)
                
                if squareoff_start <= now <= squareoff_end and self.positions:
                    symbols = list(self.positions.keys())
                    for symbol in symbols:
                        trade = self.positions.get(symbol)
                        if trade:
                            # Use current tracked price (trailing SL) for more accurate exit
                            if trade["side"] == "BUY":
                                exit_price = trade.get("highest_price", trade["entry_price"])
                                pnl = (exit_price - trade["entry_price"]) * trade["qty"]
                            else:
                                exit_price = trade.get("lowest_price", trade["entry_price"])
                                pnl = (trade["entry_price"] - exit_price) * trade["qty"]
                            print(f"🔔 INTRADAY SQUAREOFF | {symbol} @ {exit_price:.2f}")
                            await self.close_trade(symbol, trade, exit_price, pnl)
            except Exception as e:
                print(f"Squareoff error: {e}")
            await asyncio.sleep(30)