"""
Stock VWAP Retest Engine — SaaS-Grade Intraday Stock Selection & Execution

Strategy:
  1. FILTER    → Price > ₹30, already crossed Monthly VWAP, up 5%+ from MVWAP
  2. TRIGGER   → Price retests Monthly VWAP zone with volume below 20-day avg
  3. ENTRY     → BUY at Monthly VWAP zone (within 0.5%)
  4. TARGET    → 1% from entry
  5. TRAIL     → When above entry, trail with 0.5% distance for 2%+ upside
  6. EXIT      → Target hit, trailing SL hit, or time-based (max 90 min hold)

Data Source: AngelOne getCandleData (monthly/daily) + live micro_ticks
Publishes:   alpha_signals (for MDE validation) + vwap_retest_state (for UI)
"""

import json
import asyncio
import time
import traceback
from datetime import datetime, timedelta, timezone
from collections import defaultdict
import pandas as pd

from services.redis_stream import RedisStream
from services.broker_api import BrokerAPI
from config import settings as sys_config


class StockVWAPRetestEngine:

    # ─── Configuration ───────────────────────────────────────
    MIN_PRICE = 30.0             # Only stocks above ₹30
    MVWAP_CROSS_PCT = 0.02       # Must have rallied 2%+ above Monthly VWAP (was 3%)
    RETEST_ZONE_PCT = 0.01       # Entry zone: within 1% of Monthly VWAP (was 0.5% — missed IREDA/Exide/Schneider)
    VOL_BELOW_AVG_RATIO = 1.2    # Volume up to 1.2x of 20-day average (was 0.8x — too strict)
    TARGET_PCT = 0.01            # 1% target
    TRAIL_TRIGGER_PCT = 0.005    # Start trailing after 0.5% above entry
    TRAIL_DISTANCE_PCT = 0.005   # 0.5% trailing stop distance
    MAX_HOLD_MINUTES = 90        # Time-based exit
    SCAN_INTERVAL = 300          # 5-minute scan cycle (seconds)
    MAX_CANDIDATES = 50          # Max stocks to track simultaneously (was 20)
    COOLDOWN_SECONDS = 600       # 10-min cooldown per symbol after signal

    def __init__(self):
        self.redis = RedisStream()
        self.broker = BrokerAPI()  # Singleton — shares session with all engines
        self.output_stream = "alpha_signals"
        self.ui_stream = "vwap_retest_state"    # For dashboard visibility
        self.tick_stream = "micro_ticks"

        self._last_api_call = 0

        # State
        self.candidates = {}        # symbol → {mvwap, avg_vol, high_since_mvwap, ...}
        self.signaled = {}          # symbol → timestamp (cooldown tracking)
        self.scan_count = 0
        self.last_scan_time = None

        # Token map (loaded from scrip master cache)
        self.token_map = {}

        # Persistence Keys
        self.candidates_key = "vwap_retest_candidates"
        self.signaled_key = "vwap_retest_signaled"

    # ─── Engine Start ────────────────────────────────────────

    async def start(self):
        print("📊 Stock VWAP Retest Engine started | SaaS Mode (Shared Session)")
        self._load_token_map()
        self.broker.authenticate()  # Uses singleton — no duplicate login

        # Restore state from Redis
        self._load_state()

        asyncio.create_task(self._scan_loop())
        asyncio.create_task(self._monitor_ticks())
        asyncio.create_task(self._publish_ui_state())
        asyncio.create_task(self._cleanup_cooldowns())

        # Keep alive
        while True:
            await asyncio.sleep(60)

    def _load_token_map(self):
        """Load token map from scrip master cache."""
        import os
        cache_path = os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(
            os.path.abspath(__file__)))), "data", "scrip_master.json")
        try:
            with open(cache_path, 'r') as f:
                data = json.load(f)
            self.token_map = {
                item['symbol'].replace('-EQ', ''): item['token']
                for item in data if item.get('exch_seg') == 'NSE'
            }
            print(f"📊 VWAP Retest: Loaded {len(self.token_map)} scrip tokens")
        except Exception as e:
            print(f"⚠️ VWAP Retest: Token map load error: {e}")

    # ─── Rate-Limited API Call ───────────────────────────────

    def _api_fetch(self, params, symbol):
        """Rate-limited candle data fetch (max 1.5/sec)."""
        elapsed = time.time() - self._last_api_call
        if elapsed < 0.6:
            time.sleep(0.6 - elapsed)
        self._last_api_call = time.time()

        for attempt in range(2):
            try:
                resp = self.broker.api.getCandleData(params)
                if resp and resp.get('status'):
                    return resp.get('data')
                if resp and resp.get('errorcode') == 'AB1019':
                    time.sleep(2)
                    continue
            except Exception as e:
                if "b''" not in str(e):
                    print(f"  VWAP fetch error {symbol}: {e}")
                time.sleep(1 + attempt)
        return None

    # ─── Main Scan Loop ──────────────────────────────────────

    async def _scan_loop(self):
        """Every 5 minutes, scan Nifty 500 for VWAP retest candidates."""
        await asyncio.sleep(10)  # Initial delay to let system stabilize

        while True:
            try:
                ist = timezone(timedelta(hours=5, minutes=30))
                now = datetime.now(ist)
                # Only scan during market hours (9:20 AM - 3:00 PM)
                if now.hour < 9 or (now.hour == 9 and now.minute < 20) or now.hour >= 15:
                    await asyncio.sleep(60)
                    continue

                self.scan_count += 1
                self.last_scan_time = now
                print(f"\n🔍 VWAP RETEST SCAN #{self.scan_count} @ {now.strftime('%H:%M:%S')}")

                # Get Nifty 500 stock list
                symbols = await self._get_stock_universe()
                if not symbols:
                    print("⚠️ No symbols to scan")
                    await asyncio.sleep(self.SCAN_INTERVAL)
                    continue

                new_candidates = 0
                for symbol in symbols:
                    if len(self.candidates) >= self.MAX_CANDIDATES:
                        break
                    if symbol in self.candidates or symbol in self.signaled:
                        continue

                    result = await asyncio.to_thread(self._analyze_stock, symbol)
                    if result:
                        self.candidates[symbol] = result
                        self._save_state() # Persist new candidate
                        new_candidates += 1
                        print(f"  ✅ CANDIDATE: {symbol} | MVWAP: ₹{result['mvwap']:.2f} | "
                              f"LTP: ₹{result['ltp']:.2f} | AvgVol: {result['avg_vol']:,.0f}")

                print(f"📊 Scan #{self.scan_count} complete | "
                      f"New: {new_candidates} | Active: {len(self.candidates)} | "
                      f"Cooldown: {len(self.signaled)}")

            except Exception as e:
                print(f"VWAP Retest scan error: {e}")
                traceback.print_exc()

            await asyncio.sleep(self.SCAN_INTERVAL)

    async def _get_stock_universe(self):
        """Get Nifty 500 filtered by price > ₹30."""
        try:
            import psycopg2
            conn = psycopg2.connect(
                host="127.0.0.1",
                database=sys_config.POSTGRES_DB_RAW,
                user="postgres",
                password=sys_config.POSTGRES_PASSWORD_RAW
            )
            cur = conn.cursor()
            # Get symbols from intraday_signals with price > 30
            cur.execute("""
                SELECT DISTINCT symbol FROM intraday_signals
                WHERE last_price > 30
                ORDER BY symbol
            """)
            symbols = [r[0] for r in cur.fetchall()]
            conn.close()

            # Get symbols from intraday_signals with price > 30 (full market coverage)
            symbols = [s for s in symbols if s not in ("NIFTY", "BANKNIFTY", "SENSEX")]
            return symbols[:500]  # Expanded to 500 stocks
        except Exception as e:
            print(f"⚠️ VWAP Retest universe error: {e}")
            return []

    # ─── Stock Analysis ──────────────────────────────────────

    def _analyze_stock(self, symbol):
        """
        Analyze a stock for Monthly VWAP retest eligibility.
        Returns candidate dict or None.
        """
        if not self.broker._authenticated:
            return None

        token = self.token_map.get(symbol)
        if not token:
            return None

        # 1. Fetch historical candles from local DB (market_data.ohlcv)
        try:
            import psycopg2
            conn = psycopg2.connect(
                host="127.0.0.1",
                database="market_data",
                user="postgres",
                password=sys_config.POSTGRES_PASSWORD_RAW
            )
            cur = conn.cursor()
            cur.execute("""
                SELECT open, high, low, close, volume, date FROM ohlcv 
                WHERE symbol = %s 
                ORDER BY date DESC 
                LIMIT 60
            """, (symbol,))
            rows = cur.fetchall()
            conn.close()
        except Exception as e:
            print(f"⚠️ VWAP Retest DB query error for {symbol}: {e}")
            rows = []

        if not rows:
            return None

        # Reverse to get chronological order (ASC)
        rows.reverse()
        hist_df = pd.DataFrame(rows, columns=['open', 'high', 'low', 'close', 'volume', 'date'])
        
        # Convert types
        for col in ['open', 'high', 'low', 'close', 'volume']:
            hist_df[col] = hist_df[col].astype(float)

        # 2. Fetch today's live candle using getMarketData (extremely fast, no historical rate limits)
        live = None
        try:
            resp = self.broker.api.getMarketData("FULL", {"NSE": [token]})
            if resp and resp.get("status") and resp.get("data") and resp["data"].get("fetched"):
                item = resp["data"]["fetched"][0]
                live = {
                    "open": float(item.get("open", 0.0)),
                    "high": float(item.get("high", 0.0)),
                    "low": float(item.get("low", 0.0)),
                    "close": float(item.get("ltp", 0.0)),
                    "volume": float(item.get("tradeVolume", 0.0)),
                    "date": datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)
                }
        except Exception as e:
            print(f"⚠️ VWAP Retest live fetch error for {symbol}: {e}")

        if live:
            live_df = pd.DataFrame([live])
            df = pd.concat([hist_df, live_df]).drop_duplicates(subset=['date']).reset_index(drop=True)
        else:
            df = hist_df

        if df.empty or len(df) < 20:
            return None

        ltp = float(df.iloc[-1]['close'])

        # Skip penny stocks
        if ltp < self.MIN_PRICE:
            return None

        # ─── Monthly VWAP (last 22 trading days ≈ 1 month) ───
        monthly = df.tail(22)
        typical_price = (monthly['high'] + monthly['low'] + monthly['close']) / 3
        cum_tp_vol = (typical_price * monthly['volume']).sum()
        cum_vol = monthly['volume'].sum()

        if cum_vol == 0:
            return None

        mvwap = cum_tp_vol / cum_vol

        # ─── 20-day average volume ───
        avg_vol = df.tail(20)['volume'].mean()
        today_vol = float(df.iloc[-1]['volume'])

        # ─── FILTER 1: Must have rallied 3%+ above MVWAP at some point ───
        recent_high = float(monthly['high'].max())
        rally_pct = (recent_high - mvwap) / mvwap if mvwap > 0 else 0

        if rally_pct < self.MVWAP_CROSS_PCT:
            return None  # Never crossed MVWAP sufficiently

        # ─── FILTER 2: Currently near MVWAP (retest zone — within 3% for initial scan) ───
        distance_from_mvwap = abs(ltp - mvwap) / mvwap
        if distance_from_mvwap > 0.03:
            return None  # Too far from MVWAP to be a retest

        # ─── FILTER 3: Volume check (allow up to 1.2x avg — normal volume retests are valid) ───
        vol_ratio = today_vol / avg_vol if avg_vol > 0 else 1
        if vol_ratio > self.VOL_BELOW_AVG_RATIO:
            return None  # Volume too high — likely breakout, not quiet retest

        # ─── FILTER 4: Opening Strength (from Monthly VWAP trade reference) ───
        today_open = float(df.iloc[-1]['open'])
        today_low = float(df.iloc[-1]['low'])
        prev_close = float(df.iloc[-2]['close']) if len(df) > 1 else today_open
        prev_low = float(df.iloc[-2]['low']) if len(df) > 1 else today_low

        open_eq_low = abs(today_open - today_low) < 0.01 * today_open  # Open ≈ Low (bullish)
        gap_up = (today_open - prev_close) / prev_close > 0 if prev_close > 0 else False
        open_low_range = (today_open - today_low) / today_open if today_open > 0 else 0
        structure_strong = (0.003 <= open_low_range <= 0.005) and (today_low > prev_low)

        opening_strength = open_eq_low or gap_up or structure_strong
        opening_type = "OpenLow" if open_eq_low else ("GapUp" if gap_up else ("Structure" if structure_strong else "None"))

        return {
            "symbol": symbol,
            "token": token,
            "mvwap": round(mvwap, 2),
            "ltp": round(ltp, 2),
            "avg_vol": avg_vol,
            "today_vol": today_vol,
            "vol_ratio": round(vol_ratio, 2),
            "rally_pct": round(rally_pct * 100, 1),
            "distance_pct": round(distance_from_mvwap * 100, 2),
            "recent_high": round(recent_high, 2),
            "opening_strength": opening_strength,
            "opening_type": opening_type,
            "detected_at": datetime.utcnow().isoformat(),
            "status": "WATCHING"  # WATCHING → TRIGGERED → SIGNALED
        }

    # ─── Tick Monitor — Trigger on exact MVWAP touch ─────────

    async def _monitor_ticks(self):
        """Monitor alpha_signals and micro_ticks for candidate price updates."""
        last_alpha_id = self.redis.get_latest_id("alpha_signals")
        last_tick_id = self.redis.get_latest_id(self.tick_stream)

        while True:
            try:
                # Check alpha_signals for scanner price updates on candidates
                streams = self.redis.read("alpha_signals", last_alpha_id)
                if streams:
                    for stream, entries in streams:
                        for msg_id, payload in entries:
                            last_alpha_id = msg_id
                            raw = payload.get("data")
                            if not raw:
                                continue
                            sig = json.loads(raw)
                            symbol = sig.get("symbol")
                            price = sig.get("price")
                            if symbol and price and symbol in self.candidates:
                                await self._check_trigger(symbol, price)

                # Also check micro_ticks for real-time index ticks
                # (candidates won't be indices, but future stock subscriptions)
                tick_streams = self.redis.read(self.tick_stream, last_tick_id)
                if tick_streams:
                    for stream, entries in tick_streams:
                        for msg_id, payload in entries:
                            last_tick_id = msg_id
                            raw = payload.get("data")
                            if not raw:
                                continue
                            tick = json.loads(raw)
                            symbol = tick.get("symbol")
                            price = tick.get("ltp") or tick.get("price")
                            if symbol and price and symbol in self.candidates:
                                await self._check_trigger(symbol, price)

            except Exception as e:
                print(f"VWAP Retest tick monitor error: {e}")

            await asyncio.sleep(0.1)

    async def _check_trigger(self, symbol, price):
        """Check if price has retested Monthly VWAP for entry signal."""
        cand = self.candidates.get(symbol)
        if not cand or cand["status"] == "SIGNALED":
            return

        mvwap = cand["mvwap"]
        distance = abs(price - mvwap) / mvwap

        # Update LTP
        cand["ltp"] = round(price, 2)

        # ─── TRIGGER: Price within 0.5% of MVWAP AND above it ───
        if distance <= self.RETEST_ZONE_PCT and price >= mvwap * 0.998:
            cand["status"] = "TRIGGERED"

            # Generate trading signal
            target = round(price * (1 + self.TARGET_PCT), 2)
            stop = round(mvwap * 0.992, 2)  # 0.8% below MVWAP

            signal = {
                "symbol": symbol,
                "signal": "BUY",
                "price": round(price, 2),
                "score": 80,
                "timestamp": datetime.utcnow().isoformat(),
                "source": "vwap_retest",
                "strategy": "STOCK_VWAP_RETEST",
                "features": {
                    "monthly_vwap": mvwap,
                    "swing_low": stop,
                    "vwap": mvwap,
                    "vol_ratio": cand["vol_ratio"],
                    "rally_pct": cand["rally_pct"],
                    "retest_distance": round(distance * 100, 3),
                    "target": target,
                    "trail_trigger": round(price * (1 + self.TRAIL_TRIGGER_PCT), 2),
                    "trail_distance": self.TRAIL_DISTANCE_PCT,
                }
            }

            await self.redis.publish(self.output_stream, signal)
            cand["status"] = "SIGNALED"
            self.signaled[symbol] = time.time()
            self._save_state() # Persist signal & removal

            print(f"🎯 VWAP RETEST SIGNAL | BUY {symbol} @ ₹{price:.2f} | "
                  f"MVWAP: ₹{mvwap:.2f} | Target: ₹{target:.2f} | SL: ₹{stop:.2f} | "
                  f"VolRatio: {cand['vol_ratio']}")

            # Remove from candidates after signal
            self.candidates.pop(symbol, None)
            self._save_state()

    # ─── UI State Publisher ──────────────────────────────────

    async def _publish_ui_state(self):
        """Publish engine state to Redis for dashboard consumption."""
        while True:
            try:
                state = {
                    "candidates": list(self.candidates.values()),
                    "candidate_count": len(self.candidates),
                    "signaled_count": len(self.signaled),
                    "scan_count": self.scan_count,
                    "last_scan": self.last_scan_time.isoformat() if self.last_scan_time else None,
                    "config": {
                        "min_price": self.MIN_PRICE,
                        "retest_zone": f"{self.RETEST_ZONE_PCT*100}%",
                        "target": f"{self.TARGET_PCT*100}%",
                        "trail": f"{self.TRAIL_DISTANCE_PCT*100}%",
                    },
                    "timestamp": datetime.utcnow().isoformat()
                }
                await self.redis.publish(self.ui_stream, state)
            except Exception:
                pass
            await asyncio.sleep(10)

    # ---------------------------------------------------------
    # PERSISTENCE
    # ---------------------------------------------------------

    def _save_state(self):
        """Sync candidates and signaled set to Redis."""
        try:
            # Sync candidates
            for sym, data in self.candidates.items():
                self.redis.set_hash(self.candidates_key, sym, data)
            
            # Sync signaled
            for sym, ts in self.signaled.items():
                self.redis.set_hash(self.signaled_key, sym, ts)
        except Exception as e:
            print(f"Error saving VWAP state: {e}")

    def _load_state(self):
        """Restore state from Redis on startup."""
        from datetime import datetime, timedelta, timezone
        import time
        ist = timezone(timedelta(hours=5, minutes=30))
        today_date = datetime.now(ist).date()
        try:
            cands = self.redis.get_hashall(self.candidates_key)
            if cands:
                filtered_cands = {}
                for k, v in cands.items():
                    dt_str = v.get("detected_at")
                    if dt_str:
                        dt_utc = datetime.fromisoformat(dt_str).replace(tzinfo=timezone.utc)
                        if dt_utc.astimezone(ist).date() == today_date:
                            filtered_cands[k] = v
                self.candidates = filtered_cands
                print(f"✅ VWAP Retest: Restored {len(self.candidates)} candidates")
            
            sigs = self.redis.get_hashall(self.signaled_key)
            if sigs:
                now = time.time()
                filtered_sigs = {k: v for k, v in sigs.items() if now - v < self.COOLDOWN_SECONDS}
                self.signaled = filtered_sigs
                print(f"✅ VWAP Retest: Restored {len(self.signaled)} signaled cooldowns")
        except Exception as e:
            print(f"Error restoring VWAP state: {e}")

    # ─── Cooldown Cleanup ────────────────────────────────────

    async def _cleanup_cooldowns(self):
        """Remove expired cooldowns."""
        while True:
            now = time.time()
            expired = [s for s, t in self.signaled.items()
                       if now - t > self.COOLDOWN_SECONDS]
            for s in expired:
                del self.signaled[s]
                self.redis.delete_hash(self.signaled_key, s) # Remove from persistence
            await asyncio.sleep(30)
