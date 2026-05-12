import json
import asyncio
from datetime import datetime, date, timedelta, timezone

from services.redis_stream import RedisStream


class RiskEngine:

    def __init__(self):

        self.redis = RedisStream()

        self.input_stream = "trade_results"
        self.output_stream = "risk_state"

        self.last_id = self.redis.get_today_id()

        # Risk limits — relaxed for scalping mode
        self.max_trades = 50      # Was 20 — too restrictive for scalping
        self.max_daily_loss = -5000  # Was -50 — ₹5000 max daily loss (reasonable for ₹10L capital)
        self.max_positions = 10   # Was 3 — aligned with MDE's new max

        # Daily metrics
        self.trades_today = 0
        self.daily_pnl = 0

        # Date tracking
        self.current_day = date.today()

        self.trading_enabled = True
        self.market_open = True
        
        # Dual PnL Tracking
        self.live_pnl = 0.0
        self.paper_pnl = 0.0
        self.live_trades = 0
        self.paper_trades = 0

        # Publish initial state so ExecutionEngine doesn't wait forever
        self._initial_state_published = False

    # ---------------------------------------------------------
    # ENGINE START
    # ---------------------------------------------------------

    async def start(self):

        print("Risk Engine started")

        # Publish initial enabled state immediately so other engines don't block
        if not self._initial_state_published:
            self._initial_state_published = True
            await self._publish_state_async()
            print("Risk Engine: Published initial ENABLED state")

        # Start control command listener
        asyncio.create_task(self.listen_for_commands())
        
        # Periodic risk state re-broadcast to keep dashboard/engines in sync
        asyncio.create_task(self._periodic_publish())

        while True:

            try:

                self.check_day_reset()
                self.check_market_hours()

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

                        trade = json.loads(raw)

                        from datetime import datetime
                        entry_time = trade.get("entry_time", "")
                        if not entry_time.startswith(datetime.utcnow().strftime("%Y-%m-%d")):
                            continue

                        self.update_risk(trade)

            except Exception as e:

                print("RiskEngine error:", e)

                await asyncio.sleep(1)

    # ---------------------------------------------------------
    # COMMAND LISTENER
    # ---------------------------------------------------------

    async def listen_for_commands(self):
        """Listen for control commands like CONTINUE_TRADING."""
        cmd_last_id = "$"
        while True:
            try:
                streams = self.redis.read("control_commands", cmd_last_id)
                if not streams:
                    await asyncio.sleep(0.5)
                    continue

                for stream, entries in streams:
                    for msg_id, payload in entries:
                        cmd_last_id = msg_id
                        data = json.loads(payload.get("data", "{}"))
                        
                        if data.get("command") == "CONTINUE_TRADING":
                            self.max_trades += 25
                            self.trading_enabled = True
                            self.disable_reason = "" # Clear reason
                            print(f"✅ RISK: Trade limit increased by 25. New limit: {self.max_trades}. Trading re-enabled.")
                            self.publish_state()
                        
                        elif data.get("command") == "START_TRADING":
                            self.trading_enabled = True
                            self.disable_reason = ""
                            print("▶️ RISK: Trading started manually.")
                            self.publish_state()

                        elif data.get("command") == "STOP_TRADING":
                            self.trading_enabled = False
                            self.disable_reason = "MANUALLY PAUSED"
                            print("⏸️ RISK: Trading paused manually.")
                            self.publish_state()

            except Exception as e:
                print(f"RiskEngine command error: {e}")
                await asyncio.sleep(1)

    # ---------------------------------------------------------
    # DAILY RESET
    # ---------------------------------------------------------

    def check_day_reset(self):

        today = date.today()

        if today != self.current_day:

            self.current_day = today

            self.live_pnl = 0
            self.paper_pnl = 0
            self.live_trades = 0
            self.paper_trades = 0

            self.trading_enabled = True
            print("RISK RESET | new trading day")

    def check_market_hours(self):
        """Disables trading outside 9:15 AM - 3:30 PM IST."""
        # Force IST: UTC+5:30
        ist = timezone(timedelta(hours=5, minutes=30))
        now = datetime.now(ist)
        current_time = now.time()
        
        # 9:15 AM to 3:30 PM
        market_open = datetime.strptime("09:15", "%H:%M").time()
        market_close = datetime.strptime("15:30", "%H:%M").time()
        
        is_open = market_open <= current_time <= market_close
        is_weekday = now.weekday() < 5 # Monday-Friday
        
        should_be_enabled = is_open and is_weekday
        
        if self.market_open and not should_be_enabled:
            self.market_open = False
            self.trading_enabled = False
            self.disable_reason = "MARKET CLOSED"
            print(f"🕒 {self.disable_reason} | Current Time (IST): {current_time}")
            self.publish_state()
            
        elif not self.market_open and should_be_enabled:
            self.market_open = True
            self.trading_enabled = True
            print("☀️ MARKET OPEN | Trading Enabled")
            self.publish_state()

    # ---------------------------------------------------------
    # RISK UPDATE
    # ---------------------------------------------------------

    def update_risk(self, trade):
        pnl = trade.get("pnl", 0)
        self.trades_today += 1
        self.daily_pnl += pnl
        
        # Attribute to Live/Paper
        if trade.get("mode") == "LIVE":
            self.live_trades += 1
            self.live_pnl += pnl
        else:
            self.paper_trades += 1
            self.paper_pnl += pnl

        status_msg = f"RISK UPDATE | Paper: ₹{self.paper_pnl:.2f} ({self.paper_trades}) | Live: ₹{self.live_pnl:.2f} ({self.live_trades})"
        print(status_msg)

        # Check daily loss (Hard Breaker - applies to total or live?)
        # For safety, we'll use total daily loss, but could be restricted to Live.
        if self.daily_pnl <= self.max_daily_loss:
            self.trading_enabled = False
            self.disable_reason = f"DAILY LOSS LIMIT HIT (₹{self.daily_pnl:.2f})"
            print(f"🚨 {self.disable_reason} — TRADING DISABLED")

        # Check trade limit (Hard Breaker)
        elif self.trades_today >= self.max_trades:
            self.trading_enabled = False
            self.disable_reason = f"MAX TRADES REACHED ({self.trades_today}/{self.max_trades})"
            print(f"🚨 {self.disable_reason} — TRADING DISABLED")
        
        else:
            self.disable_reason = ""

        self.publish_state()

    # ---------------------------------------------------------
    # RISK STATE PUBLISH
    # ---------------------------------------------------------

    def publish_state(self):
        state = {
            "enabled": self.trading_enabled,
            "market_open": self.market_open,
            "trades": self.trades_today,
            "max_trades": self.max_trades,
            "pnl": self.daily_pnl,
            "live_pnl": self.live_pnl,
            "paper_pnl": self.paper_pnl,
            "live_trades": self.live_trades,
            "paper_trades": self.paper_trades,
            "max_loss": self.max_daily_loss,
            "reason": getattr(self, "disable_reason", ""),
            "timestamp": datetime.utcnow().isoformat()
        }
        asyncio.create_task(self.redis.publish(self.output_stream, state))

    async def _publish_state_async(self):
        """Awaitable version for startup."""
        state = {
            "enabled": self.trading_enabled,
            "market_open": self.market_open,
            "trades": self.trades_today,
            "max_trades": self.max_trades,
            "pnl": self.daily_pnl,
            "live_pnl": self.live_pnl,
            "paper_pnl": self.paper_pnl,
            "live_trades": self.live_trades,
            "paper_trades": self.paper_trades,
            "max_loss": self.max_daily_loss,
            "reason": getattr(self, "disable_reason", ""),
            "timestamp": datetime.utcnow().isoformat()
        }
        await self.redis.publish(self.output_stream, state)

    async def _periodic_publish(self):
        """Re-broadcast risk state every 30s to keep all consumers in sync."""
        while True:
            await asyncio.sleep(30)
            try:
                self.check_market_hours()  # Refresh market_open status
                await self._publish_state_async()
            except Exception as e:
                print(f"Periodic risk publish error: {e}")