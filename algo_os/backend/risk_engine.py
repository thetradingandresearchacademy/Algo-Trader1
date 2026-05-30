import json
import asyncio
from datetime import datetime, date, timedelta, timezone

from services.redis_stream import RedisStream

# Enterprise-grade typed models
from models.trade import Trade


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

        # Capital Tracking
        self.base_capital = 100000.0
        self.current_capital = 100000.0

        # Publish initial state so ExecutionEngine doesn't wait forever
        self._initial_state_published = False

    # ---------------------------------------------------------
    # ENGINE START
    # ---------------------------------------------------------

    async def start(self):

        print("Risk Engine started")

        # Capital Prompt
        try:
            import sys
            stored_cap = self.redis.get_hashall("system_config").get("current_capital")
            is_interactive = sys.stdin and sys.stdin.isatty()
            
            if stored_cap:
                print(f"\n💰 Previous Day's Capital detected: ₹{float(stored_cap):.2f}")
                if is_interactive:
                    ans = input("Do you want to continue with this as base capital? (y/n) [default: y]: ")
                    if ans.lower() == 'n':
                        new_cap = input("Enter new base capital [default: 100000]: ")
                        self.base_capital = float(new_cap) if new_cap else 100000.0
                    else:
                        self.base_capital = float(stored_cap)
                else:
                    print("🤖 Headless session: Continuing with previous day's capital.")
                    self.base_capital = float(stored_cap)
            else:
                if is_interactive:
                    new_cap = input("\n💰 Enter base capital [default: 100000]: ")
                    self.base_capital = float(new_cap) if new_cap else 100000.0
                else:
                    print("🤖 Headless session: Defaulting to base capital ₹100,000.")
                    self.base_capital = 100000.0
                
            self.current_capital = self.base_capital
            self.redis.set_hash("system_config", "base_capital", self.base_capital)
            self.redis.set_hash("system_config", "current_capital", self.current_capital)
            print(f"✅ Base Capital locked at: ₹{self.base_capital:.2f}\n")
        except Exception as e:
            print(f"⚠️ Capital setup error: {e}. Defaulting to 100000.")
            self.base_capital = 100000.0
            self.current_capital = 100000.0

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
                        trade_dict = json.loads(raw)
                        trade = Trade.from_dict(trade_dict)

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
        cmd_last_id = self.redis.get_latest_id("control_commands")
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
                            
                        elif data.get("command") == "SET_LIVE_TRADING":
                            mode = data.get("mode", "PAPER")
                            print(f"🔄 RISK: Notified of trading mode change to {mode}")
                            self.publish_state()

                        elif data.get("command") == "SET_FORCE_PAPER":
                            enabled = data.get("enabled", False)
                            state = "ON" if enabled else "OFF"
                            print(f"🔄 RISK: Notified of Force Paper state change to {state}")
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

            # Base capital resets to current capital on new day boundary
            self.base_capital = self.current_capital
            self.redis.set_hash("system_config", "base_capital", self.base_capital)

            self.trading_enabled = True
            print(f"RISK RESET | new trading day. Base Capital: ₹{self.base_capital:.2f}")

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

    def _calculate_charges(self, trade):
        """Calculate approximate broker charges for realistic Net PnL."""
        try:
            qty = trade.get("qty", 0)
            entry = trade.get("entry_price", 0)
            exit_p = trade.get("exit_price", 0)
            if not qty or not entry or not exit_p: return 0.0
            
            # Simple assumption: NFO (Options) vs NSE (Equity)
            symbol = trade.get("symbol", "")
            is_options = trade.get("is_options") or "_CE" in symbol or "_PE" in symbol
                
            turnover = qty * (entry + exit_p)
            brokerage = 40.0
            stt = (qty * exit_p) * (0.000625 if is_options else 0.00025)
            trans_charges = turnover * (0.00053 if is_options else 0.0000345)
            gst = (brokerage + trans_charges) * 0.18
            sebi_stamp = turnover * 0.00005
            
            return round(brokerage + stt + trans_charges + gst + sebi_stamp, 2)
        except Exception:
            return 0.0

    def update_risk(self, trade):
        pnl = trade.get("pnl", 0)
        charges = self._calculate_charges(trade)
        net_pnl = pnl - charges
        
        self.trades_today += 1
        self.daily_pnl += pnl
        
        # Attribute to Live/Paper
        if trade.get("mode") == "LIVE":
            self.live_trades += 1
            self.live_pnl += pnl
            self.current_capital += net_pnl
            self.redis.set_hash("system_config", "current_capital", self.current_capital)
        else:
            self.paper_trades += 1
            self.paper_pnl += pnl
            # Also update Active Capital for paper trades so it correctly tracks simulation/fallback performance
            self.current_capital += net_pnl
            self.redis.set_hash("system_config", "current_capital", self.current_capital)

        status_msg = f"RISK UPDATE | Paper: ₹{self.paper_pnl:.2f} ({self.paper_trades}) | Live: ₹{self.live_pnl:.2f} (Net: ₹{self.live_pnl - (charges * self.live_trades):.2f}) ({self.live_trades}) | Capital: ₹{self.current_capital:.2f}"
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
            "current_capital": getattr(self, "current_capital", 100000.0),
            "base_capital": getattr(self, "base_capital", 100000.0),
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
            "current_capital": getattr(self, "current_capital", 100000.0),
            "base_capital": getattr(self, "base_capital", 100000.0),
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