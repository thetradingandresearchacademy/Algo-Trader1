import asyncio
import json
from datetime import datetime, date
from collections import defaultdict

from services.redis_stream import RedisStream

class AISupervisor:

    def __init__(self):

        self.redis = RedisStream()

        # Streams
        self.trade_stream = "trade_results"
        self.signal_stream = "validated_signals"
        self.risk_stream = "risk_state"
        self.command_stream = "control_commands"

        # Start from latest messages to avoid replaying stale data on restart
        self.last_ids = {}
        for s in [self.trade_stream, self.signal_stream, self.risk_stream, self.command_stream]:
            self.last_ids[s] = self.redis.get_latest_id(s)

        # Performance tracking
        self.daily_pnl = 0
        self.trades = 0
        self.wins = 0
        self.losses = 0

        self.current_day = date.today()

        # System state
        self.trading_enabled = True
        self.mode = "NORMAL"
        self.trading_focus = "BOTH"  # INDEX, STOCK, BOTH

        # Risk control
        self.max_drawdown = -0.03  # -3%

    # ---------------------------------------------------------
    # START
    # ---------------------------------------------------------

    async def start(self):

        print("AI Supervisor Started")

        asyncio.create_task(self.monitor_trades())
        asyncio.create_task(self.monitor_risk())
        asyncio.create_task(self.monitor_ui_commands())

        while True:
            self.check_day_reset()
            await asyncio.sleep(5)

    # ---------------------------------------------------------
    # UI COMMAND MONITOR (Dashboard Integration)
    # ---------------------------------------------------------

    async def monitor_ui_commands(self):
        while True:
            streams = self.redis.read(self.command_stream, self.last_ids[self.command_stream])
            
            if not streams:
                await asyncio.sleep(0.5)
                continue
                
            for stream, entries in streams:
                for msg_id, payload in entries:
                    self.last_ids[self.command_stream] = msg_id
                    
                    cmd = json.loads(payload.get("data", "{}"))
                    
                    if cmd.get("command") == "UPDATE_MODE":
                        self.trading_focus = cmd.get("new_mode", "BOTH")
                        print(f"🚨 AI SUPERVISOR OVERRIDE: Focus switched to {self.trading_focus} via UI.")
                        
                    elif cmd.get("command") == "KILL_SWITCH":
                        self.trading_enabled = False
                        print("🚨 AI SUPERVISOR OVERRIDE: MANUAL KILL SWITCH ACTIVATED.")
                        state = {
                            "enabled": False,
                            "reason": "MANUAL_KILL_SWITCH",
                            "timestamp": datetime.utcnow().isoformat()
                        }
                        asyncio.create_task(self.redis.publish(self.risk_stream, state))

    # ---------------------------------------------------------
    # TRADE MONITOR
    # ---------------------------------------------------------

    async def monitor_trades(self):

        while True:

            streams = self.redis.read(self.trade_stream, self.last_ids[self.trade_stream])

            if not streams:
                await asyncio.sleep(0.1)
                continue

            for stream, entries in streams:
                for msg_id, payload in entries:

                    self.last_ids[self.trade_stream] = msg_id

                    trade = json.loads(payload.get("data"))
                    pnl = trade.get("pnl", 0)

                    self.daily_pnl += pnl
                    self.trades += 1

                    if pnl > 0:
                        self.wins += 1
                    elif pnl < 0:
                        self.losses += 1
                    # 0 pnl is breakeven, neither win nor loss

                    self.evaluate_performance()

    # ---------------------------------------------------------
    # PERFORMANCE LOGIC
    # ---------------------------------------------------------

    def evaluate_performance(self):

        if self.trades == 0:
            return

        win_rate = self.wins / self.trades

        # Mode switch
        if win_rate > 0.6:
            self.mode = "AGGRESSIVE"
        else:
            self.mode = "NORMAL"

        # Kill switch: Profit Target & Max Loss
        target_pnl = 25000  # Increased from 3000
        max_loss = -10000   # Increased from -3000
        
        if self.daily_pnl >= target_pnl:
            if self.trading_enabled:  # publish only once
                self.trading_enabled = False
                print(f"🤑 AI SUPERVISOR: Daily target of ₹{target_pnl} hit! Shutting down system to secure profit.")
                state = {
                    "enabled": False,
                    "reason": "PROFIT_TARGET_HIT",
                    "pnl": self.daily_pnl,
                    "timestamp": datetime.utcnow().isoformat()
                }
                asyncio.create_task(self.redis.publish(self.risk_stream, state))
                
        elif self.daily_pnl <= max_loss:
            if self.trading_enabled:  # publish only once
                self.trading_enabled = False
                print(f"🚨 AI SUPERVISOR: Daily max loss of ₹{max_loss} hit. Trading disabled for capital preservation.")
                state = {
                    "enabled": False,
                    "reason": "MAX_LOSS_HIT",
                    "pnl": self.daily_pnl,
                    "timestamp": datetime.utcnow().isoformat()
                }
                asyncio.create_task(self.redis.publish(self.risk_stream, state))

        # Publish mode + focus state so MDE/ExecutionEngine can use it
        state = {
            "enabled": self.trading_enabled,
            "mode": self.mode,
            "focus": self.trading_focus,
            "pnl": self.daily_pnl,
            "timestamp": datetime.utcnow().isoformat()
        }
        asyncio.create_task(self.redis.publish(self.risk_stream, state))

        print(f"AI STATUS | PnL={self.daily_pnl} | WR={win_rate:.2f} | Mode={self.mode} | Focus={self.trading_focus}")

    # ---------------------------------------------------------
    # RISK MONITOR
    # ---------------------------------------------------------

    async def monitor_risk(self):

        while True:

            streams = self.redis.read(self.risk_stream, self.last_ids[self.risk_stream])

            if not streams:
                await asyncio.sleep(1)
                continue

            for stream, entries in streams:
                for msg_id, payload in entries:

                    self.last_ids[self.risk_stream] = msg_id
                    state = json.loads(payload.get("data"))

                    if not state.get("enabled", True):
                        self.trading_enabled = False

    # ---------------------------------------------------------
    # DAY RESET
    # ---------------------------------------------------------

    def check_day_reset(self):

        today = date.today()

        if today != self.current_day:

            self.current_day = today

            self.daily_pnl = 0
            self.trades = 0
            self.wins = 0
            self.losses = 0

            self.trading_enabled = True

            # Re-enable trading across the system
            state = {
                "enabled": True,
                "mode": "NORMAL",
                "focus": self.trading_focus,
                "pnl": 0,
                "timestamp": datetime.utcnow().isoformat()
            }
            asyncio.create_task(self.redis.publish(self.risk_stream, state))

            print("🔄 AI RESET FOR NEW DAY")