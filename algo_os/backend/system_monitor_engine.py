import asyncio
import json
import time

from colorama import Fore, Style, init
from services.redis_stream import RedisStream

init(autoreset=True)


class SystemMonitorEngine:

    def __init__(self):

        self.redis = RedisStream()

        self.tick_stream = "micro_ticks"
        self.signal_stream = "alpha_signals"
        self.trade_stream = "trade_results"
        self.risk_stream = "risk_state"
        self.positions_stream = "active_positions"

        self.last_tick_id = self.redis.get_today_id()
        self.last_signal_id = self.redis.get_today_id()
        self.last_trade_id = self.redis.get_today_id()
        self.last_risk_id = self.redis.get_today_id()
        self.last_positions_id = self.redis.get_today_id()

        self.tick_count = 0
        self.signal_count = 0

        self.trades = 0
        self.wins = 0
        self.losses = 0
        self.pnl = 0

        self.trading_enabled = True
        self.market_open = True
        
        # Dual Tracking
        self.live_pnl = 0.0
        self.paper_pnl = 0.0
        self.live_trades = 0
        self.paper_trades = 0
        self.live_wins = 0
        self.paper_wins = 0
        self.live_losses = 0
        self.paper_losses = 0
        
        # Capital tracking
        self.current_capital = 100000.0
        self.base_capital = 100000.0

        self.last_tick_time = time.time()

        # Active positions tracking
        self.open_positions = []  # List of open position dicts

    async def start(self):

        print("System Monitor Engine started")

        asyncio.create_task(self.monitor_ticks())
        asyncio.create_task(self.monitor_signals())
        asyncio.create_task(self.monitor_trades())
        asyncio.create_task(self.monitor_risk())
        asyncio.create_task(self.monitor_positions())

        while True:

            self.render_dashboard()

            await asyncio.sleep(1)

    async def monitor_ticks(self):

        while True:

            streams = self.redis.read(self.tick_stream, self.last_tick_id)

            for stream, entries in streams:

                for msg_id, payload in entries:

                    self.last_tick_id = msg_id
                    self.tick_count += 1
                    self.last_tick_time = time.time()

            await asyncio.sleep(0.05)

    async def monitor_signals(self):

        while True:

            streams = self.redis.read(self.signal_stream, self.last_signal_id)

            for stream, entries in streams:

                for msg_id, payload in entries:

                    self.last_signal_id = msg_id
                    
                    try:
                        raw = payload.get("data")
                        if not raw: continue
                        
                        sig = json.loads(raw)
                        
                        # Only count actionable, high-probability signals for the UI to prevent pulse inflation
                        score = float(sig.get("score", 0))
                        confidence = float(sig.get("confidence", 0))
                        is_actionable = sig.get("signal") in ["BUY", "SELL"]
                        
                        if is_actionable and (score > 60 or confidence > 60):
                            self.signal_count += 1
                    except Exception:
                        pass

            await asyncio.sleep(0.05)

    async def monitor_trades(self):

        while True:

            streams = self.redis.read(self.trade_stream, self.last_trade_id)

            for stream, entries in streams:

                for msg_id, payload in entries:

                    self.last_trade_id = msg_id

                    trade = json.loads(payload.get("data", "{}"))
                    
                    from datetime import datetime, timedelta, timezone
                    # entry_time is stored in IST, so compare with IST date
                    ist = timezone(timedelta(hours=5, minutes=30))
                    today_ist = datetime.now(ist).strftime("%Y-%m-%d")
                    entry_time = trade.get("entry_time", "")
                    if not entry_time.startswith(today_ist):
                        continue
                    
                    pnl = trade.get("pnl", 0)
                    mode = trade.get("mode", "PAPER")

                    self.trades += 1
                    self.pnl += pnl

                    if mode == "LIVE":
                        self.live_trades += 1
                        self.live_pnl += pnl
                        if pnl > 0: self.live_wins += 1
                        else: self.live_losses += 1
                    else:
                        self.paper_trades += 1
                        self.paper_pnl += pnl
                        if pnl > 0: self.paper_wins += 1
                        else: self.paper_losses += 1

            await asyncio.sleep(0.05)

    async def monitor_risk(self):

        while True:

            streams = self.redis.read(self.risk_stream, self.last_risk_id)

            for stream, entries in streams:

                for msg_id, payload in entries:

                    self.last_risk_id = msg_id

                    state = json.loads(payload["data"])
                    self.trading_enabled = state.get("enabled", True)
                    self.market_open = state.get("market_open", True)
                    
                    self.live_pnl = state.get("live_pnl", self.live_pnl)
                    self.paper_pnl = state.get("paper_pnl", self.paper_pnl)
                    self.live_trades = state.get("live_trades", self.live_trades)
                    self.paper_trades = state.get("paper_trades", self.paper_trades)
                    self.current_capital = state.get("current_capital", self.current_capital)
                    self.base_capital = state.get("base_capital", self.base_capital)

            await asyncio.sleep(0.1)

    async def monitor_positions(self):
        while True:
            try:
                streams = self.redis.read(self.positions_stream, self.last_positions_id)
                for stream, entries in streams:
                    for msg_id, payload in entries:
                        self.last_positions_id = msg_id
                        data = json.loads(payload.get("data", "{}"))
                        self.open_positions = data.get("positions", [])
            except Exception:
                pass
            await asyncio.sleep(1)

    def render_dashboard(self):

        tick_rate = self.tick_count
        self.tick_count = 0

        live_pnl_color = Fore.GREEN if self.live_pnl >= 0 else Fore.RED
        paper_pnl_color = Fore.GREEN if self.paper_pnl >= 0 else Fore.RED
        risk_color = Fore.GREEN if self.trading_enabled else Fore.RED
        market_status = Fore.GREEN + "OPEN" if self.market_open else Fore.RED + "CLOSED"

        print("\033c", end="")

        print(Fore.CYAN + "====================================================")
        print(Fore.CYAN + "           ALGO TRADING OS - LIVE MONITOR")
        print(Fore.CYAN + "====================================================")

        print(Fore.BLUE + "\nMarket Status")
        print(Fore.BLUE + "-------------")
        print("Market Session   :", market_status)
        print("Tick Rate        :", Fore.YELLOW + str(tick_rate), "ticks/sec")
        print("Last Tick Age    :", round(time.time() - self.last_tick_time, 2), "sec")

        print(Fore.YELLOW + "\nSignals")
        print(Fore.YELLOW + "-------")
        print("Alpha Signals    :", self.signal_count)

        # Open Positions
        print(Fore.MAGENTA + "\nOPEN POSITIONS (" + str(len(self.open_positions)) + ")")
        print(Fore.MAGENTA + "--------------------")
        if self.open_positions:
            for p in self.open_positions:
                sym = p.get('symbol', '?')
                side = p.get('side', '?')
                entry = p.get('entry_price', 0)
                mode = p.get('mode', 'PAPER')
                side_color = Fore.GREEN if side == 'BUY' else Fore.RED
                mode_tag = Fore.RED + '⚡' if mode == 'LIVE' else Fore.WHITE + '📝'
                print(f"  {mode_tag} {side_color}{sym:14s} {side:4s}{Fore.WHITE} @ ₹{entry:.2f}")
        else:
            print(Fore.WHITE + "  No open positions")

        print(Fore.GREEN + "\nCLOSED TRADES (LIVE)")
        print(Fore.GREEN + "--------------------")
        print("Closed Trades    :", self.live_trades)
        print("Wins/Losses      :", Fore.GREEN + str(self.live_wins), "/", Fore.RED + str(self.live_losses))
        print("Realized PnL     :", live_pnl_color + "₹" + str(round(self.live_pnl, 2)))

        print(Fore.WHITE + "\nCLOSED TRADES (PAPER)")
        print(Fore.WHITE + "---------------------")
        print("Closed Trades    :", self.paper_trades)
        print("Wins/Losses      :", Fore.GREEN + str(self.paper_wins), "/", Fore.RED + str(self.paper_losses))
        print("Realized PnL     :", paper_pnl_color + "₹" + str(round(self.paper_pnl, 2)))

        print(Fore.RED + "\nSystem Safety & Capital")
        print(Fore.RED + "-----------------------")
        print("Trading Enabled  :", risk_color + str(self.trading_enabled))
        
        cap_color = Fore.GREEN if self.current_capital >= self.base_capital else Fore.RED
        print("Active Capital   :", cap_color + "₹" + str(round(self.current_capital, 2)))

        print(Fore.CYAN + "\n====================================================")