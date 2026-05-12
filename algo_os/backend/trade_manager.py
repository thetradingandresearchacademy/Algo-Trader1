import asyncio
import json
import time
from backend.redis_client import RedisClient

class TradeManager:
    """
    Monitors active positions.
    Handles: SL, T1 (Move SL to Cost), T2 (Trail), and Time Exit.
    """
    def __init__(self):
        self.redis = RedisClient()
        self.positions = {}

    async def start(self):
        await self.redis.connect()
        print("Trade Manager Started")
        
        await asyncio.gather(
            self.listen_new_trades(),
            self.monitor_market()
        )

    async def listen_new_trades(self):
        last_id = "$"
        while True:
            results = await self.redis.read_stream({"option_trades": last_id}, block=1000)
            if not results: continue
            for _, messages in results:
                for msg_id, payload in messages:
                    last_id = msg_id
                    trade = json.loads(payload["data"])
                    opt_sym = trade["option_symbol"]
                    if opt_sym not in self.positions:
                        self.positions[opt_sym] = trade
                        print(f"📦 Position Opened: {opt_sym}")

    async def monitor_market(self):
        last_id = "$"
        while True:
            if not self.positions:
                await asyncio.sleep(1)
                continue

            results = await self.redis.read_stream({"option_ticks": last_id}, block=100)
            if not results: continue
            
            for _, messages in results:
                for msg_id, payload in messages:
                    last_id = msg_id
                    data = json.loads(payload["data"])
                    opt_sym = data["symbol"]
                    ltp = data["ltp"]
                    
                    if opt_sym in self.positions:
                        await self.check_exit_logic(opt_sym, ltp)

            # Time Exit check (3:10 PM)
            now = time.localtime()
            if now.tm_hour == 15 and now.tm_min >= 10:
                await self.exit_all("TIME_EXIT")

    async def check_exit_logic(self, opt_sym, ltp):
        p = self.positions[opt_sym]
        
        # 1. SL Hit
        if ltp <= p["sl"]:
            await self.close_position(opt_sym, ltp, "EXIT_SL")
            return

        # 2. T1 Hit -> Move SL to Cost
        if ltp >= p["t1"] and p["sl"] < p["entry"]:
            p["sl"] = p["entry"]
            print(f"🛡️ SL Moved to Cost: {opt_sym}")

        # 3. T2 Hit -> Trail SL (80% of current price)
        if ltp >= p["t2"]:
            new_sl = ltp * 0.8
            if new_sl > p["sl"]:
                p["sl"] = new_sl
                print(f"📈 Trailing SL Updated: {opt_sym} @ {new_sl}")

        # 4. T3 Hit -> Optional exit or hold for runner
        if ltp >= p["t3"]:
            await self.close_position(opt_sym, ltp, "EXIT_T3")

    async def close_position(self, opt_sym, ltp, reason):
        print(f"🔴 Closing {opt_sym} @ {ltp} | Reason: {reason}")
        # Send execution command to broker service
        await self.redis.publish("execution_commands", {
            "symbol": opt_sym,
            "action": "SELL",
            "reason": reason,
            "price": ltp
        })
        del self.positions[opt_sym]

    async def exit_all(self, reason):
        for opt_sym in list(self.positions.keys()):
            # We don't have LTP for all here easily, but we send market sell
            await self.close_position(opt_sym, 0, reason)
