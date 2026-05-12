import asyncio
import json

from services.redis_stream import RedisStream


class PositionIntelligenceEngine:

    def __init__(self):

        self.redis = RedisStream()

        self.positions = {}

        self.price_stream = "micro_ticks"
        self.command_stream = "control_commands"

        self.last_price_id = "0-0"
        self.last_cmd_id = "0-0"

    async def start(self):

        print("Position Intelligence Engine started")

        asyncio.create_task(self.listen_commands())

        while True:

            streams = self.redis.read(self.price_stream, self.last_price_id)

            for stream, entries in streams:

                for msg_id, payload in entries:

                    self.last_price_id = msg_id

                    tick = json.loads(payload["data"])

                    self.update_positions(tick)

            await asyncio.sleep(0.05)

    async def listen_commands(self):

        while True:

            streams = self.redis.read(self.command_stream, self.last_cmd_id)

            for stream, entries in streams:

                for msg_id, payload in entries:

                    self.last_cmd_id = msg_id

                    cmd = json.loads(payload["data"])

                    self.handle_command(cmd)

            await asyncio.sleep(0.05)

    def handle_command(self, cmd):

        symbol = cmd.get("symbol")

        if cmd["type"] == "SET_TARGET":

            if symbol in self.positions:
                self.positions[symbol]["target"] = cmd["target"]

        elif cmd["type"] == "SET_SL":

            if symbol in self.positions:
                self.positions[symbol]["stop"] = cmd["stop"]

        elif cmd["type"] == "TRAILING_SL":

            if symbol in self.positions:
                self.positions[symbol]["trailing"] = cmd["points"]

        elif cmd["type"] == "CLOSE":

            if symbol in self.positions:

                trade = self.positions[symbol]

                asyncio.create_task(
                    self.redis.publish("trade_results", trade)
                )

                del self.positions[symbol]

    def update_positions(self, tick):

        symbol = tick["symbol"]

        if symbol not in self.positions:
            return

        price = tick["price"]

        trade = self.positions[symbol]

        # AUTO TRAILING SL
        if trade.get("trailing"):

            trail = trade["trailing"]

            if trade["side"] == "BUY":

                new_sl = price - trail

                if new_sl > trade["stop"]:
                    trade["stop"] = new_sl

            else:

                new_sl = price + trail

                if new_sl < trade["stop"]:
                    trade["stop"] = new_sl

        # TARGET HIT
        if trade["side"] == "BUY" and price >= trade["target"]:

            pnl = trade["target"] - trade["entry"]

            trade["pnl"] = pnl

            asyncio.create_task(
                self.redis.publish("trade_results", trade)
            )

            del self.positions[symbol]

        elif trade["side"] == "SELL" and price <= trade["target"]:

            pnl = trade["entry"] - trade["target"]

            trade["pnl"] = pnl

            asyncio.create_task(
                self.redis.publish("trade_results", trade)
            )

            del self.positions[symbol]

        # STOP LOSS

        elif trade["side"] == "BUY" and price <= trade["stop"]:

            pnl = trade["stop"] - trade["entry"]

            trade["pnl"] = pnl

            asyncio.create_task(
                self.redis.publish("trade_results", trade)
            )

            del self.positions[symbol]

        elif trade["side"] == "SELL" and price >= trade["stop"]:

            pnl = trade["entry"] - trade["stop"]

            trade["pnl"] = pnl

            asyncio.create_task(
                self.redis.publish("trade_results", trade)
            )

            del self.positions[symbol]