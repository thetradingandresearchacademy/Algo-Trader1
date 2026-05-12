import redis
import json


class StrategyFeedbackEngine:

    def __init__(self):

        self.redis = redis.Redis(host="127.0.0.1", port=6379)

        with open("config/strategy_parameters.json") as f:
            self.params = json.load(f)

    async def start(self):

        print("Strategy Feedback Engine started")

        last_id = "0-0"

        while True:

            result = self.redis.xread(
                {"trade_results": last_id},
                block=1000
            )

            if not result:
                continue

            for stream, entries in result:

                for msg_id, payload in entries:

                    last_id = msg_id

                    trade = json.loads(payload[b"data"])

    def update_strategy(self, trade):
        pnl = trade.get("pnl", 0)
        strategy = trade.get("features", {}).get("strategy", "UNKNOWN")

        if pnl < 0:
            # tighten thresholds if losing
            if "EXPLOSIVE_BREAKOUT" in strategy:
                self.params["rvol_spike_threshold"] = self.params.get("rvol_spike_threshold", 100000) + 5000
            elif "GAMMA" in strategy:
                self.params["gamma_time_trigger"] = self.params.get("gamma_time_trigger", 13.5) + 0.1 # wait later
            elif "TRAP" in strategy:
                self.params["trap_drop_percent"] = self.params.get("trap_drop_percent", 0.98) - 0.005 # drop more
        else:
            # loosen thresholds if winning
            if "EXPLOSIVE_BREAKOUT" in strategy:
                self.params["rvol_spike_threshold"] = max(50000, self.params.get("rvol_spike_threshold", 100000) - 2000)

        self.save()

    def save(self):

        with open("config/strategy_parameters.json", "w") as f:

            json.dump(self.params, f, indent=2)