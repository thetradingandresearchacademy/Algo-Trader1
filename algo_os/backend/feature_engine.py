import asyncio
import csv
import json
from collections import defaultdict

from services.redis_stream import RedisStream


class FeatureImportanceEngine:

    def __init__(self):

        self.redis = RedisStream()

        self.input_stream = "trade_results"
        self.last_id = "0-0"

        self.journal_file = "trade_journal.csv"
        self.weight_file = "config/feature_weights.json"

    async def start(self):

        print("Feature Importance Engine started")

        while True:

            streams = self.redis.read(self.input_stream, self.last_id)

            if not streams:
                await asyncio.sleep(5)
                continue

            for stream, entries in streams:

                for msg_id, payload in entries:

                    self.last_id = msg_id

                    # recompute weights after every new trade
                    weights = self.compute_importance()

                    self.save_weights(weights)

                    print("Feature weights updated:", weights)

            await asyncio.sleep(1)

    def compute_importance(self):

        stats = defaultdict(lambda: {"wins": 0, "trades": 0})

        try:

            with open(self.journal_file) as f:

                reader = csv.DictReader(f)

                for row in reader:

                    pnl = float(row["pnl"])

                    for feature in [
                        "footprint",
                        "vacuum",
                        "sweep",
                        "accumulation",
                        "volatility",
                        "iceberg"
                    ]:

                        if row[feature] == "True":

                            stats[feature]["trades"] += 1

                            if pnl > 0:
                                stats[feature]["wins"] += 1

        except Exception as e:

            print("Feature importance read error:", e)

            return {}

        weights = {}

        for feature, data in stats.items():

            if data["trades"] == 0:
                weights[feature] = 0
                continue

            win_rate = data["wins"] / data["trades"]

            weights[feature] = round(win_rate, 3)

        # normalize weights

        total = sum(weights.values())

        if total > 0:

            for k in weights:
                weights[k] = round(weights[k] / total, 3)

        return weights

    def save_weights(self, weights):

        try:

            with open(self.weight_file, "w") as f:

                json.dump(weights, f, indent=2)

        except Exception as e:

            print("Weight save error:", e)