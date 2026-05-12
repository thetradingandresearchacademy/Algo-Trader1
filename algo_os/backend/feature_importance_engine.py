import redis
import json
from collections import defaultdict


class FeatureImportanceEngine:

    def __init__(self):

        self.redis = redis.Redis(host="127.0.0.1", port=6379)

        self.feature_stats = defaultdict(lambda: {"wins": 0, "losses": 0})

        with open("config/feature_weights.json") as f:
            self.weights = json.load(f)

    async def start(self):

        print("Feature Importance Engine started")

        last_id = "0-0"

        while True:

            results = self.redis.xread(
                {"trade_results": last_id},
                block=1000
            )

            if not results:
                continue

            for stream, entries in results:

                for msg_id, payload in entries:

                    last_id = msg_id

                    trade = json.loads(payload[b"data"])

                    self.update_stats(trade)

    def update_stats(self, trade):

        features = trade.get("features", {})

        pnl = trade.get("pnl", 0)

        for feature, value in features.items():

            if not value:
                continue

            if pnl > 0:
                self.feature_stats[feature]["wins"] += 1
            else:
                self.feature_stats[feature]["losses"] += 1

        self.recalculate_weights()

    def recalculate_weights(self):

        scores = {}

        for feature, stats in self.feature_stats.items():

            wins = stats["wins"]
            losses = stats["losses"]

            total = wins + losses

            if total == 0:
                continue

            win_rate = wins / total

            scores[feature] = win_rate

        total_score = sum(scores.values())

        if total_score == 0:
            return

        for f in scores:

            self.weights[f] = scores[f] / total_score

        self.save()

    def save(self):

        with open("config/feature_weights.json", "w") as f:

            json.dump(self.weights, f, indent=2)