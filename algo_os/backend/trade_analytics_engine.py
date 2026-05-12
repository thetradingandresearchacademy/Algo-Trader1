import redis
import json
from datetime import datetime


class TradeAnalyticsEngine:

    def __init__(self):

        self.redis = redis.Redis(host="127.0.0.1", port=6379)

        self.trades = []

    async def record_trade(self, trade):

        trade["timestamp"] = datetime.utcnow().isoformat()

        self.trades.append(trade)

        self.redis.xadd(
            "trade_results",
            {"data": json.dumps(trade)}
        )

    def compute_metrics(self):

        wins = 0
        losses = 0
        pnl = 0

        for t in self.trades:

            pnl += t["pnl"]

            if t["pnl"] > 0:
                wins += 1
            else:
                losses += 1

        total = wins + losses

        win_rate = wins / total if total else 0

        return {
            "total_trades": total,
            "win_rate": win_rate,
            "pnl": pnl
        }