import json
import asyncio
import csv
import os
from pathlib import Path
import asyncpg

from config import settings as sys_config
from services.redis_stream import RedisStream

class TradeJournalEngine:

    def __init__(self, db_dsn: str = None):

        self.redis = RedisStream()

        self.input_stream = "trade_results"
        self.output_stream = "trade_stats"

        self.last_id = "$"

        # project root
        _project_root = Path(__file__).parent.parent.parent

        # Local CSV Backup
        self.file = str(_project_root / "data" / "trade_log.csv")
        
        # Database Connection String
        self.db_dsn = db_dsn or sys_config.POSTGRES_DSN
        self.pool = None

        self.stats = {
            "trades": 0,
            "wins": 0,
            "losses": 0,
            "pnl": 0
        }

        self.ensure_file()

    # ---------------------------------------------------------
    # FILE & DB INITIALIZATION
    # ---------------------------------------------------------

    def ensure_file(self):
        file_path = Path(self.file)
        file_path.parent.mkdir(parents=True, exist_ok=True)

        if not file_path.exists():
            with open(self.file, "w", newline="") as f:
                writer = csv.writer(f)
                writer.writerow([
                    "entry_time", "exit_time", "symbol", "side",
                    "entry_price", "exit_price", "pnl", "qty",
                    "strategy", "features"
                ])

    async def init_db(self):
        try:
            self.pool = await asyncpg.create_pool(self.db_dsn)
            print("✅ TradeJournalEngine: Connected to PostgreSQL")
            
            # Ensure schema consistency (Add id, qty, created_at columns if missing)
            async with self.pool.acquire() as conn:
                await conn.execute("""
                    ALTER TABLE algo_trades ADD COLUMN IF NOT EXISTS id SERIAL;
                    ALTER TABLE algo_trades ADD COLUMN IF NOT EXISTS qty INTEGER DEFAULT 0;
                    ALTER TABLE algo_trades ADD COLUMN IF NOT EXISTS created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP;
                """)
                print("✅ TradeJournalEngine: Schema verified (id, qty, created_at ensured)")
        except Exception as e:
            print(f"⚠️ Warning: Could not connect to PostgreSQL ({e}). Operating in CSV-only mode.")

    # ---------------------------------------------------------
    # ENGINE START
    # ---------------------------------------------------------

    async def start(self):
        print("Trade Journal Engine started")
        await self.init_db()

        # Use today's start to catch trades from today (not hardcoded backfill)
        self.last_id = self.redis.get_today_id()
        print(f"📒 TradeJournalEngine: Reading trades from today (cursor: {self.last_id})")
        
        # Reset stats
        self.stats = {"trades": 0, "pnl": 0, "wins": 0, "losses": 0}

        while True:
            try:
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
                        
                        # Await the recording process since it now handles DB I/O
                        await self.record_trade(trade)

            except Exception as e:
                print("TradeJournalEngine error:", e)
                await asyncio.sleep(1)

    # ---------------------------------------------------------
    # TRADE RECORDING (CSV + POSTGRES)
    # ---------------------------------------------------------

    async def record_trade(self, trade):
        features = trade.get("features", {})
        
        # Decide strategy label based on footprint/vwap attributes
        strategy = "Index Options" if "footprint" in features else "Stock VWAP"

        # 1. Write to local CSV
        row = [
            trade.get("entry_time"),
            trade.get("exit_time"),
            trade.get("symbol"),
            trade.get("side"),
            trade.get("entry_price"),
            trade.get("exit_price"),
            trade.get("pnl"),
            trade.get("qty", 0),
            strategy,
            json.dumps(features)
        ]

        with open(self.file, "a", newline="") as f:
            writer = csv.writer(f)
            writer.writerow(row)

        # 2. Write to PostgreSQL (if connected)
        if self.pool:
            try:
                async with self.pool.acquire() as connection:
                    # Prevent duplicates across all time for replays
                    exists = await connection.fetchval("""
                        SELECT 1 FROM algo_trades 
                        WHERE instrument_id = $1 AND entry_price = $2 AND direction = $3 AND net_pnl = $4
                        LIMIT 1
                    """, trade.get("symbol"), trade.get("entry_price"), trade.get("side"), trade.get("pnl", 0.0))
                    
                    if not exists:
                        query = """
                            INSERT INTO algo_trades 
                            (strategy_id, instrument_id, direction, entry_price, exit_price, net_pnl, exit_reason, qty)
                            VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
                        """
                        await connection.execute(
                            query,
                            strategy,
                            trade.get("symbol"),
                            trade.get("side"),
                            trade.get("entry_price"),
                            trade.get("exit_price", 0.0),
                            trade.get("pnl", 0.0),
                        trade.get("exit_reason", "SYSTEM"),
                        trade.get("qty", 0)
                    )
            except Exception as e:
                print(f"DB Insert Error: {e}")

        self.update_stats(trade)

    # ---------------------------------------------------------
    # PERFORMANCE METRICS
    # ---------------------------------------------------------

    def update_stats(self, trade):
        pnl = trade.get("pnl", 0)

        self.stats["trades"] += 1
        self.stats["pnl"] += pnl

        if pnl > 0:
            self.stats["wins"] += 1
        else:
            self.stats["losses"] += 1

        trades = self.stats["trades"]
        win_rate = 0 if trades == 0 else self.stats["wins"] / trades

        dashboard = {
            "trades": trades,
            "wins": self.stats["wins"],
            "losses": self.stats["losses"],
            "pnl": self.stats["pnl"],
            "win_rate": round(win_rate, 3)
        }

        asyncio.create_task(
            self.redis.publish(self.output_stream, dashboard)
        )

        print(
            f"JOURNAL | trades={dashboard['trades']} "
            f"pnl={dashboard['pnl']} "
            f"winrate={dashboard['win_rate']}"
        )