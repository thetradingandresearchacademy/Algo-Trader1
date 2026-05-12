import asyncio
import os
import pandas as pd
from datetime import datetime
from pathlib import Path

from config import settings as sys_config
import asyncpg

class ReportEngine:
    """
    Automated reporting engine for the SaaS dashboard.
    Aggregates data from 'algo_trades' table and populates stats tables.
    """

    def __init__(self):
        self.db_dsn = sys_config.POSTGRES_DSN
        self.pool = None
        self.poll_interval = 15  # Poll every 15 seconds for near real-time UI

    async def start(self):
        print("Report Engine started")
        try:
            self.pool = await asyncpg.create_pool(self.db_dsn)
            async with self.pool.acquire() as conn:
                # Ensure Stats Tables Exist
                await conn.execute("""
                    CREATE TABLE IF NOT EXISTS daily_stats (
                        report_date DATE PRIMARY KEY,
                        total_trades INTEGER,
                        wins INTEGER,
                        losses INTEGER,
                        total_pnl NUMERIC,
                        win_rate NUMERIC
                    );
                    CREATE TABLE IF NOT EXISTS weekly_stats (
                        year INTEGER,
                        week INTEGER,
                        total_trades INTEGER,
                        total_pnl NUMERIC,
                        win_rate NUMERIC,
                        PRIMARY KEY (year, week)
                    );
                    CREATE TABLE IF NOT EXISTS monthly_stats (
                        year INTEGER,
                        month INTEGER,
                        total_trades INTEGER,
                        total_pnl NUMERIC,
                        win_rate NUMERIC,
                        PRIMARY KEY (year, month)
                    );
                """)
                print("✅ ReportEngine: Stats tables verified")
        except Exception as e:
            print(f"ReportEngine DB Error: {e}")
            return

        while True:
            try:
                await self.generate_reports()
                print(f"✅ Reports updated at {datetime.now()}")
            except Exception as e:
                print("ReportEngine execution error:", e)
            
            await asyncio.sleep(self.poll_interval)

    async def generate_reports(self):
        async with self.pool.acquire() as conn:
            # 1. Daily Reports (Calculate from IST day)
            await conn.execute("""
                INSERT INTO daily_stats (report_date, total_trades, wins, losses, total_pnl, win_rate)
                SELECT 
                    (created_at AT TIME ZONE 'Asia/Kolkata')::DATE as report_date,
                    count(*),
                    count(*) FILTER (WHERE net_pnl > 0),
                    count(*) FILTER (WHERE net_pnl <= 0),
                    sum(net_pnl),
                    round(count(*) FILTER (WHERE net_pnl > 0)::numeric / NULLIF(count(*), 0)::numeric * 100, 2)
                FROM algo_trades
                GROUP BY 1
                ON CONFLICT (report_date) DO UPDATE SET
                    total_trades = EXCLUDED.total_trades,
                    wins = EXCLUDED.wins,
                    losses = EXCLUDED.losses,
                    total_pnl = EXCLUDED.total_pnl,
                    win_rate = EXCLUDED.win_rate
            """)

            # 2. Weekly Reports
            await conn.execute("""
                INSERT INTO weekly_stats (year, week, total_trades, total_pnl, win_rate)
                SELECT 
                    extract(isoyear from (created_at AT TIME ZONE 'Asia/Kolkata'))::integer as year,
                    extract(week from (created_at AT TIME ZONE 'Asia/Kolkata'))::integer as week,
                    count(*),
                    sum(net_pnl),
                    round(count(*) FILTER (WHERE net_pnl > 0)::numeric / NULLIF(count(*), 0)::numeric * 100, 2)
                FROM algo_trades
                GROUP BY 1, 2
                ON CONFLICT (year, week) DO UPDATE SET
                    total_trades = EXCLUDED.total_trades,
                    total_pnl = EXCLUDED.total_pnl,
                    win_rate = EXCLUDED.win_rate
            """)

            # 3. Monthly Reports
            await conn.execute("""
                INSERT INTO monthly_stats (year, month, total_trades, total_pnl, win_rate)
                SELECT 
                    extract(year from (created_at AT TIME ZONE 'Asia/Kolkata'))::integer as year,
                    extract(month from (created_at AT TIME ZONE 'Asia/Kolkata'))::integer as month,
                    count(*),
                    sum(net_pnl),
                    round(count(*) FILTER (WHERE net_pnl > 0)::numeric / NULLIF(count(*), 0)::numeric * 100, 2)
                FROM algo_trades
                GROUP BY 1, 2
                ON CONFLICT (year, month) DO UPDATE SET
                    total_trades = EXCLUDED.total_trades,
                    total_pnl = EXCLUDED.total_pnl,
                    win_rate = EXCLUDED.win_rate
            """)

    def _generate_daily(self, df):
        # Group by date
        daily = df.groupby('date').agg(
            total_trades=('pnl', 'count'),
            total_pnl=('pnl', 'sum'),
            win_rate=('pnl', lambda x: round((x > 0).mean() * 100, 2))
        ).reset_index()
        
        out_path = self.reports_dir / "daily_report.csv"
        daily.to_csv(out_path, index=False)

    def _generate_weekly(self, df):
        # Group by year and week
        weekly = df.groupby(['year', 'week']).agg(
            total_trades=('pnl', 'count'),
            total_pnl=('pnl', 'sum'),
            win_rate=('pnl', lambda x: round((x > 0).mean() * 100, 2))
        ).reset_index()
        
        out_path = self.reports_dir / "weekly_report.csv"
        weekly.to_csv(out_path, index=False)

    def _generate_monthly(self, df):
        # Group by year and month
        monthly = df.groupby(['year', 'month']).agg(
            total_trades=('pnl', 'count'),
            total_pnl=('pnl', 'sum'),
            win_rate=('pnl', lambda x: round((x > 0).mean() * 100, 2))
        ).reset_index()
        
        out_path = self.reports_dir / "monthly_report.csv"
        monthly.to_csv(out_path, index=False)
