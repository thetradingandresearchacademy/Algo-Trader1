import sys
import os

# Add project root to sys.path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import psycopg2
import redis
from config.settings import POSTGRES_DB_RAW, POSTGRES_PASSWORD_RAW

def setup_and_clear_system():
    try:
        conn = psycopg2.connect(host="localhost", database=POSTGRES_DB_RAW, user="postgres", password=POSTGRES_PASSWORD_RAW)
        conn.autocommit = True
        cur = conn.cursor()
        
        # 1. Create missing table
        cur.execute("""
        CREATE TABLE IF NOT EXISTS algo_trades (
            id SERIAL PRIMARY KEY,
            strategy_id VARCHAR(50),
            instrument_id VARCHAR(50),
            direction VARCHAR(10),
            entry_price FLOAT,
            exit_price FLOAT,
            net_pnl FLOAT,
            exit_reason VARCHAR(50),
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        """)
        print("Created algo_trades table if it did not exist.")
        
        # 2. Clear tables
        tables_to_clear = ['algo_trades', 'daily_stats', 'weekly_stats', 'monthly_stats']
        for table in tables_to_clear:
            try:
                cur.execute(f"TRUNCATE TABLE {table};")
                print(f"Cleared {table}")
            except Exception as e:
                print(f"Failed to clear {table}: {e}")
                
        conn.close()
        print("PostgreSQL setup and cleanup finished.")
    except Exception as e:
        print(f"DB Error: {e}")

    try:
        r = redis.Redis(host='localhost', port=6379, decode_responses=True)
        r.delete("trade_results")
        r.delete("trade_stats")
        r.delete("active_positions")
        print("Redis streams cleaned.")
    except Exception as e:
        print(f"Redis Error: {e}")

if __name__ == "__main__":
    setup_and_clear_system()
