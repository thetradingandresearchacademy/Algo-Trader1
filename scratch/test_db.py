
import psycopg2
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("AstroTest")

conn_params = {
    "host": "127.0.0.1",
    "database": "astro_quant",
    "user": "postgres",
    "password": "tara123"
}

try:
    conn = psycopg2.connect(**conn_params)
    logger.info("Connection successful!")
    cur = conn.cursor()
    cur.execute("SELECT version();")
    logger.info(f"DB Version: {cur.fetchone()}")
    
    # Check if table exists
    cur.execute("SELECT EXISTS (SELECT FROM information_schema.tables WHERE table_name = 'daily_timing_reference');")
    exists = cur.fetchone()[0]
    logger.info(f"Table 'daily_timing_reference' exists: {exists}")
    
    if not exists:
        logger.info("Creating table...")
        cur.execute("""
            CREATE TABLE daily_timing_reference (
                date DATE PRIMARY KEY,
                primary_window VARCHAR(50),
                avoid_window VARCHAR(50),
                confidence INTEGER
            );
        """)
        conn.commit()
        logger.info("Table created.")
    
    conn.close()
except Exception as e:
    logger.error(f"Error: {e}")
