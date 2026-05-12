
import psycopg2
from datetime import datetime

conn_params = {
    "host": "127.0.0.1",
    "database": "astro_quant",
    "user": "postgres",
    "password": "tara123"
}

today = datetime.now().strftime('%Y-%m-%d')

data = {
    "date": today,
    "primary_window": "09:15-11:30",
    "avoid_window": "13:30-14:30",
    "confidence": 10
}

try:
    conn = psycopg2.connect(**conn_params)
    cur = conn.cursor()
    
    # Upsert data
    cur.execute("""
        INSERT INTO daily_timing_reference (date, primary_window, avoid_window, confidence)
        VALUES (%s, %s, %s, %s)
        ON CONFLICT (date) DO UPDATE 
        SET primary_window = EXCLUDED.primary_window,
            avoid_window = EXCLUDED.avoid_window,
            confidence = EXCLUDED.confidence;
    """, (data["date"], data["primary_window"], data["avoid_window"], data["confidence"]))
    
    conn.commit()
    print(f"Successfully populated Astro data for {today}")
    conn.close()
except Exception as e:
    print(f"Error: {e}")
