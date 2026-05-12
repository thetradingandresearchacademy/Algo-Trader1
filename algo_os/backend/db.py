import psycopg2
from psycopg2.extras import RealDictCursor
from config.settings import POSTGRES_DSN

def get_db_connection():
    try:
        conn = psycopg2.connect(POSTGRES_DSN)
        return conn
    except psycopg2.OperationalError as e:
        # Fallback to defaults or return None if db is missing
        print(f"DB Connection failed: {e}")
        return None

def init_db():
    conn = get_db_connection()
    if not conn:
        print("Skipping DB Init - No Connection")
        return
        
    try:
        with conn.cursor() as cur:
            cur.execute("""
                CREATE TABLE IF NOT EXISTS uploaded_designs (
                    id SERIAL PRIMARY KEY,
                    upload_ts TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    filename VARCHAR(255),
                    mode VARCHAR(50),
                    row_count INT,
                    col_count INT,
                    decision_action VARCHAR(50),
                    raw_json JSONB
                );
            """)
        conn.commit()
        print("Audit table initialized.")
    except Exception as e:
        print(f"DB Init error: {e}")
    finally:
        conn.close()

def log_audit(filename, mode, row_count, col_count, decision_action, raw_json=None):
    conn = get_db_connection()
    if not conn:
        return
        
    try:
        import json
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO uploaded_designs 
                (filename, mode, row_count, col_count, decision_action, raw_json)
                VALUES (%s, %s, %s, %s, %s, %s)
            """, (filename, mode, row_count, col_count, decision_action, json.dumps(raw_json) if raw_json else None))
        conn.commit()
    except Exception as e:
        print(f"Log audit error: {e}")
    finally:
        conn.close()

# Initialize DB when this module is loaded
init_db()
