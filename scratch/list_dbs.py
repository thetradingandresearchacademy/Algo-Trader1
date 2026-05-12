import psycopg2
try:
    conn = psycopg2.connect(host="localhost", user="postgres", password="tara123", database="postgres")
    cur = conn.cursor()
    cur.execute("SELECT datname FROM pg_database;")
    dbs = cur.fetchall()
    print("Databases found:")
    for db in dbs:
        print(f"- {db[0]}")
    conn.close()
except Exception as e:
    print(f"Error: {e}")
