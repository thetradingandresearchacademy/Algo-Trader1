import redis
from algo_os.config.settings import REDIS_HOST, REDIS_PORT

print(f"Testing Redis connection to {REDIS_HOST}:{REDIS_PORT}...")
try:
    client = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, decode_responses=True, socket_connect_timeout=5)
    client.ping()
    print("✅ Redis connection successful!")
except Exception as e:
    print(f"❌ Redis connection failed: {e}")
