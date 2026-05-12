import asyncio
import redis
import json
import time
from datetime import datetime

r = redis.Redis(host='localhost', port=6379, decode_responses=True)

async def check_redis_streams():
    streams = ["market_ticks", "alpha_signals", "validated_signals", "stock_scanner", "risk_state"]
    print(f"{'Stream Name':<20} | {'Status':<15} | {'Last Activity'}")
    print("-" * 60)
    
    for stream in streams:
        try:
            info = r.xinfo_stream(stream)
            last_id = info['last-generated-id']
            last_ts = int(last_id.split('-')[0]) / 1000
            last_time = datetime.fromtimestamp(last_ts).strftime('%Y-%m-%d %H:%M:%S')
            
            # Check if updated in last 60 seconds
            status = "ACTIVE" if (time.time() - last_ts) < 60 else "STALLED"
            
            print(f"{stream:<20} | {status:<15} | {last_time}")
            
            # Print a sample if active
            if status == "ACTIVE":
                sample = r.xrevrange(stream, count=1)
                print(f"   Sample: {str(sample[0][1])[:100]}...")
        except redis.exceptions.ResponseError:
            print(f"{stream:<20} | EMPTY/MISSING")
        except Exception as e:
            print(f"{stream:<20} | ERROR: {e}")

if __name__ == "__main__":
    print(f"System Health Check - {datetime.now()}")
    asyncio.run(check_redis_streams())
