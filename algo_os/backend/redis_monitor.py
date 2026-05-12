import redis
import json
import time
import socket


class RedisStreamEngine:

    def __init__(self):
        self.host = "127.0.0.1"
        self.port = 6379
        self.client = None
        self.connect()

    # ---------- CONNECTION MANAGER ----------
    def connect(self):

        while True:
            try:
                print("🔄 Connecting to Redis...")

                self.client = redis.Redis(
                    host=self.host,
                    port=self.port,
                    db=0,
                    decode_responses=True,
                    socket_connect_timeout=5,
                    socket_timeout=5,
                    retry_on_timeout=True,
                    health_check_interval=10
                )

                self.client.ping()

                print("✅ Redis Connected Successfully\n")
                return

            except (redis.ConnectionError, socket.error) as e:
                print(f"❌ Redis not ready: {e}")
                print("⏳ Retrying in 2 sec...\n")
                time.sleep(2)

    # ---------- SAFE READ ----------
    def safe_xread(self, streams, last_ids):

        while True:
            try:
                return self.client.xread(last_ids, block=1000)

            except (redis.ConnectionError, socket.error):
                print("⚠️ Redis connection lost. Reconnecting...")
                self.connect()

            except Exception as e:
                print("⚠️ Unexpected Redis error:", e)
                time.sleep(1)


# ---------- PARSER ----------
def parse(data):

    raw = data.get("data")

    if raw:
        try:
            return json.loads(raw)
        except:
            return {}

    return {}


# ---------- MAIN MONITOR ----------
def start_monitor():

    redis_engine = RedisStreamEngine()

    streams = [
        "market_ticks",
        "micro_ticks",
        "order_flow_features",
        "iceberg_signals",
        "footprint_scores",
        "liquidity_vacuum",
        "liquidity_sweeps",
        "accumulation_signals",
        "volatility_expansion",
        "alpha_signals"
    ]

    last_ids = {s: "$" for s in streams}

    print("\n🚀 Redis Stream Monitor Started\n")

    while True:

        result = redis_engine.safe_xread(streams, last_ids)

        if not result:
            continue

        for stream, entries in result:

            for msg_id, payload in entries:

                last_ids[stream] = msg_id

                data = parse(payload)

                if not data:
                    continue

                symbol = data.get("symbol", "")

                # ---------- STREAM HANDLERS ----------
                if stream == "market_ticks":
                    print(f"MARKET   | {symbol} {data.get('price', data.get('ltp'))}")

                elif stream == "micro_ticks":
                    print(f"MICROBAR | {symbol}")

                elif stream == "order_flow_features":
                    print(
                        f"ORDERFLOW| {symbol} "
                        f"imbalance={round(data.get('bid_ask_imbalance', 0), 3)}"
                    )

                elif stream == "iceberg_signals":
                    print(f"ICEBERG  | {symbol} {data.get('signal')}")

                elif stream == "footprint_scores":
                    print(
                        f"FOOTPRINT| {symbol} "
                        f"score={round(data.get('footprint_score', 0), 2)}"
                    )

                elif stream == "liquidity_vacuum":
                    print(f"VACUUM   | {symbol}")

                elif stream == "liquidity_sweeps":
                    print(f"SWEEP    | {symbol} {data.get('signal')}")

                elif stream == "accumulation_signals":
                    print(f"ACCUM    | {symbol} {data.get('signal')}")

                elif stream == "volatility_expansion":
                    print(f"VOLATILE | {symbol}")

                elif stream == "alpha_signals":
                    print(f"ALPHA    | {symbol} {data.get('signal')}")


# ---------- ENTRY ----------
if __name__ == "__main__":
    start_monitor()