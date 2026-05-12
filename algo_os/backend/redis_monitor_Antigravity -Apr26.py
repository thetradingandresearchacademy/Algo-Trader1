import redis
import json
import time

client = redis.Redis(host="127.0.0.1", port=6379, decode_responses=True)

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


def parse(data):

    raw = data.get("data")

    if raw:
        return json.loads(raw)

    return {}


print("\nRedis Stream Monitor Started\n")

while True:

    try:

        result = client.xread(last_ids, block=1000)

        for stream, entries in result:

            stream = stream.decode() if isinstance(stream, bytes) else stream

            for msg_id, payload in entries:

                last_ids[stream] = msg_id

                data = parse(payload)

                if not data:
                    continue

                symbol = data.get("symbol", "")

                if stream == "market_ticks":

                    print(f"MARKET   | {symbol} {data.get('price', data.get('ltp'))}")

                elif stream == "micro_ticks":

                    print(f"MICROBAR | {symbol}")

                elif stream == "order_flow_features":

                    print(
                        f"ORDERFLOW| {symbol} "
                        f"imbalance={round(data.get('bid_ask_imbalance',0),3)}"
                    )

                elif stream == "iceberg_signals":

                    print(
                        f"ICEBERG  | {symbol} {data.get('signal')}"
                    )

                elif stream == "footprint_scores":

                    print(
                        f"FOOTPRINT| {symbol} "
                        f"score={round(data.get('footprint_score',0),2)}"
                    )

                elif stream == "liquidity_vacuum":

                    print(f"VACUUM   | {symbol}")

                elif stream == "liquidity_sweeps":

                    print(
                        f"SWEEP    | {symbol} {data.get('signal')}"
                    )

                elif stream == "accumulation_signals":

                    print(
                        f"ACCUM    | {symbol} {data.get('signal')}"
                    )

                elif stream == "volatility_expansion":

                    print(
                        f"VOLATILE | {symbol}"
                    )

                elif stream == "alpha_signals":

                    print(
                        f"ALPHA    | {symbol} {data.get('signal')}"
                    )

    except Exception as e:

        print("Monitor error:", e)

        time.sleep(1)