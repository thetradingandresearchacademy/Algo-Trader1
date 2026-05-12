import json
import asyncio
from collections import deque, defaultdict

from services.redis_stream import RedisStream


class VolatilityExpansionEngine:

    def __init__(self):

        self.redis = RedisStream()

        self.input_stream = "micro_ticks"

        # MUST match AlphaEngine input
        self.output_stream = "volatility_expansion_signals"

        self.last_id = self.redis.get_latest_id(self.input_stream)

        # rolling price window
        self.windows = defaultdict(lambda: deque(maxlen=20))

    # ---------------------------------------------------------
    # ENGINE START
    # ---------------------------------------------------------

    async def start(self):

        print("Volatility Expansion Engine started")

        while True:

            try:

                messages = self.redis.read(self.input_stream, self.last_id)

                if not messages:
                    await asyncio.sleep(0.05)
                    continue

                for stream, entries in messages:

                    for msg_id, data in entries:

                        self.last_id = msg_id

                        raw = data.get("data") or data.get(b"data")

                        if raw is None:
                            continue

                        if isinstance(raw, bytes):
                            raw = raw.decode()

                        tick = json.loads(raw)

                        signal = self.detect_expansion(tick)

                        if signal:
                            # Pass through all tick data + expansion features
                            payload = {
                                **tick,
                                "compression_ratio": signal["compression_ratio"],
                                "price_range": signal["price_range"],
                                "source_engine": "volatility_expansion"
                            }
                            
                            # Silence logs to prevent terminal spam
                            # print(f"⚡ VOL EXPANSION | {tick['symbol']} | Range: {signal['price_range']:.2f}")

                            await self.redis.publish(
                                self.output_stream,
                                payload
                            )
                await asyncio.sleep(0.001)

            except Exception as e:

                print("VolatilityExpansionEngine error:", e)

                await asyncio.sleep(1)

    # ---------------------------------------------------------
    # EXPANSION DETECTION
    # ---------------------------------------------------------

    def detect_expansion(self, tick):

        try:

            symbol = tick.get("symbol")

            price = tick.get("price")

            if symbol is None or price is None:
                return None

            window = self.windows[symbol]

            window.append(price)

            if len(window) < 10:
                return None

            price_range = max(window) - min(window)

            compression_ratio = price_range / (price + 1e-9)

            # Dynamic threshold: 0.04% for indices (approx 10pts for Nifty), 0.15% for stocks
            is_index = symbol in ("NIFTY", "BANKNIFTY", "SENSEX")
            
            # Special handling for INDIAVIX price scaling (often sent in paise/basis points)
            if symbol == "INDIAVIX" and price > 1000:
                price /= 100.0
                
            threshold_pct = 0.0004 if is_index else 0.0015
            dynamic_threshold = max(0.2, price * threshold_pct)

            if price_range > dynamic_threshold:

                return {

                    "price_range": price_range,
                    "compression_ratio": compression_ratio

                }

            return None

        except Exception as e:

            print("Volatility detection error:", e)

            return None