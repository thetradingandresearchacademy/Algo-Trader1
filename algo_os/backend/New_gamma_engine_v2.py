import time

class GammaEngineV2:

    def __init__(self):
        self.state = {}

    def process(self, tick, index_data):

        symbol = tick["symbol"]
        price = tick["price"]

        if symbol not in ["NIFTY", "BANKNIFTY"]:
            return None

        # --- INDEX FILTER ---
        if index_data["strength"] < 65:
            return None

        step = 50 if symbol == "NIFTY" else 100
        strike = round(price / step) * step

        s = self.state.setdefault(symbol, {
            "last_price": price,
            "last_velocity": 0,
            "strike": strike,
            "at_strike": False,
            "left_strike": False,
            "last_signal_time": 0
        })

        # --- CALCULATIONS ---
        velocity = price - s["last_price"]
        acceleration = velocity - s["last_velocity"]

        at_strike = abs(price - strike) < (step * 0.1)

        # --- STATE UPDATE ---
        if at_strike:
            s["at_strike"] = True
            s["strike"] = strike

        if s["at_strike"] and abs(price - s["strike"]) > step * 0.2:
            s["left_strike"] = True

        # --- GAMMA TRIGGER ---
        trigger = None
        direction = None

        if s["left_strike"] and abs(velocity) > (step * 0.05) and acceleration > 0:

            if velocity > 0:
                direction = "CALL"
            else:
                direction = "PUT"

            trigger = "GAMMA_EXPLOSION"

        # --- COOLDOWN ---
        now = time.time()
        if now - s["last_signal_time"] < 120:
            return None

        # --- FINAL SIGNAL ---
        if trigger:
            s["last_signal_time"] = now

            return {
                "symbol": symbol,
                "signal": f"{direction}_BUY",
                "trigger": trigger,
                "price": price,
                "velocity": velocity,
                "acceleration": acceleration,
                "timestamp": now
            }

        # --- UPDATE STATE ---
        s["last_price"] = price
        s["last_velocity"] = velocity

        return None


=========================================================
if trend_regime == True:
    velocity_threshold *= 0.8