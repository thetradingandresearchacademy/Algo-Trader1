import time

class OptionsExecutionEngine:

    def __init__(self):
        self.positions = {}

    def process_signal(self, signal, option_chain):

        symbol = signal["symbol"]
        direction = signal["signal"]
        price = signal["price"]
        velocity = signal.get("velocity", 0)

        step = 50 if symbol == "NIFTY" else 100
        atm = round(price / step) * step

        # --- STRIKE SELECTION ---
        if velocity > 10:
            strike = atm + step if "CALL" in direction else atm - step
        else:
            strike = atm

        option_symbol = f"{symbol}_{strike}_{'CE' if 'CALL' in direction else 'PE'}"

        premium = option_chain.get(option_symbol, {}).get("ltp")

        if not premium:
            return None

        # --- PREMIUM FILTER ---
        if premium < 10 or premium > 250:
            return None

        entry = premium

        # --- STOP LOSS ---
        if signal["trigger"] == "GAMMA_EXPLOSION":
            sl = entry * 0.6
        else:
            sl = entry * 0.7

        # --- TARGETS ---
        t1 = entry * 1.3
        t2 = entry * 1.6
        t3 = entry * 2.0

        trade = {
            "symbol": option_symbol,
            "entry": entry,
            "sl": sl,
            "t1": t1,
            "t2": t2,
            "t3": t3,
            "timestamp": time.time()
        }

        self.positions[option_symbol] = trade

        return trade