import asyncio
import json
import logging
from datetime import datetime

from services.redis_stream import RedisStream

class AlphaOptionStrikeEngine:
    def __init__(self):
        self.redis = RedisStream()
        self.input_stream = "validated_signals"
        self.output_stream = "portfolio_orders"
        self.last_id = self.redis.get_latest_id(self.input_stream)
        
    async def start(self):
        print("Alpha Option Strike Engine started")
        while True:
            try:
                streams = self.redis.read(self.input_stream, self.last_id)
                if not streams:
                    await asyncio.sleep(0.05)
                    continue
                    
                for stream, entries in streams:
                    for msg_id, payload in entries:
                        self.last_id = msg_id
                        raw = payload.get("data")
                        if not raw: continue
                        signal = json.loads(raw)
                        await self.process_signal(signal)
            except Exception as e:
                print(f"AlphaOptionStrikeEngine Error: {e}")
                await asyncio.sleep(1)
                
    async def process_signal(self, signal):
        symbol = signal.get("symbol", "")
        price = signal.get("price", 0)
        
        # Options Mapping Logic
        strike_step = 100
        if "NIFTY" in symbol and "BANK" not in symbol:
            strike_step = 50
        elif "FINNIFTY" in symbol:
            strike_step = 50
        elif "SENSEX" in symbol:
            strike_step = 100
            
        is_index = symbol in ["NIFTY", "BANKNIFTY", "SENSEX", "BANKEX", "FINNIFTY"]
        
        if is_index and price > 0:
            # Calculate ATM Strike
            atm_strike = int(round(price / strike_step) * strike_step)
            
            # --- Sanity Check (New) ---
            # Prevent mapping to crazy strikes if price data is glitchy
            if abs(atm_strike - price) > 500:
                print(f"⚠️ STRIKE SANITY FAILED | {symbol} price {price} -> ATM {atm_strike} (too far)")
                return

            opt_type = "CE" if signal.get("signal") == "BUY" else "PE"
            
            # Construct standard symbol (Execution/MarketData engine will resolve the specific token)
            # e.g. BANKNIFTY_47500_CE
            option_symbol = f"{symbol}_{atm_strike}_{opt_type}"
            
            print(f"🎯 OPTION MAPPED | Index: {symbol} @ {price} -> Strike: {atm_strike} {opt_type}")
            
            # Modify signal for option execution
            signal["original_symbol"] = symbol
            signal["symbol"] = option_symbol
            signal["is_options"] = True
            signal["strike"] = atm_strike
            signal["opt_type"] = opt_type
            
        await self.redis.publish(self.output_stream, signal)
