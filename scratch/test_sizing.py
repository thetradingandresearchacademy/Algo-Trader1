
import asyncio
import logging
from saio_core.ai_supervisor import AISupervisor

logging.basicConfig(level=logging.INFO, format='%(message)s')

async def test_sizing():
    supervisor = AISupervisor(capital=100000)
    
    # Mock signals
    signals = [
        {
            "symbol": "RELIANCE",
            "side": "BUY",
            "vwap_aligned": True,
            "volume_spike": True,
            "type": "TREND"
        },
        {
            "symbol": "TCS",
            "side": "SELL",
            "vwap_aligned": True,
            "volume_spike": False,
            "type": "RANGE"
        }
    ]
    
    print("\n--- Testing TREND Regime (1.5x sizing) ---")
    results = supervisor.evaluate_signals(signals, "TREND")
    for res in results:
        print(f"Result: {res}")

    print("\n--- Testing RANGE Regime (0.7x sizing) ---")
    results = supervisor.evaluate_signals(signals, "RANGE")
    for res in results:
        print(f"Result: {res}")

if __name__ == "__main__":
    asyncio.run(test_sizing())
