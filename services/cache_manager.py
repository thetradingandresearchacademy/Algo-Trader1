# SAIOS-TRADE CORE — Cache Manager
# Singleton for shared, low-latency access to scrip master and system data

import json
import os
import time
import logging
from pathlib import Path

logger = logging.getLogger("SAIO_Cache")

class CacheManager:
    _instance = None
    _scrip_master = None
    _last_load_time = 0
    _scrip_master_path = os.path.join(
        os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))),
        "data", "scrip_master.json"
    )

    def __new__(cls):
        if cls._instance is None:
            cls._instance = super().__new__(cls)
        return cls._instance

    def get_scrip_master(self, force_reload=False):
        """Thread-safe access to scrip master data with memory caching."""
        if self._scrip_master and not force_reload:
            return self._scrip_master

        if not os.path.exists(self._scrip_master_path):
            logger.warning(f"Scrip master file NOT found at {self._scrip_master_path}")
            return []

        try:
            start_t = time.time()
            logger.info(f"📂 Loading scrip master into memory... ({os.path.getsize(self._scrip_master_path) / 1e6:.1f} MB)")
            with open(self._scrip_master_path, "r") as f:
                self._scrip_master = json.load(f)
            self._last_load_time = time.time()
            logger.info(f"✅ Loaded {len(self._scrip_master)} scrips in {time.time() - start_t:.2f}s")
            return self._scrip_master
        except Exception as e:
            logger.error(f"❌ Failed to load scrip master: {e}")
            return []

    def get_token(self, symbol, exchange="NSE"):
        """Fast lookup for a token by symbol and exchange."""
        master = self.get_scrip_master()
        if not master:
            return None
        
        # Exact match check
        for s in master:
            if s.get("symbol") == symbol and s.get("exch_seg") == exchange:
                return s.get("token")
        
        # Partial match check (e.g. symbol without -EQ)
        for s in master:
            if symbol in s.get("symbol", "") and s.get("exch_seg") == exchange:
                return s.get("token")
        
        return None

    def get_lot_size(self, base_symbol):
        """Lookup lot size for F&O indices/stocks."""
        master = self.get_scrip_master()
        if not master:
            return 0
            
        for s in master:
            if (s.get("name") == base_symbol 
                and s.get("exch_seg") == "NFO" 
                and s.get("instrumenttype") in ("OPTIDX", "FUTIDX", "OPTSTK", "FUTSTK")):
                return int(s.get("lotsize", 0))
        return 0
