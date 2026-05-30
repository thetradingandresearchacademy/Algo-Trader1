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
            with open(self._scrip_master_path, "r", encoding="utf-8") as f:
                self._scrip_master = json.load(f)
            self._last_load_time = time.time()
            
            # Clear existing maps to force rebuild
            if hasattr(self, "_symbol_map"): delattr(self, "_symbol_map")
            if hasattr(self, "_partial_map"): delattr(self, "_partial_map")
            if hasattr(self, "_lot_size_map"): delattr(self, "_lot_size_map")
            if hasattr(self, "_nfo_by_name"): delattr(self, "_nfo_by_name")
            if hasattr(self, "_symbol_to_item"): delattr(self, "_symbol_to_item")
            if hasattr(self, "_nse_equities"): delattr(self, "_nse_equities")

            logger.info(f"✅ Loaded {len(self._scrip_master)} scrips in {time.time() - start_t:.2f}s")
            return self._scrip_master
        except Exception as e:
            logger.error(f"❌ Failed to load scrip master: {e}")
            return []

    def _init_indices(self):
        """Helper to initialize lookup dictionaries for O(1) queries."""
        master = self.get_scrip_master()
        if not master:
            return
            
        if not hasattr(self, "_symbol_map"):
            self._symbol_map = {}
            self._partial_map = {}
            self._lot_size_map = {}
            self._nfo_by_name = {}
            self._symbol_to_item = {}
            self._nse_equities = []
            
            for item in master:
                sym = item.get("symbol")
                exch = item.get("exch_seg")
                tok = item.get("token")
                name = item.get("name")
                
                if sym:
                    self._symbol_to_item[sym] = item
                    if exch and tok:
                        self._symbol_map[(sym, exch)] = tok
                
                if exch == "NFO":
                    if name:
                        if name not in self._nfo_by_name:
                            self._nfo_by_name[name] = []
                        self._nfo_by_name[name].append(item)
                    if name and item.get("instrumenttype") in ("OPTIDX", "FUTIDX", "OPTSTK", "FUTSTK"):
                        self._lot_size_map[name] = int(item.get("lotsize", 0))
                elif exch == "NSE":
                    self._nse_equities.append(item)

    def get_token(self, symbol, exchange="NSE"):
        """Fast lookup for a token by symbol and exchange."""
        self._init_indices()
        if not hasattr(self, "_symbol_map"):
            return None
            
        # 1. Exact match check (O(1))
        token = self._symbol_map.get((symbol, exchange))
        if token:
            return token
            
        # 2. Partial match lookup (cached O(1))
        if (symbol, exchange) in self._partial_map:
            return self._partial_map[(symbol, exchange)]
            
        # Fallback to linear search only for partial matches
        master = self.get_scrip_master() or []
        for s in master:
            if symbol in s.get("symbol", "") and s.get("exch_seg") == exchange:
                tok = s.get("token")
                self._partial_map[(symbol, exchange)] = tok
                return tok
                
        self._partial_map[(symbol, exchange)] = None
        return None

    def get_lot_size(self, base_symbol):
        """Lookup lot size for F&O indices/stocks."""
        self._init_indices()
        if not hasattr(self, "_lot_size_map"):
            return 0
        return self._lot_size_map.get(base_symbol, 0)

    def get_nfo_by_name(self, base_symbol):
        """Get all NFO instruments for a base symbol (e.g. NIFTY)."""
        self._init_indices()
        if not hasattr(self, "_nfo_by_name"):
            return []
        return self._nfo_by_name.get(base_symbol, [])

    def get_scrip_by_symbol(self, symbol):
        """Get full scrip item by symbol (O(1))."""
        self._init_indices()
        if not hasattr(self, "_symbol_to_item"):
            return None
        return self._symbol_to_item.get(symbol)

    def get_nse_scrips_starting_with(self, symbol):
        """Get NSE equities starting with a prefix."""
        self._init_indices()
        if not hasattr(self, "_nse_equities"):
            return []
        return [item for item in self._nse_equities if item.get("symbol", "").startswith(symbol)]
