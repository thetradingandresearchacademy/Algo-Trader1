"""
BrokerAPI — Production Angel One SmartConnect Order Gateway

SEBI-compliant rate limiting: max 2.5 orders/sec (Angel limit is 3/sec).
Singleton pattern ensures a single authenticated session is shared.
"""
import asyncio
import time
import logging
from SmartApi import SmartConnect
import pyotp

from config import settings as sys_config

logger = logging.getLogger("BrokerAPI")
logger.setLevel(logging.INFO)

# ---------------------------------------------------------------------------
# Async Token Bucket — strict rate limiter
# ---------------------------------------------------------------------------

class AsyncTokenBucket:
    """Ensures strict rate limiting. Angel One limit = 3 req/sec."""
    def __init__(self, rate: float, capacity: int):
        self.rate = rate
        self.capacity = capacity
        self.tokens = float(capacity)
        self.last_update = time.monotonic()
        self.lock = asyncio.Lock()

    async def consume(self, tokens: int = 1):
        async with self.lock:
            while True:
                now = time.monotonic()
                elapsed = now - self.last_update
                self.tokens = min(self.capacity, self.tokens + elapsed * self.rate)
                self.last_update = now
                if self.tokens >= tokens:
                    self.tokens -= tokens
                    return True
                wait_time = (tokens - self.tokens) / self.rate
                await asyncio.sleep(wait_time)


# ---------------------------------------------------------------------------
# BrokerAPI Singleton
# ---------------------------------------------------------------------------

class BrokerAPI:
    _instance = None
    _smart_connect = None
    _authenticated = False

    def __new__(cls):
        if cls._instance is None:
            cls._instance = super().__new__(cls)
            cls._instance._init_once()
        return cls._instance

    def _init_once(self):
        # 2.5 orders/sec — safely under Angel One's 3/sec ceiling
        self.rate_limiter = AsyncTokenBucket(rate=2.5, capacity=3)
        self._smart_connect = None
        self._authenticated = False
        self._last_auth_time = 0
        self._order_log = []  # Audit trail
        logger.info("BrokerAPI initialized | Rate limit: 2.5 req/sec (Angel ceiling: 3)")

    # ------------------------------------------------------------------
    # Authentication — re-uses existing session, refreshes every 4 hours
    # ------------------------------------------------------------------

    def authenticate(self, force=False):
        """Authenticate with Angel One. Returns True on success."""
        now = time.time()
        if self._authenticated and not force and (now - self._last_auth_time) < 14400:
            return True  # Session valid (< 4 hours)

        try:
            self._smart_connect = SmartConnect(api_key=sys_config.ANGEL_API_KEY)
            totp = pyotp.TOTP(sys_config.ANGEL_TOTP_SECRET).now()
            data = self._smart_connect.generateSession(
                sys_config.ANGEL_CLIENT_CODE,
                sys_config.ANGEL_PASSWORD,
                totp
            )
            if data.get("status"):
                self._authenticated = True
                self._last_auth_time = time.time()
                logger.info("✅ BrokerAPI: Angel One session authenticated")
                return True
            else:
                logger.error(f"❌ BrokerAPI auth failed: {data.get('message')}")
                return False
        except Exception as e:
            logger.error(f"❌ BrokerAPI auth exception: {e}")
            return False

    @property
    def api(self):
        if not self._authenticated:
            self.authenticate()
        return self._smart_connect

    # ------------------------------------------------------------------
    # Place Order — REAL order to Angel One
    # ------------------------------------------------------------------

    async def place_order(self, symbol: str, token: str, qty: int, side: str,
                          exchange: str = "NSE", order_type: str = "MARKET",
                          product_type: str = "INTRADAY", price: float = 0) -> dict:
        """
        Place a REAL order via Angel One SmartConnect.

        Returns: {"success": True, "order_id": "..."} or {"success": False, "error": "..."}
        """
        await self.rate_limiter.consume(1)

        if not self._authenticated:
            if not self.authenticate():
                return {"success": False, "error": "Authentication failed"}

        # Angel One order params
        order_params = {
            "variety": "NORMAL",
            "tradingsymbol": symbol,
            "symboltoken": token,
            "transactiontype": side,  # BUY or SELL
            "exchange": exchange,
            "ordertype": order_type,   # MARKET or LIMIT
            "producttype": product_type,  # INTRADAY or DELIVERY
            "duration": "DAY",
            "quantity": str(qty),
        }

        if order_type == "LIMIT" and price > 0:
            order_params["price"] = str(round(price, 2))
        else:
            order_params["price"] = "0"

        order_params["squareoff"] = "0"
        order_params["stoploss"] = "0"
        order_params["triggerprice"] = "0"

        try:
            resp = self._smart_connect.placeOrder(order_params)
            if resp:
                order_id = str(resp)
                log_entry = {
                    "time": time.strftime("%H:%M:%S"),
                    "symbol": symbol,
                    "side": side,
                    "qty": qty,
                    "order_id": order_id,
                    "status": "PLACED"
                }
                self._order_log.append(log_entry)
                logger.info(f"⚡ LIVE ORDER PLACED | {side} {qty} {symbol} | OrderID: {order_id}")
                return {"success": True, "order_id": order_id}
            else:
                logger.error(f"❌ Order response empty for {symbol}")
                return {"success": False, "error": "Empty response from broker"}

        except Exception as e:
            error_msg = str(e)
            logger.error(f"❌ Order FAILED | {side} {qty} {symbol} | Error: {error_msg}")
            return {"success": False, "error": error_msg}

    # ------------------------------------------------------------------
    # Cancel Order
    # ------------------------------------------------------------------

    async def cancel_order(self, order_id: str, variety: str = "NORMAL") -> dict:
        await self.rate_limiter.consume(1)
        try:
            resp = self._smart_connect.cancelOrder(order_id, variety)
            logger.info(f"🛑 Order cancelled: {order_id}")
            return {"success": True, "response": resp}
        except Exception as e:
            logger.error(f"❌ Cancel failed: {order_id} | {e}")
            return {"success": False, "error": str(e)}

    # ------------------------------------------------------------------
    # Exit position (place opposite order)
    # ------------------------------------------------------------------

    async def exit_position(self, symbol: str, token: str, qty: int, side: str,
                            exchange: str = "NSE") -> dict:
        """Place an exit order (opposite side)."""
        exit_side = "SELL" if side == "BUY" else "BUY"
        return await self.place_order(
            symbol=symbol, token=token, qty=qty, side=exit_side,
            exchange=exchange, order_type="MARKET", product_type="INTRADAY"
        )

    # ------------------------------------------------------------------
    # Audit trail
    # ------------------------------------------------------------------

    def get_order_log(self) -> list:
        return list(self._order_log)

    def get_order_book(self) -> list:
        """Fetch today's order book from broker."""
        try:
            if self._authenticated and self._smart_connect:
                return self._smart_connect.orderBook().get("data", []) or []
        except Exception as e:
            logger.error(f"Order book fetch error: {e}")
        return []