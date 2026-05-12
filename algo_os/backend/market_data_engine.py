import asyncio
import time
from collections import deque

from SmartApi import SmartConnect
from SmartApi.smartWebSocketV2 import SmartWebSocketV2
import pyotp
import threading

from services.redis_stream import RedisStream
from config.settings import (
    ANGEL_API_KEY,
    ANGEL_CLIENT_CODE,
    ANGEL_PASSWORD,
    ANGEL_TOTP_SECRET,
    TOKENS
)


class MarketDataEngine:

    def __init__(self):

        self.redis = RedisStream()

        self.api_key = ANGEL_API_KEY
        self.client_code = ANGEL_CLIENT_CODE
        self.password = ANGEL_PASSWORD
        self.totp_key = ANGEL_TOTP_SECRET

        self.jwt_token = None
        self.feed_token = None
        self.smart_connect = None

        self.ws = None

        self.token_symbol_map = TOKENS

        # watchdog
        self.last_tick_time = time.time()

        # reconnect control
        self.reconnecting = False
        self.reconnect_attempt = 0

        # tick batching buffer
        self.tick_buffer = deque(maxlen=5000)

        # Reconnect stability
        self.reconnect_lock = asyncio.Lock()
        self.last_login_timestamp = 0
        self.consecutive_failures = 0

        # event loop for thread safety
        self.loop = None

    # ---------------------------------------------------------
    # ENGINE START
    # ---------------------------------------------------------

    async def start(self):
        print("MarketDataEngine started")
        print(f"   Tokens configured: {len(self.token_symbol_map)}")
        self.loop = asyncio.get_running_loop()

        await self.login()
        
        # Verify login succeeded
        if self.jwt_token and self.feed_token:
            print(f"✅ Login verified | JWT={self.jwt_token[:20]}... | Feed={self.feed_token[:10]}...")
        else:
            print("❌ LOGIN FAILED: Missing JWT or Feed token. WebSocket will not connect.")
        
        await self.connect_socket()

        asyncio.create_task(self.watchdog())
        asyncio.create_task(self.flush_ticks())

        while True:
            await asyncio.sleep(1)

    # ---------------------------------------------------------
    # LOGIN
    # ---------------------------------------------------------

    async def login(self):
        """Authenticated with Angel One and retrieve tokens."""
        now = time.time()
        if now - self.last_login_timestamp < 10:
            print("⏳ Login requested too soon, skipping...")
            return

        while True:
            try:
                print(f"🔑 Attempting Angel login (Attempt {self.consecutive_failures + 1})...")
                self.smart_connect = SmartConnect(api_key=self.api_key)
                totp = pyotp.TOTP(self.totp_key).now()

                data = self.smart_connect.generateSession(
                    self.client_code,
                    self.password,
                    totp
                )

                if not data.get("status"):
                    raise Exception(f"Login status false: {data.get('message')}")

                # Ensure JWT token has the required Bearer prefix
                raw_token = data["data"]["jwtToken"]
                self.jwt_token = raw_token if raw_token.startswith("Bearer ") else f"Bearer {raw_token}"
                
                # Use feedToken from data response
                self.feed_token = data["data"]["feedToken"]
                
                self.last_login_timestamp = time.time()
                self.consecutive_failures = 0

                print("✅ Angel login successful (Tokens refreshed)")
                return

            except Exception as e:
                self.consecutive_failures += 1
                print(f"❌ Angel login failed: {e}")
                
                if self.consecutive_failures > 5:
                    print("🚨 Max login failures reached. Cooling down for 60s...")
                    await asyncio.sleep(60)
                    self.consecutive_failures = 0
                
                delay = min(5 * self.consecutive_failures, 30)
                print(f"🔄 Retrying login in {delay}s...")
                await asyncio.sleep(delay)

    # ---------------------------------------------------------
    # CONNECT SOCKET
    # ---------------------------------------------------------

    async def connect_socket(self):
        print("🌐 Connecting Angel WebSocket...")

        if not self.jwt_token or not self.feed_token:
            print("⚠️ Missing tokens. Deep login required.")
            await self.login()

        # Initialize with max_retry_attempt=0 to muzzle SDK's internal rapid-fire loop
        # and let our managed reconnect() handle it with proper backoff.
        self.ws = SmartWebSocketV2(
            auth_token=self.jwt_token,
            api_key=self.api_key,
            client_code=self.client_code,
            feed_token=self.feed_token,
            max_retry_attempt=0
        )

        self.ws.on_open = self.on_open
        self.ws.on_data = self.on_tick
        self.ws.on_error = self.on_error
        self.ws.on_close = self.on_close

        threading.Thread(target=self.ws.connect, daemon=True).start()

    # ---------------------------------------------------------
    # SOCKET OPEN
    # ---------------------------------------------------------

    def on_open(self, ws):
        print("🟢 Angel WebSocket Connected")
        print(f"   JWT token: {self.jwt_token[:20] if self.jwt_token else 'NONE'}...")
        print(f"   Feed token: {self.feed_token[:10] if self.feed_token else 'NONE'}...")

        self.reconnecting = False
        self.reconnect_attempt = 0

        # Delegate subscription to async loop to avoid blocking the SDK thread
        if self.loop:
            asyncio.run_coroutine_threadsafe(self.async_subscribe(), self.loop)

    async def async_subscribe(self):
        """Non-blocking subscription handler with chunking for large token lists."""
        await asyncio.sleep(2)  # Stability delay
        
        all_tokens = list(self.token_symbol_map.keys())
        print(f"📡 Subscribing to {len(all_tokens)} tokens...")
        chunk_size = 50
        
        for i in range(0, len(all_tokens), chunk_size):
            chunk = all_tokens[i : i + chunk_size]
            
            # Group chunk by exchange
            nse_tokens = [str(t) for t in chunk if str(t) != "26037"]
            bse_tokens = [str(t) for t in chunk if str(t) == "26037"]

            token_list = []
            if nse_tokens:
                token_list.append({"exchangeType": 1, "tokens": nse_tokens})
            if bse_tokens:
                token_list.append({"exchangeType": 3, "tokens": bse_tokens})

            correlation_id = f"algo_os_{int(time.time())}_{i}"
            
            try:
                if self.ws:
                    self.ws.subscribe(correlation_id, 3, token_list)
                    print(f"📡 Subscribed to chunk {i//chunk_size + 1} ({len(chunk)} tokens)")
                    await asyncio.sleep(0.2) # Small delay between chunks
                else:
                    print("❌ Cannot subscribe: WebSocket instance is None")
            except Exception as e:
                print(f"❌ Subscription failed for chunk {i}: {e}")
        
        print(f"✅ Full subscription complete for {len(all_tokens)} tokens | Waiting for ticks...")

    # ---------------------------------------------------------
    # NORMALIZE TICK
    # ---------------------------------------------------------

    def normalize_tick(self, message):

        token = str(message.get("token"))

        symbol = self.token_symbol_map.get(token, token)

        # Standardize price: Angel One WebSocket V2 always sends prices in paise
        raw_price = message.get("last_traded_price") or message.get("ltp") or 0
        price = round(float(raw_price) / 100.0, 2)

        return {
            "symbol": symbol,
            "token": token,
            "timestamp": message.get("exchange_time_stamp", 0),
            "price": price,
            "ltp": price,
            "bid_price": round(message.get("best_bid_price", 0) / 100.0, 2),
            "ask_price": round(message.get("best_ask_price", 0) / 100.0, 2),
            "day_high": round(message.get("high", 0) / 100.0, 2),
            "day_low": round(message.get("low", 0) / 100.0, 2),
            "volume": message.get("volume", 0),
            "trade_volume": message.get("volume", 0)
        }

    # ---------------------------------------------------------
    # TICK RECEIVED
    # ---------------------------------------------------------

    def on_tick(self, ws, message):

        try:

            self.last_tick_time = time.time()

            tick = self.normalize_tick(message)

            self.tick_buffer.append(tick)

        except Exception as e:

            print("Tick error:", e)

    # ---------------------------------------------------------
    # TICK FLUSH TO REDIS
    # ---------------------------------------------------------

    async def flush_ticks(self):
        """High-performance tick flushing with micro-batching."""
        while True:
            try:
                if self.tick_buffer:
                    # Drain buffer and publish in a tight loop to minimize overhead
                    batch = []
                    while self.tick_buffer and len(batch) < 100:
                        batch.append(self.tick_buffer.popleft())
                    
                    for tick in batch:
                        await self.redis.publish("micro_ticks", tick)
                
                # Dynamic sleep: if buffer is empty, sleep longer; else yield
                await asyncio.sleep(0.001 if self.tick_buffer else 0.01)

            except Exception as e:
                print("Tick flush error:", e)
                await asyncio.sleep(1)

    # ---------------------------------------------------------
    # WATCHDOG
    # ---------------------------------------------------------

    async def watchdog(self):
        """Aggressive watchdog: forces reconnect if no ticks for 20s."""
        stale_count = 0
        while True:
            await asyncio.sleep(15)

            now = time.time()
            tick_age = now - self.last_tick_time

            if tick_age > 20:
                stale_count += 1
                print(f"⚠ Market data stalled | Age={tick_age:.0f}s | Stale count={stale_count} | Reconnecting={self.reconnecting}")

                if stale_count >= 3 and self.reconnecting:
                    # Force reset stuck reconnect flag after 3 stale cycles
                    print("🔧 FORCE RESET: Reconnect flag was stuck. Clearing...")
                    self.reconnecting = False

                if not self.reconnecting:
                    await self.reconnect()
            else:
                if stale_count > 0:
                    print(f"✅ Market data resumed after {stale_count} stale cycles")
                stale_count = 0

    # ---------------------------------------------------------
    # SAFE RECONNECT
    # ---------------------------------------------------------

    async def reconnect(self):
        """Safe reconnect with proper flag management."""
        if self.reconnecting:
            return
        
        self.reconnecting = True

        try:
            self.reconnect_attempt += 1
            delay = min(3 * self.reconnect_attempt, 15)  # Faster: max 15s delay (was 30s)

            print(f"🔄 Reconnecting WebSocket in {delay}s (Attempt {self.reconnect_attempt})...")

            if self.ws:
                try: 
                    self.ws.close_connection() 
                except: 
                    pass

            await asyncio.sleep(delay)

            # Re-login every 2 failures (was 3) — session tokens expire fast
            if self.reconnect_attempt % 2 == 0:
                print("🔑 Forcing fresh login before reconnect...")
                self.last_login_timestamp = 0  # Reset cooldown
                await self.login()
            
            await self.connect_socket()
            
            # Shorter timeout guard: 20s (was 45s)
            asyncio.create_task(self._reconnect_timeout_guard())

        except Exception as e:
            print(f"❌ Reconnect method error: {e}")
            self.reconnecting = False

    async def _reconnect_timeout_guard(self):
        """Resets the reconnecting flag if on_open is never reached."""
        await asyncio.sleep(20)  # Was 45s — too slow
        if self.reconnecting:
            print("🕒 Reconnect timed out. Resetting flag for next watchdog cycle.")
            self.reconnecting = False

    # ---------------------------------------------------------
    # SOCKET ERROR
    # ---------------------------------------------------------

    def on_error(self, ws, error):
        print(f"❌ WebSocket error: {error}")
        # Always trigger reconnect on any error
        if self.loop:
            asyncio.run_coroutine_threadsafe(self.reconnect(), self.loop)

    # ---------------------------------------------------------
    # SOCKET CLOSED
    # ---------------------------------------------------------

    def on_close(self, ws, close_status_code, close_msg):
        print(f"🚪 WebSocket closed. Code: {close_status_code}, Message: {close_msg}")
        # ALWAYS reconnect on close — regardless of status code
        # The old code skipped reconnect for code 1000 (normal close), causing permanent silence
        if self.loop:
            asyncio.run_coroutine_threadsafe(self.reconnect(), self.loop)