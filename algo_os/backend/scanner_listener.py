import asyncio
import json
import psycopg2
from datetime import datetime
import sys
import os
import traceback

# Add parent directory to path so we can import scanner from d:\Algo Trader1\scanner.py
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

try:
    from scanner import TARA_Apex_Scanner
    import tara_config as config
except ImportError as e:
    print(f"Warning: Could not import scanner modules: {e}")
    TARA_Apex_Scanner = None

from config.settings import POSTGRES_DB_RAW
from services.redis_stream import RedisStream

class ScannerListenerEngine:

    def __init__(self):
        self.redis = RedisStream()
        self.output_stream = "stock_scanner"
        self.signal_stream = "alpha_signals"       # direct signal publishing
        self.command_stream = "scanner_commands"
        self.last_cmd_id = "$"
        
        self.fetch_interval_minutes = 5
        self.manual_trigger = False
        self._scanning = False  # prevent overlapping scans
        
        # Health tracking
        self._scan_count = 0
        self._last_scan_time = None
        self._consecutive_failures = 0
        self._last_signaled = {}

        self.db_pass = getattr(config, "DB_PASS", "tara123") if 'config' in globals() else "tara123"

    async def start(self):
        print("Scanner Listener Engine started")
        
        self.setup_db()
        
        # Extract and publish watchlist immediately on startup from DB cache
        try:
            watchlist, bias = self.extract_watchlist()
            if watchlist:
                symbol_names = [s["symbol"] for s in watchlist]
                payload = {
                    "symbols": symbol_names,
                    "bias": bias,
                    "timestamp": datetime.utcnow().isoformat(),
                    "source": "apex_scanner"
                }
                await self.redis.publish(self.output_stream, payload)
                print(f"ScannerListener: Published initial watchlist of {len(symbol_names)} symbols on startup")
                
                # Generate direct signals from existing watchlist
                signals_generated = 0
                for stock in watchlist:
                    if stock.get("score", 0) >= 1 and stock.get("last_price", 0) > 0:
                        symbol = stock.get("symbol")
                        signal = self._create_signal_from_scan(stock)
                        if signal:
                            await self.redis.publish(self.signal_stream, signal)
                            signals_generated += 1
                if signals_generated > 0:
                    print(f"🚀 ScannerListener: Published {signals_generated} initial trading signals on startup")
        except Exception as e:
            print(f"⚠️ ScannerListener: Initial watchlist load failed: {e}")

        # Start command listener for UI interaction
        asyncio.create_task(self.listen_for_commands())
        asyncio.create_task(self._heartbeat_logger())

        while True:
            try:
                if self._scanning:
                    await asyncio.sleep(5)
                    continue

                self._scanning = True
                scan_start = datetime.now()
                self._scan_count += 1
                print(f"🔄 ScannerListener: Starting Scan #{self._scan_count} @ {scan_start.strftime('%H:%M:%S')} (Interval: {self.fetch_interval_minutes}m)")
                
                # 1. Run the scanner (with 10-minute timeout to prevent hangs)
                if TARA_Apex_Scanner:
                    try:
                        await asyncio.wait_for(
                            asyncio.to_thread(self._run_scanner_sync),
                            timeout=600  # 10 minutes max
                        )
                        self._consecutive_failures = 0  # Reset on success
                    except asyncio.TimeoutError:
                        self._consecutive_failures += 1
                        print(f"⚠️ Scanner timed out after 10 minutes — failures: {self._consecutive_failures}")
                    except Exception as e:
                        self._consecutive_failures += 1
                        print(f"Scanner fetch error (failure #{self._consecutive_failures}): {e}")
                        traceback.print_exc()
                
                # 2. Extract best signals — ALWAYS attempt even if scanner errored
                #    (we may still have valid data from a previous scan in DB)
                watchlist, bias = self.extract_watchlist()
                
                # 3. Publish watchlist to Redis (for MDE's watchlist filter)
                if watchlist:
                    # Publish symbol names list for MDE watchlist filtering
                    symbol_names = [s["symbol"] for s in watchlist]
                    payload = {
                        "symbols": symbol_names,
                        "bias": bias,
                        "timestamp": datetime.utcnow().isoformat(),
                        "source": "apex_scanner"
                    }
                    await self.redis.publish(self.output_stream, payload)
                    print(f"ScannerListener: Published {len(symbol_names)} stocks to {self.output_stream}")

                    # 4. Generate DIRECT trading signals from scan results
                    signals_generated = 0
                    for stock in watchlist:
                        if stock.get("score", 0) >= 1 and stock.get("last_price", 0) > 0:
                            symbol = stock.get("symbol")
                            now = datetime.now()
                            # 10-minute per-symbol cooldown to prevent signal storming
                            if symbol in self._last_signaled and (now - self._last_signaled[symbol]).total_seconds() < 600:
                                continue
                            signal = self._create_signal_from_scan(stock)
                            if signal:
                                self._last_signaled[symbol] = now
                                await self.redis.publish(self.signal_stream, signal)
                                signals_generated += 1
                    
                    if signals_generated > 0:
                        print(f"🚀 ScannerListener: Published {signals_generated} trading signals to {self.signal_stream}")
                    else:
                        print(f"ℹ️ ScannerListener: {len(watchlist)} in watchlist, but 0 met signal threshold")
                else:
                    print("⚠️ ScannerListener: No watchlist extracted — check DB for recent signals")

                scan_duration = (datetime.now() - scan_start).total_seconds()
                self._last_scan_time = datetime.now()
                print(f"✅ Scan #{self._scan_count} complete in {scan_duration:.0f}s | Next in {self.fetch_interval_minutes}m")

            except Exception as e:
                self._consecutive_failures += 1
                print(f"ScannerListener iteration error (failure #{self._consecutive_failures}): {e}")
                traceback.print_exc()
            finally:
                self._scanning = False

            # Wait for next interval (timer starts AFTER scan completion)
            target_sleep = self.fetch_interval_minutes * 60
            slept = 0
            self.manual_trigger = False
            
            while slept < target_sleep and not self.manual_trigger:
                await asyncio.sleep(1)
                slept += 1
            
            if self.manual_trigger:
                print("⚡ Manual scan triggered — skipping remaining wait")

    async def _heartbeat_logger(self):
        """Log scanner health every 2 minutes so health checks know we're alive."""
        while True:
            await asyncio.sleep(120)
            status = "SCANNING" if self._scanning else "IDLE"
            last_scan = self._last_scan_time.strftime('%H:%M:%S') if self._last_scan_time else "NEVER"
            print(f"💓 Scanner Heartbeat | Status: {status} | Scans: {self._scan_count} | Last: {last_scan} | Failures: {self._consecutive_failures}")

    def _run_scanner_sync(self):
        """Run scanner in a thread with proper cleanup and timeout."""
        print("Running TARA Apex Scanner...")
        scanner = None
        try:
            scanner = TARA_Apex_Scanner()
            scanner.run()
        except Exception as e:
            print(f"Scanner run error: {e}")
            traceback.print_exc()
        finally:
            # Close all connections to prevent resource leaks on next run
            if scanner:
                try: scanner.conn_source.close()
                except: pass
                try: scanner.conn_dest.close()
                except: pass
                try: scanner.cur_source.close()
                except: pass
                try: scanner.cur_dest.close()
                except: pass

    def _create_signal_from_scan(self, stock):
        """Convert a scanner result into a trading signal for the alpha pipeline.
        
        The scanner has already done the heavy analysis (confluence of multiple
        technical signals). This method bridges that analysis directly to the
        execution pipeline, so we don't depend on WebSocket tick data for stocks.
        """
        symbol = stock.get("symbol")
        last_price = stock.get("last_price", 0)
        score = stock.get("score", 0)
        direction = stock.get("signal", "")
        bias = stock.get("bias", "NEUTRAL")
        
        if not symbol or last_price <= 0:
            return None
        
        # Determine side from scanner's direction analysis
        if "Bullish" in direction:
            side = "BUY"
        elif "Bearish" in direction:
            side = "SELL"
        else:
            return None  # skip neutral signals
        
        return {
            "symbol": symbol,
            "side": side,
            "price": last_price,
            "score": min(score * 15 + 70, 100),  # 1 conf=85, 2=100 (capped)
            "timestamp": datetime.utcnow().isoformat(),
            "source": "apex_scanner",
            "features": {
                "confluence_count": score,
                "scanner_direction": direction,
                "scanner_bias": bias,
                "swing_low": round(last_price * 0.97, 2),  # 3% default SL
                "vwap": round(last_price * 0.99, 2),
            }
        }

    def setup_db(self):
        try:
            conn = psycopg2.connect(
                host="127.0.0.1", database=POSTGRES_DB_RAW, 
                user="postgres", password=self.db_pass
            )
            cur = conn.cursor()
            cur.execute("""
                CREATE TABLE IF NOT EXISTS intraday_stocks (
                    id SERIAL PRIMARY KEY,
                    symbol VARCHAR(50) UNIQUE,
                    score INT,
                    bias VARCHAR(20),
                    flag VARCHAR(50),
                    detected_at TIMESTAMP
                )
            """)
            conn.commit()
            conn.close()
            print("ScannerListener: Intraday stocks table verified.")
        except Exception as e:
            print(f"ScannerListener DB Setup Error: {e}")

    def extract_watchlist(self):
        try:
            conn = psycopg2.connect(
                host="127.0.0.1", database=POSTGRES_DB_RAW, 
                user="postgres", password=self.db_pass
            )
            cur = conn.cursor()
            # Fetch recent signals from the intraday_signals table
            # Aggressive Mode: Lowered threshold to 1 for higher trade frequency
            cur.execute("""
                SELECT symbol, confluence_count, direction, priority_flag, last_price 
                FROM intraday_signals 
                WHERE last_updated >= NOW() - INTERVAL '4 hours'
                AND confluence_count >= 1
                ORDER BY confluence_count DESC
                LIMIT 100
            """)
            rows = cur.fetchall()
            
            symbols = []
            bullish = 0
            bearish = 0
            
            # Save to intraday_stocks table
            cur.execute("TRUNCATE TABLE intraday_stocks")
            
            for r in rows:
                sym, count, direction, flag, last_price = r
                symbols.append(sym)
                if "Bullish" in direction: bullish += 1
                if "Bearish" in direction: bearish += 1
                
                bias = "BULLISH" if "Bullish" in direction else "BEARISH"
                cur.execute("""
                    INSERT INTO intraday_stocks (symbol, score, bias, flag, detected_at)
                    VALUES (%s, %s, %s, %s, %s)
                """, (sym, count, bias, flag, datetime.now()))
            
            conn.commit()
            conn.close()
            
            # Enrich signal data for UI AND for direct signal generation
            enriched_watchlist = []
            for r in rows:
                sym, count, direction, flag, last_price = r
                enriched_watchlist.append({
                    "symbol": sym,
                    "score": count,
                    "bias": "BULLISH" if "Bullish" in direction else "BEARISH",
                    "flag": flag,
                    "signal": direction,
                    "last_price": float(last_price) if last_price else 0
                })

            market_bias = "NEUTRAL"
            if bullish > bearish * 2: market_bias = "BULLISH"
            elif bearish > bullish * 2: market_bias = "BEARISH"
            
            return enriched_watchlist, market_bias
            
        except Exception as e:
            print(f"ScannerListener Extract Error: {e}")
            traceback.print_exc()
            return [], "NEUTRAL"

    async def listen_for_commands(self):
        while True:
            try:
                streams = self.redis.read(self.command_stream, self.last_cmd_id)
                if not streams:
                    await asyncio.sleep(0.5)
                    continue
                    
                for stream, entries in streams:
                    for msg_id, payload in entries:
                        self.last_cmd_id = msg_id
                        cmd = json.loads(payload.get("data", "{}"))
                        
                        if cmd.get("command") == "SET_INTERVAL":
                            val = cmd.get("minutes", 10)
                            self.fetch_interval_minutes = int(val)
                            print(f"ScannerListener: Update interval set to {self.fetch_interval_minutes}m")
                            
                        elif cmd.get("command") == "FETCH_NOW":
                            print("ScannerListener: Manual fetch triggered!")
                            self.manual_trigger = True
            except Exception as e:
                print(f"ScannerListener command error: {e}")
                await asyncio.sleep(1)
