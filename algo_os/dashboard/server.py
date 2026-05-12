from fastapi import FastAPI, WebSocket
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import HTMLResponse
from fastapi.openapi.docs import get_swagger_ui_html
import asyncio
import json
import redis
import sys
import os
from pathlib import Path
import asyncpg

# Add project root to sys.path
_dashboard_dir = Path(__file__).parent.absolute()
_algo_os_dir = _dashboard_dir.parent
_project_root = _algo_os_dir.parent
sys.path.append(str(_algo_os_dir))
sys.path.append(str(_project_root))

from backend.upload_engine import upload_router
import config.settings as sys_config

app = FastAPI(docs_url=None, redoc_url=None)

# Initialize Redis early — using WSL host from config
try:
    r = redis.Redis(
        host=sys_config.REDIS_HOST, 
        port=sys_config.REDIS_PORT, 
        decode_responses=True,
        socket_connect_timeout=2
    )
    # Test connection immediately
    r.ping()
    print(f"Redis connected to {sys_config.REDIS_HOST}")
except Exception as e:
    print(f"⚠️ Redis Connection Warning: {e}")
    # We don't crash here, but APIs will fail gracefully

# Global DB pool for asyncpg
db_pool = None

@app.on_event("startup")
async def startup_event():
    global db_pool
    # Diagnostic: Check if we are binding to a weird host
    host_arg = next((arg for i, arg in enumerate(sys.argv) if arg == "--host" and i+1 < len(sys.argv)), None)
    if host_arg and host_arg == "127":
        print("🛑 WARNING: Binding to host '127' is invalid. Use '127.0.0.1' instead.")

    try:
        db_pool = await asyncpg.create_pool(sys_config.POSTGRES_DSN)
        print("AsyncPG Pool initialized")
    except Exception as e:
        print(f"Failed to initialize AsyncPG pool: {e}")

import psycopg2

def get_enriched_scanner_data():
    """Fetch enriched scanner data from DB for dashboard display."""
    try:
        conn = psycopg2.connect(
            host="127.0.0.1", database=sys_config.POSTGRES_DB_RAW,
            user="postgres", password=sys_config.POSTGRES_PASSWORD_RAW
        )
        cur = conn.cursor()
        # Join intraday_stocks with intraday_signals to get last_price
        cur.execute("""
            SELECT s.symbol, s.score, s.bias, s.flag, s.detected_at,
                   COALESCE(i.last_price, 0) as last_price
            FROM intraday_stocks s
            LEFT JOIN intraday_signals i ON s.symbol = i.symbol
            ORDER BY s.score DESC
            LIMIT 100
        """)
        rows = cur.fetchall()
        conn.close()

        return {
            "symbols": [
                {
                    "symbol": r[0],
                    "score": r[1],
                    "bias": r[2],
                    "flag": r[3],
                    "signal": f"{r[2]} Signal",
                    "last_price": float(r[5]) if r[5] else 0
                }
                for r in rows
            ]
        }
    except Exception as e:
        print(f"Scanner DB Error: {e}")
        return None

def get_market_status():
    """Determine if Indian markets are currently open (09:15 - 15:30 IST)."""
    from datetime import datetime, time, timedelta, timezone
    ist = timezone(timedelta(hours=5, minutes=30))
    now = datetime.now(ist)
    
    # 0 = Monday, 6 = Sunday
    if now.weekday() >= 5:
        return {"status": "CLOSED", "reason": "WEEKEND", "color": "#ef4444"}
        
    current_time = now.time()
    if current_time < time(9, 15):
        return {"status": "PRE-MARKET", "reason": "OPENS @ 09:15", "color": "#fbbf24"}
    elif current_time > time(15, 30):
        return {"status": "CLOSED", "reason": "MARKET CLOSED", "color": "#ef4444"}
    
    return {"status": "OPEN", "reason": "LIVE TRADING", "color": "#10b981"}

@app.get("/docs", include_in_schema=False)
async def custom_swagger_ui_html():
    html = get_swagger_ui_html(
        openapi_url=app.openapi_url,
        title=app.title + " - API Docs",
        swagger_js_url="https://cdn.jsdelivr.net/npm/swagger-ui-dist@5/swagger-ui-bundle.js",
        swagger_css_url="https://cdn.jsdelivr.net/npm/swagger-ui-dist@5/swagger-ui.css",
    )
    
    custom_css = """
    <style>
      @import url('https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600;700&display=swap');
      
      body {
          background-color: #0b0f19 !important; /* Deep dark blue for SaaS feel */
          color: #e2e8f0 !important;
          font-family: 'Inter', sans-serif !important;
      }
      
      .swagger-ui {
          font-family: 'Inter', sans-serif !important;
          color: #e2e8f0 !important;
      }

      /* Make all base text 2 points larger */
      .swagger-ui, .swagger-ui p, .swagger-ui table, .swagger-ui select, .swagger-ui input, .swagger-ui textarea {
          font-size: 16px !important;
      }
      
      .swagger-ui .info .title, .swagger-ui .info h1, .swagger-ui .info h2, .swagger-ui .info h3, .swagger-ui .info h4, .swagger-ui .info h5 {
          color: #ffffff !important;
      }
      
      .swagger-ui .scheme-container {
          background-color: #111827 !important;
          box-shadow: 0 4px 10px rgba(0,0,0,0.5) !important;
          border-bottom: 1px solid #1f2937 !important;
      }

      /* POST Method styling (Neon Green) */
      .swagger-ui .opblock.opblock-post {
          background: rgba(16, 185, 129, 0.04) !important;
          border: 1px solid rgba(16, 185, 129, 0.4) !important;
          box-shadow: 0 0 12px rgba(16, 185, 129, 0.1) !important;
          border-radius: 6px !important;
      }
      .swagger-ui .opblock.opblock-post .opblock-summary-method {
          background: #10b981 !important;
          color: #000 !important;
          box-shadow: 0 0 10px rgba(16, 185, 129, 0.5) !important;
      }
      .swagger-ui .opblock.opblock-post:hover {
          border-color: #10b981 !important;
          box-shadow: 0 0 20px rgba(16, 185, 129, 0.25) !important;
      }

      /* GET Method styling (Neon Cyan) */
      .swagger-ui .opblock.opblock-get {
          background: rgba(56, 189, 248, 0.04) !important;
          border: 1px solid rgba(56, 189, 248, 0.4) !important;
          box-shadow: 0 0 12px rgba(56, 189, 248, 0.1) !important;
          border-radius: 6px !important;
      }
      .swagger-ui .opblock.opblock-get .opblock-summary-method {
          background: #38bdf8 !important;
          color: #000 !important;
          box-shadow: 0 0 10px rgba(56, 189, 248, 0.5) !important;
      }
      .swagger-ui .opblock.opblock-get:hover {
          border-color: #38bdf8 !important;
          box-shadow: 0 0 20px rgba(56, 189, 248, 0.25) !important;
      }

      .swagger-ui .opblock .opblock-summary-method {
          font-weight: 800 !important;
          border-radius: 4px !important;
          font-size: 16px !important;
          padding: 8px 12px !important;
      }

      .swagger-ui section.models {
          background-color: #111827 !important;
          border: 1px solid #1f2937 !important;
          border-radius: 8px !important;
      }
      
      .swagger-ui section.models.is-open h4 {
          color: #ffffff !important;
          border-bottom: 1px solid #1f2937 !important;
      }

      /* Text colors */
      .swagger-ui .opblock .opblock-summary-operation-id, 
      .swagger-ui .opblock .opblock-summary-path,
      .swagger-ui .opblock .opblock-summary-path__deprecated,
      .swagger-ui .opblock-description-wrapper p,
      .swagger-ui .responses-inner h4, .swagger-ui .responses-inner h5,
      .swagger-ui .parameter__name, .swagger-ui .parameter__type,
      .swagger-ui table thead tr td, .swagger-ui table thead tr th,
      .swagger-ui .response-col_status, .swagger-ui .response-col_description,
      .swagger-ui .tab li {
          color: #e2e8f0 !important;
          font-size: 16px !important; 
      }
      
      .swagger-ui .opblock .opblock-summary-path {
          font-weight: 600 !important;
          font-size: 18px !important;
      }

      /* Neon Buttons */
      .swagger-ui .btn {
          font-size: 15px !important;
          font-weight: 700 !important;
          border-radius: 6px !important;
      }
      
      .swagger-ui .btn.execute {
          background-color: transparent !important;
          color: #a78bfa !important;
          border: 2px solid #a78bfa !important;
          box-shadow: 0 0 10px rgba(167, 139, 250, 0.3) !important;
          transition: all 0.3s ease !important;
      }
      
      .swagger-ui .btn.execute:hover {
          background-color: #8b5cf6 !important;
          color: #fff !important;
          box-shadow: 0 0 20px rgba(139, 92, 246, 0.7) !important;
          border-color: #8b5cf6 !important;
      }

      /* Inputs and Code blocks */
      .swagger-ui input, .swagger-ui select, .swagger-ui textarea {
          background-color: #1e293b !important;
          color: #ffffff !important;
          border: 1px solid #475569 !important;
          border-radius: 4px !important;
          font-size: 16px !important;
      }
      
      .swagger-ui .topbar {
          background-color: #0b0f19 !important;
          border-bottom: 2px solid #38bdf8 !important;
          box-shadow: 0 0 20px rgba(56, 189, 248, 0.3) !important;
      }

      .swagger-ui .topbar a span {
          color: #38bdf8 !important;
          font-weight: 800 !important;
          font-size: 20px !important;
      }

      .swagger-ui .highlight-code {
          background-color: #1e293b !important;
      }
      
      .swagger-ui .model, .swagger-ui .model-title {
          color: #cbd5e1 !important;
          font-size: 16px !important;
      }

      /* Improve error messages text visibility */
      .swagger-ui .errors-wrapper {
          background-color: rgba(239, 68, 68, 0.1) !important;
          border: 1px solid #ef4444 !important;
      }
      .swagger-ui .errors-wrapper h4, .swagger-ui .errors-wrapper p {
          color: #fca5a5 !important;
      }
    </style>
    """
    
    new_html = html.body.decode("utf-8")
    new_html = new_html.replace("</head>", f"{custom_css}\n</head>")
    return HTMLResponse(new_html)

@app.get("/", include_in_schema=False)
async def get_index():
    """Default entry point serving the premium SAIO dashboard."""
    return await get_saio_ui()

@app.get("/app", include_in_schema=False)
async def get_legacy_ui():
    try:
        ui_path = _dashboard_dir / "app.html"
        with open(ui_path, "r", encoding="utf-8") as f:
            content = f.read()
        return HTMLResponse(content)
    except FileNotFoundError:
        return HTMLResponse(f"<h1>Error: app.html not found!</h1>", status_code=404)

@app.get("/saio", include_in_schema=False)
async def get_saio_ui():
    try:
        ui_path = _dashboard_dir / "saio_app.html"
        with open(ui_path, "r", encoding="utf-8") as f:
            content = f.read()
        return HTMLResponse(content)
    except FileNotFoundError:
        return HTMLResponse(f"<h1>Error: saio_app.html not found!</h1>", status_code=404)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

app.include_router(upload_router, prefix="/api")

from pydantic import BaseModel

class ScannerCommand(BaseModel):
    command: str
    minutes: int = 10

class TradingMode(BaseModel):
    mode: str

@app.post("/api/scanner/command", include_in_schema=False)
async def scanner_command(cmd: ScannerCommand):
    try:
        r.xadd("scanner_commands", {"data": json.dumps(cmd.dict())})
        return {"status": "success", "message": f"Command {cmd.command} dispatched"}
    except Exception as e:
        return {"status": "error", "message": str(e)}

@app.post("/api/settings/toggle_live", include_in_schema=False)
async def toggle_live_trading(mode_data: TradingMode):
    try:
        r.xadd("control_commands", {"data": json.dumps({"command": "SET_LIVE_TRADING", "mode": mode_data.mode})})
        return {"status": "success", "message": f"Mode switched to {mode_data.mode}"}
    except Exception as e:
        return {"status": "error", "message": str(e)}

class PowerCommand(BaseModel):
    command: str

@app.post("/api/system/trading_status", include_in_schema=False)
async def toggle_trading_status(cmd: PowerCommand):
    try:
        r.xadd("control_commands", {"data": json.dumps({"command": cmd.command})})
        return {"status": "success", "message": f"Trading command {cmd.command} sent"}
    except Exception as e:
        return {"status": "error", "message": str(e)}

@app.post("/api/system/shutdown", include_in_schema=False)
async def system_shutdown():
    try:
        r.xadd("control_commands", {"data": json.dumps({"command": "SHUTDOWN_SYSTEM"})})
        return {"status": "success", "message": "System shutdown initiated. All positions will be squared off."}
    except Exception as e:
        return {"status": "error", "message": str(e)}

@app.post("/api/risk/continue_trading", include_in_schema=False)
async def continue_trading():
    """Allows user to bypass the trade limit and continue for another 25 trades."""
    try:
        r.xadd("control_commands", {"data": json.dumps({"command": "CONTINUE_TRADING"})})
        return {"status": "success", "message": "Continue command dispatched. Limit increased by 25."}
    except Exception as e:
        return {"status": "error", "message": str(e)}

class ForcePaperToggle(BaseModel):
    enabled: bool

@app.post("/api/settings/toggle_force_paper", include_in_schema=False)
async def toggle_force_paper(toggle: ForcePaperToggle):
    try:
        r.xadd("control_commands", {"data": json.dumps({"command": "SET_FORCE_PAPER", "enabled": toggle.enabled})})
        state = "ON" if toggle.enabled else "OFF"
        return {"status": "success", "message": f"Force Paper Mode: {state}"}
    except Exception as e:
        return {"status": "error", "message": str(e)}

def calculate_charges(qty, entry, exit, side, is_options=False):
    """
    Calculate estimated charges for Indian Intraday Trading (Angel One/discount broker).
    """
    if not qty or not entry or not exit:
        return 0.0
        
    turnover = qty * (entry + exit)
    premium_turnover = turnover if is_options else (qty * (entry + exit)) 
    
    brokerage = 40.0 
    
    if is_options:
        stt = (qty * exit) * 0.000625 
    else:
        stt = (qty * exit) * 0.00025 
        
    if is_options:
        trans_charges = premium_turnover * 0.00053 
    else:
        trans_charges = turnover * 0.0000345 
        
    gst = (brokerage + trans_charges) * 0.18
    sebi_stamp = turnover * 0.00005 
    
    return round(brokerage + stt + trans_charges + gst + sebi_stamp, 2)

@app.get("/api/trades/history", include_in_schema=False)
async def get_trade_history():
    try:
        conn = psycopg2.connect(
            host="127.0.0.1", database=sys_config.POSTGRES_DB_RAW,
            user="postgres", password=sys_config.POSTGRES_PASSWORD_RAW
        )
        cur = conn.cursor()
        cur.execute("""
            SELECT instrument_id, direction, entry_price, exit_price, net_pnl, exit_reason, created_at, qty
            FROM algo_trades 
            ORDER BY created_at DESC 
            LIMIT 200
        """)
        rows = cur.fetchall()
        conn.close()
        
        trades = []
        from datetime import datetime, timedelta, timezone
        ist = timezone(timedelta(hours=5, minutes=30))
        for row in rows:
            trades.append({
                "symbol": row[0], "side": row[1], "entry_price": float(row[2]), "exit_price": float(row[3]),
                "pnl": float(row[4]), "reason": row[5], "timestamp": row[6].isoformat() if row[6] else None
            })
        return {"status": "success", "trades": trades}
    except Exception as e:
        return {"status": "error", "trades": [], "error": str(e)}

@app.get("/api/trades/journal", include_in_schema=False)
async def get_trade_journal(range: str = "today", start: str = None, end: str = None):
    """Professional Trade Journal with date filtering.
    
    range: 'today' | 'yesterday' | 'week' | 'month' | 'custom'
    start/end: ISO date strings for custom range (YYYY-MM-DD)
    """
    try:
        from datetime import datetime, timedelta, timezone, date
        ist = timezone(timedelta(hours=5, minutes=30))
        now = datetime.now(ist)
        
        if range == "today":
            date_filter = now.strftime('%Y-%m-%d')
            where_clause = f"WHERE created_at::date = '{date_filter}'"
        elif range == "yesterday":
            date_filter = (now - timedelta(days=1)).strftime('%Y-%m-%d')
            where_clause = f"WHERE created_at::date = '{date_filter}'"
        elif range == "week":
            week_start = (now - timedelta(days=now.weekday())).strftime('%Y-%m-%d')
            where_clause = f"WHERE created_at::date >= '{week_start}'"
        elif range == "month":
            month_start = now.replace(day=1).strftime('%Y-%m-%d')
            where_clause = f"WHERE created_at::date >= '{month_start}'"
        elif range == "custom" and start and end:
            where_clause = f"WHERE created_at::date BETWEEN '{start}' AND '{end}'"
        else:
            where_clause = "WHERE created_at >= NOW() - INTERVAL '7 days'"
        
        conn = psycopg2.connect(
            host="127.0.0.1", database=sys_config.POSTGRES_DB_RAW,
            user="postgres", password=sys_config.POSTGRES_PASSWORD_RAW
        )
        cur = conn.cursor()
        cur.execute(f"""
            SELECT id, strategy_id, instrument_id, direction, entry_price, exit_price, 
                   net_pnl, exit_reason, qty, created_at
            FROM algo_trades 
            {where_clause}
            ORDER BY created_at DESC 
            LIMIT 500
        """)
        rows = cur.fetchall()
        
        # Also get summary stats for the period
        cur.execute(f"""
            SELECT 
                COUNT(*) as total,
                COALESCE(SUM(net_pnl), 0) as total_pnl,
                COALESCE(SUM(CASE WHEN net_pnl > 0 THEN 1 ELSE 0 END), 0) as wins,
                COALESCE(SUM(CASE WHEN net_pnl <= 0 THEN 1 ELSE 0 END), 0) as losses,
                COALESCE(AVG(net_pnl), 0) as avg_pnl,
                COALESCE(MAX(net_pnl), 0) as best_trade,
                COALESCE(MIN(net_pnl), 0) as worst_trade
            FROM algo_trades {where_clause}
        """)
        summary = cur.fetchone()
        conn.close()
        
        trades = []
        for row in rows:
            trades.append({
                "id": row[0],
                "strategy": row[1] or "AUTO",
                "symbol": row[2],
                "side": row[3],
                "entry_price": float(row[4]) if row[4] else 0,
                "exit_price": float(row[5]) if row[5] else 0,
                "pnl": float(row[6]) if row[6] else 0,
                "reason": row[7] or "SYSTEM",
                "qty": row[8] or 0,
                "timestamp": row[9].isoformat() if row[9] else None
            })
        
        total = summary[0] if summary else 0
        win_rate = round((summary[2] / total * 100), 1) if total > 0 else 0
        
        return {
            "status": "success",
            "range": range,
            "trades": trades,
            "summary": {
                "total_trades": total,
                "total_pnl": round(float(summary[1]), 2) if summary else 0,
                "wins": int(summary[2]) if summary else 0,
                "losses": int(summary[3]) if summary else 0,
                "win_rate": win_rate,
                "avg_pnl": round(float(summary[4]), 2) if summary else 0,
                "best_trade": round(float(summary[5]), 2) if summary else 0,
                "worst_trade": round(float(summary[6]), 2) if summary else 0
            }
        }
    except Exception as e:
        return {"status": "error", "trades": [], "summary": {}, "error": str(e)}

@app.get("/api/analytics/insights", include_in_schema=False)
async def get_analytics_insights():
    """Self-learning analytics: strategy performance, win patterns, feedback loop."""
    try:
        conn = psycopg2.connect(
            host="127.0.0.1", database=sys_config.POSTGRES_DB_RAW,
            user="postgres", password=sys_config.POSTGRES_PASSWORD_RAW
        )
        cur = conn.cursor()
        
        # 1. Strategy performance breakdown
        cur.execute("""
            SELECT strategy_id, 
                   COUNT(*) as trades,
                   ROUND(AVG(net_pnl)::numeric, 2) as avg_pnl,
                   ROUND(SUM(CASE WHEN net_pnl > 0 THEN 1 ELSE 0 END)::numeric / NULLIF(COUNT(*), 0) * 100, 1) as win_rate,
                   ROUND(SUM(net_pnl)::numeric, 2) as total_pnl
            FROM algo_trades 
            GROUP BY strategy_id
            ORDER BY total_pnl DESC
        """)
        strategy_perf = [{
            "strategy": r[0] or "UNKNOWN",
            "trades": r[1], "avg_pnl": float(r[2]),
            "win_rate": float(r[3]), "total_pnl": float(r[4])
        } for r in cur.fetchall()]
        
        # 2. Best performing symbols
        cur.execute("""
            SELECT instrument_id,
                   COUNT(*) as trades,
                   ROUND(SUM(net_pnl)::numeric, 2) as total_pnl,
                   ROUND(AVG(net_pnl)::numeric, 2) as avg_pnl
            FROM algo_trades
            GROUP BY instrument_id
            HAVING COUNT(*) >= 2
            ORDER BY total_pnl DESC
            LIMIT 10
        """)
        symbol_perf = [{
            "symbol": r[0], "trades": r[1],
            "total_pnl": float(r[2]), "avg_pnl": float(r[3])
        } for r in cur.fetchall()]
        
        # 3. Exit reason analysis
        cur.execute("""
            SELECT exit_reason,
                   COUNT(*) as count,
                   ROUND(AVG(net_pnl)::numeric, 2) as avg_pnl
            FROM algo_trades
            GROUP BY exit_reason
            ORDER BY count DESC
        """)
        exit_analysis = [{
            "reason": r[0] or "UNKNOWN",
            "count": r[1], "avg_pnl": float(r[2])
        } for r in cur.fetchall()]
        
        # 4. Daily PnL trend (last 30 days)
        cur.execute("""
            SELECT created_at::date as trade_date,
                   COUNT(*) as trades,
                   ROUND(SUM(net_pnl)::numeric, 2) as daily_pnl
            FROM algo_trades
            WHERE created_at >= NOW() - INTERVAL '30 days'
            GROUP BY created_at::date
            ORDER BY trade_date DESC
        """)
        daily_trend = [{
            "date": str(r[0]), "trades": r[1], "pnl": float(r[2])
        } for r in cur.fetchall()]
        
        # 5. Time-of-day analysis
        cur.execute("""
            SELECT EXTRACT(HOUR FROM created_at)::int as hour,
                   COUNT(*) as trades,
                   ROUND(AVG(net_pnl)::numeric, 2) as avg_pnl
            FROM algo_trades
            GROUP BY hour
            ORDER BY hour
        """)
        hourly = [{
            "hour": r[0], "trades": r[1], "avg_pnl": float(r[2])
        } for r in cur.fetchall()]
        
        conn.close()
        
        # 6. Self-learning recommendations
        recommendations = []
        for sp in strategy_perf:
            if sp["win_rate"] < 40 and sp["trades"] >= 5:
                recommendations.append(f"⚠️ Strategy '{sp['strategy']}' has {sp['win_rate']}% win rate over {sp['trades']} trades. Consider reducing allocation.")
            if sp["win_rate"] > 65 and sp["trades"] >= 5:
                recommendations.append(f"✅ Strategy '{sp['strategy']}' is performing well ({sp['win_rate']}% WR). Consider increasing allocation.")
        
        for ep in exit_analysis:
            if ep["reason"] in ("EXIT_SL", "STOPLOSS") and ep["avg_pnl"] < -100:
                recommendations.append(f"⚠️ Stop-loss exits averaging ₹{ep['avg_pnl']} loss. Consider widening initial SL.")
        
        best_hours = [h for h in hourly if h["avg_pnl"] > 0]
        worst_hours = [h for h in hourly if h["avg_pnl"] < 0 and h["trades"] >= 3]
        if worst_hours:
            worst = min(worst_hours, key=lambda x: x["avg_pnl"])
            recommendations.append(f"⚠️ Hour {worst['hour']}:00 IST has negative avg PnL (₹{worst['avg_pnl']}). Consider avoiding entries at this hour.")
        
        return {
            "strategy_performance": strategy_perf,
            "symbol_performance": symbol_perf,
            "exit_analysis": exit_analysis,
            "daily_trend": daily_trend,
            "hourly_analysis": hourly,
            "recommendations": recommendations
        }
    except Exception as e:
        return {"status": "error", "message": str(e)}

@app.get("/api/performance/stats", include_in_schema=False)
async def get_performance_stats():
    try:
        conn = psycopg2.connect(
            host="127.0.0.1", database=sys_config.POSTGRES_DB_RAW,
            user="postgres", password=sys_config.POSTGRES_PASSWORD_RAW
        )
        cur = conn.cursor()
        cur.execute("SELECT net_pnl FROM algo_trades WHERE exit_price IS NOT NULL")
        pnls = [float(r[0]) for r in cur.fetchall() if r[0] is not None]
        conn.close()

        if not pnls:
            return {"profit_factor": 0, "avg_winner": 0, "avg_loser": 0, "sharpe": 0}

        winners = [p for p in pnls if p > 0]
        losers = [p for p in pnls if p < 0]
        
        profit_factor = round(abs(sum(winners) / sum(losers)), 2) if losers else (1.0 if winners else 0)
        avg_winner = round(sum(winners) / len(winners), 2) if winners else 0
        avg_loser = round(sum(losers) / len(losers), 2) if losers else 0
        
        import numpy as np
        sharpe = round(np.mean(pnls) / np.std(pnls), 2) if len(pnls) > 1 and np.std(pnls) > 0 else 0

        return {
            "profit_factor": profit_factor,
            "avg_winner": avg_winner,
            "avg_loser": avg_loser,
            "sharpe": sharpe
        }
    except Exception as e:
        return {"status": "error", "message": str(e)}

from datetime import datetime, timedelta, timezone

def _get_today_id():
    """Compute fresh today_id every call to handle day-boundary transitions."""
    ist = timezone(timedelta(hours=5, minutes=30))
    today_start = datetime.now(ist).replace(hour=0, minute=0, second=0, microsecond=0)
    return f"{int(today_start.timestamp() * 1000)}-0"

# Initialize with fresh today_id — will be refreshed in the WebSocket loop
_initial_today_id = _get_today_id()
_last_today_id_date = datetime.now(timezone(timedelta(hours=5, minutes=30))).date()

last_ids = {
    "trade_stats": _initial_today_id,
    "risk_state": _initial_today_id,
    "alpha_signals": _initial_today_id,
    "stock_scanner": _initial_today_id,
    "active_positions": _initial_today_id,
    "vwap_retest_state": _initial_today_id,
    "regime_state": _initial_today_id
}

def _refresh_today_ids_if_needed():
    """Reset stream cursors to fresh today_id on day boundary."""
    global _last_today_id_date
    ist = timezone(timedelta(hours=5, minutes=30))
    current_date = datetime.now(ist).date()
    if current_date != _last_today_id_date:
        new_id = _get_today_id()
        for key in last_ids:
            last_ids[key] = new_id
        _last_today_id_date = current_date
        print(f"🔄 Dashboard stream cursors reset for new day: {current_date}")

@app.get("/api/scanner/results", include_in_schema=False)
async def get_scanner_results():
    """On-demand scanner results for dashboard tab load."""
    data = get_enriched_scanner_data()
    if data:
        return data
    return {"symbols": []}


@app.get("/api/reports/{timeframe}", include_in_schema=False)
async def get_reports(timeframe: str):
    """Fetch aggregated performance metrics from the DB."""
    try:
        if timeframe not in ["daily", "weekly", "monthly"]:
            return {"status": "error", "message": "Invalid timeframe"}
            
        if not db_pool:
            return {"status": "error", "message": "Database pool not initialized"}

        async with db_pool.acquire() as conn:
            if timeframe == "daily":
                rows = await conn.fetch("SELECT * FROM daily_stats ORDER BY report_date DESC LIMIT 100")
            elif timeframe == "weekly":
                rows = await conn.fetch("SELECT * FROM weekly_stats ORDER BY year DESC, week DESC LIMIT 12")
            else:
                rows = await conn.fetch("SELECT * FROM monthly_stats ORDER BY year DESC, month DESC LIMIT 12")
            
            return [dict(r) for r in rows]
    except Exception as e:
        print(f"Reports API Error: {e}")
        return {"status": "error", "message": str(e)}

async def read_stream(stream):
    global last_ids
    
    # Initialize ID if not present to prevent KeyError
    current_id = last_ids.get(stream, "0-0")

    try:
        data = r.xread({stream: current_id}, count=10, block=100)
        if not data:
            return None

        latest_payload = None
        for s, messages in data:
            for msg_id, payload in messages:
                last_ids[stream] = msg_id
                if "data" in payload:
                    latest_payload = json.loads(payload["data"])
        
        return latest_payload
    except Exception:
        # Log or handle Redis/Parsing errors silently to keep websocket alive
        return None


@app.websocket("/ws")

async def websocket_endpoint(ws: WebSocket):

    await ws.accept()
    scanner_refresh_counter = 0

    while True:
        try:
            # Refresh stream cursors if day has changed
            _refresh_today_ids_if_needed()

            # Read from multiple streams
            stats = await read_stream("trade_stats")
            risk = await read_stream("risk_state")
            signal = await read_stream("alpha_signals")
            scanner_trigger = await read_stream("stock_scanner")
            positions = await read_stream("active_positions")
            vwap_retest = await read_stream("vwap_retest_state")
            regime = await read_stream("regime_state")

            # Scanner: fetch from DB on trigger OR every 30 iterations (~30s)
            scanner = None
            scanner_refresh_counter += 1
            if scanner_trigger or scanner_refresh_counter >= 30:
                scanner = await asyncio.to_thread(get_enriched_scanner_data)
                scanner_refresh_counter = 0

            payload = {
                "stats": stats,
                "risk": risk,
                "signal": signal,
                "scanner": scanner,
                "positions": positions,
                "vwap_retest": vwap_retest,
                "regime": regime,
                "market": get_market_status()
            }

            # Only send if at least one item is present
            if any(payload.values()):
                await ws.send_json(payload)

            await asyncio.sleep(1)
        except Exception as e:
            print(f"WebSocket error: {e}")
            break