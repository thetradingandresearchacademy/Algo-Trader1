import importlib.util
from pathlib import Path
import urllib.parse

# Dynamically load from tara_config.py
_config_path = Path(__file__).parent.parent / "tara_config.py"
_spec = importlib.util.spec_from_file_location("tara_config", str(_config_path))
_tara_config = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(_tara_config)

ANGEL_API_KEY = getattr(_tara_config, "API_KEY", "")
ANGEL_CLIENT_CODE = getattr(_tara_config, "CLIENT_ID", "")
ANGEL_PASSWORD = getattr(_tara_config, "PWD", "")
ANGEL_TOTP_SECRET = getattr(_tara_config, "TOTP_KEY", "").replace(" ", "")
PAPER_TRADING = not getattr(_tara_config, "CONFIRM_REAL_TRADING", False)

# Market instruments (Expanded F&O Universe - Nifty 100+)
TOKENS = {
    "26000": "NIFTY", "26009": "BANKNIFTY", "26037": "SENSEX", "26017": "INDIAVIX",
    "11536": "RELIANCE", "1594": "INFY", "1333": "HDFC", "3045": "SBIN", "1270": "ICICIBANK",
    "1348": "HDFCBANK", "1922": "KOTAKBANK", "11630": "NTPC", "14366": "TECHM", "1363": "HCLTECH",
    "3787": "WIPRO", "10604": "BHARTIARTL", "1660": "ITC", "1394": "HINDUNILVR", "11483": "LT",
    "2031": "M&M", "3456": "TATAMOTORS", "10245": "MARUTI", "3351": "SUNPHARMA", "881": "DRREDDY",
    "694": "CIPLA", "1232": "HDFCLIFE", "7929": "SBILIFE", "11703": "ADANIENT", "15083": "ADANIPORTS",
    "1171": "GRASIM", "21808": "JSWSTEEL", "3499": "TATASTEEL", "471": "BEL", "2475": "ONGC",
    "526": "BPCL", "1107": "GAIL", "236": "ASIANPAINT", "3150": "TATACONSUM", "1512": "INDUSINDBK",
    "467": "AXISBANK", "11287": "POWERGRID", "17963": "NESTLEIND", "20374": "COALINDIA", "32105": "SBICARD",
    "10666": "BAJFINANCE", "16675": "BAJAJFINSV", "11654": "TITAN", "18391": "HEROMOTOCO",
    "11532": "TCS", "11262": "ULTRACEMCO", "10940": "SHREECEM", "11654": "TITAN", "11717": "HINDALCO",
    "15044": "VEDL", "15332": "NMDC", "3001": "PFC", "11543": "REC", "4306": "APOLLOHOSP",
    "1247": "ICICIPRULI", "3405": "TATAELXSI", "4610": "CANBK", "15201": "AUBANK", "14299": "DLF",
    "4963": "CHOLAFIN", "3506": "TIRUMALCHM", "1997": "LTIM", "14977": "PERSISTENT", "4244": "COFORGE",
    "11184": "MPHASIS", "1515": "DIVISLAB", "6733": "ABBOTINDIA", "1633": "IPCALAB", "1503": "AUROPHARMA",
    "2018": "LUPIN", "3006": "PIDILITIND", "4351": "PAGEIND", "315": "BAJAJ-AUTO", "2412": "EICHERMOT",
    "11023": "TVSMOTOR", "3502": "TATACOMM", "1160": "HINDPETRO", "2914": "IOC", "11351": "ONGC",
    "4391": "GMRINFRA", "11195": "TATACHEM", "11696": "SRF", "1558": "PIDILITIND", "19034": "PIIND",
    "3103": "UPL", "11114": "COROMANDEL", "11532": "TCS", "14366": "TECHM"
}

# Redis settings — WSL Ubuntu environment
REDIS_HOST = getattr(_tara_config, "REDIS_HOST", "172.28.140.29")
REDIS_PORT = 6379
PAPER_TRADING = not getattr(_tara_config, "CONFIRM_REAL_TRADING", False)

# Trading & Upload Settings
LIVE_TRADING = getattr(_tara_config, "LIVE_TRADING", False)
# Use project relative paths for uploads
_project_root = Path(__file__).parent.parent.parent
UPLOAD_DIR = _project_root / "data" / "uploads"
UPLOAD_DIR.mkdir(parents=True, exist_ok=True)

# Postgres settings (default to local intraday_fo db)
POSTGRES_USER = "postgres"
_raw_password = getattr(_tara_config, "DB_PASS", "")
POSTGRES_PASSWORD_RAW = _raw_password
POSTGRES_PASSWORD = urllib.parse.quote(_raw_password)
POSTGRES_HOST = "127.0.0.1"
POSTGRES_PORT = 5432
_raw_db = getattr(_tara_config, "DB_NAME", "intraday_fo")
POSTGRES_DB_RAW = _raw_db
POSTGRES_DB = urllib.parse.quote(_raw_db)
POSTGRES_DSN = f"postgresql://{POSTGRES_USER}:{POSTGRES_PASSWORD}@{POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DB}"