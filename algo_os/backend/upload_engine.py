import os
import io
import tempfile
import pandas as pd
from datetime import datetime
from fastapi import APIRouter, UploadFile, File, Form, Body
from fastapi.responses import JSONResponse
from pydantic import BaseModel

from config.settings import UPLOAD_DIR, LIVE_TRADING
from backend.db import log_audit

upload_router = APIRouter()

def clean_numeric(val):
    if pd.isna(val):
        return 0.0
    if isinstance(val, str):
        val = val.replace(',', '').replace('%', '').strip()
    try:
        return float(val)
    except:
        return 0.0

def evaluate_oi_matrix(df, mode):
    # Ensure there's enough data for comparative analysis
    if df.empty or len(df) < 2:
        return {
            "action": "NO_TRADE",
            "confidence": 0.0,
            "reasons": ["Not enough data rows to evaluate trend."],
            "risk_flags": ["INSUFFICIENT_DATA"],
            "mode": mode
        }

    cols = [str(c).lower().strip().replace('.', '') for c in df.columns]
    df.columns = cols
    
    pcr_col = next((c for c in cols if 'pcr' in c), None)
    diff_oi_col = next((c for c in cols if 'diff' in c and 'oi' in c), None)
    dir_pct_col = next((c for c in cols if 'direction' in c and '%' in c), None)
    time_col = next((c for c in cols if 'time' in c), None)
    
    if not dir_pct_col:
        dir_pct_col = next((c for c in cols if 'direction of' in c), None)

    if not pcr_col or not diff_oi_col or not dir_pct_col:
        return {
            "action": "NO_TRADE",
            "confidence": 0.0,
            "reasons": [f"Missing required columns. Found: {list(cols)}"],
            "risk_flags": ["MISSING_METRICS"],
            "mode": mode
        }

    # We will assess the last 5 frames (15 minutes of action) rather than just the strict Top 1
    # This prevents missing a massive ignition bar that happened 6 mins ago.
    depth = min(5, len(df) - 1)
    
    latest_pcr_overall = clean_numeric(df.iloc[0][pcr_col])
    latest_dir_overall = clean_numeric(df.iloc[0][dir_pct_col])

    for i in range(depth):
        latest_row = df.iloc[i]
        prev_row = df.iloc[i+1]

        latest_pcr = clean_numeric(latest_row[pcr_col])
        prev_pcr = clean_numeric(prev_row[pcr_col])
        
        latest_diff_oi = clean_numeric(latest_row[diff_oi_col])
        prev_diff_oi = clean_numeric(prev_row[diff_oi_col])
        
        latest_dir_pct = clean_numeric(latest_row[dir_pct_col])
        
        time_str = latest_row[time_col] if time_col else f"Row {i}"

        # 1. ENTRY_LONG_CE 
        # Slightly relaxed thresholds: PCR > 1.2 & rising, Diff OI increasing, Direction > 3%
        if latest_pcr > 1.2 and latest_pcr > prev_pcr and latest_diff_oi > prev_diff_oi and latest_dir_pct > 3.0:
            return {
                "action": "ENTER_LONG_CE",
                "confidence": 0.85,
                "reasons": [
                    f"Ignition at {time_str}: PCR surged to {latest_pcr} (from {prev_pcr}).",
                    f"Diff in OI actively increased to {latest_diff_oi}.",
                    f"Direction heavily positive ({latest_dir_pct}%)."
                ],
                "risk_flags": [],
                "mode": mode
            }
            
        # 2. ENTRY_LONG_PE 
        # PCR falling sharply (>0.1 drop), Diff OI decreasing, Direction < -5%
        elif latest_pcr < prev_pcr - 0.1 and latest_diff_oi < prev_diff_oi and latest_dir_pct < -5.0:
            return {
                "action": "ENTER_LONG_PE",
                "confidence": 0.85,
                "reasons": [
                    f"Breakdown at {time_str}: PCR dropped to {latest_pcr} (from {prev_pcr}).",
                    f"Diff in OI collapsed to {latest_diff_oi}.",
                    f"Direction strongly negative ({latest_dir_pct}%)."
                ],
                "risk_flags": [],
                "mode": mode
            }
            
    # Fallback to Choppy / No Trade if NO ignition found in last 15 mins
    return {
        "action": "NO_TRADE",
        "confidence": 0.6,
        "reasons": [
            f"No strong signals in the last ~15 mins.",
            f"Current state -> PCR: {latest_pcr_overall}, Dir: {latest_dir_overall}%."
        ],
        "risk_flags": ["CHOPPY_RANGING"],
        "mode": mode
    }

@upload_router.post("/paste")
async def handle_paste(content: str = Body(..., media_type="text/plain", description="Paste your raw CSV/Excel data here")):
    mode = "LIVE" if LIVE_TRADING else "PAPER"
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    safe_filename = f"{timestamp}_pasted_data.txt"
    
    try:
        file_path = UPLOAD_DIR / safe_filename
        with open(file_path, "w", encoding="utf-8") as f:
            f.write(content)
            
        df = pd.read_csv(io.StringIO(content), sep='\t')
        row_count, col_count = df.shape
        
        decision = evaluate_oi_matrix(df, mode)
        
        log_audit(safe_filename, mode, row_count, col_count, decision["action"], decision)
        return JSONResponse(content=decision)
        
    except Exception as e:
        decision = {
            "action": "ERROR",
            "confidence": 0.0,
            "reasons": [f"Failed to process pasted data: {str(e)}"],
            "risk_flags": [],
            "mode": mode
        }
        log_audit(safe_filename, mode, 0, 0, "ERROR", decision)
        return JSONResponse(status_code=400, content=decision)

@upload_router.post("/upload")
async def handle_upload(file: UploadFile = File(...)):
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    file_extension = os.path.splitext(file.filename)[1]
    safe_filename = f"{timestamp}_{file.filename}"
    file_path = UPLOAD_DIR / safe_filename
    
    with open(file_path, "wb") as f:
        f.write(await file.read())
        
    mode = "LIVE" if LIVE_TRADING else "PAPER"
    
    if file_extension.lower() in [".xls", ".xlsx", ".csv"]:
        try:
            if file_extension.lower() == ".csv":
                df = pd.read_csv(file_path)
            else:
                df = pd.read_excel(file_path)
                
            row_count, col_count = df.shape
            
            decision = evaluate_oi_matrix(df, mode)
            
            log_audit(safe_filename, mode, row_count, col_count, decision["action"], decision)
            return JSONResponse(content=decision)
            
        except Exception as e:
            decision = {
                "action": "ERROR",
                "confidence": 0.0,
                "reasons": [f"Failed to process {file_extension} file: {str(e)}"],
                "risk_flags": [],
                "mode": mode
            }
            log_audit(safe_filename, mode, 0, 0, "ERROR", decision)
            return JSONResponse(status_code=400, content=decision)
            
    decision = {
        "action": "NO_TRADE",
        "confidence": 0.0,
        "reasons": [f"File {file.filename} saved. No tabular data to process."],
        "risk_flags": ["Unsupported format for auto-analysis"],
        "mode": mode
    }
    
    log_audit(safe_filename, mode, 0, 0, "NO_TRADE", decision)
    return JSONResponse(content=decision)

@upload_router.get("/analytics")
async def get_analytics():
    """Smart Analysis - Fetch Daily, Weekly, Monthly Trade Journal summary."""
    reports_dir = os.path.join(os.path.dirname(os.path.dirname(UPLOAD_DIR)), "reports")
    
    results = {}
    for report_type in ["daily", "weekly", "monthly"]:
        path = os.path.join(reports_dir, f"{report_type}_report.csv")
        if os.path.exists(path):
            try:
                df = pd.read_csv(path)
                results[report_type] = df.tail(10).to_dict(orient="records")
            except Exception as e:
                results[report_type] = f"Error reading: {e}"
        else:
            results[report_type] = "Not generated yet"
            
    return JSONResponse(content={"smart_analysis": results})
