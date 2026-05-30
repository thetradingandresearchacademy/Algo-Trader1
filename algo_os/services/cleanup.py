import os
import shutil
from datetime import datetime
from pathlib import Path

def cleanup_temp_files():
    """Delete temporary files, uploads, and old logs (excluding today's logs and db/data files)."""
    try:
        project_root = Path("D:/Algo Trader1")
        
        # 1. Clear uploads folder contents
        uploads_dir = project_root / "data" / "uploads"
        if uploads_dir.exists():
            for item in os.listdir(uploads_dir):
                path = uploads_dir / item
                try:
                    if path.is_file():
                        os.remove(path)
                    elif path.is_dir():
                        shutil.rmtree(path)
                except Exception as e:
                    print(f"Error removing upload item {item}: {e}")
            print("🧹 Cleaned up uploads folder.")
            
        # 2. Clear old logs (except today's date)
        logs_dir = project_root / "algo_os" / "logs"
        today_str = datetime.now().strftime("%Y-%m-%d")
        if logs_dir.exists():
            for item in os.listdir(logs_dir):
                if item == today_str:
                    continue  # Keep today's logs
                path = logs_dir / item
                try:
                    if path.is_dir():
                        shutil.rmtree(path)
                    elif path.is_file():
                        os.remove(path)
                except Exception as e:
                    print(f"Error removing log item {item}: {e}")
            print("🧹 Cleaned up old log folders.")
            
        # 3. Clean up __pycache__ folders recursively under algo_os
        algo_os_dir = project_root / "algo_os"
        if algo_os_dir.exists():
            for root, dirs, files in os.walk(algo_os_dir):
                if "__pycache__" in dirs:
                    pycache_path = Path(root) / "__pycache__"
                    try:
                        shutil.rmtree(pycache_path)
                    except Exception as e:
                        pass
            print("🧹 Cleaned up __pycache__ directories.")
            
    except Exception as e:
        print(f"⚠️ Error during cleanup: {e}")
