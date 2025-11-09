#!/usr/bin/env python3
"""
Launchd wrapper for BMW scraping script
This Python wrapper avoids some macOS permission issues
"""
import os
import subprocess
import sys
import time
from pathlib import Path

SCRIPT_DIR = Path("/Users/ardonisshalaj/Documents/car_scraping")
os.chdir(SCRIPT_DIR)

LAST_CHECK_FILE = SCRIPT_DIR / ".bmw_last_wake_check"
RUN_LOG_FILE = SCRIPT_DIR / ".bmw_scraping_runs.log"
LOG_FILE = SCRIPT_DIR / "launchd.log"

CURRENT_TIME = int(time.time())
TODAY = time.strftime("%Y-%m-%d")

def log(message):
    """Write to log file"""
    with open(LOG_FILE, "a") as f:
        f.write(f"{time.strftime('%Y-%m-%d %H:%M:%S')} - {message}\n")

# Check if Mac just woke up (if last check was more than 30 minutes ago)
if LAST_CHECK_FILE.exists():
    LAST_CHECK = int(LAST_CHECK_FILE.read_text().strip())
    TIME_DIFF = CURRENT_TIME - LAST_CHECK

    # If less than 30 minutes passed, Mac is awake - skip wake detection
    if TIME_DIFF <= 1800:
        log("Mac is awake, skipping wake check")
        LAST_CHECK_FILE.write_text(str(CURRENT_TIME))
        sys.exit(0)

# Update last check time
LAST_CHECK_FILE.write_text(str(CURRENT_TIME))

# Count runs today
RUNS_TODAY = 0
if RUN_LOG_FILE.exists():
    with open(RUN_LOG_FILE) as f:
        RUNS_TODAY = sum(1 for line in f if line.startswith(TODAY))

# Check if we've already run twice today
if RUNS_TODAY >= 2:
    log(f"Skipping: Already ran {RUNS_TODAY} times today (max 2)")
    sys.exit(0)

# Log this run
with open(RUN_LOG_FILE, "a") as f:
    f.write(f"{TODAY} {time.strftime('%H:%M:%S')}\n")

# Run the actual scraping script
venv_python = SCRIPT_DIR / "venv" / "bin" / "python3"
if venv_python.exists():
    result = subprocess.run(
        [str(venv_python), "-m", "src.bmw.main"],
        cwd=str(SCRIPT_DIR),
        capture_output=False
    )
    sys.exit(result.returncode)
else:
    log("ERROR: Virtual environment not found")
    sys.exit(1)

