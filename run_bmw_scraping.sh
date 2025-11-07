#!/bin/bash
# BMW scraping script with wake detection and daily limit (max 2 runs per day)

cd /Users/ardonisshalaj/Documents/car_scraping

# File to track last check time (for wake detection)
LAST_CHECK_FILE=".bmw_last_wake_check"
CURRENT_TIME=$(date +%s)

# File to track daily runs
RUN_LOG_FILE=".bmw_scraping_runs.log"
TODAY=$(date +%Y-%m-%d)

# Check if Mac just woke up (if last check was more than 30 minutes ago)
if [ -f "$LAST_CHECK_FILE" ]; then
    LAST_CHECK=$(cat "$LAST_CHECK_FILE")
    TIME_DIFF=$((CURRENT_TIME - LAST_CHECK))

    # If less than 30 minutes passed, Mac is awake - skip wake detection
    if [ "$TIME_DIFF" -le 1800 ]; then
        echo "$(date '+%Y-%m-%d %H:%M:%S') - Mac is awake, skipping wake check" >> launchd.log
        echo "$CURRENT_TIME" > "$LAST_CHECK_FILE"
        exit 0
    fi
fi

# Update last check time
echo "$CURRENT_TIME" > "$LAST_CHECK_FILE"

# Count runs today
if [ -f "$RUN_LOG_FILE" ]; then
    RUNS_TODAY=$(grep -c "^$TODAY" "$RUN_LOG_FILE" 2>/dev/null || echo "0")
else
    RUNS_TODAY=0
fi

# Check if we've already run twice today
if [ "$RUNS_TODAY" -ge 2 ]; then
    echo "$(date '+%Y-%m-%d %H:%M:%S') - Skipping: Already ran $RUNS_TODAY times today (max 2)" >> launchd.log
    exit 0
fi

# Log this run
echo "$TODAY $(date '+%H:%M:%S')" >> "$RUN_LOG_FILE"

# Run the actual scraping script
source venv/bin/activate
python3 -m src.bmw.main
deactivate
