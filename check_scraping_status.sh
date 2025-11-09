#!/bin/bash
# Script to check BMW scraping job status

cd /Users/ardonisshalaj/Documents/car_scraping

echo "=== BMW Scraping Job Status ==="
echo ""

# Check if launchd job is loaded
echo "1. Launchd Job Status:"
if launchctl list | grep -q "com.bmw.scraping"; then
    echo "   ✅ Job is loaded and active"
    launchctl list com.bmw.scraping
else
    echo "   ❌ Job is not loaded"
fi
echo ""

# Check daily run log
echo "2. Daily Run History:"
if [ -f ".bmw_scraping_runs.log" ]; then
    TODAY=$(date +%Y-%m-%d)
    RUNS_TODAY=$(grep -c "^$TODAY" .bmw_scraping_runs.log 2>/dev/null || echo "0")
    echo "   Runs today: $RUNS_TODAY/2"
    echo "   Recent runs:"
    tail -5 .bmw_scraping_runs.log | sed 's/^/     /'
else
    echo "   No runs recorded yet (script hasn't executed)"
fi
echo ""

# Check wake detection
echo "3. Wake Detection:"
if [ -f ".bmw_last_wake_check" ]; then
    LAST_CHECK=$(cat .bmw_last_wake_check)
    LAST_CHECK_DATE=$(date -r "$LAST_CHECK" '+%Y-%m-%d %H:%M:%S')
    CURRENT_TIME=$(date +%s)
    TIME_DIFF=$((CURRENT_TIME - LAST_CHECK))
    MINUTES_AGO=$((TIME_DIFF / 60))
    echo "   Last check: $LAST_CHECK_DATE ($MINUTES_AGO minutes ago)"
else
    echo "   No wake check file (job hasn't run yet)"
fi
echo ""

# Check if process is currently running
echo "4. Current Process:"
if ps aux | grep -i "src.bmw.main" | grep -v grep > /dev/null; then
    echo "   ✅ Scraping is currently running"
    ps aux | grep -i "src.bmw.main" | grep -v grep
else
    echo "   ⏸️  No scraping process running"
fi
echo ""

# Check logs
echo "5. Recent Logs:"
if [ -f "launchd.log" ] && [ -s "launchd.log" ]; then
    echo "   Launchd log (last 5 lines):"
    tail -5 launchd.log | sed 's/^/     /'
else
    echo "   No launchd log entries yet"
fi

if [ -f "launchd_error.log" ] && [ -s "launchd_error.log" ]; then
    echo "   Error log (last 5 lines):"
    tail -5 launchd_error.log | sed 's/^/     /'
fi
echo ""

# Check when job will run next
echo "6. Job Schedule:"
echo "   Job checks every 10 minutes for wake events"
echo "   Will run when Mac wakes up (if limit not reached)"

