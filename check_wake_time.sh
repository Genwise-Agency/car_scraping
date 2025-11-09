#!/bin/bash
# Script to check when the Mac last woke up

echo "=== Last Wake Time Check ==="
echo ""

# Method 1: Check system uptime (time since last boot/wake)
echo "1. System Uptime (time since last boot/wake):"
uptime
echo ""

# Method 2: Check last boot time
echo "2. Last Boot Time:"
sysctl kern.boottime | awk '{print $4, $5, $6, $7, $8}'
echo ""

# Method 3: Check power management log for wake events
echo "3. Recent Wake Events (from power management log):"
pmset -g log | grep -i "wake from\|darkwake\|wake reason" | tail -5
echo ""

# Method 4: Check if there's a wake check file from our script
if [ -f ".bmw_last_wake_check" ]; then
    LAST_CHECK=$(cat .bmw_last_wake_check)
    LAST_CHECK_DATE=$(date -r "$LAST_CHECK" '+%Y-%m-%d %H:%M:%S')
    echo "4. Last time our script checked (may indicate wake):"
    echo "   $LAST_CHECK_DATE"
else
    echo "4. No wake check file found (script hasn't run yet)"
fi
echo ""

# Method 5: Check system sleep/wake statistics
echo "5. Sleep/Wake Statistics:"
pmset -g stats | grep -E "Sleep|Wake" | head -5

