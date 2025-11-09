#!/bin/sh
# Wrapper script that uses osascript to run the main script
# This may help with macOS permissions

SCRIPT_DIR="/Users/ardonisshalaj/Documents/car_scraping"
cd "$SCRIPT_DIR" || exit 1

# Use osascript to execute the script with proper permissions
osascript -e "do shell script \"cd '$SCRIPT_DIR' && bash '$SCRIPT_DIR/run_bmw_scraping.sh'\" with administrator privileges" 2>&1 || \
bash "$SCRIPT_DIR/run_bmw_scraping.sh"

