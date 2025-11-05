#!/bin/bash
# Startup script for Azure Functions to install Playwright browsers
# This script runs when the function app starts

echo "Starting Playwright browser installation..."

# Set the browsers path to a writable location
export PLAYWRIGHT_BROWSERS_PATH=/home/site/wwwroot/.playwright

# Create the directory if it doesn't exist
mkdir -p $PLAYWRIGHT_BROWSERS_PATH

# Check if Chromium is already installed
if [ -d "$PLAYWRIGHT_BROWSERS_PATH/chromium" ]; then
    echo "Playwright browsers already installed, skipping..."
    exit 0
fi

# Install Chromium browser with dependencies
echo "Installing Chromium browser..."
python -m playwright install chromium --with-deps

# Verify installation
if [ $? -eq 0 ]; then
    echo "Playwright browsers installed successfully"
else
    echo "Warning: Playwright browser installation may have failed, but continuing..."
    exit 0  # Don't fail the startup if installation fails
fi

