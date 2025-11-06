import logging
import os
import subprocess
import sys
from pathlib import Path

import azure.functions as func

# Configure logging for Azure Functions
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger(__name__)

# Install Playwright browsers if in Azure environment
# Note: This is attempted but may fail due to permissions/timeouts
# Better to install browsers manually via SSH before deployment
if os.getenv("FUNCTIONS_WORKER_RUNTIME"):
    playwright_browsers_path = os.getenv("PLAYWRIGHT_BROWSERS_PATH", "/home/site/wwwroot/.playwright")
    os.environ["PLAYWRIGHT_BROWSERS_PATH"] = playwright_browsers_path

    # Try to install browsers (may fail - that's okay, they should be pre-installed)
    try:
        logger.info("Checking Playwright browsers...")
        result = subprocess.run(
            ["playwright", "install", "chromium", "--with-deps"],
            check=False,
            capture_output=True,
            timeout=600,  # Increased timeout
            env={**os.environ, "PLAYWRIGHT_BROWSERS_PATH": playwright_browsers_path}
        )
        if result.returncode == 0:
            logger.info("Playwright browsers installed successfully")
        else:
            logger.warning(f"Playwright installation returned code {result.returncode}")
            logger.warning("Browsers may need to be installed manually via SSH")
    except subprocess.TimeoutExpired:
        logger.warning("Playwright installation timed out - browsers should be pre-installed")
    except Exception as e:
        logger.warning(f"Could not install Playwright browsers: {e}")
        logger.warning("Browsers should be installed manually via SSH before deployment")

# Add parent directories to path to import BMW scraping modules
# In Azure Functions, the function app root is typically /home/site/wwwroot
# The project structure is:
# /home/site/wwwroot/
#   - BmwScrapingFunction/
#     - __init__.py (this file)
#   - src/
#     - bmw/
#   - data/
function_app_root = Path(__file__).parent.parent
src_path = function_app_root / "src"

# Add paths to sys.path for imports
if function_app_root.exists():
    sys.path.insert(0, str(function_app_root))
if src_path.exists():
    sys.path.insert(0, str(src_path))


def BmwScrapingFunction(timer: func.TimerRequest) -> None:
    """
    Azure Function Timer trigger for BMW car scraping pipeline
    Runs at 10:00 AM and 3:00 PM Brussels time (CET/CEST)

    Schedule: 0 0 8,13 * * * (runs at 8:00 UTC and 13:00 UTC)
    - 8:00 UTC = 9:00 CET (winter) / 10:00 CEST (summer) - targets 10:00 CEST
    - 13:00 UTC = 14:00 CET (winter) / 15:00 CEST (summer) - targets 15:00 CEST

    We verify the actual Brussels time to ensure we only run at 10:00 and 15:00
    """
    from datetime import datetime

    import pytz

    # Get current time in Brussels timezone
    brussels_tz = pytz.timezone('Europe/Brussels')
    current_time = datetime.now(brussels_tz)
    current_hour = current_time.hour
    current_minute = current_time.minute

    logger.info("=" * 60)
    logger.info("BMW SCRAPING FUNCTION TRIGGERED (Timer)")
    logger.info(f"Current time (Brussels): {current_time.strftime('%Y-%m-%d %H:%M:%S %Z')}")
    logger.info("=" * 60)

    # Only run at 10:00 AM and 3:00 PM Brussels time (within 5 minute window)
    # This handles both CET and CEST timezone changes
    if not ((current_hour == 10 and current_minute < 5) or
            (current_hour == 15 and current_minute < 5)):
        logger.info(f"Skipping execution - current time is {current_hour:02d}:{current_minute:02d}, only runs at 10:00 and 15:00 Brussels time")
        return

    try:
        # Get configuration from environment variables
        url = os.getenv(
            "BMW_URL",
            "https://www.bmw.be/fr-be/sl/stocklocator_uc/results?filters=%257B%2522MARKETING_MODEL_RANGE%2522%253A%255B%2522i4_G26E%2522%255D%252C%2522COLOR%2522%253A%255B%2522GRAY%2522%252C%2522BLACK%2522%255D%252C%2522USED_CAR_MILEAGE%2522%253A%255B0%252C20000%255D%252C%2522REGISTRATION_YEAR%2522%253A%255B2025%252C2025%255D%252C%2522EQUIPMENT_GROUPS%2522%253A%257B%2522favorites%2522%253A%255B%2522M%2520Sport%2520package%2522%255D%257D%257D"
        )

        test_limit_str = os.getenv("TEST_LIMIT", "0")
        test_limit = int(test_limit_str) if test_limit_str and test_limit_str != "0" else None

        sync_db_str = os.getenv("SYNC_DB", "true")
        sync_db = sync_db_str.lower() == "true"

        logger.info(f"Configuration:")
        logger.info(f"  URL: {url}")
        logger.info(f"  Test limit: {test_limit}")
        logger.info(f"  Sync to DB: {sync_db}")

        # Import main inside function to avoid Azure Functions scanning it
        try:
            from bmw.main import main
        except ImportError as e:
            logger.error(f"Failed to import BMW scraping modules: {e}")
            logger.error(f"Function app root: {function_app_root}")
            logger.error(f"Python path: {sys.path}")
            raise

        # Run the main scraping pipeline
        main(url=url, test_limit=test_limit, sync_db=sync_db)

        logger.info("=" * 60)
        logger.info("BMW SCRAPING FUNCTION COMPLETED SUCCESSFULLY")
        logger.info("=" * 60)

    except Exception as e:
        error_msg = f"Error in BMW scraping function: {str(e)}"
        logger.error(error_msg, exc_info=True)
        raise  # Re-raise so Azure Functions logs the error

