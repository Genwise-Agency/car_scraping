import os
from datetime import datetime
from pathlib import Path

from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# Detect Azure Functions environment
IS_AZURE = os.getenv("FUNCTIONS_WORKER_RUNTIME") is not None

# Supabase Configuration
SUPABASE_URL = os.getenv("SUPABASE_URL", "")
SUPABASE_KEY = os.getenv("SUPABASE_KEY", "")

# Scraping Configuration
BMW_URL = "https://www.bmw.be/fr-be/sl/stocklocator_uc/results?filters=%257B%2522MARKETING_MODEL_RANGE%2522%253A%255B%2522i4_G26E%2522%255D%252C%2522COLOR%2522%253A%255B%2522GRAY%2522%252C%2522BLACK%2522%255D%252C%2522USED_CAR_MILEAGE%2522%253A%255B0%252C20000%255D%252C%2522REGISTRATION_YEAR%2522%253A%255B2025%252C2025%255D%252C%2522EQUIPMENT_GROUPS%2522%253A%257B%2522favorites%2522%253A%255B%2522M%2520Sport%2520package%2522%255D%257D%257D"
HEADLESS_MODE = True
BROWSER_TIMEOUT = 10000

# File Paths - Azure Functions use /tmp for writable storage
if IS_AZURE:
    # In Azure Functions, use /tmp for file outputs (writable location)
    OUTPUT_DIR = "/tmp/results/bmw"
    # For preferences file, find project root (where data/ folder is)
    # In Azure: /home/site/wwwroot/src/bmw/config.py -> /home/site/wwwroot/data/
    # Go up from src/bmw/config.py to project root
    project_root = Path(__file__).parent.parent.parent
    PREFERENCES_FILE = str(project_root / "data" / "ardonis_bmw_preferences.json")
else:
    # Local development paths
    OUTPUT_DIR = "results/bmw"
    PREFERENCES_FILE = "data/ardonis_bmw_preferences.json"

# Tracking Columns
TRACKING_COLUMNS = [
    'car_id', 'model_name', 'price', 'kilometers', 'registration_date',
    'horse_power_kw', 'horse_power_ps', 'battery_range_km', 'equipments'
]

HISTORY_COLUMNS = [
    'car_id', 'model_name', 'price', 'kilometers', 'registration_date',
    'horse_power_kw', 'horse_power_ps', 'battery_range_km', 'equipments',
    'first_seen_date', 'last_seen_date', 'valid_from', 'valid_to',
    'is_latest', 'status', 'link', 'scrape_date'
]

EQUIPMENT_COLUMNS = [
    'car_id', 'category', 'equipment_name', 'valid_from', 'valid_to',
    'is_latest', 'scrape_date'
]

SCORES_COLUMNS = [
    'car_id', 'value_efficiency_score', 'age_usage_score',
    'performance_range_score', 'equipment_score', 'final_score', 'valid_from', 'valid_to',
    'is_latest', 'scrape_date'
]

# French month mapping
FRENCH_MONTHS = {
    'janvier': 1, 'février': 2, 'mars': 3, 'avril': 4,
    'mai': 5, 'juin': 6, 'juillet': 7, 'août': 8,
    'septembre': 9, 'octobre': 10, 'novembre': 11, 'décembre': 12
}
