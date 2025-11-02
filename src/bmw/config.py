import os
from datetime import datetime

from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# Supabase Configuration
SUPABASE_URL = os.getenv("SUPABASE_URL", "")
SUPABASE_KEY = os.getenv("SUPABASE_KEY", "")

# Scraping Configuration
BMW_URL = "https://www.bmw.be/fr-be/sl/stocklocator_uc/results"
HEADLESS_MODE = True
BROWSER_TIMEOUT = 10000

# File Paths
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
