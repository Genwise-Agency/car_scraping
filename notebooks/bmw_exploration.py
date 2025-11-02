import json
import logging
import os
import re
from datetime import datetime

import pandas as pd
from playwright.sync_api import sync_playwright

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger(__name__)

# Historical tracking columns
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


def load_historical_data(history_file):
    """Load historical car data from CSV"""
    if os.path.exists(history_file):
        try:
            df = pd.read_csv(history_file, dtype={'car_id': 'Int64'})
            # Convert date columns to datetime
            for col in ['first_seen_date', 'last_seen_date', 'valid_from', 'valid_to', 'scrape_date']:
                if col in df.columns:
                    df[col] = pd.to_datetime(df[col], errors='coerce')
            logger.info(f"Loaded {len(df)} historical records from {history_file}")
            return df
        except Exception as e:
            logger.warning(f"Error loading history file: {e}. Starting fresh.")
            return pd.DataFrame(columns=HISTORY_COLUMNS)
    return pd.DataFrame(columns=HISTORY_COLUMNS)


def get_latest_records(history_df):
    """Get only the latest version of each car"""
    if history_df.empty:
        return history_df
    return history_df[history_df['is_latest'] == True].copy()


def compare_records(old_record, new_record, tracking_cols):
    """Check if any tracked columns have changed"""
    for col in tracking_cols:
        if col in ['price', 'kilometers', 'horse_power_kw', 'horse_power_ps', 'battery_range_km']:
            # Handle numeric comparisons with NaN
            old_val = old_record[col] if pd.notna(old_record[col]) else None
            new_val = new_record[col] if pd.notna(new_record[col]) else None
            if old_val != new_val:
                return True
        else:
            # String comparisons
            if str(old_record[col]) != str(new_record[col]):
                return True
    return False


def merge_historical_data(current_data, history_df, scrape_date):
    """Merge current scrape with historical data using SCD Type 2"""
    today = scrape_date.date()
    today_str = today.isoformat()

    # Get latest records
    latest_df = get_latest_records(history_df)

    # Ensure car_id is integer type
    current_data['car_id'] = current_data['car_id'].astype('Int64')
    if not latest_df.empty:
        latest_df['car_id'] = latest_df['car_id'].astype('Int64')

    new_records = []
    processed_ids = set()

    logger.info("=" * 60)
    logger.info("HISTORICAL DATA MERGE")
    logger.info("=" * 60)

    # Process current data
    for idx, row in current_data.iterrows():
        car_id = row['car_id']
        processed_ids.add(car_id)

        # Check if car exists in latest history
        old_record = latest_df[latest_df['car_id'] == car_id]

        if old_record.empty:
            # New car
            logger.info(f"[NEW] Car ID {car_id}: {row['model_name']}")
            new_row = row.to_dict()
            new_row.update({
                'first_seen_date': today_str,
                'last_seen_date': today_str,
                'valid_from': today_str,
                'valid_to': None,
                'is_latest': True,
                'status': 'active',
                'scrape_date': today_str
            })
            new_records.append(new_row)
        else:
            old_record = old_record.iloc[0]

            # Check if values changed
            if compare_records(old_record, row, TRACKING_COLUMNS):
                # Values changed - end old record
                logger.info(f"[CHANGED] Car ID {car_id}: {row['model_name']}")

                # Mark old record as not latest
                old_row = history_df[
                    (history_df['car_id'] == car_id) &
                    (history_df['is_latest'] == True)
                ].iloc[0].to_dict()
                old_row['valid_to'] = today_str
                old_row['is_latest'] = False
                new_records.append(old_row)

                # Add new version
                new_row = row.to_dict()
                new_row.update({
                    'first_seen_date': old_record['first_seen_date'],
                    'last_seen_date': today_str,
                    'valid_from': today_str,
                    'valid_to': None,
                    'is_latest': True,
                    'status': 'active',
                    'scrape_date': today_str
                })
                new_records.append(new_row)
            else:
                # No changes - just update last_seen_date and scrape_date
                logger.info(f"[UPDATED] Car ID {car_id}: {row['model_name']}")
                old_row = old_record.to_dict()
                old_row['last_seen_date'] = today_str
                old_row['scrape_date'] = today_str
                new_records.append(old_row)

    # Mark disappeared cars as sold
    if not latest_df.empty:
        disappeared_cars = latest_df[
            ~latest_df['car_id'].isin(processed_ids) &
            (latest_df['status'] == 'active')
        ]

        for idx, old_record in disappeared_cars.iterrows():
            car_id = old_record['car_id']
            logger.info(f"[SOLD/REMOVED] Car ID {car_id}: {old_record['model_name']}")

            # Mark as sold
            old_row = old_record.to_dict()
            old_row['valid_to'] = today_str
            old_row['is_latest'] = False
            old_row['status'] = 'sold'
            new_records.append(old_row)

    # Combine old history (non-latest) with new records
    old_history = history_df[history_df['is_latest'] == False].copy() if not history_df.empty else pd.DataFrame(columns=HISTORY_COLUMNS)

    # Create new history dataframe
    new_records_df = pd.DataFrame(new_records)

    if not old_history.empty:
        merged_history = pd.concat([old_history, new_records_df], ignore_index=True)
    else:
        merged_history = new_records_df

    logger.info("=" * 60)
    logger.info(f"Summary: {len(new_records)} records in current state")
    logger.info(f"Total historical records: {len(merged_history)}")
    logger.info("=" * 60)

    return merged_history


def extract_equipment_from_json(car_id, equipments_json, valid_from, valid_to, is_latest, scrape_date):
    """Extract equipment items from JSON and create normalized records"""
    equipment_records = []

    if not equipments_json:
        return equipment_records

    try:
        equipment_data = json.loads(equipments_json) if isinstance(equipments_json, str) else equipments_json

        for category, equipment_list in equipment_data.items():
            if equipment_list:
                for equipment_name in equipment_list:
                    equipment_records.append({
                        'car_id': car_id,
                        'category': category,
                        'equipment_name': equipment_name,
                        'valid_from': valid_from,
                        'valid_to': valid_to,
                        'is_latest': is_latest,
                        'scrape_date': scrape_date
                    })
    except Exception as e:
        logger.warning(f"Error parsing equipment JSON for car {car_id}: {e}")

    return equipment_records


def load_equipment_history(equipment_file):
    """Load historical equipment data from CSV"""
    if os.path.exists(equipment_file):
        try:
            df = pd.read_csv(equipment_file, dtype={'car_id': 'Int64'})
            # Convert date columns to datetime
            for col in ['valid_from', 'valid_to', 'scrape_date']:
                if col in df.columns:
                    df[col] = pd.to_datetime(df[col], errors='coerce')
            logger.info(f"Loaded {len(df)} equipment records from {equipment_file}")
            return df
        except Exception as e:
            logger.warning(f"Error loading equipment file: {e}. Starting fresh.")
            return pd.DataFrame(columns=EQUIPMENT_COLUMNS)
    return pd.DataFrame(columns=EQUIPMENT_COLUMNS)


def merge_equipment_history(car_history_df, equipment_history_df, scrape_date):
    """Merge equipment data from car history with equipment history"""
    today = scrape_date.date()
    today_str = today.isoformat()

    # Get latest car records (to extract current equipment)
    latest_cars = get_latest_records(car_history_df)

    # Extract equipment from latest car records
    new_equipment_records = []

    logger.info("=" * 60)
    logger.info("PROCESSING EQUIPMENT DATA...")
    logger.info("=" * 60)

    for idx, car_row in latest_cars.iterrows():
        car_id = car_row['car_id']
        equipments_json = car_row.get('equipments')
        valid_from = car_row['valid_from']
        valid_to = car_row['valid_to']
        is_latest = car_row['is_latest']
        scrape_date_str = car_row['scrape_date']

        # Convert dates to string if needed
        if pd.notna(valid_from):
            valid_from = valid_from.isoformat() if hasattr(valid_from, 'isoformat') else str(valid_from)
        if pd.notna(valid_to):
            valid_to = valid_to.isoformat() if hasattr(valid_to, 'isoformat') else str(valid_to)
        else:
            valid_to = None
        if pd.notna(scrape_date_str):
            scrape_date_str = scrape_date_str.isoformat() if hasattr(scrape_date_str, 'isoformat') else str(scrape_date_str)

        equipment_items = extract_equipment_from_json(
            car_id, equipments_json, valid_from, valid_to, is_latest, scrape_date_str
        )
        new_equipment_records.extend(equipment_items)

    # Create new equipment dataframe
    new_equipment_df = pd.DataFrame(new_equipment_records)

    if new_equipment_df.empty:
        logger.info("      No equipment records to process")
        return equipment_history_df if not equipment_history_df.empty else pd.DataFrame(columns=EQUIPMENT_COLUMNS)

    # Get current equipment for each car (latest records)
    car_ids = new_equipment_df['car_id'].unique()

    if not equipment_history_df.empty:
        current_equipment = equipment_history_df[equipment_history_df['is_latest'] == True].copy()
        merged_equipment_records = []

        # Keep old non-latest records
        old_equipment = equipment_history_df[equipment_history_df['is_latest'] == False].copy()
        if not old_equipment.empty:
            merged_equipment_records.append(old_equipment)

        existing_car_ids = set(current_equipment['car_id'].unique()) if not current_equipment.empty else set()

        for car_id in car_ids:
            car_new_equipment = new_equipment_df[new_equipment_df['car_id'] == car_id]
            car_old_equipment = current_equipment[current_equipment['car_id'] == car_id] if car_id in existing_car_ids else pd.DataFrame()

            if car_old_equipment.empty:
                # New car - add all equipment
                merged_equipment_records.append(car_new_equipment)
            else:
                # Create sets for comparison
                new_set = set()
                for _, row in car_new_equipment.iterrows():
                    new_set.add((row['category'], row['equipment_name']))

                old_set = set()
                for _, row in car_old_equipment.iterrows():
                    old_set.add((row['category'], row['equipment_name']))

                # Mark old equipment as not latest if car equipment changed
                if new_set != old_set:
                    # End old equipment records
                    for _, old_row in car_old_equipment.iterrows():
                        old_record = old_row.to_dict()
                        old_record['valid_to'] = today_str
                        old_record['is_latest'] = False
                        merged_equipment_records.append(old_record)

                    # Add new equipment records
                    merged_equipment_records.append(car_new_equipment)
                else:
                    # No change - just update scrape_date
                    for _, old_row in car_old_equipment.iterrows():
                        old_record = old_row.to_dict()
                        old_record['scrape_date'] = today_str
                        merged_equipment_records.append(old_record)

        # Combine all records
        if merged_equipment_records:
            merged_equipment = pd.concat(merged_equipment_records, ignore_index=True)
        else:
            merged_equipment = new_equipment_df
    else:
        # First time - no history
        merged_equipment = new_equipment_df

    logger.info(f"      Processed equipment for {len(car_ids)} cars")
    logger.info(f"      Total equipment records: {len(merged_equipment)}")
    logger.info("=" * 60)

    return merged_equipment


#url = "https://www.bmw.be/fr-be/sl/stocklocator_uc/results?filters=%257B%2522MARKETING_MODEL_RANGE%2522%253A%255B%2522i4_G26E%2522%252C%2522i5_G61E%2522%252C%2522i5_G60E%2522%255D%252C%2522PRICE%2522%253A%255Bnull%252C60000%255D%252C%2522REGISTRATION_YEAR%2522%253A%255B2024%252C-1%255D%252C%2522EQUIPMENT_GROUPS%2522%253A%257B%2522Default%2522%253A%255B%2522M%2520leather%2520steering%2520wheel%2522%255D%252C%2522favorites%2522%253A%255B%2522M%2520Sport%2520package%2522%255D%257D%257D"

# avec toit ouvrant
url="https://www.bmw.be/fr-be/sl/stocklocator_uc/results?filters=%257B%2522MARKETING_MODEL_RANGE%2522%253A%255B%2522i4_G26E%2522%252C%2522i5_G61E%2522%252C%2522i5_G60E%2522%255D%252C%2522PRICE%2522%253A%255Bnull%252C60000%255D%252C%2522REGISTRATION_YEAR%2522%253A%255B2024%252C-1%255D%252C%2522EQUIPMENT_GROUPS%2522%253A%257B%2522Default%2522%253A%255B%2522M%2520leather%2520steering%2520wheel%2522%252C%2522Sun%2520roof%2522%255D%252C%2522favorites%2522%253A%255B%2522M%2520Sport%2520package%2522%255D%257D%257D"

def parse_price(price_str):
    """Convert price string like '59 950,00 €' to float like 59950.0"""
    if not price_str:
        return None
    try:
        # Remove currency symbol and all whitespace (including non-breaking spaces)
        cleaned = price_str.replace('€', '').strip()
        # Remove all whitespace characters (spaces, non-breaking spaces, etc.)
        cleaned = re.sub(r'\s+', '', cleaned)
        # Replace comma with dot for decimal separator
        cleaned = cleaned.replace(',', '.')
        # Remove any remaining non-numeric characters except dot and minus
        cleaned = re.sub(r'[^\d\.\-]', '', cleaned)
        return float(cleaned)
    except Exception as e:
        return None


def parse_kilometers(km_str):
    """Convert kilometers string like '9500 km' to integer like 9500"""
    if not km_str:
        return None
    # Extract numbers only
    numbers = re.findall(r'\d+', km_str.replace(' ', ''))
    if numbers:
        try:
            return int(numbers[0])
        except:
            return None
    return None


def parse_car_id(car_id_str):
    """Convert car ID string to integer"""
    if not car_id_str:
        return None
    try:
        return int(car_id_str.strip())
    except:
        return None


def parse_horse_power(power_str):
    """Extract kW and PS from power string like '210 kW (286 PS)'"""
    if not power_str:
        return None, None
    # Extract kW value
    kw_match = re.search(r'(\d+)\s*kW', power_str)
    kw = int(kw_match.group(1)) if kw_match else None
    # Extract PS value
    ps_match = re.search(r'\((\d+)\s*PS\)', power_str)
    ps = int(ps_match.group(1)) if ps_match else None
    return kw, ps


def parse_battery_range(range_str):
    """Extract battery range from string like '475 km' to integer like 475"""
    if not range_str:
        return None
    # Extract numbers only
    numbers = re.findall(r'\d+', range_str.replace(' ', ''))
    if numbers:
        try:
            return int(numbers[0])
        except:
            return None
    return None


def parse_registration_date(date_str):
    """Convert French date string like 'août 2025' to datetime object"""
    if not date_str:
        return None

    # French month names mapping
    french_months = {
        'janvier': 1, 'février': 2, 'mars': 3, 'avril': 4,
        'mai': 5, 'juin': 6, 'juillet': 7, 'août': 8,
        'septembre': 9, 'octobre': 10, 'novembre': 11, 'décembre': 12
    }

    try:
        # Extract month and year
        parts = date_str.strip().lower().split()
        if len(parts) >= 2:
            month_name = parts[0]
            year = int(parts[1])

            if month_name in french_months:
                month = french_months[month_name]
                # Create datetime object (using first day of month)
                return datetime(year, month, 1)
        return None
    except Exception as e:
        return None

logger.info("=" * 60)
logger.info("Starting BMW car scraping script")
logger.info("=" * 60)

with sync_playwright() as p:
    logger.info("[1/4] Launching browser...")
    browser = p.chromium.launch(headless=False)
    page = browser.new_page()

    logger.info(f"[2/4] Navigating to URL...")
    logger.info(f"      {url[:80]}...")
    page.goto(url)

    # Wait for and click the accept cookies button
    logger.info("[3/4] Waiting for cookies popup...")
    accept_button = page.get_by_role("button", name="Tout accepter")
    accept_button.wait_for(state='visible', timeout=10000)
    logger.info("      ✓ Cookies popup found, accepting...")
    accept_button.click()

    # Wait for page to load after accepting cookies
    page.wait_for_timeout(2000)
    logger.info("      ✓ Cookies accepted, page loaded")

    # Scroll down and click "Montrer plus" button until it's no longer visible
    logger.info("[4/4] Loading all car listings...")
    show_more_button = page.locator('[data-test="stolo-plp-show-more-button"]')
    click_count = 0

    while True:
        # Scroll down to load more content
        page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
        page.wait_for_timeout(2000)

        # Try to find and click the button
        try:
            show_more_button.wait_for(state='visible', timeout=5000)
            show_more_button.scroll_into_view_if_needed()
            click_count += 1
            logger.info(f"      → Clicking 'Montrer plus' button (click #{click_count})...")
            show_more_button.click()
            # Wait for content to load after clicking
            page.wait_for_timeout(3000)
            logger.info(f"      ✓ Content loaded (click #{click_count} completed)")
        except:
            # Button is no longer visible or doesn't exist, we're done
            logger.info(f"      ✓ No more 'Montrer plus' buttons found. Total clicks: {click_count}")
            break

    # Extract all model card links
    logger.info("[Extracting links] Finding all car detail links...")
    model_card_links = page.locator('a.model-card-link')
    links = []

    count = model_card_links.count()
    logger.info(f"      Found {count} model card elements")

    for i in range(count):
        href = model_card_links.nth(i).get_attribute('href')
        if href:
            # Construct full URL if it's a relative path
            if href.startswith('/'):
                full_url = f"https://www.bmw.be{href}"
            else:
                full_url = href
            links.append(full_url)

        # Progress indicator every 10 links
        if (i + 1) % 10 == 0:
            logger.info(f"      Processed {i + 1}/{count} links...")

    logger.info("=" * 60)
    logger.info(f"SUMMARY: Found {len(links)} car detail links")
    logger.info("=" * 60)
    for i, link in enumerate(links, 1):
        logger.debug(f"{i:3d}. {link}")

    # Function to extract car data from a single page
    def extract_car_data(page, link):
        """Extract all car information from a detail page"""
        car_data = {}

        # Navigate to car detail page
        page.goto(link)
        page.wait_for_timeout(3000)

        # Check if cookies need to be accepted
        try:
            accept_button = page.get_by_role("button", name="Tout accepter")
            if accept_button.is_visible(timeout=2000):
                accept_button.click()
                page.wait_for_timeout(2000)
        except:
            pass  # No cookies popup, continue

        # Model name
        try:
            model_name = page.locator('h1#stock-locator__details-heading-1').inner_text()
            car_data['model_name'] = model_name.strip()
            logger.info(f"      → model_name: {car_data['model_name']}")
        except Exception as e:
            car_data['model_name'] = None
            logger.warning(f"      → model_name: Not found ({str(e)})")

        # Car ID
        try:
            car_id_element = page.locator('div.vehicle-intro__vin')
            car_id_text = car_id_element.inner_text()
            car_id_raw = car_id_text.replace('CAR-ID', '').strip()
            car_data['car_id'] = parse_car_id(car_id_raw)
            logger.info(f"      → car_id: {car_data['car_id']} (raw: {car_id_raw})")
        except Exception as e:
            car_data['car_id'] = None
            logger.warning(f"      → car_id: Not found ({str(e)})")

        # Price
        try:
            price_element = page.locator('div.subtitle-0.price strong')
            price_text = price_element.inner_text().strip()
            car_data['price_raw'] = price_text
            car_data['price'] = parse_price(price_text)
            logger.info(f"      → price: {car_data['price']} (raw: {car_data['price_raw']})")
        except Exception as e:
            car_data['price_raw'] = None
            car_data['price'] = None
            logger.warning(f"      → price: Not found ({str(e)})")

        # Link
        car_data['link'] = link
        logger.info(f"      → link: {link}")

        # Kilometers
        try:
            # Wait for the kilometers key-fact to be visible
            mileage_key_fact = page.locator('#stock-locator__key-facts-section div.key-fact[title="Kilomètres"]')
            mileage_key_fact.wait_for(state='visible', timeout=5000)
            # Get the value from the nested div
            mileage_value = mileage_key_fact.locator('div.value-disclaimer div.value.caption').inner_text().strip()
            if not mileage_value:
                # Fallback: try direct child selector
                mileage_value = mileage_key_fact.locator('div.value.caption').inner_text().strip()
            car_data['kilometers_raw'] = mileage_value
            car_data['kilometers'] = parse_kilometers(mileage_value)
            logger.info(f"      → kilometers: {car_data['kilometers']} (raw: {car_data['kilometers_raw']})")
        except Exception as e:
            car_data['kilometers_raw'] = None
            car_data['kilometers'] = None
            logger.warning(f"      → kilometers: Not found ({str(e)})")

        # Registration date
        try:
            registration_key_fact = page.locator('#stock-locator__key-facts-section div.key-fact[title="Date d\'immatriculation"]')
            registration_key_fact.wait_for(state='visible', timeout=5000)
            registration_value = registration_key_fact.locator('div.value-disclaimer div.value.caption').inner_text().strip()
            if not registration_value:
                registration_value = registration_key_fact.locator('div.value.caption').inner_text().strip()
            car_data['registration_date_raw'] = registration_value
            car_data['registration_date'] = parse_registration_date(registration_value)
            logger.info(f"      → registration_date: {car_data['registration_date']} (raw: {car_data['registration_date_raw']})")
        except Exception as e:
            car_data['registration_date_raw'] = None
            car_data['registration_date'] = None
            logger.warning(f"      → registration_date: Not found ({str(e)})")

        # Horse power
        try:
            power_key_fact = page.locator('#stock-locator__key-facts-section div.key-fact[title="Power Based on Degree of Electrification"]')
            power_key_fact.wait_for(state='visible', timeout=5000)
            power_value = power_key_fact.locator('div.value-disclaimer div.value.caption').inner_text().strip()
            if not power_value:
                power_value = power_key_fact.locator('div.value.caption').inner_text().strip()
            car_data['horse_power_raw'] = power_value
            kw, ps = parse_horse_power(power_value)
            car_data['horse_power_kw'] = kw
            car_data['horse_power_ps'] = ps
            logger.info(f"      → horse_power_kw: {car_data['horse_power_kw']}, horse_power_ps: {car_data['horse_power_ps']} (raw: {car_data['horse_power_raw']})")
        except Exception as e:
            car_data['horse_power_raw'] = None
            car_data['horse_power_kw'] = None
            car_data['horse_power_ps'] = None
            logger.warning(f"      → horse_power: Not found ({str(e)})")

        # Battery range (Autonomie électrique)
        try:
            # Find the technical data table row containing the battery range
            battery_range_container = page.locator('div[data-technical-data-key="wltpPureElectricRangeCombinedKilometer"]').locator('xpath=ancestor::div[contains(@class, "technical-data_table")]')
            battery_range_container.wait_for(state='visible', timeout=5000)
            # Get the value from the headline-5 div within the same container
            battery_range_value = battery_range_container.locator('div.headline-5 span').inner_text().strip()
            if not battery_range_value:
                # Fallback: try direct sibling
                battery_range_label = page.locator('div[data-technical-data-key="wltpPureElectricRangeCombinedKilometer"]')
                battery_range_value = battery_range_label.locator('xpath=following-sibling::div[contains(@class, "headline-5")]//span').inner_text().strip()
            car_data['battery_range_raw'] = battery_range_value
            car_data['battery_range_km'] = parse_battery_range(battery_range_value)
            logger.info(f"      → battery_range_km: {car_data['battery_range_km']} (raw: {car_data['battery_range_raw']})")
        except Exception as e:
            car_data['battery_range_raw'] = None
            car_data['battery_range_km'] = None
            logger.warning(f"      → battery_range: Not found ({str(e)})")

        # Extract equipment information
        equipment_data = {}
        try:
            # Look for all equipment sections (section-7, section-8, etc.)
            # Use the class selector to find all equipment sections
            equipment_sections = page.locator('section.equipment-section-container')
            section_count = equipment_sections.count()

            # Process all equipment sections found on the page
            for section_idx in range(section_count):
                try:
                    equipment_section = equipment_sections.nth(section_idx)
                    accordion_panels = equipment_section.locator('neo-accordion-panel')
                    panel_count = accordion_panels.count()

                    for i in range(panel_count):
                        panel = accordion_panels.nth(i)
                        try:
                            header = panel.locator('.content-header')
                            category_name = header.locator('.header-label').inner_text().strip()
                            equipment_items = panel.locator('div.details-card')
                            item_count = equipment_items.count()

                            equipment_list = []
                            for j in range(item_count):
                                item = equipment_items.nth(j)
                                equipment_name = item.locator('div.headline-7.tw-mb-ng-300').inner_text().strip()
                                if equipment_name:
                                    equipment_list.append(equipment_name)

                            # If category already exists, merge the lists (to handle duplicates across sections)
                            if category_name and equipment_list:
                                if category_name in equipment_data:
                                    # Merge lists, avoiding duplicates
                                    existing_items = set(equipment_data[category_name])
                                    new_items = [item for item in equipment_list if item not in existing_items]
                                    equipment_data[category_name].extend(new_items)
                                else:
                                    equipment_data[category_name] = equipment_list
                        except Exception as e:
                            continue
                except Exception as e:
                    continue

            car_data['equipments'] = json.dumps(equipment_data, ensure_ascii=False, indent=2) if equipment_data else None
            if car_data['equipments']:
                equipment_count = sum(len(items) for items in equipment_data.values())
                logger.info(f"      → equipments: Found {len(equipment_data)} categories with {equipment_count} total items")
            else:
                logger.warning(f"      → equipments: Not found")
        except Exception as e:
            car_data['equipments'] = None
            logger.warning(f"      → equipments: Error extracting ({str(e)})")

        return car_data

    # Process car links (testing with first 10)
    logger.info("=" * 60)
    logger.info("PROCESSING CARS (TESTING WITH FIRST 10)...")
    logger.info("=" * 60)

    # Limit to first 10 links for testing
    #test_links = links[:10]
    test_links = links
    logger.info(f"Processing {len(test_links)} out of {len(links)} total links")

    all_cars_data = []

    for idx, link in enumerate(test_links, 1):
        logger.info(f"[{idx}/{len(test_links)}] Processing car {idx}...")
        logger.info(f"      Link: {link[:80]}...")

        try:
            car_data = extract_car_data(page, link)
            all_cars_data.append(car_data)
            logger.info(f"      ✓ Car {idx} data extracted successfully")
            if car_data.get('model_name'):
                logger.info(f"      → Model: {car_data['model_name']}")
        except Exception as e:
            logger.error(f"      ✗ Error processing car {idx}: {str(e)}")
            # Still add a record with link and error info
            error_data = {'link': link, 'error': str(e)}
            all_cars_data.append(error_data)

        # Small delay between requests
        page.wait_for_timeout(1000)

    logger.info(f"      ✓ Successfully processed {len(all_cars_data)} cars")

    # Create pandas DataFrame from all cars
    df = pd.DataFrame(all_cars_data)

    # Reorder columns for better readability
    column_order = [
        'model_name', 'car_id', 'price', 'price_raw',
        'kilometers', 'kilometers_raw',
        'registration_date', 'registration_date_raw',
        'horse_power_kw', 'horse_power_ps', 'horse_power_raw',
        'battery_range_km', 'battery_range_raw',
        'equipments', 'link'
    ]
    # Only include columns that exist
    existing_columns = [col for col in column_order if col in df.columns]
    df = df[existing_columns]

    # Display summary
    logger.info("=" * 60)
    logger.info("DATA SUMMARY:")
    logger.info("=" * 60)
    logger.info(f"Total cars processed: {len(df)}")
    logger.info(f"Total columns: {len(df.columns)}")

    if len(df) > 0:
        logger.info("Sample data (first car):")
        if 'model_name' in df.columns:
            logger.info(f"  Model: {df.iloc[0].get('model_name', 'N/A')}")
        if 'price' in df.columns:
            logger.info(f"  Price: {df.iloc[0].get('price', 'N/A')}")
        if 'kilometers' in df.columns:
            logger.info(f"  Kilometers: {df.iloc[0].get('kilometers', 'N/A')}")

    logger.info(f"DataFrame shape: {df.shape}")
    logger.info(f"Columns: {', '.join(df.columns.tolist())}")

    # Historical tracking
    logger.info("=" * 60)
    logger.info("HISTORICAL DATA TRACKING...")
    logger.info("=" * 60)

    # Create results/bmw directory if it doesn't exist
    output_dir = "results/bmw"
    os.makedirs(output_dir, exist_ok=True)
    logger.info(f"      ✓ Directory created/verified: {output_dir}")

    # Keep tracking columns + link for historical data
    tracking_cols_with_link = TRACKING_COLUMNS + ['link']
    df_tracking = df[tracking_cols_with_link].copy()

    # Load historical data
    history_file = f"{output_dir}/bmw_cars_history.csv"
    history_df = load_historical_data(history_file)

    # Merge current data with history using SCD Type 2
    scrape_date = datetime.now()
    merged_history = merge_historical_data(df_tracking, history_df, scrape_date)

    # Save merged history to CSV
    try:
        # Convert datetime columns to string format for CSV
        df_history_export = merged_history.copy()
        for col in ['first_seen_date', 'last_seen_date', 'valid_from', 'valid_to', 'scrape_date']:
            if col in df_history_export.columns:
                df_history_export[col] = df_history_export[col].astype(str)

        df_history_export.to_csv(history_file, index=False)
        logger.info(f"      ✓ Historical data saved: {history_file}")
        logger.info(f"      ✓ Total historical records: {len(df_history_export)}")
    except Exception as e:
        logger.error(f"      ✗ Error saving history: {str(e)}")

    # Process equipment history
    equipment_file = f"{output_dir}/bmw_cars_equipment_history.csv"
    equipment_history_df = load_equipment_history(equipment_file)
    merged_equipment = merge_equipment_history(merged_history, equipment_history_df, scrape_date)

    # Save equipment history to CSV
    try:
        # Convert datetime columns to string format for CSV
        df_equipment_export = merged_equipment.copy()
        for col in ['valid_from', 'valid_to', 'scrape_date']:
            if col in df_equipment_export.columns:
                df_equipment_export[col] = df_equipment_export[col].astype(str)

        df_equipment_export.to_csv(equipment_file, index=False)
        logger.info(f"      ✓ Equipment history saved: {equipment_file}")
        logger.info(f"      ✓ Total equipment records: {len(df_equipment_export)}")
    except Exception as e:
        logger.error(f"      ✗ Error saving equipment history: {str(e)}")

    # Export current state (latest records only) to Excel
    logger.info("=" * 60)
    logger.info("EXPORTING CURRENT STATE TO EXCEL...")
    logger.info("=" * 60)

    # Get latest records for current state export
    latest_records = get_latest_records(merged_history)

    # Generate filename with timestamp
    date_str = datetime.now().strftime("%Y-%m-%d")
    excel_filename = f"{output_dir}/bmw_cars_{date_str}.xlsx"

    # Export DataFrame to Excel
    try:
        # Create a copy for export
        df_export = latest_records.copy()

        # Convert datetime objects to strings for Excel compatibility
        for col in ['first_seen_date', 'last_seen_date', 'valid_from', 'valid_to', 'scrape_date']:
            if col in df_export.columns:
                df_export[col] = df_export[col].astype(str)

        df_export.to_excel(excel_filename, index=False, engine='openpyxl')
        logger.info(f"      ✓ Excel file exported: {excel_filename}")
        logger.info(f"      ✓ Total rows exported: {len(df_export)}")

        # Show summary
        logger.info("")
        logger.info("=" * 60)
        logger.info("INVENTORY SUMMARY")
        logger.info("=" * 60)
        active_cars = len(df_export[df_export['status'] == 'active'])
        sold_cars = len(df_export[df_export['status'] == 'sold'])
        logger.info(f"Active cars: {active_cars}")
        logger.info(f"Sold/Removed cars: {sold_cars}")
        logger.info(f"Total unique cars seen: {len(merged_history['car_id'].unique())}")
    except Exception as e:
        logger.error(f"      ✗ Error exporting to Excel: {str(e)}")
        logger.warning(f"      → Make sure openpyxl is installed: pip install openpyxl")

    logger.info("\nPress Enter to close the browser...")
    input()
    browser.close()

