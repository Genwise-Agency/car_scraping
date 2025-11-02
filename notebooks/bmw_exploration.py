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

SCORES_COLUMNS = [
    'car_id', 'value_efficiency_score', 'age_usage_score',
    'performance_range_score', 'equipment_score', 'final_score', 'valid_from', 'valid_to',
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
        try:
            car_id = car_row['car_id']
            if pd.isna(car_id):
                continue

            equipments_json = car_row.get('equipments')
            valid_from = car_row.get('valid_from')
            valid_to = car_row.get('valid_to')
            is_latest = car_row.get('is_latest', True)
            scrape_date_str = car_row.get('scrape_date')

            # Convert dates to string if needed
            if pd.notna(valid_from):
                valid_from = valid_from.isoformat() if hasattr(valid_from, 'isoformat') else str(valid_from)
            else:
                valid_from = today_str

            if pd.notna(valid_to):
                valid_to = valid_to.isoformat() if hasattr(valid_to, 'isoformat') else str(valid_to)
            else:
                valid_to = None

            if pd.notna(scrape_date_str):
                scrape_date_str = scrape_date_str.isoformat() if hasattr(scrape_date_str, 'isoformat') else str(scrape_date_str)
            else:
                scrape_date_str = today_str

            equipment_items = extract_equipment_from_json(
                car_id, equipments_json, valid_from, valid_to, is_latest, scrape_date_str
            )
            if equipment_items:
                new_equipment_records.extend(equipment_items)
        except Exception as e:
            logger.warning(f"      Error processing equipment for car row {idx}: {e}")
            continue

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
        if not old_equipment.empty and isinstance(old_equipment, pd.DataFrame):
            merged_equipment_records.append(old_equipment)

        existing_car_ids = set(current_equipment['car_id'].unique()) if not current_equipment.empty else set()

        for car_id in car_ids:
            try:
                if pd.isna(car_id):
                    continue

                car_new_equipment = new_equipment_df[new_equipment_df['car_id'] == car_id]
                car_old_equipment = current_equipment[current_equipment['car_id'] == car_id] if car_id in existing_car_ids else pd.DataFrame()

                if car_old_equipment.empty:
                    # New car - add all equipment
                    if not car_new_equipment.empty and isinstance(car_new_equipment, pd.DataFrame):
                        merged_equipment_records.append(car_new_equipment)
                else:
                    # Create sets for comparison
                    new_set = set()
                    for _, row in car_new_equipment.iterrows():
                        try:
                            category = row.get('category')
                            equipment_name = row.get('equipment_name')
                            if pd.notna(category) and pd.notna(equipment_name):
                                new_set.add((str(category), str(equipment_name)))
                        except Exception as e:
                            logger.warning(f"      Error processing new equipment row for car {car_id}: {e}")
                            continue

                    old_set = set()
                    for _, row in car_old_equipment.iterrows():
                        try:
                            category = row.get('category')
                            equipment_name = row.get('equipment_name')
                            if pd.notna(category) and pd.notna(equipment_name):
                                old_set.add((str(category), str(equipment_name)))
                        except Exception as e:
                            logger.warning(f"      Error processing old equipment row for car {car_id}: {e}")
                            continue

                    # Mark old equipment as not latest if car equipment changed
                    if new_set != old_set:
                        # End old equipment records
                        old_records_list = []
                        for _, old_row in car_old_equipment.iterrows():
                            try:
                                old_record = old_row.to_dict()
                                old_record['valid_to'] = today_str
                                old_record['is_latest'] = False
                                old_records_list.append(old_record)
                            except Exception as e:
                                logger.warning(f"      Error converting old equipment record for car {car_id}: {e}")
                                continue

                        # Convert list of dicts to DataFrame before appending
                        if old_records_list:
                            try:
                                old_records_df = pd.DataFrame(old_records_list)
                                if not old_records_df.empty:
                                    merged_equipment_records.append(old_records_df)
                            except Exception as e:
                                logger.warning(f"      Error creating DataFrame for old equipment records (car {car_id}): {e}")

                        # Add new equipment records
                        if not car_new_equipment.empty and isinstance(car_new_equipment, pd.DataFrame):
                            merged_equipment_records.append(car_new_equipment)
                    else:
                        # No change - just update scrape_date
                        updated_records_list = []
                        for _, old_row in car_old_equipment.iterrows():
                            try:
                                old_record = old_row.to_dict()
                                old_record['scrape_date'] = today_str
                                updated_records_list.append(old_record)
                            except Exception as e:
                                logger.warning(f"      Error converting updated equipment record for car {car_id}: {e}")
                                continue

                        # Convert list of dicts to DataFrame before appending
                        if updated_records_list:
                            try:
                                updated_records_df = pd.DataFrame(updated_records_list)
                                if not updated_records_df.empty:
                                    merged_equipment_records.append(updated_records_df)
                            except Exception as e:
                                logger.warning(f"      Error creating DataFrame for updated equipment records (car {car_id}): {e}")
            except Exception as e:
                logger.warning(f"      Error processing equipment for car {car_id}: {e}")
                continue

        # Combine all records - ensure all items are DataFrames
        if merged_equipment_records:
            # Filter out empty DataFrames and ensure all are DataFrames
            valid_records = []
            for df in merged_equipment_records:
                if isinstance(df, pd.DataFrame) and not df.empty:
                    # Ensure all required columns exist
                    missing_cols = set(EQUIPMENT_COLUMNS) - set(df.columns)
                    if missing_cols:
                        logger.warning(f"      Missing columns in equipment DataFrame: {missing_cols}")
                        # Add missing columns with None values
                        for col in missing_cols:
                            df[col] = None
                    valid_records.append(df)

            if valid_records:
                try:
                    merged_equipment = pd.concat(valid_records, ignore_index=True)
                except Exception as e:
                    logger.error(f"      Error concatenating equipment records: {e}")
                    logger.warning(f"      Falling back to new equipment DataFrame only")
                    merged_equipment = new_equipment_df
            else:
                merged_equipment = new_equipment_df
        else:
            merged_equipment = new_equipment_df
    else:
        # First time - no history
        merged_equipment = new_equipment_df

    logger.info(f"      Processed equipment for {len(car_ids)} cars")
    logger.info(f"      Total equipment records: {len(merged_equipment)}")
    logger.info("=" * 60)

    return merged_equipment


def load_scores_history(scores_file):
    """Load historical scores data from CSV"""
    if os.path.exists(scores_file):
        try:
            df = pd.read_csv(scores_file, dtype={'car_id': 'Int64'})
            # Convert date columns to datetime
            for col in ['valid_from', 'valid_to', 'scrape_date']:
                if col in df.columns:
                    df[col] = pd.to_datetime(df[col], errors='coerce')
            logger.info(f"Loaded {len(df)} scores records from {scores_file}")
            return df
        except Exception as e:
            logger.warning(f"Error loading scores file: {e}. Starting fresh.")
            return pd.DataFrame(columns=SCORES_COLUMNS)
    return pd.DataFrame(columns=SCORES_COLUMNS)


def merge_scores_history(car_history_df, scores_history_df, scrape_date):
    """Merge scores data from car history with scores history"""
    today = scrape_date.date()
    today_str = today.isoformat()

    # Get latest car records (to extract current scores)
    latest_cars = get_latest_records(car_history_df)

    # Extract scores from latest car records
    new_scores_records = []

    logger.info("=" * 60)
    logger.info("PROCESSING SCORES DATA...")
    logger.info("=" * 60)

    for idx, car_row in latest_cars.iterrows():
        try:
            car_id = car_row['car_id']
            if pd.isna(car_id):
                continue

            # Extract score columns
            value_efficiency_score = car_row.get('value_efficiency_score')
            age_usage_score = car_row.get('age_usage_score')
            performance_range_score = car_row.get('performance_range_score')
            equipment_score = car_row.get('equipment_score')
            final_score = car_row.get('final_score')
            valid_from = car_row.get('valid_from')
            valid_to = car_row.get('valid_to')
            is_latest = car_row.get('is_latest', True)
            scrape_date_str = car_row.get('scrape_date')

            # Convert dates to string if needed
            if pd.notna(valid_from):
                valid_from = valid_from.isoformat() if hasattr(valid_from, 'isoformat') else str(valid_from)
            else:
                valid_from = today_str

            if pd.notna(valid_to):
                valid_to = valid_to.isoformat() if hasattr(valid_to, 'isoformat') else str(valid_to)
            else:
                valid_to = None

            if pd.notna(scrape_date_str):
                scrape_date_str = scrape_date_str.isoformat() if hasattr(scrape_date_str, 'isoformat') else str(scrape_date_str)
            else:
                scrape_date_str = today_str

            # Only add record if at least one score exists
            if pd.notna(value_efficiency_score) or pd.notna(age_usage_score) or \
               pd.notna(performance_range_score) or pd.notna(equipment_score) or pd.notna(final_score):
                score_record = {
                    'car_id': car_id,
                    'value_efficiency_score': value_efficiency_score if pd.notna(value_efficiency_score) else None,
                    'age_usage_score': age_usage_score if pd.notna(age_usage_score) else None,
                    'performance_range_score': performance_range_score if pd.notna(performance_range_score) else None,
                    'equipment_score': equipment_score if pd.notna(equipment_score) else None,
                    'final_score': final_score if pd.notna(final_score) else None,
                    'valid_from': valid_from,
                    'valid_to': valid_to,
                    'is_latest': is_latest,
                    'scrape_date': scrape_date_str
                }
                new_scores_records.append(score_record)
        except Exception as e:
            logger.warning(f"      Error processing scores for car row {idx}: {e}")
            continue

    # Create new scores dataframe
    new_scores_df = pd.DataFrame(new_scores_records)

    if new_scores_df.empty:
        logger.info("      No scores records to process")
        return scores_history_df if not scores_history_df.empty else pd.DataFrame(columns=SCORES_COLUMNS)

    # Get current scores for each car (latest records)
    car_ids = new_scores_df['car_id'].unique()

    if not scores_history_df.empty:
        current_scores = scores_history_df[scores_history_df['is_latest'] == True].copy()
        merged_scores_records = []

        # Keep old non-latest records
        old_scores = scores_history_df[scores_history_df['is_latest'] == False].copy()
        if not old_scores.empty and isinstance(old_scores, pd.DataFrame):
            merged_scores_records.append(old_scores)

        existing_car_ids = set(current_scores['car_id'].unique()) if not current_scores.empty else set()

        for car_id in car_ids:
            try:
                if pd.isna(car_id):
                    continue

                car_new_scores = new_scores_df[new_scores_df['car_id'] == car_id]
                car_old_scores = current_scores[current_scores['car_id'] == car_id] if car_id in existing_car_ids else pd.DataFrame()

                if car_old_scores.empty:
                    # New car - add all scores
                    if not car_new_scores.empty and isinstance(car_new_scores, pd.DataFrame):
                        merged_scores_records.append(car_new_scores)
                else:
                    # Compare scores to see if they changed
                    # Create comparison dicts (round to 2 decimals for comparison)
                    new_scores_dict = {}
                    if not car_new_scores.empty:
                        row = car_new_scores.iloc[0]
                        new_scores_dict = {
                            'value_efficiency_score': round(row.get('value_efficiency_score', 0), 2) if pd.notna(row.get('value_efficiency_score')) else None,
                            'age_usage_score': round(row.get('age_usage_score', 0), 2) if pd.notna(row.get('age_usage_score')) else None,
                            'performance_range_score': round(row.get('performance_range_score', 0), 2) if pd.notna(row.get('performance_range_score')) else None,
                            'equipment_score': round(row.get('equipment_score', 0), 2) if pd.notna(row.get('equipment_score')) else None,
                            'final_score': round(row.get('final_score', 0), 2) if pd.notna(row.get('final_score')) else None
                        }

                    old_scores_dict = {}
                    if not car_old_scores.empty:
                        row = car_old_scores.iloc[0]
                        old_scores_dict = {
                            'value_efficiency_score': round(row.get('value_efficiency_score', 0), 2) if pd.notna(row.get('value_efficiency_score')) else None,
                            'age_usage_score': round(row.get('age_usage_score', 0), 2) if pd.notna(row.get('age_usage_score')) else None,
                            'performance_range_score': round(row.get('performance_range_score', 0), 2) if pd.notna(row.get('performance_range_score')) else None,
                            'equipment_score': round(row.get('equipment_score', 0), 2) if pd.notna(row.get('equipment_score')) else None,
                            'final_score': round(row.get('final_score', 0), 2) if pd.notna(row.get('final_score')) else None
                        }

                    # Check if scores changed
                    scores_changed = new_scores_dict != old_scores_dict

                    if scores_changed:
                        # End old scores record
                        old_records_list = []
                        for _, old_row in car_old_scores.iterrows():
                            try:
                                old_record = old_row.to_dict()
                                old_record['valid_to'] = today_str
                                old_record['is_latest'] = False
                                old_records_list.append(old_record)
                            except Exception as e:
                                logger.warning(f"      Error converting old scores record for car {car_id}: {e}")
                                continue

                        # Convert list of dicts to DataFrame before appending
                        if old_records_list:
                            try:
                                old_records_df = pd.DataFrame(old_records_list)
                                if not old_records_df.empty:
                                    merged_scores_records.append(old_records_df)
                            except Exception as e:
                                logger.warning(f"      Error creating DataFrame for old scores records (car {car_id}): {e}")

                        # Add new scores records
                        if not car_new_scores.empty and isinstance(car_new_scores, pd.DataFrame):
                            merged_scores_records.append(car_new_scores)
                    else:
                        # No change - just update scrape_date
                        updated_records_list = []
                        for _, old_row in car_old_scores.iterrows():
                            try:
                                old_record = old_row.to_dict()
                                old_record['scrape_date'] = today_str
                                updated_records_list.append(old_record)
                            except Exception as e:
                                logger.warning(f"      Error converting updated scores record for car {car_id}: {e}")
                                continue

                        # Convert list of dicts to DataFrame before appending
                        if updated_records_list:
                            try:
                                updated_records_df = pd.DataFrame(updated_records_list)
                                if not updated_records_df.empty:
                                    merged_scores_records.append(updated_records_df)
                            except Exception as e:
                                logger.warning(f"      Error creating DataFrame for updated scores records (car {car_id}): {e}")
            except Exception as e:
                logger.warning(f"      Error processing scores for car {car_id}: {e}")
                continue

        # Combine all records - ensure all items are DataFrames
        if merged_scores_records:
            # Filter out empty DataFrames and ensure all are DataFrames
            valid_records = []
            for df in merged_scores_records:
                if isinstance(df, pd.DataFrame) and not df.empty:
                    # Ensure all required columns exist
                    missing_cols = set(SCORES_COLUMNS) - set(df.columns)
                    if missing_cols:
                        logger.warning(f"      Missing columns in scores DataFrame: {missing_cols}")
                        # Add missing columns with None values
                        for col in missing_cols:
                            df[col] = None
                    valid_records.append(df)

            if valid_records:
                try:
                    merged_scores = pd.concat(valid_records, ignore_index=True)
                except Exception as e:
                    logger.error(f"      Error concatenating scores records: {e}")
                    logger.warning(f"      Falling back to new scores DataFrame only")
                    merged_scores = new_scores_df
            else:
                merged_scores = new_scores_df
        else:
            merged_scores = new_scores_df
    else:
        # First time - no history
        merged_scores = new_scores_df

    logger.info(f"      Processed scores for {len(car_ids)} cars")
    logger.info(f"      Total scores records: {len(merged_scores)}")
    logger.info("=" * 60)

    return merged_scores


def export_equipment_list(equipment_history_df, output_dir):
    """Extract all unique equipment categories and items, export as JSON"""
    logger.info("=" * 60)
    logger.info("EXTRACTING EQUIPMENT LIST...")
    logger.info("=" * 60)

    try:
        if equipment_history_df.empty:
            logger.warning("      No equipment data available")
            return

        # Get all unique categories
        categories = equipment_history_df['category'].dropna().unique().tolist()
        categories = sorted([str(cat) for cat in categories])

        # Build equipment list grouped by category
        equipment_list = {}

        for category in categories:
            # Get all equipment items for this category
            category_equipment = equipment_history_df[
                equipment_history_df['category'] == category
            ]['equipment_name'].dropna().unique().tolist()

            # Sort and remove duplicates
            category_equipment = sorted(list(set([str(item) for item in category_equipment])))

            if category_equipment:
                equipment_list[category] = category_equipment

        # Create summary statistics
        total_categories = len(equipment_list)
        total_equipment_items = sum(len(items) for items in equipment_list.values())

        # Build output structure
        output_data = {
            'metadata': {
                'extraction_date': datetime.now().isoformat(),
                'total_categories': total_categories,
                'total_unique_equipment_items': total_equipment_items,
                'categories': categories
            },
            'equipment_by_category': equipment_list
        }

        # Export to JSON
        equipment_list_file = f"{output_dir}/equipment_list.json"
        with open(equipment_list_file, 'w', encoding='utf-8') as f:
            json.dump(output_data, f, ensure_ascii=False, indent=2)

        logger.info(f"      ✓ Equipment list exported: {equipment_list_file}")
        logger.info(f"      ✓ Total categories: {total_categories}")
        logger.info(f"      ✓ Total unique equipment items: {total_equipment_items}")
        logger.info("=" * 60)

    except Exception as e:
        logger.error(f"      ✗ Error exporting equipment list: {str(e)}")


#url = "https://www.bmw.be/fr-be/sl/stocklocator_uc/results?filters=%257B%2522MARKETING_MODEL_RANGE%2522%253A%255B%2522i4_G26E%2522%252C%2522i5_G61E%2522%252C%2522i5_G60E%2522%255D%252C%2522PRICE%2522%253A%255Bnull%252C60000%255D%252C%2522REGISTRATION_YEAR%2522%253A%255B2024%252C-1%255D%252C%2522EQUIPMENT_GROUPS%2522%253A%257B%2522Default%2522%253A%255B%2522M%2520leather%2520steering%2520wheel%2522%255D%252C%2522favorites%2522%253A%255B%2522M%2520Sport%2520package%2522%255D%257D%257D"

# avec toit ouvrant
url="https://www.bmw.be/fr-be/sl/stocklocator_uc/results?filters=%257B%2522MARKETING_MODEL_RANGE%2522%253A%255B%2522i4_G26E%2522%255D%252C%2522PRICE%2522%253A%255Bnull%252C60000%255D%252C%2522REGISTRATION_YEAR%2522%253A%255B2024%252C-1%255D%252C%2522EQUIPMENT_GROUPS%2522%253A%257B%2522Default%2522%253A%255B%2522M%2520leather%2520steering%2520wheel%2522%252C%2522Sun%2520roof%2522%255D%252C%2522favorites%2522%253A%255B%2522M%2520Sport%2520package%2522%255D%257D%257D"

# all BMW i4
#url = "https://www.bmw.be/fr-be/sl/stocklocator_uc/results?filters=%257B%2522MARKETING_MODEL_RANGE%2522%253A%255B%2522i4_G26E%2522%255D%257D"

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


def calculate_age_metrics(df):
    """Calculate age and usage metrics"""
    df = df.copy()
    current_date = datetime.now()

    # Calculate age in months
    df['age_months'] = df['registration_date'].apply(
        lambda x: (current_date.year - x.year) * 12 + (current_date.month - x.month)
        if pd.notna(x) else None
    )

    # Calculate age in years (for annual mileage)
    df['age_years'] = df['age_months'] / 12.0

    # Calculate annual mileage (kilometers per year)
    df['annual_mileage'] = df.apply(
        lambda row: row['kilometers'] / row['age_years']
        if pd.notna(row['kilometers']) and pd.notna(row['age_years']) and row['age_years'] > 0
        else None,
        axis=1
    )

    # Newness score (higher for newer cars, 0-100 scale)
    # 2024 cars = 100, decreasing by 10 per year
    df['newness_score'] = df['registration_date'].apply(
        lambda x: max(0, 100 - (current_date.year - x.year) * 10)
        if pd.notna(x) else None
    )

    return df


def calculate_value_efficiency_metrics(df):
    """Calculate value efficiency metrics and scores"""
    df = df.copy()

    # Price per kilometer (lower is better)
    df['price_per_km'] = df.apply(
        lambda row: row['price'] / row['kilometers']
        if pd.notna(row['price']) and pd.notna(row['kilometers']) and row['kilometers'] > 0
        else None,
        axis=1
    )

    # Price per kW (lower is better)
    df['price_per_kw'] = df.apply(
        lambda row: row['price'] / row['horse_power_kw']
        if pd.notna(row['price']) and pd.notna(row['horse_power_kw']) and row['horse_power_kw'] > 0
        else None,
        axis=1
    )

    # Price per km range (lower is better)
    df['price_per_km_range'] = df.apply(
        lambda row: row['price'] / row['battery_range_km']
        if pd.notna(row['price']) and pd.notna(row['battery_range_km']) and row['battery_range_km'] > 0
        else None,
        axis=1
    )

    # Normalize to 0-100 scores (lower values = higher scores)
    # Price per km score
    valid_price_per_km = df['price_per_km'].dropna()
    if len(valid_price_per_km) > 0:
        min_val = valid_price_per_km.min()
        max_val = valid_price_per_km.max()
        if max_val > min_val:
            df['value_score_price_per_km'] = df['price_per_km'].apply(
                lambda x: 100 * (1 - (x - min_val) / (max_val - min_val))
                if pd.notna(x) else None
            )
        else:
            df['value_score_price_per_km'] = 50 if len(valid_price_per_km) > 0 else None
    else:
        df['value_score_price_per_km'] = None

    # Price per kW score
    valid_price_per_kw = df['price_per_kw'].dropna()
    if len(valid_price_per_kw) > 0:
        min_val = valid_price_per_kw.min()
        max_val = valid_price_per_kw.max()
        if max_val > min_val:
            df['value_score_price_per_kw'] = df['price_per_kw'].apply(
                lambda x: 100 * (1 - (x - min_val) / (max_val - min_val))
                if pd.notna(x) else None
            )
        else:
            df['value_score_price_per_kw'] = 50 if len(valid_price_per_kw) > 0 else None
    else:
        df['value_score_price_per_kw'] = None

    # Price per km range score
    valid_price_per_range = df['price_per_km_range'].dropna()
    if len(valid_price_per_range) > 0:
        min_val = valid_price_per_range.min()
        max_val = valid_price_per_range.max()
        if max_val > min_val:
            df['value_score_price_per_range'] = df['price_per_km_range'].apply(
                lambda x: 100 * (1 - (x - min_val) / (max_val - min_val))
                if pd.notna(x) else None
            )
        else:
            df['value_score_price_per_range'] = 50 if len(valid_price_per_range) > 0 else None
    else:
        df['value_score_price_per_range'] = None

    # Overall value efficiency score (average of the three)
    df['value_efficiency_score'] = df.apply(
        lambda row: pd.Series([
            row['value_score_price_per_km'],
            row['value_score_price_per_kw'],
            row['value_score_price_per_range']
        ]).mean() if any(pd.notna([row['value_score_price_per_km'],
                                   row['value_score_price_per_kw'],
                                   row['value_score_price_per_range']])) else None,
        axis=1
    )

    return df


def calculate_age_usage_scores(df):
    """Calculate age and usage scores"""
    df = df.copy()

    # Age score (newer = better, 0-100 scale)
    # Penalize age: newer cars get higher scores
    valid_age = df['age_months'].dropna()
    if len(valid_age) > 0:
        min_age = valid_age.min()
        max_age = valid_age.max()
        if max_age > min_age:
            df['age_score'] = df['age_months'].apply(
                lambda x: 100 * (1 - (x - min_age) / (max_age - min_age))
                if pd.notna(x) else None
            )
        else:
            df['age_score'] = 100 if len(valid_age) > 0 else None
    else:
        df['age_score'] = None

    # Annual mileage score (lower mileage = better, 0-100 scale)
    # Bonus for < 10k km/year, penalty for > 20k km/year
    valid_annual = df['annual_mileage'].dropna()
    if len(valid_annual) > 0:
        # Use a scoring function: 100 for 0 km/year, decreasing linearly
        # Optimal: < 10k km/year = high score (80-100)
        # Good: 10-15k km/year = medium-high (60-80)
        # Acceptable: 15-20k km/year = medium (40-60)
        # High: > 20k km/year = low (0-40)
        def mileage_score(annual_km):
            if pd.isna(annual_km):
                return None
            if annual_km <= 10000:
                return 80 + (10000 - annual_km) / 10000 * 20  # 80-100
            elif annual_km <= 15000:
                return 60 + (15000 - annual_km) / 5000 * 20  # 60-80
            elif annual_km <= 20000:
                return 40 + (20000 - annual_km) / 5000 * 20  # 40-60
            else:
                return max(0, 40 - (annual_km - 20000) / 10000 * 40)  # 0-40

        df['usage_score'] = df['annual_mileage'].apply(mileage_score)
    else:
        df['usage_score'] = None

    # Overall age & usage score (weighted average: 40% age, 40% usage, 20% newness)
    df['age_usage_score'] = df.apply(
        lambda row: pd.Series([
            row['age_score'] * 0.4 if pd.notna(row['age_score']) else None,
            row['usage_score'] * 0.4 if pd.notna(row['usage_score']) else None,
            row['newness_score'] * 0.2 if pd.notna(row['newness_score']) else None
        ]).sum() if any(pd.notna([row['age_score'], row['usage_score'], row['newness_score']])) else None,
        axis=1
    )

    return df


def calculate_performance_range_scores(df):
    """Calculate performance and range metrics and scores"""
    df = df.copy()

    # Range efficiency (km per kW) - higher is better
    df['range_efficiency'] = df.apply(
        lambda row: row['battery_range_km'] / row['horse_power_kw']
        if pd.notna(row['battery_range_km']) and pd.notna(row['horse_power_kw']) and row['horse_power_kw'] > 0
        else None,
        axis=1
    )

    # Range adequacy score (bonus for >= 400 km)
    def range_adequacy_score(range_km):
        if pd.isna(range_km):
            return None
        if range_km >= 500:
            return 100
        elif range_km >= 450:
            return 90
        elif range_km >= 400:
            return 80
        elif range_km >= 350:
            return 60
        elif range_km >= 300:
            return 40
        else:
            return 20

    df['range_adequacy_score'] = df['battery_range_km'].apply(range_adequacy_score)

    # Power adequacy score (bonus for >= 200 kW)
    def power_adequacy_score(power_kw):
        if pd.isna(power_kw):
            return None
        if power_kw >= 300:
            return 100
        elif power_kw >= 250:
            return 90
        elif power_kw >= 200:
            return 80
        elif power_kw >= 150:
            return 60
        elif power_kw >= 100:
            return 40
        else:
            return 20

    df['power_adequacy_score'] = df['horse_power_kw'].apply(power_adequacy_score)

    # Range efficiency score (normalized 0-100, higher is better)
    valid_efficiency = df['range_efficiency'].dropna()
    if len(valid_efficiency) > 0:
        min_val = valid_efficiency.min()
        max_val = valid_efficiency.max()
        if max_val > min_val:
            df['range_efficiency_score'] = df['range_efficiency'].apply(
                lambda x: 100 * (x - min_val) / (max_val - min_val)
                if pd.notna(x) else None
            )
        else:
            df['range_efficiency_score'] = 50 if len(valid_efficiency) > 0 else None
    else:
        df['range_efficiency_score'] = None

    # Overall performance/range score (weighted: 40% range adequacy, 30% power adequacy, 30% efficiency)
    df['performance_range_score'] = df.apply(
        lambda row: pd.Series([
            row['range_adequacy_score'] * 0.4 if pd.notna(row['range_adequacy_score']) else None,
            row['power_adequacy_score'] * 0.3 if pd.notna(row['power_adequacy_score']) else None,
            row['range_efficiency_score'] * 0.3 if pd.notna(row['range_efficiency_score']) else None
        ]).sum() if any(pd.notna([row['range_adequacy_score'],
                                  row['power_adequacy_score'],
                                  row['range_efficiency_score']])) else None,
        axis=1
    )

    return df


def calculate_all_scores(df, preferences_file=None):
    """Calculate all scoring metrics and add them to the DataFrame"""
    logger.info("=" * 60)
    logger.info("CALCULATING SCORING METRICS...")
    logger.info("=" * 60)

    # Step 1: Calculate age metrics
    logger.info("  → Calculating age and usage metrics...")
    df = calculate_age_metrics(df)

    # Step 2: Calculate value efficiency metrics
    logger.info("  → Calculating value efficiency metrics...")
    df = calculate_value_efficiency_metrics(df)

    # Step 3: Calculate age & usage scores
    logger.info("  → Calculating age & usage scores...")
    df = calculate_age_usage_scores(df)

    # Step 4: Calculate performance/range scores
    logger.info("  → Calculating performance/range scores...")
    df = calculate_performance_range_scores(df)

    # Step 5: Calculate equipment scores
    if preferences_file:
        logger.info("  → Calculating equipment scores...")
        df = calculate_equipment_scores(df, preferences_file)
    else:
        logger.warning("  → Skipping equipment scores (no preferences file provided)")
        df['equipment_score'] = None

    # Step 6: Calculate final overall score
    logger.info("  → Calculating final overall score...")
    df = calculate_final_score(df)

    logger.info("  ✓ All scoring metrics calculated")
    logger.info("=" * 60)

    return df


def load_preferences(preferences_file):
    """Load desired equipment preferences from JSON file"""
    try:
        with open(preferences_file, 'r', encoding='utf-8') as f:
            preferences_data = json.load(f)
            desired_equipment = preferences_data.get('desired_equipment', [])
            logger.info(f"      ✓ Loaded {len(desired_equipment)} desired equipment items from preferences")
            return set(desired_equipment)
    except Exception as e:
        logger.warning(f"      ✗ Error loading preferences file: {e}")
        return set()


def extract_all_equipment_items(equipments_json):
    """Extract all equipment items from JSON, flattening across all categories"""
    all_equipment = set()

    if not equipments_json:
        return all_equipment

    try:
        equipment_data = json.loads(equipments_json) if isinstance(equipments_json, str) else equipments_json

        # Flatten all equipment items across all categories
        for category, equipment_list in equipment_data.items():
            if equipment_list:
                for equipment_name in equipment_list:
                    if equipment_name:
                        all_equipment.add(str(equipment_name).strip())
    except Exception as e:
        logger.warning(f"      Error parsing equipment JSON: {e}")

    return all_equipment


def calculate_equipment_scores(df, preferences_file):
    """Calculate equipment scores based on desired equipment preferences"""
    logger.info("  → Loading equipment preferences...")
    desired_equipment = load_preferences(preferences_file)

    if not desired_equipment:
        logger.warning("      ✗ No desired equipment found, equipment scores will be None")
        df['equipment_score'] = None
        return df

    logger.info("  → Calculating equipment scores...")
    df = df.copy()

    equipment_scores_raw = []

    for idx, row in df.iterrows():
        try:
            equipments_json = row.get('equipments')
            car_equipment = extract_all_equipment_items(equipments_json)

            if not car_equipment:
                equipment_scores_raw.append(None)
                continue

            # Calculate raw score: 1 base per feature, 2 if desired feature present
            raw_score = 0
            matched_desired = 0

            for equipment in car_equipment:
                if equipment in desired_equipment:
                    raw_score += 2  # Desired feature
                    matched_desired += 1
                else:
                    raw_score += 1  # Base score for any feature

            equipment_scores_raw.append({
                'raw_score': raw_score,
                'matched_desired': matched_desired,
                'total_equipment': len(car_equipment)
            })
        except Exception as e:
            logger.warning(f"      Error calculating equipment score for car {idx}: {e}")
            equipment_scores_raw.append(None)

    # Normalize to 0-100 scale
    valid_scores = [s['raw_score'] for s in equipment_scores_raw if s is not None]

    if len(valid_scores) > 0:
        min_score = min(valid_scores)
        max_score = max(valid_scores)

        if max_score > min_score:
            # Calculate normalized scores
            normalized_scores = []
            for score_data in equipment_scores_raw:
                if score_data is None:
                    normalized_scores.append(None)
                else:
                    normalized_score = 100 * (score_data['raw_score'] - min_score) / (max_score - min_score)
                    normalized_scores.append(normalized_score)
            df['equipment_score'] = normalized_scores
        else:
            # All scores are the same
            df['equipment_score'] = 50 if len(valid_scores) > 0 else None
    else:
        df['equipment_score'] = None

    # Log summary
    valid_equipment_scores = df['equipment_score'].dropna()
    if len(valid_equipment_scores) > 0:
        logger.info(f"      ✓ Equipment scores calculated - Avg: {valid_equipment_scores.mean():.1f}, Min: {valid_equipment_scores.min():.1f}, Max: {valid_equipment_scores.max():.1f}")
        # Count matched desired equipment
        matched_counts = [s['matched_desired'] for s in equipment_scores_raw if s is not None]
        if matched_counts:
            logger.info(f"      ✓ Desired equipment matches - Avg: {sum(matched_counts)/len(matched_counts):.1f}/{len(desired_equipment)}")

    return df


def calculate_final_score(df):
    """Calculate final overall score combining all category scores"""
    df = df.copy()

    # Overall score (weighted average: 25% value, 25% age/usage, 25% performance, 25% equipment)
    df['final_score'] = df.apply(
        lambda row: pd.Series([
            row['value_efficiency_score'] * 0.25 if pd.notna(row['value_efficiency_score']) else None,
            row['age_usage_score'] * 0.25 if pd.notna(row['age_usage_score']) else None,
            row['performance_range_score'] * 0.25 if pd.notna(row['performance_range_score']) else None,
            row['equipment_score'] * 0.25 if pd.notna(row['equipment_score']) else None
        ]).sum() if any(pd.notna([row['value_efficiency_score'],
                                  row['age_usage_score'],
                                  row['performance_range_score'],
                                  row['equipment_score']])) else None,
        axis=1
    )

    return df

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

    # Calculate all scoring metrics
    preferences_file = "data/ardonis_bmw_preferences.json"
    df = calculate_all_scores(df, preferences_file=preferences_file)

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

    # Display scoring summary
    if len(df) > 0 and 'value_efficiency_score' in df.columns:
        logger.info("")
        logger.info("=" * 60)
        logger.info("SCORING SUMMARY:")
        logger.info("=" * 60)

        # Value efficiency scores
        if 'value_efficiency_score' in df.columns:
            valid_scores = df['value_efficiency_score'].dropna()
            if len(valid_scores) > 0:
                logger.info(f"Value Efficiency Score - Avg: {valid_scores.mean():.1f}, Min: {valid_scores.min():.1f}, Max: {valid_scores.max():.1f}")

        # Age & usage scores
        if 'age_usage_score' in df.columns:
            valid_scores = df['age_usage_score'].dropna()
            if len(valid_scores) > 0:
                logger.info(f"Age & Usage Score - Avg: {valid_scores.mean():.1f}, Min: {valid_scores.min():.1f}, Max: {valid_scores.max():.1f}")

        # Performance/range scores
        if 'performance_range_score' in df.columns:
            valid_scores = df['performance_range_score'].dropna()
            if len(valid_scores) > 0:
                logger.info(f"Performance/Range Score - Avg: {valid_scores.mean():.1f}, Min: {valid_scores.min():.1f}, Max: {valid_scores.max():.1f}")

        # Equipment scores
        if 'equipment_score' in df.columns:
            valid_scores = df['equipment_score'].dropna()
            if len(valid_scores) > 0:
                logger.info(f"Equipment Score - Avg: {valid_scores.mean():.1f}, Min: {valid_scores.min():.1f}, Max: {valid_scores.max():.1f}")

        # Final overall score
        if 'final_score' in df.columns:
            valid_scores = df['final_score'].dropna()
            if len(valid_scores) > 0:
                logger.info(f"Final Score - Avg: {valid_scores.mean():.1f}, Min: {valid_scores.min():.1f}, Max: {valid_scores.max():.1f}")

        logger.info("=" * 60)

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

    # Export equipment list for standardization review
    export_equipment_list(merged_equipment, output_dir)

    # Process scores history
    # First, merge scores from scraped data into merged_history (for scores extraction)
    scores_file = f"{output_dir}/bmw_cars_scores_history.csv"
    scores_history_df = load_scores_history(scores_file)

    # Extract scores from df and merge into merged_history temporarily
    score_cols = ['car_id', 'value_efficiency_score', 'age_usage_score',
                  'performance_range_score', 'equipment_score', 'final_score']
    if all(col in df.columns for col in score_cols):
        df_scores = df[score_cols].copy()
        # Merge scores into merged_history for scores history processing
        merged_history_with_scores = merged_history.merge(
            df_scores,
            on='car_id',
            how='left'
        )
    else:
        merged_history_with_scores = merged_history

    merged_scores = merge_scores_history(merged_history_with_scores, scores_history_df, scrape_date)

    # Save scores history to CSV
    try:
        # Convert datetime columns to string format for CSV
        df_scores_export = merged_scores.copy()
        for col in ['valid_from', 'valid_to', 'scrape_date']:
            if col in df_scores_export.columns:
                df_scores_export[col] = df_scores_export[col].astype(str)

        df_scores_export.to_csv(scores_file, index=False)
        logger.info(f"      ✓ Scores history saved: {scores_file}")
        logger.info(f"      ✓ Total scores records: {len(df_scores_export)}")
    except Exception as e:
        logger.error(f"      ✗ Error saving scores history: {str(e)}")

    # Export current state (latest records only) to Excel
    logger.info("=" * 60)
    logger.info("EXPORTING CURRENT STATE TO EXCEL...")
    logger.info("=" * 60)

    # Get latest records for current state export
    latest_records = get_latest_records(merged_history)

    # Join scores from scores history (latest scores)
    logger.info("      → Joining scores from scores history...")
    latest_scores = get_latest_records(merged_scores)
    if not latest_scores.empty:
        # Merge scores on car_id
        latest_records = latest_records.merge(
            latest_scores[['car_id', 'value_efficiency_score', 'age_usage_score',
                          'performance_range_score', 'equipment_score', 'final_score']],
            on='car_id',
            how='left'
        )
        logger.info(f"      ✓ Joined scores for {len(latest_records[latest_records['final_score'].notna()])} cars")
    else:
        logger.warning("      → No scores found in history, scores will be missing in export")

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