import json
import logging
import os
from datetime import datetime

import pandas as pd
from config import EQUIPMENT_COLUMNS, HISTORY_COLUMNS, SCORES_COLUMNS, TRACKING_COLUMNS

logger = logging.getLogger(__name__)


def load_historical_data(history_file):
    """Load historical car data from CSV"""
    if os.path.exists(history_file):
        try:
            df = pd.read_csv(history_file, dtype={'car_id': 'Int64'})
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
            old_val = old_record[col] if pd.notna(old_record[col]) else None
            new_val = new_record[col] if pd.notna(new_record[col]) else None
            if old_val != new_val:
                return True
        else:
            if str(old_record[col]) != str(new_record[col]):
                return True
    return False


def merge_historical_data(current_data, history_df, scrape_date):
    """Merge current scrape with historical data using SCD Type 2"""
    today = scrape_date.date()
    today_str = today.isoformat()

    latest_df = get_latest_records(history_df)

    current_data['car_id'] = current_data['car_id'].astype('Int64')
    if not latest_df.empty:
        latest_df['car_id'] = latest_df['car_id'].astype('Int64')

    new_records = []
    processed_ids = set()

    logger.info("=" * 60)
    logger.info("HISTORICAL DATA MERGE")
    logger.info("=" * 60)

    for idx, row in current_data.iterrows():
        car_id = row['car_id']
        processed_ids.add(car_id)

        old_record = latest_df[latest_df['car_id'] == car_id]

        if old_record.empty:
            # NEW CAR - just add it without SCD marking
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

            if compare_records(old_record, row, TRACKING_COLUMNS):
                # DATA CHANGED - apply SCD Type 2: mark old as expired, add new
                logger.info(f"[CHANGED] Car ID {car_id}: {row['model_name']}")

                old_row = history_df[
                    (history_df['car_id'] == car_id) &
                    (history_df['is_latest'] == True)
                ].iloc[0].to_dict()
                old_row['valid_to'] = today_str
                old_row['is_latest'] = False
                new_records.append(old_row)

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
                # NO CHANGE - just update timestamps, keep as is_latest=True
                logger.info(f"[UNCHANGED] Car ID {car_id}: {row['model_name']}")
                old_row = old_record.to_dict()
                old_row['last_seen_date'] = today_str
                old_row['scrape_date'] = today_str
                new_records.append(old_row)

    if not latest_df.empty:
        disappeared_cars = latest_df[
            ~latest_df['car_id'].isin(processed_ids) &
            (latest_df['status'] == 'active')
        ]

        for idx, old_record in disappeared_cars.iterrows():
            car_id = old_record['car_id']
            logger.info(f"[SOLD/REMOVED] Car ID {car_id}: {old_record['model_name']}")

            old_row = old_record.to_dict()
            old_row['valid_to'] = today_str
            old_row['is_latest'] = False
            old_row['status'] = 'sold'
            new_records.append(old_row)

    old_history = history_df[
        (history_df['is_latest'] == False) &
        (pd.to_datetime(history_df['valid_to']) < pd.Timestamp(today))
    ].copy() if not history_df.empty else pd.DataFrame(columns=HISTORY_COLUMNS)
    new_records_df = pd.DataFrame(new_records)

    if not old_history.empty:
        merged_history = pd.concat([old_history, new_records_df], ignore_index=True)
    else:
        merged_history = new_records_df

    logger.info("=" * 60)
    logger.info(f"Summary: {len([r for r in new_records if r.get('is_latest')])} current cars")
    logger.info(f"Total historical records: {len(merged_history)}")
    logger.info("=" * 60)

    return merged_history


def load_equipment_history(equipment_file):
    """Load historical equipment data from CSV"""
    if os.path.exists(equipment_file):
        try:
            df = pd.read_csv(equipment_file, dtype={'car_id': 'Int64'})
            for col in ['valid_from', 'valid_to', 'scrape_date']:
                if col in df.columns:
                    df[col] = pd.to_datetime(df[col], errors='coerce')
            logger.info(f"Loaded {len(df)} equipment records from {equipment_file}")
            return df
        except Exception as e:
            logger.warning(f"Error loading equipment file: {e}. Starting fresh.")
            return pd.DataFrame(columns=EQUIPMENT_COLUMNS)
    return pd.DataFrame(columns=EQUIPMENT_COLUMNS)


def extract_equipment_from_json(car_id, equipments_json, valid_from, valid_to, is_latest, scrape_date):
    """Extract equipment items from JSON and create normalized records (deduplicated)"""
    equipment_records = []

    if not equipments_json:
        return equipment_records

    try:
        equipment_data = json.loads(equipments_json) if isinstance(equipments_json, str) else equipments_json

        seen = set()  # Track seen equipment to prevent duplicates

        for category, equipment_list in equipment_data.items():
            if equipment_list:
                for equipment_name in equipment_list:
                    # Create unique key to prevent duplicates within same car
                    unique_key = (category, equipment_name)

                    # Skip if already seen for this car
                    if unique_key in seen:
                        continue
                    seen.add(unique_key)

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


def merge_equipment_history(car_history_df, equipment_history_df, scrape_date):
    """Merge equipment data from car history with equipment history"""
    today = scrape_date.date()
    today_str = today.isoformat()

    latest_cars = get_latest_records(car_history_df)
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

    new_equipment_df = pd.DataFrame(new_equipment_records)

    if new_equipment_df.empty:
        logger.info("      No equipment records to process")
        return equipment_history_df if not equipment_history_df.empty else pd.DataFrame(columns=EQUIPMENT_COLUMNS)

    # Deduplicate new equipment records by (car_id, category, equipment_name, is_latest)
    # Keep the last occurrence (most recent)
    new_equipment_df = new_equipment_df.drop_duplicates(
        subset=['car_id', 'category', 'equipment_name'],
        keep='last'
    )

    if not equipment_history_df.empty:
        merged_equipment = pd.concat([equipment_history_df, new_equipment_df], ignore_index=True)
    else:
        merged_equipment = new_equipment_df

    logger.info(f"      Processed equipment for {len(new_equipment_df['car_id'].unique())} cars")
    logger.info(f"      Total equipment records: {len(merged_equipment)}")
    logger.info("=" * 60)

    return merged_equipment


def load_scores_history(scores_file):
    """Load historical scores data from CSV"""
    if os.path.exists(scores_file):
        try:
            df = pd.read_csv(scores_file, dtype={'car_id': 'Int64'})
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

    latest_cars = get_latest_records(car_history_df)
    new_scores_records = []

    logger.info("=" * 60)
    logger.info("PROCESSING SCORES DATA...")
    logger.info("=" * 60)

    for idx, car_row in latest_cars.iterrows():
        try:
            car_id = car_row['car_id']
            if pd.isna(car_id):
                continue

            value_efficiency_score = car_row.get('value_efficiency_score')
            age_usage_score = car_row.get('age_usage_score')
            performance_range_score = car_row.get('performance_range_score')
            equipment_score = car_row.get('equipment_score')
            final_score = car_row.get('final_score')
            valid_from = car_row.get('valid_from')
            valid_to = car_row.get('valid_to')
            is_latest = car_row.get('is_latest', True)
            scrape_date_str = car_row.get('scrape_date')

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

    new_scores_df = pd.DataFrame(new_scores_records)

    if new_scores_df.empty:
        logger.info("      No scores records to process")
        return scores_history_df if not scores_history_df.empty else pd.DataFrame(columns=SCORES_COLUMNS)

    # Deduplicate new scores records by car_id (keep last/most recent)
    new_scores_df = new_scores_df.drop_duplicates(
        subset=['car_id'],
        keep='last'
    )

    if not scores_history_df.empty:
        merged_scores = pd.concat([scores_history_df, new_scores_df], ignore_index=True)
    else:
        merged_scores = new_scores_df

    logger.info(f"      Processed scores for {len(new_scores_df['car_id'].unique())} cars")
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

        categories = equipment_history_df['category'].dropna().unique().tolist()
        categories = sorted([str(cat) for cat in categories])

        equipment_list = {}

        for category in categories:
            category_equipment = equipment_history_df[
                equipment_history_df['category'] == category
            ]['equipment_name'].dropna().unique().tolist()

            category_equipment = sorted(list(set([str(item) for item in category_equipment])))

            if category_equipment:
                equipment_list[category] = category_equipment

        total_categories = len(equipment_list)
        total_equipment_items = sum(len(items) for items in equipment_list.values())

        output_data = {
            'metadata': {
                'extraction_date': datetime.now().isoformat(),
                'total_categories': total_categories,
                'total_unique_equipment_items': total_equipment_items,
                'categories': categories
            },
            'equipment_by_category': equipment_list
        }

        equipment_list_file = f"{output_dir}/equipment_list.json"
        with open(equipment_list_file, 'w', encoding='utf-8') as f:
            json.dump(output_data, f, ensure_ascii=False, indent=2)

        logger.info(f"      ✓ Equipment list exported: {equipment_list_file}")
        logger.info(f"      ✓ Total categories: {total_categories}")
        logger.info(f"      ✓ Total unique equipment items: {total_equipment_items}")
        logger.info("=" * 60)

    except Exception as e:
        logger.error(f"      ✗ Error exporting equipment list: {str(e)}")
