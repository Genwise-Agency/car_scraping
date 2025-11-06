#!/usr/bin/env python3
"""
BMW Car Scraping and Data Processing Pipeline
Scrapes BMW inventory, processes data with scoring metrics, and syncs to Supabase
"""

import logging
import os
import sys
from datetime import datetime

import pandas as pd

from .config import OUTPUT_DIR, PREFERENCES_FILE, TRACKING_COLUMNS
from .data_processor import (
    export_equipment_list,
    get_latest_records,
    load_equipment_history,
    load_historical_data,
    load_scores_history,
    merge_equipment_history,
    merge_historical_data,
    merge_scores_history,
)
from .database import SupabaseClient
from .scorer import calculate_all_scores
from .scraper import scrape_bmw_inventory

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger(__name__)


def main(url: str, test_limit: int = None, sync_db: bool = False):
    """
    Main pipeline orchestrator

    Args:
        url: BMW inventory URL to scrape
        test_limit: Limit number of cars to process (for testing)
        sync_db: Whether to sync data to Supabase (currently disabled)
    """
    logger.info("=" * 60)
    logger.info("BMW CAR SCRAPING AND PROCESSING PIPELINE")
    logger.info("=" * 60)

    # Create output directory
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    logger.info(f"✓ Output directory: {OUTPUT_DIR}")

    # ============================================================
    # STEP 1: SCRAPE DATA
    # ============================================================
    logger.info("\n[STEP 1/6] Scraping BMW inventory...")
    try:
        all_cars_data = scrape_bmw_inventory(url, max_links=test_limit)
        logger.info(f"✓ Scraped {len(all_cars_data)} cars")
    except Exception as e:
        logger.error(f"✗ Error during scraping: {e}")
        return

    # ============================================================
    # STEP 2: CREATE DATAFRAME AND CALCULATE SCORES
    # ============================================================
    logger.info("\n[STEP 2/6] Processing and scoring data...")
    try:
        df = pd.DataFrame(all_cars_data)

        # Reorder columns
        column_order = [
            'model_name', 'car_id', 'price', 'price_raw',
            'kilometers', 'kilometers_raw',
            'registration_date', 'registration_date_raw',
            'horse_power_kw', 'horse_power_ps', 'horse_power_raw',
            'battery_range_km', 'battery_range_raw',
            'equipments', 'link'
        ]
        existing_columns = [col for col in column_order if col in df.columns]
        df = df[existing_columns]

        # Calculate scoring metrics
        df = calculate_all_scores(df, preferences_file=PREFERENCES_FILE)
        logger.info(f"✓ Processed {len(df)} cars with scoring metrics")
    except Exception as e:
        logger.error(f"✗ Error during data processing: {e}")
        return

    # ============================================================
    # STEP 3: HISTORICAL DATA TRACKING
    # ============================================================
    logger.info("\n[STEP 3/6] Merging with historical data...")
    try:
        scrape_date = datetime.now()

        # Load historical data
        history_file = f"{OUTPUT_DIR}/bmw_cars_history.csv"
        history_df = load_historical_data(history_file)

        # Prepare tracking data
        tracking_cols_with_link = TRACKING_COLUMNS + ['link']
        df_tracking = df[tracking_cols_with_link].copy()

        # Merge with history
        merged_history = merge_historical_data(df_tracking, history_df, scrape_date)

        # Save to CSV
        df_history_export = merged_history.copy()
        for col in ['first_seen_date', 'last_seen_date', 'valid_from', 'valid_to', 'scrape_date']:
            if col in df_history_export.columns:
                df_history_export[col] = df_history_export[col].astype(str)

        df_history_export.to_csv(history_file, index=False)
        logger.info(f"✓ Saved {len(merged_history)} historical records to {history_file}")
    except Exception as e:
        logger.error(f"✗ Error during historical data merge: {e}")
        return

    # ============================================================
    # STEP 4: EQUIPMENT TRACKING
    # ============================================================
    logger.info("\n[STEP 4/6] Processing equipment data...")
    try:
        equipment_file = f"{OUTPUT_DIR}/bmw_cars_equipment_history.csv"
        equipment_history_df = load_equipment_history(equipment_file)
        merged_equipment = merge_equipment_history(merged_history, equipment_history_df, scrape_date)

        df_equipment_export = merged_equipment.copy()
        for col in ['valid_from', 'valid_to', 'scrape_date']:
            if col in df_equipment_export.columns:
                df_equipment_export[col] = df_equipment_export[col].astype(str)

        df_equipment_export.to_csv(equipment_file, index=False)
        logger.info(f"✓ Saved {len(merged_equipment)} equipment records to {equipment_file}")

        # Export equipment list
        export_equipment_list(merged_equipment, OUTPUT_DIR)
    except Exception as e:
        logger.error(f"✗ Error during equipment processing: {e}")
        return

    # ============================================================
    # STEP 5: SCORES TRACKING
    # ============================================================
    logger.info("\n[STEP 5/6] Processing scores data...")
    try:
        scores_file = f"{OUTPUT_DIR}/bmw_cars_scores_history.csv"
        scores_history_df = load_scores_history(scores_file)

        # Merge scores with history for processing
        score_cols = ['car_id', 'value_efficiency_score', 'age_usage_score',
                      'performance_range_score', 'equipment_score', 'final_score']
        if all(col in df.columns for col in score_cols):
            df_scores = df[score_cols].copy()
            merged_history_with_scores = merged_history.merge(df_scores, on='car_id', how='left')
        else:
            merged_history_with_scores = merged_history

        merged_scores = merge_scores_history(merged_history_with_scores, scores_history_df, scrape_date)

        df_scores_export = merged_scores.copy()
        for col in ['valid_from', 'valid_to', 'scrape_date']:
            if col in df_scores_export.columns:
                df_scores_export[col] = df_scores_export[col].astype(str)

        df_scores_export.to_csv(scores_file, index=False)
        logger.info(f"✓ Saved {len(merged_scores)} scores records to {scores_file}")
    except Exception as e:
        logger.error(f"✗ Error during scores processing: {e}")
        return

    # ============================================================
    # STEP 6: EXPORT TO EXCEL
    # ============================================================
    logger.info("\n[STEP 6/6] Exporting data...")
    try:
        # Get latest records for current state
        latest_records = get_latest_records(merged_history)

        # Join scores
        latest_scores = get_latest_records(merged_scores)
        if not latest_scores.empty:
            latest_records = latest_records.merge(
                latest_scores[['car_id', 'value_efficiency_score', 'age_usage_score',
                              'performance_range_score', 'equipment_score', 'final_score']],
                on='car_id',
                how='left'
            )

        # Export to Excel
        date_str = datetime.now().strftime("%Y-%m-%d")
        excel_filename = f"{OUTPUT_DIR}/bmw_cars_{date_str}.xlsx"

        df_export = latest_records.copy()
        for col in ['first_seen_date', 'last_seen_date', 'valid_from', 'valid_to', 'scrape_date']:
            if col in df_export.columns:
                df_export[col] = df_export[col].astype(str)

        df_export.to_excel(excel_filename, index=False, engine='openpyxl')
        logger.info(f"✓ Exported Excel file: {excel_filename}")

        # Summary
        logger.info("")
        logger.info("=" * 60)
        logger.info("INVENTORY SUMMARY")
        logger.info("=" * 60)
        active_cars = len(df_export[df_export['status'] == 'active'])
        sold_cars = len(df_export[df_export['status'] == 'sold'])
        logger.info(f"Active cars: {active_cars}")
        logger.info(f"Sold/Removed cars: {sold_cars}")
        logger.info(f"Total unique cars seen: {len(merged_history['car_id'].unique())}")
        logger.info("=" * 60)

        # ========================================================
        # PUSH TO SUPABASE DATABASE
        # ========================================================
        if sync_db:
            try:
                db_client = SupabaseClient()
                db_client.sync_all(merged_history, merged_equipment, merged_scores)
            except ValueError as e:
                logger.error(f"✗ Database configuration error: {e}")
                logger.error("      Please set SUPABASE_URL and SUPABASE_KEY in your .env file")
            except Exception as e:
                logger.error(f"✗ Error during database sync: {e}")

    except Exception as e:
        logger.error(f"✗ Error during export: {e}")
        return

    logger.info("\n✓ Pipeline completed successfully!")


if __name__ == "__main__":
    # Example: Basic run with full scrape (database sync enabled)
    main(
        url="https://www.bmw.be/fr-be/sl/stocklocator_uc/results?filters=%257B%2522MARKETING_MODEL_RANGE%2522%253A%255B%2522i4_G26E%2522%255D%252C%2522COLOR%2522%253A%255B%2522GRAY%2522%252C%2522BLACK%2522%255D%252C%2522USED_CAR_MILEAGE%2522%253A%255B0%252C20000%255D%252C%2522REGISTRATION_YEAR%2522%253A%255B2025%252C2025%255D%252C%2522EQUIPMENT_GROUPS%2522%253A%257B%2522favorites%2522%253A%255B%2522M%2520Sport%2520package%2522%255D%257D%257D",
        test_limit=None,
        sync_db=True
    )

    # Example: Test run with limited cars (no database sync)
    # main(test_limit=5, sync_db=False)

    # Example: Custom URL
    # main(url="your-custom-url", test_limit=10, sync_db=True)
