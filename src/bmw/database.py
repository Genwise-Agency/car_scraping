import json
import logging
from datetime import datetime
from typing import Optional

import pandas as pd
from supabase import Client, create_client

from .config import SUPABASE_KEY, SUPABASE_URL

logger = logging.getLogger(__name__)


class SupabaseClient:
    """Client for syncing BMW car data to Supabase database"""

    def __init__(self):
        """Initialize Supabase client"""
        if not SUPABASE_URL or not SUPABASE_KEY:
            raise ValueError("SUPABASE_URL and SUPABASE_KEY must be set in environment variables")

        self.client: Client = create_client(SUPABASE_URL, SUPABASE_KEY)
        logger.info("✓ Supabase client initialized")

    def sync_cars_table(self, merged_history_df: pd.DataFrame) -> bool:
        """
        Sync main bmw_cars table with current car records

        Args:
            merged_history_df: DataFrame with historical car data

        Returns:
            bool: True if successful
        """
        try:
            logger.info("=" * 60)
            logger.info("SYNCING BMW_CARS TABLE...")
            logger.info("=" * 60)

            latest_records = merged_history_df[
                merged_history_df['is_latest'] == True
            ].copy()

            if latest_records.empty:
                logger.warning("      No latest records to sync")
                return True

            records_to_insert = []
            records_to_update = []

            today = datetime.now().date().isoformat()

            for _, row in latest_records.iterrows():
                # Ensure first_seen_date is never null - use fallback chain
                first_seen = self._parse_date(row.get('first_seen_date'))
                if not first_seen:
                    first_seen = self._parse_date(row.get('last_seen_date'))
                if not first_seen:
                    first_seen = self._parse_date(row.get('valid_from'))
                if not first_seen:
                    first_seen = today

                # Ensure last_seen_date is never null
                last_seen = self._parse_date(row.get('last_seen_date'))
                if not last_seen:
                    last_seen = today

                car_data = {
                    'car_id': int(row['car_id']) if pd.notna(row['car_id']) else None,
                    'first_seen_date': first_seen,
                    'last_seen_date': last_seen,
                    'current_status': row.get('status', 'active'),
                    'link': row.get('link')
                }

                if car_data['car_id'] is None:
                    continue

                # Check if car exists
                existing = self.client.table('bmw_cars').select('car_id').eq('car_id', car_data['car_id']).execute()

                if existing.data:
                    records_to_update.append(car_data)
                else:
                    records_to_insert.append(car_data)

            # Insert new cars
            if records_to_insert:
                logger.info(f"      Inserting {len(records_to_insert)} new cars...")
                self.client.table('bmw_cars').insert(records_to_insert).execute()
                logger.info(f"      ✓ Inserted {len(records_to_insert)} cars")

            # Update existing cars
            if records_to_update:
                logger.info(f"      Updating {len(records_to_update)} existing cars...")
                for record in records_to_update:
                    car_id = record.pop('car_id')
                    self.client.table('bmw_cars').update(record).eq('car_id', car_id).execute()
                logger.info(f"      ✓ Updated {len(records_to_update)} cars")

            logger.info("=" * 60)
            return True

        except Exception as e:
            logger.error(f"      ✗ Error syncing bmw_cars table: {e}")
            return False

    def sync_cars_history(self, merged_history_df: pd.DataFrame) -> bool:
        """
        Sync bmw_cars_history table with historical records

        Args:
            merged_history_df: DataFrame with historical car data

        Returns:
            bool: True if successful
        """
        try:
            logger.info("=" * 60)
            logger.info("SYNCING BMW_CARS_HISTORY TABLE...")
            logger.info("=" * 60)

            if merged_history_df.empty:
                logger.warning("      No history records to sync")
                return True

            records_to_insert = []
            today = datetime.now().date().isoformat()
            now_iso = datetime.now().isoformat()

            for _, row in merged_history_df.iterrows():
                # Ensure first_seen_date is never null
                first_seen = self._parse_date(row.get('first_seen_date'))
                if not first_seen:
                    first_seen = self._parse_date(row.get('last_seen_date'))
                if not first_seen:
                    first_seen = self._parse_date(row.get('valid_from'))
                if not first_seen:
                    first_seen = today

                # Ensure last_seen_date is never null
                last_seen = self._parse_date(row.get('last_seen_date'))
                if not last_seen:
                    last_seen = today

                # Ensure valid_from is never null
                valid_from = self._parse_date(row.get('valid_from'))
                if not valid_from:
                    valid_from = self._parse_date(row.get('first_seen_date'))
                if not valid_from:
                    valid_from = today

                # Ensure scrape_date is never null
                scrape_date = self._parse_datetime(row.get('scrape_date'))
                if not scrape_date:
                    scrape_date = now_iso

                history_data = {
                    'car_id': int(row['car_id']) if pd.notna(row['car_id']) else None,
                    'model_name': row.get('model_name'),
                    'price': self._parse_numeric(row.get('price')),
                    'kilometers': self._parse_int(row.get('kilometers')),
                    'registration_date': self._parse_date(row.get('registration_date')),
                    'horse_power_kw': self._parse_int(row.get('horse_power_kw')),
                    'horse_power_ps': self._parse_int(row.get('horse_power_ps')),
                    'battery_range_km': self._parse_int(row.get('battery_range_km')),
                    'equipments': self._parse_json(row.get('equipments')),
                    'first_seen_date': first_seen,
                    'last_seen_date': last_seen,
                    'valid_from': valid_from,
                    'valid_to': self._parse_date(row.get('valid_to')),
                    'is_latest': bool(row.get('is_latest', True)),
                    'status': row.get('status', 'active'),
                    'link': row.get('link'),
                    'scrape_date': scrape_date
                }

                if history_data['car_id'] is None:
                    continue

                records_to_insert.append(history_data)

            if not records_to_insert:
                logger.warning("      No valid history records to insert")
                return True

            # Insert in batches
            batch_size = 100
            total_batches = (len(records_to_insert) + batch_size - 1) // batch_size
            logger.info(f"      Inserting {len(records_to_insert)} history records in {total_batches} batches...")

            for i in range(0, len(records_to_insert), batch_size):
                batch = records_to_insert[i:i + batch_size]
                batch_num = (i // batch_size) + 1
                try:
                    self.client.table('bmw_cars_history').insert(batch).execute()
                    logger.info(f"      ✓ Batch {batch_num}/{total_batches} inserted ({len(batch)} records)")
                except Exception as e:
                    logger.error(f"      ✗ Error inserting batch {batch_num}: {e}")

            logger.info("=" * 60)
            return True

        except Exception as e:
            logger.error(f"      ✗ Error syncing bmw_cars_history table: {e}")
            return False

    def sync_equipment(self, merged_equipment_df: pd.DataFrame) -> bool:
        """
        Sync bmw_cars_equipment table with equipment records

        Args:
            merged_equipment_df: DataFrame with equipment data

        Returns:
            bool: True if successful
        """
        try:
            logger.info("=" * 60)
            logger.info("SYNCING BMW_CARS_EQUIPMENT TABLE...")
            logger.info("=" * 60)

            if merged_equipment_df.empty:
                logger.warning("      No equipment records to sync")
                return True

            # Get all existing car_ids from bmw_cars to filter out invalid references
            existing_cars_result = self.client.table('bmw_cars').select(
                'car_id'
            ).execute()
            existing_car_ids = {row['car_id'] for row in existing_cars_result.data} if existing_cars_result.data else set()

            if not existing_car_ids:
                logger.warning("      No cars found in bmw_cars table. Skipping.")
                return True

            logger.info(f"      Found {len(existing_car_ids)} cars in database")

            records_to_insert = []
            records_to_update = []
            today = datetime.now().date().isoformat()
            now_iso = datetime.now().isoformat()
            seen_keys = set()

            for _, row in merged_equipment_df.iterrows():
                car_id = int(row['car_id']) if pd.notna(row['car_id']) else None
                category = row.get('category')
                equipment_name = row.get('equipment_name')

                # Skip invalid records
                if car_id is None or not category or not equipment_name:
                    continue

                # Skip if car doesn't exist in bmw_cars (foreign key constraint)
                if car_id not in existing_car_ids:
                    continue

                # Create a unique key for this equipment record
                unique_key = (car_id, category, equipment_name)

                # Skip duplicates in the current batch
                if unique_key in seen_keys:
                    continue
                seen_keys.add(unique_key)

                # Ensure valid_from is never null
                valid_from = self._parse_date(row.get('valid_from'))
                if not valid_from:
                    valid_from = today

                # Ensure scrape_date is never null
                scrape_date = self._parse_datetime(row.get('scrape_date'))
                if not scrape_date:
                    scrape_date = now_iso

                equipment_data = {
                    'car_id': car_id,
                    'category': category,
                    'equipment_name': equipment_name,
                    'valid_from': valid_from,
                    'valid_to': self._parse_date(row.get('valid_to')),
                    'is_latest': bool(row.get('is_latest', True)),
                    'scrape_date': scrape_date
                }

                # Check if record already exists in database (for latest records only)
                if equipment_data['is_latest']:
                    try:
                        existing = self.client.table('bmw_cars_equipment').select(
                            'id'
                        ).eq('car_id', car_id).eq('category', category).eq(
                            'equipment_name', equipment_name
                        ).eq('is_latest', True).execute()

                        if existing.data:
                            # Update existing record
                            record_id = existing.data[0]['id']
                            records_to_update.append((record_id, equipment_data))
                            continue
                    except Exception:
                        # If check fails, continue with insert
                        pass

                records_to_insert.append(equipment_data)

            logger.info(f"      Deduplicated to {len(records_to_insert)} unique records")

            # Update existing records
            if records_to_update:
                logger.info(f"      Updating {len(records_to_update)} existing")
                for record_id, equipment_data in records_to_update:
                    try:
                        self.client.table('bmw_cars_equipment').update(
                            equipment_data
                        ).eq('id', record_id).execute()
                    except Exception as update_error:
                        logger.warning(f"        Failed to update: {update_error}")

            if not records_to_insert:
                logger.info("      No new equipment records to insert")
                logger.info("=" * 60)
                return True

            # Insert new records in batches
            batch_size = 100
            total_batches = (len(records_to_insert) + batch_size - 1) // batch_size
            logger.info(f"      Inserting {len(records_to_insert)} records")

            successful_inserts = 0
            for i in range(0, len(records_to_insert), batch_size):
                batch = records_to_insert[i:i + batch_size]
                batch_num = (i // batch_size) + 1
                try:
                    self.client.table('bmw_cars_equipment').insert(batch).execute()
                    successful_inserts += len(batch)
                    logger.info(f"      ✓ Batch {batch_num}/{total_batches}")
                except Exception:
                    # Fallback to individual inserts
                    for record in batch:
                        try:
                            self.client.table('bmw_cars_equipment').insert(
                                record
                            ).execute()
                            successful_inserts += 1
                        except Exception:
                            pass

            logger.info(f"      ✓ Inserted {successful_inserts}/{len(records_to_insert)}")
            logger.info("=" * 60)
            return True

        except Exception as e:
            logger.error(f"      ✗ Error: {e}")
            return False

    def sync_scores(self, merged_scores_df: pd.DataFrame) -> bool:
        """
        Sync bmw_cars_scores table with scores records

        Args:
            merged_scores_df: DataFrame with scores data

        Returns:
            bool: True if successful
        """
        try:
            logger.info("=" * 60)
            logger.info("SYNCING BMW_CARS_SCORES TABLE...")
            logger.info("=" * 60)

            if merged_scores_df.empty:
                logger.warning("      No scores records to sync")
                return True

            records_to_insert = []
            records_to_update = []
            today = datetime.now().date().isoformat()
            now_iso = datetime.now().isoformat()

            # First, get all existing car_ids from bmw_cars to filter
            existing_cars_result = self.client.table('bmw_cars').select('car_id').execute()
            existing_car_ids = {row['car_id'] for row in existing_cars_result.data} if existing_cars_result.data else set()

            # Track which car_ids we've seen in this sync to avoid duplicates
            seen_car_ids = set()

            for _, row in merged_scores_df.iterrows():
                car_id = int(row['car_id']) if pd.notna(row['car_id']) else None

                # Skip if car doesn't exist in bmw_cars
                if car_id is None or car_id not in existing_car_ids:
                    continue

                # Skip duplicate car_ids in this batch (keep only latest)
                if car_id in seen_car_ids:
                    continue
                seen_car_ids.add(car_id)

                # Ensure valid_from is never null
                valid_from = self._parse_date(row.get('valid_from'))
                if not valid_from:
                    valid_from = today

                # Ensure scrape_date is never null
                scrape_date = self._parse_datetime(row.get('scrape_date'))
                if not scrape_date:
                    scrape_date = now_iso

                scores_data = {
                    'car_id': car_id,
                    'value_efficiency_score': self._parse_numeric(row.get('value_efficiency_score')),
                    'age_usage_score': self._parse_numeric(row.get('age_usage_score')),
                    'performance_range_score': self._parse_numeric(row.get('performance_range_score')),
                    'equipment_score': self._parse_numeric(row.get('equipment_score')),
                    'final_score': self._parse_numeric(row.get('final_score')),
                    'valid_from': valid_from,
                    'valid_to': self._parse_date(row.get('valid_to')),
                    'is_latest': bool(row.get('is_latest', True)),
                    'scrape_date': scrape_date
                }

                # Check if scores record already exists for this car
                if scores_data['is_latest']:
                    try:
                        existing = self.client.table('bmw_cars_scores').select('id').eq('car_id', car_id).eq('is_latest', True).execute()
                        if existing.data:
                            # Update existing record
                            record_id = existing.data[0]['id']
                            records_to_update.append((record_id, scores_data))
                            continue
                    except Exception:
                        pass

                records_to_insert.append(scores_data)

            if not records_to_insert:
                logger.warning("      No valid scores records to insert")
                return True

            # Insert in batches
            batch_size = 100
            total_batches = (len(records_to_insert) + batch_size - 1) // batch_size
            logger.info(f"      Inserting {len(records_to_insert)} scores records in {total_batches} batches...")

            successful_inserts = 0
            for i in range(0, len(records_to_insert), batch_size):
                batch = records_to_insert[i:i + batch_size]
                batch_num = (i // batch_size) + 1
                try:
                    self.client.table('bmw_cars_scores').insert(batch).execute()
                    successful_inserts += len(batch)
                    logger.info(f"      ✓ Batch {batch_num}/{total_batches} inserted ({len(batch)} records)")
                except Exception as e:
                    # Try inserting records one by one to identify problematic ones
                    logger.warning(f"      ✗ Error inserting batch {batch_num}, trying individual inserts...")
                    for record in batch:
                        try:
                            self.client.table('bmw_cars_scores').insert(record).execute()
                            successful_inserts += 1
                        except Exception as individual_error:
                            logger.warning(f"        Skipped scores record (car_id={record.get('car_id')}): {individual_error}")

            # Handle updates
            if records_to_update:
                logger.info(f"      Updating {len(records_to_update)} existing scores records...")
                for record_id, scores_data in records_to_update:
                    try:
                        self.client.table('bmw_cars_scores').update(scores_data).eq('id', record_id).execute()
                    except Exception as update_error:
                        logger.warning(f"        Failed to update scores record {record_id}: {update_error}")

            logger.info(f"      ✓ Successfully inserted {successful_inserts}/{len(records_to_insert)} scores records")
            logger.info("=" * 60)
            return True

        except Exception as e:
            logger.error(f"      ✗ Error syncing bmw_cars_scores table: {e}")
            return False

    def sync_all(self, merged_history_df: pd.DataFrame, merged_equipment_df: pd.DataFrame,
                 merged_scores_df: pd.DataFrame) -> bool:
        """
        Sync all tables to Supabase

        Args:
            merged_history_df: DataFrame with historical car data
            merged_equipment_df: DataFrame with equipment data
            merged_scores_df: DataFrame with scores data

        Returns:
            bool: True if all syncs successful
        """
        try:
            logger.info("\n" + "=" * 60)
            logger.info("STARTING DATABASE SYNC...")
            logger.info("=" * 60)

            success = True
            success &= self.sync_cars_table(merged_history_df)
            success &= self.sync_cars_history(merged_history_df)
            success &= self.sync_equipment(merged_equipment_df)
            success &= self.sync_scores(merged_scores_df)

            if success:
                logger.info("\n" + "=" * 60)
                logger.info("DATABASE SYNC COMPLETED SUCCESSFULLY")
                logger.info("=" * 60)
            else:
                logger.warning("\n" + "=" * 60)
                logger.warning("DATABASE SYNC COMPLETED WITH ERRORS")
                logger.warning("=" * 60)

            return success

        except Exception as e:
            logger.error(f"\n✗ Critical error during database sync: {e}")
            return False

    @staticmethod
    def _parse_date(value) -> Optional[str]:
        """Parse date value to ISO format string"""
        if pd.isna(value) or value is None:
            return None

        if isinstance(value, str):
            try:
                # Try parsing as datetime first
                dt = pd.to_datetime(value)
                return dt.date().isoformat() if hasattr(dt, 'date') else dt.isoformat()[:10]
            except (ValueError, TypeError):
                return value[:10] if len(value) >= 10 else None

        if hasattr(value, 'date'):
            return value.date().isoformat()
        elif hasattr(value, 'isoformat'):
            return value.isoformat()[:10]

        return None

    @staticmethod
    def _parse_datetime(value) -> Optional[str]:
        """Parse datetime value to ISO format string"""
        if pd.isna(value) or value is None:
            return None

        if isinstance(value, str):
            try:
                dt = pd.to_datetime(value)
                return dt.isoformat()
            except (ValueError, TypeError):
                return value

        if hasattr(value, 'isoformat'):
            return value.isoformat()

        return None

    @staticmethod
    def _parse_numeric(value) -> Optional[float]:
        """Parse numeric value"""
        if pd.isna(value) or value is None:
            return None
        try:
            return float(value)
        except (ValueError, TypeError):
            return None

    @staticmethod
    def _parse_int(value) -> Optional[int]:
        """Parse integer value"""
        if pd.isna(value) or value is None:
            return None
        try:
            return int(value)
        except (ValueError, TypeError):
            return None

    @staticmethod
    def _parse_json(value) -> Optional[dict]:
        """Parse JSON value"""
        if pd.isna(value) or value is None:
            return None

        if isinstance(value, dict):
            return value

        if isinstance(value, str):
            try:
                return json.loads(value)
            except (json.JSONDecodeError, TypeError):
                return None

        return None

