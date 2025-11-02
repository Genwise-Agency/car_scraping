import json
import logging
from datetime import datetime

import pandas as pd

logger = logging.getLogger(__name__)


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

        for category, equipment_list in equipment_data.items():
            if equipment_list:
                for equipment_name in equipment_list:
                    if equipment_name:
                        all_equipment.add(str(equipment_name).strip())
    except Exception as e:
        logger.warning(f"      Error parsing equipment JSON: {e}")

    return all_equipment


def calculate_age_metrics(df):
    """Calculate age and usage metrics"""
    df = df.copy()
    current_date = datetime.now()

    df['age_months'] = df['registration_date'].apply(
        lambda x: (current_date.year - x.year) * 12 + (current_date.month - x.month)
        if pd.notna(x) else None
    )

    df['age_years'] = df['age_months'] / 12.0

    df['annual_mileage'] = df.apply(
        lambda row: row['kilometers'] / row['age_years']
        if pd.notna(row['kilometers']) and pd.notna(row['age_years']) and row['age_years'] > 0
        else None,
        axis=1
    )

    df['newness_score'] = df['registration_date'].apply(
        lambda x: max(0, 100 - (current_date.year - x.year) * 10)
        if pd.notna(x) else None
    )

    return df


def calculate_value_efficiency_metrics(df):
    """Calculate value efficiency metrics and scores"""
    df = df.copy()

    df['price_per_km'] = df.apply(
        lambda row: row['price'] / row['kilometers']
        if pd.notna(row['price']) and pd.notna(row['kilometers']) and row['kilometers'] > 0
        else None,
        axis=1
    )

    df['price_per_kw'] = df.apply(
        lambda row: row['price'] / row['horse_power_kw']
        if pd.notna(row['price']) and pd.notna(row['horse_power_kw']) and row['horse_power_kw'] > 0
        else None,
        axis=1
    )

    df['price_per_km_range'] = df.apply(
        lambda row: row['price'] / row['battery_range_km']
        if pd.notna(row['price']) and pd.notna(row['battery_range_km']) and row['battery_range_km'] > 0
        else None,
        axis=1
    )

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

    valid_annual = df['annual_mileage'].dropna()
    if len(valid_annual) > 0:
        def mileage_score(annual_km):
            if pd.isna(annual_km):
                return None
            if annual_km <= 10000:
                return 80 + (10000 - annual_km) / 10000 * 20
            elif annual_km <= 15000:
                return 60 + (15000 - annual_km) / 5000 * 20
            elif annual_km <= 20000:
                # More severe: 15k-20k now scores 10-30 instead of 40-60
                return 10 + (20000 - annual_km) / 5000 * 20
            else:
                # More severe: >20k decreases faster and caps lower
                return max(0, 10 - (annual_km - 20000) / 5000 * 10)

        df['usage_score'] = df['annual_mileage'].apply(mileage_score)
    else:
        df['usage_score'] = None

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

    df['range_efficiency'] = df.apply(
        lambda row: row['battery_range_km'] / row['horse_power_kw']
        if pd.notna(row['battery_range_km']) and pd.notna(row['horse_power_kw']) and row['horse_power_kw'] > 0
        else None,
        axis=1
    )

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

            raw_score = 0
            matched_desired = 0

            for equipment in car_equipment:
                if equipment in desired_equipment:
                    raw_score += 2
                    matched_desired += 1
                else:
                    raw_score += 1

            equipment_scores_raw.append({
                'raw_score': raw_score,
                'matched_desired': matched_desired,
                'total_equipment': len(car_equipment)
            })
        except Exception as e:
            logger.warning(f"      Error calculating equipment score for car {idx}: {e}")
            equipment_scores_raw.append(None)

    valid_scores = [s['raw_score'] for s in equipment_scores_raw if s is not None]

    if len(valid_scores) > 0:
        min_score = min(valid_scores)
        max_score = max(valid_scores)

        if max_score > min_score:
            normalized_scores = []
            for score_data in equipment_scores_raw:
                if score_data is None:
                    normalized_scores.append(None)
                else:
                    normalized_score = 100 * (score_data['raw_score'] - min_score) / (max_score - min_score)
                    normalized_scores.append(normalized_score)
            df['equipment_score'] = normalized_scores
        else:
            df['equipment_score'] = 50 if len(valid_scores) > 0 else None
    else:
        df['equipment_score'] = None

    valid_equipment_scores = df['equipment_score'].dropna()
    if len(valid_equipment_scores) > 0:
        logger.info(f"      ✓ Equipment scores calculated - Avg: {valid_equipment_scores.mean():.1f}, Min: {valid_equipment_scores.min():.1f}, Max: {valid_equipment_scores.max():.1f}")
        matched_counts = [s['matched_desired'] for s in equipment_scores_raw if s is not None]
        if matched_counts:
            logger.info(f"      ✓ Desired equipment matches - Avg: {sum(matched_counts)/len(matched_counts):.1f}/{len(desired_equipment)}")

    return df


def calculate_final_score(df):
    """Calculate final overall score combining all category scores"""
    df = df.copy()

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


def calculate_all_scores(df, preferences_file=None):
    """Calculate all scoring metrics and add them to the DataFrame"""
    logger.info("=" * 60)
    logger.info("CALCULATING SCORING METRICS...")
    logger.info("=" * 60)

    logger.info("  → Calculating age and usage metrics...")
    df = calculate_age_metrics(df)

    logger.info("  → Calculating value efficiency metrics...")
    df = calculate_value_efficiency_metrics(df)

    logger.info("  → Calculating age & usage scores...")
    df = calculate_age_usage_scores(df)

    logger.info("  → Calculating performance/range scores...")
    df = calculate_performance_range_scores(df)

    if preferences_file:
        logger.info("  → Calculating equipment scores...")
        df = calculate_equipment_scores(df, preferences_file)
    else:
        logger.warning("  → Skipping equipment scores (no preferences file provided)")
        df['equipment_score'] = None

    logger.info("  → Calculating final overall score...")
    df = calculate_final_score(df)

    logger.info("  ✓ All scoring metrics calculated")
    logger.info("=" * 60)

    return df
