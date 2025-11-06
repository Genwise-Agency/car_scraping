import logging
import re
from datetime import datetime

from .config import FRENCH_MONTHS

logger = logging.getLogger(__name__)


def parse_price(price_str):
    """Convert price string like '59 950,00 €' to float like 59950.0"""
    if not price_str:
        return None
    try:
        cleaned = price_str.replace('€', '').strip()
        cleaned = re.sub(r'\s+', '', cleaned)
        cleaned = cleaned.replace(',', '.')
        cleaned = re.sub(r'[^\d\.\-]', '', cleaned)
        return float(cleaned)
    except Exception as e:
        logger.warning(f"Error parsing price: {e}")
        return None


def parse_kilometers(km_str):
    """Convert kilometers string like '9500 km' to integer like 9500"""
    if not km_str:
        return None
    try:
        numbers = re.findall(r'\d+', km_str.replace(' ', ''))
        if numbers:
            return int(numbers[0])
    except Exception as e:
        logger.warning(f"Error parsing kilometers: {e}")
    return None


def parse_car_id(car_id_str):
    """Convert car ID string to integer"""
    if not car_id_str:
        return None
    try:
        return int(car_id_str.strip())
    except Exception as e:
        logger.warning(f"Error parsing car ID: {e}")
        return None


def parse_horse_power(power_str):
    """Extract kW and PS from power string like '210 kW (286 PS)'"""
    if not power_str:
        return None, None
    try:
        kw_match = re.search(r'(\d+)\s*kW', power_str)
        kw = int(kw_match.group(1)) if kw_match else None
        ps_match = re.search(r'\((\d+)\s*PS\)', power_str)
        ps = int(ps_match.group(1)) if ps_match else None
        return kw, ps
    except Exception as e:
        logger.warning(f"Error parsing horse power: {e}")
        return None, None


def parse_battery_range(range_str):
    """Extract battery range from string like '475 km' to integer like 475"""
    if not range_str:
        return None
    try:
        numbers = re.findall(r'\d+', range_str.replace(' ', ''))
        if numbers:
            return int(numbers[0])
    except Exception as e:
        logger.warning(f"Error parsing battery range: {e}")
    return None


def parse_registration_date(date_str):
    """Convert French date string like 'août 2025' to datetime object"""
    if not date_str:
        return None

    try:
        parts = date_str.strip().lower().split()
        if len(parts) >= 2:
            month_name = parts[0]
            year = int(parts[1])

            if month_name in FRENCH_MONTHS:
                month = FRENCH_MONTHS[month_name]
                return datetime(year, month, 1)
    except Exception as e:
        logger.warning(f"Error parsing registration date: {e}")
    return None
