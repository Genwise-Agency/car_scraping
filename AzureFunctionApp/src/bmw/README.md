# BMW Car Scraping and Processing Pipeline

A comprehensive, modular Python application for scraping BMW inventory, processing car data with advanced scoring metrics, tracking historical changes, and syncing to Supabase.

## Project Structure

```
src/bmw/
├── __init__.py              # Package initialization
├── config.py               # Configuration and constants
├── parser.py               # String parsing utilities
├── scraper.py              # Web scraping functionality
├── scorer.py               # Scoring calculations
├── data_processor.py       # Historical data tracking and merging
├── database.py             # Supabase integration
├── main.py                 # Main pipeline orchestrator
└── README.md               # This file
```

## Modules Overview

### config.py

Centralized configuration for the entire pipeline including:

- Supabase credentials (loaded from environment variables)
- BMW URL and browser settings
- File paths and output directories
- Column definitions for tracking
- French month mapping for date parsing

### parser.py

String parsing utilities for extracting data from web pages:

- `parse_price()` - Convert price strings (e.g., "59 950,00 €" → 59950.0)
- `parse_kilometers()` - Extract km values
- `parse_car_id()` - Convert car ID to integer
- `parse_horse_power()` - Extract kW and PS values
- `parse_battery_range()` - Extract battery range
- `parse_registration_date()` - Parse French date strings

### scraper.py

Playwright-based web scraping:

- `scrape_bmw_inventory()` - Main scraping orchestrator
- `extract_car_data()` - Extract detailed car information from individual pages
- Handles cookie acceptance, pagination, and equipment extraction

### scorer.py

Advanced scoring system with multiple metrics:

- Age and usage metrics calculation
- Value efficiency scoring (price per km, price per kW, etc.)
- Performance and range scoring
- Equipment preference matching
- Final composite score (weighted average of all scores)

### data_processor.py

Historical data tracking using Slowly Changing Dimensions (SCD Type 2):

- `merge_historical_data()` - Track car changes over time
- `merge_equipment_history()` - Normalize and track equipment changes
- `merge_scores_history()` - Track score changes
- `export_equipment_list()` - Export standardized equipment catalog

### database.py

Supabase database integration:

- `SupabaseClient` class with batch operations
- Sync methods for car history, equipment, and scores
- Error handling and logging

### main.py

Complete pipeline orchestrator that:

1. Scrapes BMW inventory
2. Calculates scores and metrics
3. Merges with historical data
4. Processes equipment data
5. Exports to Excel
6. Syncs to Supabase (optional)

## Installation

1. Install dependencies:

```bash
pip install -r requirements.txt
```

2. Set up environment variables:

```bash
export SUPABASE_URL="your-supabase-url"
export SUPABASE_KEY="your-supabase-key"
```

3. Install Playwright browsers:

```bash
playwright install
```

## Usage

### Basic Usage

```bash
cd src/bmw
python main.py
```

### With Custom URL

```bash
python main.py --url "https://www.bmw.be/fr-be/sl/stocklocator_uc/results?filters=..."
```

### Testing with Limited Cars

```bash
python main.py --limit 5
```

### Skip Database Sync

```bash
python main.py --no-db-sync
```

### From Python

```python
from main import main

main(
    url="https://www.bmw.be/...",
    test_limit=10,
    sync_db=True
)
```

## Database Schema

The pipeline syncs to Supabase tables:

### bmw_cars

Main car records with current status

### bmw_cars_history

Complete historical tracking with SCD Type 2:

- `is_latest`: Boolean flag for current version
- `status`: 'active' or 'sold'
- `valid_from` / `valid_to`: Date range validity
- Tracks all changes to car attributes

### bmw_cars_equipment

Normalized equipment records with history tracking

### bmw_cars_scores

Scoring metrics with historical tracking

## Scoring System

### Components

1. **Value Efficiency Score** (25% weight)

   - Price per kilometer
   - Price per kW
   - Price per battery range

2. **Age & Usage Score** (25% weight)

   - Car age (newer = better)
   - Annual mileage (lower = better)
   - Newness penalty

3. **Performance/Range Score** (25% weight)

   - Range adequacy (≥500km = 100)
   - Power adequacy (≥300kW = 100)
   - Range efficiency

4. **Equipment Score** (25% weight)
   - Match against desired equipment list
   - Configurable preferences file

### Final Score

Weighted average of all four components (0-100 scale)

## Configuration

### Preferences File

Create `data/ardonis_bmw_preferences.json`:

```json
{
  "desired_equipment": [
    "M leather steering wheel",
    "M Sport package",
    "Sun roof",
    "Adaptive LED headlights"
  ]
}
```

## Output Files

Generated in `results/bmw/`:

- `bmw_cars_YYYY-MM-DD.xlsx` - Current inventory with all metrics
- `bmw_cars_history.csv` - Complete historical tracking
- `bmw_cars_equipment_history.csv` - Equipment tracking
- `bmw_cars_scores_history.csv` - Scores tracking
- `equipment_list.json` - Standardized equipment catalog

## Features

✓ Modular architecture for easy maintenance and testing
✓ Comprehensive error handling and logging
✓ Batch database operations with pagination
✓ Historical data tracking with SCD Type 2
✓ Equipment normalization and tracking
✓ Advanced multi-factor scoring system
✓ Excel export with all metrics
✓ Supabase synchronization
✓ Command-line interface with options

## Environment Variables

```
SUPABASE_URL       # Your Supabase project URL
SUPABASE_KEY       # Your Supabase API key
```

## Logging

Comprehensive logging output with timestamps and log levels:

- INFO: General pipeline progress
- WARNING: Non-critical issues
- ERROR: Critical errors that may stop pipeline

## Error Handling

- Graceful fallback for missing data
- CSV loading errors recover with fresh data
- Database sync errors don't stop local exports
- Detailed error messages for debugging

## Future Enhancements

- Add support for other BMW markets
- Implement email notifications
- Add data visualization dashboard
- Support for other car brands
- Performance optimization for large datasets

---

**Version:** 1.0.0
**Last Updated:** 2025-11-02
