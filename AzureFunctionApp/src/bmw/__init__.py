"""
BMW Car Scraping and Data Processing Package

Modules:
- config: Configuration and constants
- parser: String parsing utilities
- scraper: Web scraping functionality
- scorer: Scoring calculations
- data_processor: Historical data processing
- database: Supabase integration
- main: Main pipeline orchestrator
"""

__version__ = "1.0.0"
__author__ = "Ardonis Shalaj"

from . import config, data_processor, database, parser, scorer, scraper

__all__ = [
    'config',
    'parser',
    'scraper',
    'scorer',
    'data_processor',
    'database'
]
