import logging
import os

import requests
from dotenv import load_dotenv

load_dotenv()

pushover_url = "https://api.pushover.net/1/messages.json"
pushover_token = os.getenv("PUSHOVER_TOKEN", "")
pushover_user = os.getenv("PUSHOVER_USER", "")

logger = logging.getLogger(__name__)


class Pushover:
    """Pushover notification service for BMW scraping pipeline"""

    def __init__(self):
        """Initialize Pushover client"""
        self.token = pushover_token
        self.user = pushover_user
        self.enabled = bool(self.token and self.user)

        if not self.enabled:
            logger.warning("Pushover notifications disabled: PUSHOVER_TOKEN and PUSHOVER_USER must be set in environment variables")

    def _push(self, message: str, title: str = "BMW Scraping"):
        """Send notification via Pushover"""
        if not self.enabled:
            logger.debug(f"Pushover disabled - would send: {title}: {message}")
            return

        try:
            payload = {
                "user": self.user,
                "token": self.token,
                "message": message,
                "title": title
            }
            response = requests.post(pushover_url, data=payload, timeout=10)
            response.raise_for_status()
            logger.info(f"Pushover notification sent: {title}")
        except Exception as e:
            logger.error(f"Failed to send Pushover notification: {e}")

    def notify_scraping_complete(self, stats: dict):
        """
        Send notification when scraping pipeline completes

        Args:
            stats: Dictionary with scraping statistics including:
                - success: bool
                - cars_scraped: int
                - db_synced: bool
                - error: str (optional)
        """
        if stats.get("success"):
            title = "✅ BMW Scraping - Success"
            message = f"Cars scraped: {stats.get('cars_scraped', 0)}\n"

            if stats.get("sync_db"):
                if stats.get("db_synced"):
                    message += "Database: ✅ Synced"
                else:
                    message += "Database: ❌ Sync failed"
            else:
                message += "Database: ⏭️ Not synced"
        else:
            title = "❌ BMW Scraping - Failed"
            error_msg = stats.get("error", "Unknown error")
            message = f"Status: Failed\nError: {error_msg}"

        self._push(message, title)

    def notify_error(self, error_message: str):
        """Send error notification"""
        self._push(f"Error: {error_message}", "❌ BMW Scraping - Error")

