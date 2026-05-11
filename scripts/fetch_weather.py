import requests
import json
import os
from dotenv import load_dotenv
from scripts.logger import get_logger

load_dotenv()
logger = get_logger("fetch_weather")

RAW_DIR = "data/raw"

# NYC coordinates
NYC_LAT = 40.7128
NYC_LON = -74.0060


def fetch_nyc_weather():
    """
    Fetches hourly weather data for NYC for
    January 2024 using Open-Meteo API.
    Same API you used in Project 1.
    """
    os.makedirs(RAW_DIR, exist_ok=True)
    filepath = os.path.join(RAW_DIR, "nyc_weather_2026_02.json")

    if os.path.exists(filepath):
        logger.info(f"Weather file already exists: {filepath} — skipping")
        return filepath

    logger.info("Fetching NYC weather data for February 2026...")

    url = (
        f"https://archive-api.open-meteo.com/v1/archive"
        f"?latitude={NYC_LAT}&longitude={NYC_LON}"
        f"&start_date=2026-02-01&end_date=2026-02-28"
        f"&hourly=temperature_2m,precipitation,windspeed_10m,weathercode"
        f"&timezone=America/New_York"
    )

    response = requests.get(url)

    if response.status_code == 200:
        data = response.json()
        with open(filepath, "w") as f:
            json.dump(data, f)
        logger.info(f"Weather data saved: {filepath}")
        logger.info(f"Total hourly records: {len(data['hourly']['time'])}")
        return filepath
    else:
        logger.error(f"Weather API failed. Status: {response.status_code}")
        raise Exception("Weather data fetch failed")


if __name__ == "__main__":
    fetch_nyc_weather()