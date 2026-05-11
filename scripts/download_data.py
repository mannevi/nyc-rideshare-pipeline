import requests
import os
from dotenv import load_dotenv
from scripts.logger import get_logger

load_dotenv()
logger = get_logger("download_data")

# Feb 2026 HVFHV (Uber + Lyft) data
TLC_URL = "https://d37ci6vzurychx.cloudfront.net/trip-data/fhvhv_tripdata_2026-02.parquet"
RAW_DIR = "data/raw"


def download_tlc_data():
    """
    Downloads NYC TLC rideshare Parquet file
    directly from the official TLC website.
    Free, no account needed.
    """
    os.makedirs(RAW_DIR, exist_ok=True)
    filepath = os.path.join(RAW_DIR, "fhvhv_tripdata_2026-02.parquet")

    if os.path.exists(filepath):
        logger.info(f"File already exists: {filepath} — skipping download")
        return filepath

    logger.info("Downloading NYC TLC rideshare data (Feb 2026)...")
    logger.info("Large parquet file detected — download may take a few minutes...")
    response = requests.get(TLC_URL, stream=True)

    if response.status_code == 200:
        total = 0
        with open(filepath, "wb") as f:
            for chunk in response.iter_content(chunk_size=8192):
                f.write(chunk)
                total += len(chunk)

        size_mb = total / (1024 * 1024)
        logger.info(f"Download complete: {filepath} ({size_mb:.1f} MB)")
        return filepath
    else:
        logger.error(f"Download failed. Status: {response.status_code}")
        raise Exception("TLC data download failed")


if __name__ == "__main__":
    download_tlc_data()