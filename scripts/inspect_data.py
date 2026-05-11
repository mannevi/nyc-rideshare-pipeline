import pandas as pd
import json
import os
from scripts.logger import get_logger

logger = get_logger("inspect_data")

RAW_DIR = "data/raw"


def inspect_trips():
    """
    Reads a small sample of the Parquet file
    to understand structure before PySpark processing.
    """
    filepath = os.path.join(RAW_DIR, "fhvhv_tripdata_2026-02.parquet")
    logger.info("Inspecting trips data...")

    # Read just 5 rows — no need to load all 400MB
    df = pd.read_parquet(filepath, engine="pyarrow")
    sample = df.head(5)

    logger.info(f"Total rows: {len(df):,}")
    logger.info(f"Total columns: {len(df.columns)}")
    logger.info(f"Columns: {list(df.columns)}")
    logger.info(f"Data types:\n{df.dtypes}")
    logger.info(f"Null counts:\n{df.isnull().sum()}")
    logger.info(f"Sample rows:\n{sample}")


def inspect_weather():
    """
    Reads the weather JSON to understand structure.
    """
    filepath = os.path.join(RAW_DIR, "nyc_weather_2026_02.json")
    logger.info("Inspecting weather data...")

    with open(filepath, "r") as f:
        data = json.load(f)

    hourly = data["hourly"]
    logger.info(f"Weather fields: {list(hourly.keys())}")
    logger.info(f"Total hourly records: {len(hourly['time'])}")
    logger.info(f"First 5 timestamps: {hourly['time'][:5]}")
    logger.info(f"First 5 temperatures: {hourly['temperature_2m'][:5]}")
    logger.info(f"First 5 precipitation: {hourly['precipitation'][:5]}")


if __name__ == "__main__":
    inspect_trips()
    inspect_weather()