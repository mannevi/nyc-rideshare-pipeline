import os
import pandas as pd
from scripts.logger import get_logger

logger = get_logger("quality_checks")

PROCESSED_DIR = "data/processed"


def check_file_exists():
    """
    Confirms processed Parquet file was created.
    """
    logger.info("Running file existence check...")
    filepath = os.path.join(PROCESSED_DIR, "trips_with_weather.parquet")

    if os.path.exists(filepath):
        size_mb = os.path.getsize(filepath) / (1024 * 1024)
        logger.info(f"File exists: {filepath} ({size_mb:.1f} MB)")
        return True
    else:
        logger.warning(f"File NOT found: {filepath}")
        return False


def check_row_count(min_rows=100000):
    """
    Confirms minimum expected rows were processed.
    """
    logger.info("Running row count check...")
    filepath = os.path.join(PROCESSED_DIR, "trips_with_weather.parquet")

    try:
        df = pd.read_parquet(filepath)
        count = len(df)

        if count >= min_rows:
            logger.info(f"Row count check PASSED: {count:,} rows")
            return True
        else:
            logger.warning(f"Row count check FAILED: {count:,} rows — expected {min_rows:,}")
            return False

    except Exception as e:
        logger.error(f"Row count check error: {e}")
        return False


def check_nulls():
    """
    Confirms no nulls in critical columns.
    """
    logger.info("Running null check...")
    filepath = os.path.join(PROCESSED_DIR, "trips_with_weather.parquet")

    try:
        df = pd.read_parquet(filepath)
        critical_columns = ["pickup_date", "company",
                           "trip_miles", "base_passenger_fare"]

        passed = True
        for col in critical_columns:
            null_count = df[col].isnull().sum()
            if null_count > 0:
                logger.warning(f"Null check FAILED: {null_count} nulls in '{col}'")
                passed = False
            else:
                logger.info(f"Null check PASSED: '{col}'")

        return passed

    except Exception as e:
        logger.error(f"Null check error: {e}")
        return False


def check_company_values():
    """
    Confirms company column only has Uber or Lyft.
    """
    logger.info("Running company values check...")
    filepath = os.path.join(PROCESSED_DIR, "trips_with_weather.parquet")

    try:
        df = pd.read_parquet(filepath)
        valid_companies = {"Uber", "Lyft", "Other"}
        actual_companies = set(df["company"].unique())
        invalid = actual_companies - valid_companies

        if invalid:
            logger.warning(f"Company check FAILED: unexpected values {invalid}")
            return False

        logger.info(f"Company check PASSED: {actual_companies}")
        return True

    except Exception as e:
        logger.error(f"Company check error: {e}")
        return False


def check_fare_range():
    """
    Confirms all fares are positive.
    """
    logger.info("Running fare range check...")
    filepath = os.path.join(PROCESSED_DIR, "trips_with_weather.parquet")

    try:
        df = pd.read_parquet(filepath)
        invalid = len(df[df["base_passenger_fare"] <= 0])

        if invalid > 0:
            logger.warning(f"Fare range check FAILED: {invalid} rows with fare <= 0")
            return False

        logger.info("Fare range check PASSED: all fares positive")
        return True

    except Exception as e:
        logger.error(f"Fare range check error: {e}")
        return False


def run_all_checks():
    """
    Runs all quality checks and returns overall pass/fail.
    """
    logger.info("=" * 50)
    logger.info("Starting data quality checks...")
    logger.info("=" * 50)

    results = {
        "file_exists":     check_file_exists(),
        "row_count":       check_row_count(),
        "null_check":      check_nulls(),
        "company_values":  check_company_values(),
        "fare_range":      check_fare_range(),
    }

    logger.info("=" * 50)
    logger.info("Data Quality Summary:")
    all_passed = True
    for check, passed in results.items():
        status = "PASSED" if passed else "FAILED"
        logger.info(f"  {check}: {status}")
        if not passed:
            all_passed = False

    if all_passed:
        logger.info("Overall: ALL CHECKS PASSED")
    else:
        logger.warning("Overall: SOME CHECKS FAILED")

    logger.info("=" * 50)
    return all_passed


if __name__ == "__main__":
    run_all_checks()