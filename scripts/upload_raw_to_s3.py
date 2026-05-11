import boto3
import os
from dotenv import load_dotenv
from scripts.logger import get_logger

load_dotenv()
logger = get_logger("upload_raw_to_s3")

BUCKET = os.getenv("AWS_BUCKET_NAME")
REGION = os.getenv("AWS_REGION")


def get_s3_client():
    return boto3.client(
        "s3",
        aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
        aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY"),
        region_name=REGION
    )


def upload_to_s3(local_path, s3_key):
    """
    Uploads a file to the raw zone in S3.
    raw/ folder = landing zone for source data.
    """
    try:
        client = get_s3_client()
        file_size = os.path.getsize(local_path) / (1024 * 1024)
        logger.info(f"Uploading {local_path} ({file_size:.1f} MB) to s3://{BUCKET}/{s3_key}")

        client.upload_file(local_path, BUCKET, s3_key)
        logger.info(f"Upload complete: s3://{BUCKET}/{s3_key}")

    except Exception as e:
        logger.error(f"Upload failed: {e}")
        raise


def upload_all_raw_files():
    logger.info("Starting raw data upload to S3...")

    upload_to_s3(
        "data/raw/fhvhv_tripdata_2026-02.parquet",
        "raw/trips/fhvhv_tripdata_2026-02.parquet"
    )

    upload_to_s3(
        "data/raw/nyc_weather_2026_02.json",
        "raw/weather/nyc_weather_2026_02.json"
    )

    logger.info("All raw files uploaded to S3 raw zone.")


if __name__ == "__main__":
    upload_all_raw_files()