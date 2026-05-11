import os
import json
import pandas as pd
from dotenv import load_dotenv
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from scripts.logger import get_logger

load_dotenv()
logger = get_logger("process_with_spark")

RAW_DIR       = "data/raw"
PROCESSED_DIR = "data/processed"


def create_spark_session():
    import os
    os.environ["PYSPARK_PYTHON"] = "python"
    os.environ["PYSPARK_DRIVER_PYTHON"] = "python"

    spark = SparkSession.builder \
        .appName("NYC Rideshare Pipeline") \
        .config("spark.driver.memory", "2g") \
        .config("spark.sql.shuffle.partitions", "4") \
        .config("spark.driver.maxResultSize", "1g") \
        .config("spark.driver.extraJavaOptions",
                "--add-opens=java.base/sun.nio.ch=ALL-UNNAMED "
                "--add-opens=java.base/java.nio=ALL-UNNAMED "
                "--add-opens=java.base/java.util=ALL-UNNAMED") \
        .config("spark.executor.extraJavaOptions",
                "--add-opens=java.base/sun.nio.ch=ALL-UNNAMED "
                "--add-opens=java.base/java.nio=ALL-UNNAMED "
                "--add-opens=java.base/java.util=ALL-UNNAMED") \
        .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")
    logger.info("SparkSession created successfully")
    return spark

def process_trips_with_spark(spark):
    """
    Uses Spark to process large trips Parquet file.
    Spark is the right tool here — 1.9M rows.
    """
    logger.info("Reading trips Parquet file...")
    filepath = os.path.join(RAW_DIR, "fhvhv_tripdata_2026-02.parquet")

    df = spark.read.parquet(filepath)
    logger.info(f"Raw trips loaded: {df.count():,} rows")

    # Sample 10% for local processing
    df = df.sample(fraction=0.1, seed=42)
    logger.info(f"Sampled 10%: {df.count():,} rows")

    # Map license number to company name
    df = df.withColumn("company",
        F.when(F.col("hvfhs_license_num") == "HV0003", "Uber")
         .when(F.col("hvfhs_license_num") == "HV0005", "Lyft")
         .otherwise("Other")
    )

    # Extract date and hour
    df = df.withColumn("pickup_date", F.to_date("pickup_datetime")) \
           .withColumn("pickup_hour", F.hour("pickup_datetime")) \
           .withColumn("day_of_week", F.dayofweek("pickup_datetime")) \
           .withColumn("is_weekend",
               F.when(F.dayofweek("pickup_datetime").isin([1, 7]), True)
                .otherwise(False)
           )

    # Convert trip_time to minutes
    df = df.withColumn("trip_minutes",
        F.round(F.col("trip_time") / 60, 2)
    )

    # Filter invalid records
    logger.info("Filtering invalid records...")
    before = df.count()
    df = df.filter(
        (F.col("trip_miles") > 0) &
        (F.col("trip_minutes") > 0) &
        (F.col("base_passenger_fare") > 0) &
        (F.col("pickup_datetime").isNotNull()) &
        (F.col("dropoff_datetime").isNotNull())
    )
    after = df.count()
    logger.info(f"Removed {before - after:,} invalid rows")
    logger.info(f"Clean trips remaining: {after:,} rows")

    # Select only needed columns
    df = df.select(
        "pickup_date",
        "pickup_hour",
        "day_of_week",
        "is_weekend",
        "company",
        "PULocationID",
        "DOLocationID",
        "trip_miles",
        "trip_minutes",
        "base_passenger_fare",
        "driver_pay",
        "tips",
        "congestion_surcharge",
        "cbd_congestion_fee"
    )

    # Save as Parquet locally
    os.makedirs(PROCESSED_DIR, exist_ok=True)
    trips_path = os.path.join(PROCESSED_DIR, "trips_clean")
    df.write.mode("overwrite").parquet(trips_path)
    logger.info(f"Trips saved to {trips_path}")

    return trips_path


def process_weather_with_pandas():
    """
    Uses Pandas for weather — only 672 rows.
    Pandas is the right tool here — small lookup table.
    No need for Spark on tiny data.
    """
    logger.info("Processing weather data with Pandas...")
    filepath = os.path.join(RAW_DIR, "nyc_weather_2026_02.json")

    with open(filepath, "r") as f:
        data = json.load(f)

    hourly = data["hourly"]

    weather_df = pd.DataFrame({
        "datetime":      hourly["time"],
        "temperature_c": hourly["temperature_2m"],
        "precipitation": hourly["precipitation"],
        "windspeed_kmh": hourly["windspeed_10m"],
        "weathercode":   hourly["weathercode"],
    })

    def get_weather_label(code):
        if code == 0:              return "Clear Sky"
        elif code == 1:            return "Mainly Clear"
        elif code == 2:            return "Partly Cloudy"
        elif code == 3:            return "Overcast"
        elif code in [61, 63, 65]: return "Rainy"
        elif code in [71, 73, 75]: return "Snowy"
        elif code in [95, 96, 99]: return "Thunderstorm"
        else:                      return "Unknown"

    weather_df["weather_label"] = weather_df["weathercode"].apply(get_weather_label)
    weather_df["pickup_date"]   = pd.to_datetime(weather_df["datetime"]).dt.date.astype(str)
    weather_df["pickup_hour"]   = pd.to_datetime(weather_df["datetime"]).dt.hour

    logger.info(f"Weather records processed: {len(weather_df)}")
    return weather_df


def join_trips_weather(trips_path, weather_df):
    """
    Reloads Spark-processed trips in Pandas.
    Joins with weather on date and hour.
    Pandas is fine here — trips are already filtered
    and sampled to a manageable size.
    """
    logger.info("Loading processed trips into Pandas...")
    trips_df = pd.read_parquet(trips_path)
    trips_df["pickup_date"] = trips_df["pickup_date"].astype(str)
    logger.info(f"Trips loaded: {len(trips_df):,} rows")

    logger.info("Joining trips with weather...")
    weather_slim = weather_df[[
        "pickup_date", "pickup_hour",
        "temperature_c", "precipitation",
        "windspeed_kmh", "weather_label"
    ]]

    joined_df = trips_df.merge(
        weather_slim,
        on=["pickup_date", "pickup_hour"],
        how="left"
    )

    logger.info(f"Joined dataset: {len(joined_df):,} rows")
    logger.info(f"Columns: {list(joined_df.columns)}")
    return joined_df


def save_final_data(joined_df):
    """
    Saves final joined dataset as Parquet.
    """
    output_path = os.path.join(PROCESSED_DIR, "trips_with_weather.parquet")
    joined_df.to_parquet(output_path, index=False)
    logger.info(f"Final dataset saved to: {output_path}")
    logger.info(f"Sample:\n{joined_df.head(3)}")


def run_spark_processing():
    spark = None
    try:
        spark = create_spark_session()

        # Step 1 — Process large trips with Spark
        trips_path = process_trips_with_spark(spark)

        # Step 2 — Stop Spark before Pandas work
        spark.stop()
        spark = None
        logger.info("SparkSession stopped.")

        # Step 3 — Process small weather with Pandas
        weather_df = process_weather_with_pandas()

        # Step 4 — Join in Pandas
        joined_df = join_trips_weather(trips_path, weather_df)

        # Step 5 — Save final result
        save_final_data(joined_df)

        logger.info("Processing complete.")

    except Exception as e:
        logger.error(f"Processing failed: {e}")
        raise

    finally:
        if spark:
            spark.stop()
            logger.info("SparkSession stopped.")


if __name__ == "__main__":
    run_spark_processing()