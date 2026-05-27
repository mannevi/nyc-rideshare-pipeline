from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator

from scripts.download_data import download_tlc_data
from scripts.fetch_weather import fetch_nyc_weather
from scripts.upload_raw_to_s3 import upload_all_raw_files, upload_processed_data
from scripts.process_with_spark import run_spark_processing
from scripts.quality_checks import run_all_checks

default_args = {
    "owner": "manne_vaishnavi",
    "depends_on_past": False,
    "start_date": datetime(2026, 2, 1),
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    "email_on_failure": False,
}

with DAG(
    dag_id="nyc_rideshare_pipeline",
    default_args=default_args,
    description="End-to-end NYC Rideshare + Weather ETL Pipeline",
    schedule_interval="@monthly",
    catchup=False,
    tags=["etl", "pyspark", "snowflake", "dbt", "nyc"],
) as dag:

    download_tlc = PythonOperator(
        task_id="download_tlc_data",
        python_callable=download_tlc_data,
    )

    fetch_weather = PythonOperator(
        task_id="fetch_weather_data",
        python_callable=fetch_nyc_weather,
    )

    upload_s3 = PythonOperator(
        task_id="upload_raw_to_s3",
        python_callable=upload_all_raw_files,
    )

    spark_process = PythonOperator(
        task_id="spark_processing",
        python_callable=run_spark_processing,
    )

    upload_processed = PythonOperator(
        task_id="upload_processed_to_s3",
        python_callable=upload_processed_data,
    )

    quality_check = PythonOperator(
        task_id="run_quality_checks",
        python_callable=run_all_checks,
    )

    dbt_run = BashOperator(
        task_id="dbt_run",
        bash_command="cd /app/nyc_dbt && dbt run --profiles-dir /app/nyc_dbt",
    )

    dbt_test = BashOperator(
        task_id="dbt_test",
        bash_command="cd /app/nyc_dbt && dbt test --profiles-dir /app/nyc_dbt",
    )

    # Pipeline order
    [download_tlc, fetch_weather] >> upload_s3 >> spark_process >> upload_processed >> quality_check >> dbt_run >> dbt_test
