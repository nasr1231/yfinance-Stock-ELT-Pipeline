import yfinance as yf
from airflow.decorators import dag, task
from airflow.sensors.base import PokeReturnValue
from airflow.operators.python import PythonOperator
from airflow.providers.docker.operators.docker import DockerOperator
from datetime import datetime, timedelta
from tasks.jobs import check_snowflake_conn, fetch_stock_data, store_stock_files
import logging

default_args = {
    "owner": "airflow",
    "retries": 1,
    "retry_delay": timedelta(minutes=5)
}

@dag(
    dag_id="stock_market_ELT_Pipeline",
    start_date=datetime(2025, 8, 5),
    schedule_interval='@daily',
    catchup=False,
    default_args=default_args,
    tags=["stock_market"]
)
def stock_market_ETL_Pipeline():
    
    @task.sensor(poke_interval=30, timeout=300, mode='reschedule')
    def is_api_available() -> PokeReturnValue:
        """
        Check if Yahoo Finance API is reachable by returning data
        for a specific ticker.
        """
        logging.info("Starting checking API Availability...")
        ticker = "AAPL"
        try:
            df = yf.download(ticker, period="1d", interval="1d", progress=False)
            condition = not df.empty
            logging.info("API Availability Check: %s", "Available ✅✅" if condition else "Not Available❌❌")
        except Exception as e:
            logging.error("❌ Error: %s", e)
            condition = False

        return PokeReturnValue(is_done=condition)

    fetch_task = PythonOperator(
        task_id='get_stock_prices',
        python_callable = fetch_stock_data,
        provide_context=True        
    )
    
    check_snowflake_connection = PythonOperator(
        task_id="check_snowflake",
        python_callable = check_snowflake_conn
    )
    
    load_to_snowflake = PythonOperator(
        task_id="load_data_to_snowflake",
        python_callable = store_stock_files,
        provide_context=True
    )
    
    transform_data_load_DWH = DockerOperator(
        task_id="run_spark_job",
        image="spark-job:latest",
        container_name="spark-job-container",
        docker_url="unix://var/run/docker.sock",
        network_mode="yfinance-stock-elt-pipeline_stock_market_network",        
        command="/spark/bin/spark-submit --master spark://spark-master:7077 /app/spark.py",
        api_version="auto",
        auto_remove="success",
        mount_tmp_dir = False,
    )
    
    is_api_available() >> fetch_task >> check_snowflake_connection >> load_to_snowflake >> transform_data_load_DWH


stock_market_ETL_Pipeline()
