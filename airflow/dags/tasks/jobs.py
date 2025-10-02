import os
import tempfile
from datetime import datetime
import yfinance as yf
import json
import logging
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook

tickers = ["AAPL","MSFT","AMZN","GOOGL","META","NVDA","TSLA","BRK-B","JPM","V"]

def fetch_stock_data(**context):
    """
    Fetches the last 10 years of daily stock data for the provided tickers
    using yfinance, and pushes the result to XCom as a JSON string.
    """
    if not tickers:
        logging.error("❌ No tickers provided for data fetch.")
        raise ValueError("No tickers provided for data fetch.")
    
    logging.info("Starting stock data fetch job...")
    data = {}
    request_period = "10y"
    request_interval = "1d"
    
    for t in tickers:
        try:        
            logging.info(f"Fetching data for {t} from yfinance API...")               
            df = yf.download(
                t, 
                period = request_period, 
                interval= request_interval, 
                progress=False, 
                auto_adjust=False
            ).reset_index()
            
            if df.empty:
                logging.warning(f"⚠️ No data returned for {t}.")
                data[t] = {"error": f"No data returned for {t}"}
                continue
            
            df.columns = df.columns.get_level_values(0) ## Revoking multi-index columns            
            df['Date'] = df['Date'].dt.strftime('%Y-%m-%d')            
            df = df[["Date", "Open", "High", "Low", "Close", "Adj Close", "Volume"]]
            data[t] = df.to_dict(orient="records")

            #df.to_json(f"/opt/airflow/dags/{t}_data.json", orient="records") to store locally for testing
            logging.info("✅ Successfully fetched %d rows for %s", len(df), t)            

        except Exception as e:
            logging.exception("❌ Failed to fetch data for %s. Error: %s", t, e)
            data[t] = {"error": str(e)}
    
    context['ti'].xcom_push(key='stock_prices', value=data)
    context['ti'].xcom_push(key='request_period', value=request_period)
    context['ti'].xcom_push(key='request_interval', value=request_interval)
    
    logging.info(f"Finished pushing {len(data)} tickers to XCom.")
       
def check_snowflake_conn():    
    hook = SnowflakeHook(snowflake_conn_id="snowflake_conn")
    conn = hook.get_conn()
    cursor = conn.cursor()
    try:
        cursor.execute("SELECT 1")
        result = cursor.fetchone()
        if result and result[0] == 1:
            print("✅ Snowflake connection is ready.")
        else:
            raise Exception("❌ Snowflake connection has failed.")
    finally:
        cursor.close()
        conn.close()      
    
def store_stock_files(**context):
    """
    Inserts stock JSON data directly into Snowflake table raw_json_data.
    """

    data = context['ti'].xcom_pull(task_ids='get_stock_prices', key='stock_prices')
    request_period = context['ti'].xcom_pull(task_ids='get_stock_prices', key='request_period')
    request_interval = context['ti'].xcom_pull(task_ids='get_stock_prices', key='request_interval')

    if not data:
        logging.error("❌ No data found in XCom.")
        raise ValueError("No stock data found in XCom")

    hook = SnowflakeHook(snowflake_conn_id='snowflake_conn')

    table_name = 'DATA_LAKE.RAW_JSON.raw_json_data'

    for ticker, records in data.items():
        try:
            try:
                json_data = json.dumps(records)
            except (TypeError, ValueError) as e:
                logging.error(f"❌ Failed to serialize data for ticker {ticker}: {str(e)}")
                continue

            # Escape single quotes for SQL safety
            json_data_safe = json_data.replace("'", "''")

            # Insert directly into Snowflake table
            insert_sql = f"""
                INSERT INTO {table_name} (data, ticker, request_period, request_interval, load_timestamp)
                SELECT PARSE_JSON('{json_data_safe}'),
                       '{ticker}',
                       '{request_period}',
                       '{request_interval}',
                       CURRENT_TIMESTAMP();
            """
            hook.run(insert_sql, autocommit=True)
            logging.info(f"✅ Data for {ticker} inserted successfully into {table_name}.")

        except Exception as e:
            logging.error(f"❌ Error inserting data for ticker {ticker}: {str(e)}")
            continue

    logging.info("✅ All records inserted successfully.")
    