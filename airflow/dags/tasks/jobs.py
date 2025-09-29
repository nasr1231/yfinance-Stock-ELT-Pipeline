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

            df.to_json(f"/opt/airflow/dags/{t}_data.json", orient="records")
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
    Uploads a JSON file retrieved from Airflow XComs to a Snowflake internal stage
    only if the file for that ticker does not already exist.
    """        
        
    data = context['ti'].xcom_pull(task_ids='get_stock_prices', key='stock_prices')
    request_period = context['ti'].xcom_pull(task_ids='get_stock_prices', key='request_period')
    request_interval = context['ti'].xcom_pull(task_ids='get_stock_prices', key='request_interval')    

    if not data:        
        logging.error("❌ No data found in XCom.")
        raise ValueError("No stock data found in XCom")

    hook = SnowflakeHook(snowflake_conn_id='snowflake_conn')
    
    for ticker, records in data.items():        
        dest_file_name = f"{datetime.now().year}_{request_period}_{request_interval}_{ticker}.json"
        stage_name = 'DATA_LAKE.RAW_JSON.raw_json'
        
        try:
            # Check if file already exists in stage
            list_sql = f"LIST @{stage_name} pattern='{dest_file_name}'"
            result = hook.run(list_sql, autocommit=True, return_dictionaries=True)
            if result and len(result) > 0:
                logging.info(f"❗ File {dest_file_name} already exists in stage @{stage_name}. Skipping upload.")
                continue
            
            try:
                json_data = json.dumps(records)
            except (TypeError, ValueError) as e:
                logging.error(f"❌ Failed to serialize data for ticker {ticker}: {str(e)}")
                continue
            
            with tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False) as tmp_file:
                tmp_file.write(json_data)
                tmp_file_path = tmp_file.name

            try:                
                put_sql = f"""
                    PUT file://{tmp_file_path} @{stage_name}/{dest_file_name}                    
                    OVERWRITE = TRUE
                """
                hook.run(put_sql, autocommit=True)
                logging.info(f"✅ File {dest_file_name} uploaded successfully to @{stage_name}.")
            
            except Exception as e:
                logging.error(f"❌ Failed to upload {dest_file_name} to @{stage_name}: {str(e)}")
                raise
                        
            finally: # Clean up temporary file                
                if os.path.exists(tmp_file_path):
                    os.unlink(tmp_file_path)

        except Exception as e:
            logging.error(f"❌ Error processing ticker {ticker}: {str(e)}")
            continue    

        logging.info(f"✅✅ File {dest_file_name} uploaded successfully. ✅✅")
        
    logging.info("✅ All files processed successfully.")        