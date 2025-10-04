from pyspark.sql import SparkSession
from pyspark.sql.functions import col, round, to_date, from_json, current_timestamp, explode, ArrayType
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, LongType
import logging
import os
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv("/app/secrets.env")
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

if __name__ == "__main__":

    def run_spark_job():
        try:
            # Snowflake connection options for read
            read_sf_options = {
                "sfURL": os.getenv("SNOWFLAKE_URL"),
                "sfAccount": os.getenv("SNOWFLAKE_ACCOUNT"),
                "sfUser": os.getenv("SNOWFLAKE_USER"),
                "sfPassword": os.getenv("SNOWFLAKE_PASSWORD"),
                "sfDatabase": os.getenv("SNOWFLAKE_DATABASE"),
                "sfSchema": os.getenv("SNOWFLAKE_SCHEMA"),      
                "sfRole": os.getenv("SNOWFLAKE_ROLE"),
                "sfWarehouse": os.getenv("SNOWFLAKE_WAREHOUSE")
            }

            # Snowflake connection options for write
            write_sf_options = {
                "sfURL": os.getenv("SNOWFLAKE_URL"),
                "sfAccount": os.getenv("SNOWFLAKE_ACCOUNT"),
                "sfUser": os.getenv("SNOWFLAKE_USER"),
                "sfPassword": os.getenv("SNOWFLAKE_PASSWORD"),
                "sfDatabase": os.getenv("SNOWFLAKE_DWH_DATABASE"),
                "sfSchema": os.getenv("SNOWFLAKE_DWH_SCHEMA"),
                "sfRole": os.getenv("SNOWFLAKE_ROLE"),
                "sfWarehouse": os.getenv("SNOWFLAKE_WAREHOUSE")
            }

            spark = SparkSession.builder.appName("Formatting-Stock-Data") \
                .config("spark.master", "spark://spark-master:7077") \
                .getOrCreate()

            df = spark.read \
                .format("snowflake") \
                .options(**read_sf_options) \
                .option("dbtable", "RAW_JSON_DATA") \
                .load()

            logging.info(f"Rows read from source: {df.count()}")
            df.show(5, truncate=False)

            # Define the JSON schema based on expected stock data structure
            json_schema = ArrayType(StructType([
                StructField("Date", StringType(), True),
                StructField("Open", DoubleType(), True),
                StructField("High", DoubleType(), True),
                StructField("Low", DoubleType(), True),
                StructField("Close", DoubleType(), True),
                StructField("Adj Close", DoubleType(), True),
                StructField("Volume", LongType(), True)
            ]))

            # Parse and explode JSON array
            df_parsed = df.withColumn("parsed", from_json(col("DATA"), json_schema))
            df_exploded = df_parsed.withColumn("record", explode(col("parsed")))

            # Selecting and renaming Columns
            df_final = df_exploded.select(
                col("ticker").alias("Symbol"),             
                col("record.Date").alias("Date"),           
                col("record.Open").alias("Open"),         
                col("record.High").alias("High"),         
                col("record.Low").alias("Low"),            
                col("record.Close").alias("Close"),        
                col("record.`Adj Close`").alias("Adj_Close"), 
                col("record.Volume").alias("Volume"),    
                col("ticker").alias("Ticker"),              
                col("load_timestamp").alias("Load_Timestamp"),
                current_timestamp().alias("Inserted_At")
            )

            # Date Tranformation
            df_final = df_final.withColumn("Date", to_date(col("Date"), "yyyy-MM-dd"))

            # Drop rows with missing essential fields
            essential_fields = ["Date", "Open", "High", "Low", "Close", "Volume"]
            df_final = df_final.na.drop(subset=essential_fields)

            # Deduplicate based on Date and Symbol
            df_final = df_final.dropDuplicates(["Date", "Symbol"])

            logging.info(f"ℹ️ℹ️Rows after cleaning/dedup (to be written): {df_final.count()}")                        

            # Write to Snowflake
            df_final.write \
                .format("snowflake") \
                .options(**write_sf_options) \
                .option("dbtable", "STOCK_PRICES_DWH") \
                .mode("append") \
                .save()

            logging.info("Spark job completed successfully. Data written to STOCK_PRICES_DWH Table")

        except Exception as e:
            logging.error(f"Error occurred: {str(e)}")
            raise
        finally:
            spark.stop()

    run_spark_job()