from pyspark.sql import SparkSession
from pyspark.sql.functions import col, round, to_date, from_json
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, LongType
import logging
import os
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv("/opt/spark/secrets.env")
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

if __name__ == '__main__':

    def run_spark_job():

        # Snowflake connection options
        sf_options = {            
            "sfAccount": os.getenv("SNOWFLAKE_ACCOUNT"),            
            "sfUser": os.getenv("SNOWFLAKE_USER"),
            "sfPassword": os.getenv("SNOWFLAKE_PASSWORD"),
            "sfDatabase": os.getenv("SNOWFLAKE_DATABASE"),
            "sfURL": os.getenv("SNOWFLAKE_URL"),
            "sfSchema": os.getenv("SNOWFLAKE_SCHEMA"),            
            "sfRole": os.getenv("SNOWFLAKE_ROLE"),
            "sfWarehouse": os.getenv("SNOWFLAKE_WAREHOUSE")
        }    
                
        spark = SparkSession.builder.appName("Formatting-Stock-Data") \
            .config("spark.master", "spark://spark-master:7077") \
            .getOrCreate()             
        
        df = spark.read \
        .format("snowflake") \
        .options(**sf_options) \
        .option("dbtable", os.getenv("SNOWFLAKE_TABLE")) \
        .load()
        
        # Define the JSON schema based on expected stock data structure
        json_schema = StructType([
            StructField("Date", StringType(), True),
            StructField("Symbol", StringType(), True),
            StructField("Open", DoubleType(), True),
            StructField("High", DoubleType(), True),
            StructField("Low", DoubleType(), True),
            StructField("Close", DoubleType(), True),
            StructField("Adj Close", DoubleType(), True),
            StructField("Volume", LongType(), True)
        ])
        
        df_parsed = df.withColumn("parsed", from_json(col("data"), json_schema))

        df_parsed = df_parsed.select(
            "ticker", "request_period", "request_interval", "load_timestamp",
            col("parsed.Date").alias("Date"),
            col("parsed.Symbol").alias("Symbol"),
            col("parsed.Open").alias("Open"),
            col("parsed.High").alias("High"),
            col("parsed.Low").alias("Low"),
            col("parsed.Close").alias("Close"),
            col("parsed.Volume").alias("Volume"),
            col("parsed.`Adj Close`").alias("Adj_Close")  # Rename during selection
        )
        
        df_final = df_parsed.withColumn("Date", to_date(col("Date"), "yyyy-MM-dd"))
        
        # Drop rows with missing essential fields
        essential_fields = ["Date", "Open", "High", "Low", "Close", "Volume"]
        df_final = df_final.na.drop(subset=essential_fields)
        
        # Deduplicate based on Date and Symbol
        df_final = df_final.dropDuplicates(["Date", "Symbol"])                
        
        output_path = "/opt/spark"        
        df_final.write.partitionBy("Symbol").mode("append").parquet(output_path)

        logging.info("Spark job completed successfully. Data written to %s", output_path)
        
        spark.stop()    
            
    # Run the Spark job
    run_spark_job()