"""
Data Ingestion and Preprocessing Module
Downloads and prepares the Online Retail Dataset for analysis
"""

import requests
import pandas as pd
from pathlib import Path
import logging
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *
import sys
import os

# Add parent directory to path to import config
sys.path.append(str(Path(__file__).parent.parent))
from config import DATASET_CONFIG, RAW_DATA_DIR, PROCESSED_DATA_DIR, SPARK_CONFIG

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


def download_dataset():
    """
    Download the Online Retail Dataset from UCI repository
    """
    url = DATASET_CONFIG["url"]
    local_path = DATASET_CONFIG["local_path"]
    
    if local_path.exists():
        logger.info(f"Dataset already exists at {local_path}")
        return str(local_path)
    
    logger.info(f"Downloading dataset from {url}")
    try:
        response = requests.get(url, stream=True)
        response.raise_for_status()
        
        # Create directory if it doesn't exist
        local_path.parent.mkdir(parents=True, exist_ok=True)
        
        # Download with progress
        total_size = int(response.headers.get('content-length', 0))
        downloaded = 0
        
        with open(local_path, 'wb') as f:
            for chunk in response.iter_content(chunk_size=8192):
                if chunk:
                    f.write(chunk)
                    downloaded += len(chunk)
                    if total_size > 0:
                        progress = (downloaded / total_size) * 100
                        print(f"\rDownloading: {progress:.1f}%", end="", flush=True)
        
        print()  # New line after progress
        logger.info(f"Dataset downloaded successfully to {local_path}")
        return str(local_path)
        
    except Exception as e:
        logger.error(f"Error downloading dataset: {str(e)}")
        raise


def create_spark_session():
    """
    Create and configure Spark session
    """
    try:
        spark = SparkSession.builder \
            .appName(SPARK_CONFIG["app_name"]) \
            .master(SPARK_CONFIG["master"]) \
            .config("spark.executor.memory", SPARK_CONFIG["memory"]) \
            .config("spark.driver.maxResultSize", SPARK_CONFIG["max_result_size"]) \
            .config("spark.serializer", SPARK_CONFIG["serializer"]) \
            .config("spark.sql.adaptive.enabled", SPARK_CONFIG["sql.adaptive.enabled"]) \
            .config("spark.sql.adaptive.coalescePartitions.enabled", 
                   SPARK_CONFIG["sql.adaptive.coalescePartitions.enabled"]) \
            .getOrCreate()
        
        # Set log level to reduce verbosity
        spark.sparkContext.setLogLevel("WARN")
        logger.info("Spark session created successfully")
        return spark
        
    except Exception as e:
        logger.error(f"Error creating Spark session: {str(e)}")
        raise


def load_data_to_spark(spark, file_path):
    """
    Load Excel data into Spark DataFrame using pandas as intermediate step
    """
    try:
        # First load with pandas (since Spark doesn't natively read Excel)
        logger.info("Loading Excel file with pandas...")
        pandas_df = pd.read_excel(file_path)
        
        logger.info(f"Loaded {len(pandas_df)} rows from Excel file")
        
        # Define Spark schema
        schema = StructType([
            StructField("InvoiceNo", StringType(), True),
            StructField("StockCode", StringType(), True),
            StructField("Description", StringType(), True),
            StructField("Quantity", IntegerType(), True),
            StructField("InvoiceDate", TimestampType(), True),
            StructField("UnitPrice", DoubleType(), True),
            StructField("CustomerID", DoubleType(), True),
            StructField("Country", StringType(), True)
        ])
        
        # Convert pandas DataFrame to Spark DataFrame
        logger.info("Converting to Spark DataFrame...")
        spark_df = spark.createDataFrame(pandas_df, schema)
        
        logger.info(f"Successfully loaded data into Spark DataFrame with {spark_df.count()} rows")
        return spark_df
        
    except Exception as e:
        logger.error(f"Error loading data: {str(e)}")
        raise


def clean_and_preprocess(df):
    """
    Clean and preprocess the data
    """
    logger.info("Starting data cleaning and preprocessing...")
    
    initial_count = df.count()
    logger.info(f"Initial record count: {initial_count}")
    
    # Remove records with null CustomerID (can't analyze customer behavior without ID)
    df_cleaned = df.filter(col("CustomerID").isNotNull())
    logger.info(f"After removing null CustomerID: {df_cleaned.count()} records")
    
    # Remove records with null/empty Description
    df_cleaned = df_cleaned.filter(col("Description").isNotNull() & (col("Description") != ""))
    logger.info(f"After removing null/empty Description: {df_cleaned.count()} records")
    
    # Remove records with negative or zero quantities and prices
    df_cleaned = df_cleaned.filter((col("Quantity") > 0) & (col("UnitPrice") > 0))
    logger.info(f"After removing invalid quantities/prices: {df_cleaned.count()} records")
    
    # Add calculated columns
    df_enhanced = df_cleaned.withColumn("TotalAmount", col("Quantity") * col("UnitPrice")) \
                           .withColumn("Year", year(col("InvoiceDate"))) \
                           .withColumn("Month", month(col("InvoiceDate"))) \
                           .withColumn("DayOfWeek", dayofweek(col("InvoiceDate"))) \
                           .withColumn("Hour", hour(col("InvoiceDate"))) \
                           .withColumn("Date", to_date(col("InvoiceDate")))
    
    # Add day name for better readability
    df_enhanced = df_enhanced.withColumn("DayName", 
        when(col("DayOfWeek") == 1, "Sunday")
        .when(col("DayOfWeek") == 2, "Monday")
        .when(col("DayOfWeek") == 3, "Tuesday")
        .when(col("DayOfWeek") == 4, "Wednesday")
        .when(col("DayOfWeek") == 5, "Thursday")
        .when(col("DayOfWeek") == 6, "Friday")
        .when(col("DayOfWeek") == 7, "Saturday")
    )
    
    # Clean product descriptions (trim whitespace, convert to title case)
    df_enhanced = df_enhanced.withColumn("Description", 
                                       initcap(trim(col("Description"))))
    
    logger.info(f"Final cleaned dataset: {df_enhanced.count()} records")
    logger.info("Data cleaning completed successfully")
    
    return df_enhanced


def save_processed_data(df, output_path):
    """
    Save processed data to parquet format for efficient reading
    """
    try:
        logger.info(f"Saving processed data to {output_path}")
        
        # Save as parquet (more efficient than CSV for Spark)
        df.coalesce(1).write.mode("overwrite").parquet(str(output_path))
        
        logger.info("Processed data saved successfully")
        
    except Exception as e:
        logger.error(f"Error saving processed data: {str(e)}")
        raise


def display_data_summary(df):
    """
    Display summary statistics of the dataset
    """
    print("\n" + "="*60)
    print("DATASET SUMMARY")
    print("="*60)
    
    print(f"Total Records: {df.count():,}")
    print(f"Total Columns: {len(df.columns)}")
    
    print("\nColumn Names and Types:")
    for col_name, col_type in df.dtypes:
        print(f"  {col_name}: {col_type}")
    
    print("\nDate Range:")
    date_range = df.select(min("InvoiceDate").alias("min_date"), 
                          max("InvoiceDate").alias("max_date")).collect()[0]
    print(f"  From: {date_range['min_date']}")
    print(f"  To: {date_range['max_date']}")
    
    print("\nUnique Values:")
    print(f"  Unique Customers: {df.select('CustomerID').distinct().count():,}")
    print(f"  Unique Products: {df.select('StockCode').distinct().count():,}")
    print(f"  Unique Countries: {df.select('Country').distinct().count():,}")
    
    print("\nTransaction Summary:")
    summary_stats = df.select(
        sum("TotalAmount").alias("total_revenue"),
        avg("TotalAmount").alias("avg_order_value"),
        sum("Quantity").alias("total_quantity")
    ).collect()[0]
    
    print(f"  Total Revenue: ${summary_stats['total_revenue']:,.2f}")
    print(f"  Average Order Value: ${summary_stats['avg_order_value']:.2f}")
    print(f"  Total Items Sold: {summary_stats['total_quantity']:,}")
    
    print("="*60)


def main():
    """
    Main function to run the data ingestion pipeline
    """
    try:
        # Download dataset
        dataset_path = download_dataset()
        
        # Create Spark session
        spark = create_spark_session()
        
        # Load data
        raw_df = load_data_to_spark(spark, dataset_path)
        
        # Clean and preprocess
        processed_df = clean_and_preprocess(raw_df)
        
        # Display summary
        display_data_summary(processed_df)
        
        # Save processed data
        output_path = PROCESSED_DATA_DIR / "retail_data_processed.parquet"
        save_processed_data(processed_df, output_path)
        
        print(f"\n✅ Data ingestion completed successfully!")
        print(f"📁 Processed data saved to: {output_path}")
        
        # Stop Spark session
        spark.stop()
        
    except Exception as e:
        logger.error(f"Data ingestion failed: {str(e)}")
        return 1
    
    return 0


if __name__ == "__main__":
    exit_code = main()
    sys.exit(exit_code)
