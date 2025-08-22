"""
Configuration file for Customer Purchase Behavior Analysis
"""
import os
from pathlib import Path

# Base paths
BASE_DIR = Path(__file__).parent
DATA_DIR = BASE_DIR / "data"
RAW_DATA_DIR = DATA_DIR / "raw"
PROCESSED_DATA_DIR = DATA_DIR / "processed"
OUTPUTS_DIR = BASE_DIR / "outputs"
REPORTS_DIR = OUTPUTS_DIR / "reports"
CHARTS_DIR = OUTPUTS_DIR / "charts"

# Dataset configuration
DATASET_CONFIG = {
    "name": "Online Retail Dataset",
    "url": "https://archive.ics.uci.edu/ml/machine-learning-databases/00352/Online%20Retail.xlsx",
    "filename": "Online_Retail.xlsx",
    "local_path": RAW_DATA_DIR / "Online_Retail.xlsx"
}

# Spark configuration
SPARK_CONFIG = {
    "app_name": "CustomerPurchaseBehaviorAnalysis",
    "master": "local[*]",  # Use all available cores
    "memory": "4g",
    "max_result_size": "2g",
    "serializer": "org.apache.spark.serializer.KryoSerializer",
    "sql.adaptive.enabled": "true",
    "sql.adaptive.coalescePartitions.enabled": "true"
}

# Data schema for Online Retail Dataset
DATA_SCHEMA = {
    "InvoiceNo": "string",
    "StockCode": "string", 
    "Description": "string",
    "Quantity": "integer",
    "InvoiceDate": "timestamp",
    "UnitPrice": "double",
    "CustomerID": "double",
    "Country": "string"
}

# Analysis parameters
ANALYSIS_CONFIG = {
    "top_products_limit": 20,
    "top_customers_limit": 20,
    "date_format": "yyyy-MM-dd HH:mm:ss",
    "min_quantity": 0,  # Filter out returns/negative quantities
    "min_unit_price": 0,  # Filter out free/negative priced items
    "customer_segments": {
        "high_value": 1000,  # Customers spending > $1000
        "medium_value": 100,  # Customers spending $100-$1000
        "low_value": 0       # Customers spending < $100
    }
}

# Visualization settings
VIZ_CONFIG = {
    "figure_size": (12, 8),
    "dpi": 300,
    "style": "whitegrid",
    "color_palette": "Set2",
    "save_format": "png"
}

# Create directories if they don't exist
for directory in [DATA_DIR, RAW_DATA_DIR, PROCESSED_DATA_DIR, 
                 OUTPUTS_DIR, REPORTS_DIR, CHARTS_DIR]:
    directory.mkdir(parents=True, exist_ok=True)
