"""
Data Analysis Module
Contains all the main analysis functions for customer purchase behavior
"""

import sys
from pathlib import Path
import logging
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.window import Window
import json

# Add parent directory to path to import config
sys.path.append(str(Path(__file__).parent.parent))
from config import ANALYSIS_CONFIG, REPORTS_DIR

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


class CustomerPurchaseAnalyzer:
    """
    Main analyzer class for customer purchase behavior analysis
    """
    
    def __init__(self, spark_df):
        """
        Initialize analyzer with Spark DataFrame
        """
        self.df = spark_df
        self.results = {}
        logger.info("CustomerPurchaseAnalyzer initialized")
    
    def analyze_top_products(self):
        """
        Analyze top-selling products by quantity and revenue
        """
        logger.info("Analyzing top products...")
        
        # Top products by quantity sold
        top_by_quantity = self.df.groupBy("StockCode", "Description") \
                                .agg(sum("Quantity").alias("total_quantity"),
                                     sum("TotalAmount").alias("total_revenue"),
                                     count("*").alias("order_count")) \
                                .orderBy(desc("total_quantity")) \
                                .limit(ANALYSIS_CONFIG["top_products_limit"])
        
        # Top products by revenue
        top_by_revenue = self.df.groupBy("StockCode", "Description") \
                               .agg(sum("TotalAmount").alias("total_revenue"),
                                    sum("Quantity").alias("total_quantity"),
                                    count("*").alias("order_count")) \
                               .orderBy(desc("total_revenue")) \
                               .limit(ANALYSIS_CONFIG["top_products_limit"])
        
        self.results['top_products_by_quantity'] = top_by_quantity.toPandas()
        self.results['top_products_by_revenue'] = top_by_revenue.toPandas()
        
        logger.info("Top products analysis completed")
        return self.results['top_products_by_quantity'], self.results['top_products_by_revenue']
    
    def analyze_customer_segments(self):
        """
        Analyze customer segments based on spending behavior
        """
        logger.info("Analyzing customer segments...")
        
        # Calculate customer lifetime value
        customer_stats = self.df.groupBy("CustomerID") \
                               .agg(sum("TotalAmount").alias("total_spent"),
                                    count("InvoiceNo").alias("order_count"),
                                    countDistinct("StockCode").alias("unique_products"),
                                    avg("TotalAmount").alias("avg_order_value"),
                                    min("InvoiceDate").alias("first_purchase"),
                                    max("InvoiceDate").alias("last_purchase"))
        
        # Add customer segments based on spending
        customer_segments = customer_stats.withColumn("segment",
            when(col("total_spent") >= ANALYSIS_CONFIG["customer_segments"]["high_value"], "High Value")
            .when(col("total_spent") >= ANALYSIS_CONFIG["customer_segments"]["medium_value"], "Medium Value")
            .otherwise("Low Value")
        )
        
        # Get segment statistics
        segment_summary = customer_segments.groupBy("segment") \
                                          .agg(count("*").alias("customer_count"),
                                               avg("total_spent").alias("avg_spent"),
                                               avg("order_count").alias("avg_orders"),
                                               avg("avg_order_value").alias("avg_order_value"))
        
        self.results['customer_segments'] = customer_segments.toPandas()
        self.results['segment_summary'] = segment_summary.toPandas()
        
        logger.info("Customer segmentation analysis completed")
        return self.results['customer_segments'], self.results['segment_summary']
    
    def analyze_top_customers(self):
        """
        Analyze top customers by spending and order frequency
        """
        logger.info("Analyzing top customers...")
        
        # Top customers by total spending
        top_customers = self.df.groupBy("CustomerID", "Country") \
                              .agg(sum("TotalAmount").alias("total_spent"),
                                   count("InvoiceNo").alias("order_count"),
                                   countDistinct("StockCode").alias("unique_products"),
                                   avg("TotalAmount").alias("avg_order_value")) \
                              .orderBy(desc("total_spent")) \
                              .limit(ANALYSIS_CONFIG["top_customers_limit"])
        
        self.results['top_customers'] = top_customers.toPandas()
        
        logger.info("Top customers analysis completed")
        return self.results['top_customers']
    
    def analyze_temporal_patterns(self):
        """
        Analyze sales patterns over time (hourly, daily, monthly)
        """
        logger.info("Analyzing temporal patterns...")
        
        # Hourly sales pattern
        hourly_sales = self.df.groupBy("Hour") \
                             .agg(sum("TotalAmount").alias("total_revenue"),
                                  count("*").alias("order_count")) \
                             .orderBy("Hour")
        
        # Daily sales pattern (day of week)
        daily_sales = self.df.groupBy("DayOfWeek", "DayName") \
                            .agg(sum("TotalAmount").alias("total_revenue"),
                                 count("*").alias("order_count")) \
                            .orderBy("DayOfWeek")
        
        # Monthly sales pattern
        monthly_sales = self.df.groupBy("Year", "Month") \
                              .agg(sum("TotalAmount").alias("total_revenue"),
                                   count("*").alias("order_count"),
                                   countDistinct("CustomerID").alias("unique_customers")) \
                              .orderBy("Year", "Month")
        
        # Peak shopping hours
        peak_hours = hourly_sales.orderBy(desc("order_count")).limit(5)
        
        self.results['hourly_sales'] = hourly_sales.toPandas()
        self.results['daily_sales'] = daily_sales.toPandas()
        self.results['monthly_sales'] = monthly_sales.toPandas()
        self.results['peak_hours'] = peak_hours.toPandas()
        
        logger.info("Temporal patterns analysis completed")
        return (self.results['hourly_sales'], self.results['daily_sales'], 
                self.results['monthly_sales'], self.results['peak_hours'])
    
    def analyze_geographic_distribution(self):
        """
        Analyze sales distribution by country
        """
        logger.info("Analyzing geographic distribution...")
        
        country_analysis = self.df.groupBy("Country") \
                                 .agg(sum("TotalAmount").alias("total_revenue"),
                                      count("*").alias("order_count"),
                                      countDistinct("CustomerID").alias("unique_customers"),
                                      avg("TotalAmount").alias("avg_order_value")) \
                                 .orderBy(desc("total_revenue"))
        
        self.results['country_analysis'] = country_analysis.toPandas()
        
        logger.info("Geographic distribution analysis completed")
        return self.results['country_analysis']
    
    def analyze_product_categories(self):
        """
        Analyze product categories (based on stock code patterns)
        """
        logger.info("Analyzing product categories...")
        
        # Extract category from stock code (first character/digit pattern)
        df_with_category = self.df.withColumn("category", 
                                             regexp_extract(col("StockCode"), "^([A-Z]+)", 1))
        
        # Filter out empty categories
        df_with_category = df_with_category.filter(col("category") != "")
        
        category_analysis = df_with_category.groupBy("category") \
                                          .agg(sum("TotalAmount").alias("total_revenue"),
                                               sum("Quantity").alias("total_quantity"),
                                               count("*").alias("order_count"),
                                               countDistinct("StockCode").alias("unique_products")) \
                                          .orderBy(desc("total_revenue"))
        
        self.results['category_analysis'] = category_analysis.toPandas()
        
        logger.info("Product categories analysis completed")
        return self.results['category_analysis']
    
    def calculate_kpis(self):
        """
        Calculate key performance indicators
        """
        logger.info("Calculating KPIs...")
        
        # Overall KPIs
        total_revenue = self.df.agg(sum("TotalAmount")).collect()[0][0]
        total_orders = self.df.count()
        unique_customers = self.df.select("CustomerID").distinct().count()
        unique_products = self.df.select("StockCode").distinct().count()
        avg_order_value = self.df.agg(avg("TotalAmount")).collect()[0][0]
        
        # Date range
        date_range = self.df.select(min("InvoiceDate").alias("start_date"),
                                   max("InvoiceDate").alias("end_date")).collect()[0]
        
        # Customer metrics
        orders_per_customer = total_orders / unique_customers if unique_customers > 0 else 0
        revenue_per_customer = total_revenue / unique_customers if unique_customers > 0 else 0
        
        kpis = {
            "total_revenue": float(total_revenue) if total_revenue else 0,
            "total_orders": total_orders,
            "unique_customers": unique_customers,
            "unique_products": unique_products,
            "average_order_value": float(avg_order_value) if avg_order_value else 0,
            "orders_per_customer": orders_per_customer,
            "revenue_per_customer": revenue_per_customer,
            "date_range": {
                "start": str(date_range.start_date),
                "end": str(date_range.end_date)
            }
        }
        
        self.results['kpis'] = kpis
        
        logger.info("KPI calculation completed")
        return kpis
    
    def analyze_customer_retention(self):
        """
        Analyze customer retention patterns
        """
        logger.info("Analyzing customer retention...")
        
        # Customer purchase frequency
        customer_frequency = self.df.groupBy("CustomerID") \
                                   .agg(count("InvoiceNo").alias("purchase_count"),
                                        min("InvoiceDate").alias("first_purchase"),
                                        max("InvoiceDate").alias("last_purchase"))
        
        # Calculate days between first and last purchase
        customer_frequency = customer_frequency.withColumn(
            "customer_lifespan_days",
            datediff(col("last_purchase"), col("first_purchase"))
        )
        
        # Categorize customers by purchase frequency
        retention_analysis = customer_frequency.withColumn("customer_type",
            when(col("purchase_count") == 1, "One-time")
            .when(col("purchase_count").between(2, 5), "Occasional")
            .when(col("purchase_count").between(6, 20), "Regular")
            .otherwise("Frequent")
        )
        
        # Retention summary
        retention_summary = retention_analysis.groupBy("customer_type") \
                                             .agg(count("*").alias("customer_count"),
                                                  avg("purchase_count").alias("avg_purchases"),
                                                  avg("customer_lifespan_days").alias("avg_lifespan_days"))
        
        self.results['customer_retention'] = retention_analysis.toPandas()
        self.results['retention_summary'] = retention_summary.toPandas()
        
        logger.info("Customer retention analysis completed")
        return self.results['customer_retention'], self.results['retention_summary']
    
    def run_complete_analysis(self):
        """
        Run all analysis functions
        """
        logger.info("Starting complete analysis...")
        
        # Run all analyses
        self.calculate_kpis()
        self.analyze_top_products()
        self.analyze_customer_segments()
        self.analyze_top_customers()
        self.analyze_temporal_patterns()
        self.analyze_geographic_distribution()
        self.analyze_product_categories()
        self.analyze_customer_retention()
        
        logger.info("Complete analysis finished")
        return self.results
    
    def save_results_to_json(self, filename="analysis_results.json"):
        """
        Save analysis results to JSON file
        """
        output_path = REPORTS_DIR / filename
        
        # Convert pandas DataFrames to dictionaries for JSON serialization
        json_results = {}
        for key, value in self.results.items():
            if hasattr(value, 'to_dict'):  # pandas DataFrame
                json_results[key] = value.to_dict('records')
            else:
                json_results[key] = value
        
        with open(output_path, 'w') as f:
            json.dump(json_results, f, indent=2, default=str)
        
        logger.info(f"Analysis results saved to {output_path}")
        return output_path
    
    def generate_text_report(self):
        """
        Generate a comprehensive text report
        """
        report_lines = []
        report_lines.append("=" * 80)
        report_lines.append("CUSTOMER PURCHASE BEHAVIOR ANALYSIS REPORT")
        report_lines.append("=" * 80)
        report_lines.append("")
        
        # KPIs Section
        if 'kpis' in self.results:
            kpis = self.results['kpis']
            report_lines.append("📊 KEY PERFORMANCE INDICATORS")
            report_lines.append("-" * 40)
            report_lines.append(f"Total Revenue: ${kpis['total_revenue']:,.2f}")
            report_lines.append(f"Total Orders: {kpis['total_orders']:,}")
            report_lines.append(f"Unique Customers: {kpis['unique_customers']:,}")
            report_lines.append(f"Unique Products: {kpis['unique_products']:,}")
            report_lines.append(f"Average Order Value: ${kpis['average_order_value']:.2f}")
            report_lines.append(f"Orders per Customer: {kpis['orders_per_customer']:.1f}")
            report_lines.append(f"Revenue per Customer: ${kpis['revenue_per_customer']:.2f}")
            report_lines.append("")
        
        # Top Products Section
        if 'top_products_by_revenue' in self.results:
            report_lines.append("🏆 TOP 10 PRODUCTS BY REVENUE")
            report_lines.append("-" * 40)
            top_products = self.results['top_products_by_revenue'].head(10)
            for idx, product in top_products.iterrows():
                report_lines.append(f"{idx+1:2d}. {product['Description'][:40]:<40} ${product['total_revenue']:>10,.2f}")
            report_lines.append("")
        
        # Customer Segments Section
        if 'segment_summary' in self.results:
            report_lines.append("👥 CUSTOMER SEGMENTS")
            report_lines.append("-" * 40)
            segments = self.results['segment_summary']
            for _, segment in segments.iterrows():
                report_lines.append(f"{segment['segment']}: {segment['customer_count']:,} customers "
                                  f"(Avg Spent: ${segment['avg_spent']:.2f})")
            report_lines.append("")
        
        # Geographic Analysis Section
        if 'country_analysis' in self.results:
            report_lines.append("🌍 TOP 10 COUNTRIES BY REVENUE")
            report_lines.append("-" * 40)
            countries = self.results['country_analysis'].head(10)
            for idx, country in countries.iterrows():
                report_lines.append(f"{idx+1:2d}. {country['Country']:<20} ${country['total_revenue']:>12,.2f} "
                                  f"({country['unique_customers']:,} customers)")
            report_lines.append("")
        
        # Peak Hours Section
        if 'peak_hours' in self.results:
            report_lines.append("⏰ PEAK SHOPPING HOURS")
            report_lines.append("-" * 40)
            peak_hours = self.results['peak_hours']
            for _, hour in peak_hours.iterrows():
                report_lines.append(f"{hour['Hour']:2d}:00 - {hour['order_count']:,} orders "
                                  f"(${hour['total_revenue']:,.2f} revenue)")
            report_lines.append("")
        
        # Save report
        report_content = "\n".join(report_lines)
        report_path = REPORTS_DIR / "analysis_report.txt"
        
        with open(report_path, 'w', encoding='utf-8') as f:
            f.write(report_content)
        
        logger.info(f"Text report saved to {report_path}")
        
        # Also print to console
        print(report_content)
        
        return report_path
