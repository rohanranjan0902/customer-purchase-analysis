"""
Main Execution Script
Runs the complete customer purchase behavior analysis pipeline
"""

import sys
import logging
import time
from pathlib import Path

# Import our custom modules
from src.data_ingestion import create_spark_session, download_dataset, load_data_to_spark, clean_and_preprocess, save_processed_data, display_data_summary
from src.data_analysis import CustomerPurchaseAnalyzer
from src.visualization import AnalysisVisualizer, create_interactive_plotly_dashboard
from config import PROCESSED_DATA_DIR, REPORTS_DIR, CHARTS_DIR

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


def main():
    """
    Main function to execute the complete analysis pipeline
    """
    print("🚀 Starting Customer Purchase Behavior Analysis")
    print("=" * 60)
    
    start_time = time.time()
    
    try:
        # Step 1: Data Ingestion and Preprocessing
        print("\n📥 Step 1: Data Ingestion and Preprocessing")
        print("-" * 40)
        
        # Check if processed data already exists
        processed_data_path = PROCESSED_DATA_DIR / "retail_data_processed.parquet"
        
        if processed_data_path.exists():
            print("✅ Processed data found, skipping ingestion...")
            # Create Spark session
            spark = create_spark_session()
            # Load processed data
            df = spark.read.parquet(str(processed_data_path))
        else:
            print("📊 Processing raw data...")
            # Download and process data
            dataset_path = download_dataset()
            spark = create_spark_session()
            raw_df = load_data_to_spark(spark, dataset_path)
            df = clean_and_preprocess(raw_df)
            save_processed_data(df, processed_data_path)
        
        # Display data summary
        display_data_summary(df)
        
        # Step 2: Data Analysis
        print("\n🔍 Step 2: Running Analysis")
        print("-" * 40)
        
        # Initialize analyzer
        analyzer = CustomerPurchaseAnalyzer(df)
        
        # Run complete analysis
        print("Running comprehensive analysis...")
        results = analyzer.run_complete_analysis()
        
        # Generate reports
        print("Generating analysis reports...")
        json_report_path = analyzer.save_results_to_json()
        text_report_path = analyzer.generate_text_report()
        
        print(f"✅ JSON report saved: {json_report_path}")
        print(f"✅ Text report saved: {text_report_path}")
        
        # Step 3: Visualization
        print("\n📊 Step 3: Creating Visualizations")
        print("-" * 40)
        
        # Initialize visualizer
        visualizer = AnalysisVisualizer(results)
        
        # Create all visualizations
        print("Creating static charts...")
        charts = visualizer.create_all_visualizations()
        
        # Create interactive dashboard
        print("Creating interactive dashboard...")
        interactive_dashboard_path = create_interactive_plotly_dashboard(results)
        
        print(f"✅ Created {len(charts)} static charts")
        print(f"✅ Interactive dashboard saved: {interactive_dashboard_path}")
        
        # Step 4: Summary
        print("\n🎉 Analysis Complete!")
        print("=" * 60)
        
        execution_time = time.time() - start_time
        print(f"⏱️  Total execution time: {execution_time:.2f} seconds")
        
        print("\n📋 Generated Files:")
        print(f"   📊 Data: {processed_data_path}")
        print(f"   📄 JSON Report: {json_report_path}")
        print(f"   📄 Text Report: {text_report_path}")
        print(f"   🌐 Interactive Dashboard: {interactive_dashboard_path}")
        print(f"   📈 Static Charts: {CHARTS_DIR}")
        
        # Display key insights
        if 'kpis' in results:
            kpis = results['kpis']
            print("\n💡 Key Insights:")
            print(f"   💰 Total Revenue: ${kpis['total_revenue']:,.2f}")
            print(f"   🛒 Total Orders: {kpis['total_orders']:,}")
            print(f"   👥 Unique Customers: {kpis['unique_customers']:,}")
            print(f"   📦 Unique Products: {kpis['unique_products']:,}")
            print(f"   💳 Average Order Value: ${kpis['average_order_value']:.2f}")
            print(f"   🔄 Orders per Customer: {kpis['orders_per_customer']:.1f}")
        
        print("\n🎯 Project completed successfully!")
        print("You can now:")
        print("  1. Open the interactive dashboard in your browser")
        print("  2. View static charts in the outputs/charts folder")
        print("  3. Review detailed reports in the outputs/reports folder")
        print("  4. Run the Jupyter notebook for further exploration")
        
        # Stop Spark session
        spark.stop()
        
        return 0
        
    except Exception as e:
        logger.error(f"Analysis failed: {str(e)}")
        print(f"\n❌ Analysis failed: {str(e)}")
        
        # Try to stop Spark session if it exists
        try:
            spark.stop()
        except:
            pass
            
        return 1


if __name__ == "__main__":
    exit_code = main()
    
    print(f"\n\nPress Enter to exit...")
    input()  # Keep window open to see results
    
    sys.exit(exit_code)
