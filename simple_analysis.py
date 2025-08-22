#!/usr/bin/env python3
"""
Simple Customer Purchase Analysis using Pandas
This version avoids PySpark compatibility issues and runs directly with pandas
"""

import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import numpy as np
from datetime import datetime
import warnings
warnings.filterwarnings('ignore')

def load_and_analyze_data():
    """Load and analyze customer purchase data using pandas"""
    
    print("🚀 Starting Simple Customer Purchase Analysis")
    print("=" * 60)
    
    # Load the data
    print("📥 Loading data...")
    try:
        data_path = "data/raw/Online_Retail.xlsx"
        df = pd.read_excel(data_path, engine='openpyxl')
        print(f"✅ Loaded {len(df)} rows of data")
        
        # Display basic info
        print(f"\n📊 Dataset Overview:")
        print(f"   • Total transactions: {len(df):,}")
        print(f"   • Date range: {df['InvoiceDate'].min()} to {df['InvoiceDate'].max()}")
        print(f"   • Unique customers: {df['CustomerID'].nunique():,}")
        print(f"   • Unique products: {df['StockCode'].nunique():,}")
        print(f"   • Countries: {df['Country'].nunique()}")
        
    except FileNotFoundError:
        print("❌ Data file not found. Please ensure the Excel file is in data/raw/")
        return
    except Exception as e:
        print(f"❌ Error loading data: {e}")
        return
    
    # Clean the data
    print("\n🧹 Cleaning data...")
    original_size = len(df)
    
    # Remove rows with missing CustomerID
    df = df.dropna(subset=['CustomerID'])
    
    # Remove cancelled orders (negative quantities)
    df = df[df['Quantity'] > 0]
    df = df[df['UnitPrice'] > 0]
    
    # Calculate total price
    df['TotalPrice'] = df['Quantity'] * df['UnitPrice']
    
    print(f"   • Removed {original_size - len(df):,} invalid records")
    print(f"   • Clean dataset: {len(df):,} rows")
    
    # Analysis 1: Top 10 Best Selling Products
    print("\n📈 Analysis 1: Top 10 Best Selling Products")
    print("-" * 50)
    top_products = df.groupby(['StockCode', 'Description']).agg({
        'Quantity': 'sum',
        'TotalPrice': 'sum'
    }).sort_values('Quantity', ascending=False).head(10)
    
    print(top_products)
    
    # Analysis 2: Top 10 Customers by Spending
    print("\n💰 Analysis 2: Top 10 Customers by Total Spending")
    print("-" * 50)
    top_customers = df.groupby('CustomerID').agg({
        'TotalPrice': 'sum',
        'InvoiceNo': 'nunique',
        'Quantity': 'sum'
    }).sort_values('TotalPrice', ascending=False).head(10)
    top_customers.columns = ['Total Spent', 'Number of Orders', 'Items Purchased']
    print(top_customers)
    
    # Analysis 3: Sales by Country
    print("\n🌍 Analysis 3: Sales by Country (Top 10)")
    print("-" * 50)
    country_sales = df.groupby('Country').agg({
        'TotalPrice': 'sum',
        'CustomerID': 'nunique'
    }).sort_values('TotalPrice', ascending=False).head(10)
    country_sales.columns = ['Total Sales', 'Unique Customers']
    print(country_sales)
    
    # Analysis 4: Monthly Sales Trend
    print("\n📅 Analysis 4: Monthly Sales Trend")
    print("-" * 50)
    df['YearMonth'] = df['InvoiceDate'].dt.to_period('M')
    monthly_sales = df.groupby('YearMonth').agg({
        'TotalPrice': 'sum',
        'CustomerID': 'nunique'
    }).reset_index()
    monthly_sales.columns = ['Month', 'Total Sales', 'Unique Customers']
    print(monthly_sales)
    
    # Create visualizations
    print("\n📊 Creating visualizations...")
    
    # Set up the plotting style
    plt.style.use('default')
    fig, axes = plt.subplots(2, 2, figsize=(15, 12))
    fig.suptitle('Customer Purchase Analysis Dashboard', fontsize=16, fontweight='bold')
    
    # Plot 1: Top 10 Products by Quantity
    top_products_plot = df.groupby('Description')['Quantity'].sum().sort_values(ascending=False).head(10)
    ax1 = axes[0, 0]
    top_products_plot.plot(kind='barh', ax=ax1, color='skyblue')
    ax1.set_title('Top 10 Products by Quantity Sold')
    ax1.set_xlabel('Quantity Sold')
    
    # Plot 2: Sales by Country (Top 10)
    country_sales_plot = df.groupby('Country')['TotalPrice'].sum().sort_values(ascending=False).head(10)
    ax2 = axes[0, 1]
    country_sales_plot.plot(kind='bar', ax=ax2, color='lightgreen')
    ax2.set_title('Top 10 Countries by Sales')
    ax2.set_ylabel('Total Sales')
    ax2.tick_params(axis='x', rotation=45)
    
    # Plot 3: Monthly Sales Trend
    ax3 = axes[1, 0]
    monthly_sales['Total Sales'].plot(ax=ax3, marker='o', color='coral')
    ax3.set_title('Monthly Sales Trend')
    ax3.set_xlabel('Month')
    ax3.set_ylabel('Total Sales')
    ax3.tick_params(axis='x', rotation=45)
    
    # Plot 4: Customer Distribution (Sales per customer)
    customer_spending = df.groupby('CustomerID')['TotalPrice'].sum()
    ax4 = axes[1, 1]
    ax4.hist(customer_spending, bins=50, color='plum', alpha=0.7)
    ax4.set_title('Customer Spending Distribution')
    ax4.set_xlabel('Total Spending per Customer')
    ax4.set_ylabel('Number of Customers')
    
    plt.tight_layout()
    
    # Save the plots
    output_dir = "outputs/charts"
    import os
    os.makedirs(output_dir, exist_ok=True)
    
    plt.savefig(f"{output_dir}/customer_analysis_dashboard.png", dpi=300, bbox_inches='tight')
    print(f"📊 Dashboard saved to {output_dir}/customer_analysis_dashboard.png")
    
    # Show the plots
    plt.show()
    
    # Summary Statistics
    print("\n📋 Summary Statistics")
    print("=" * 60)
    print(f"📈 Total Revenue: ${df['TotalPrice'].sum():,.2f}")
    print(f"🛍️  Total Items Sold: {df['Quantity'].sum():,}")
    print(f"👥 Total Customers: {df['CustomerID'].nunique():,}")
    print(f"📦 Average Order Value: ${df.groupby('InvoiceNo')['TotalPrice'].sum().mean():.2f}")
    print(f"🛒 Average Items per Order: {df.groupby('InvoiceNo')['Quantity'].sum().mean():.1f}")
    print(f"💳 Average Customer Spending: ${df.groupby('CustomerID')['TotalPrice'].sum().mean():.2f}")
    
    print("\n✅ Analysis Complete!")
    print(f"📁 Results saved in outputs/ directory")
    print(f"🎨 Charts saved in outputs/charts/ directory")

if __name__ == "__main__":
    load_and_analyze_data()
    input("Press Enter to exit...")
