"""
Visualization Module
Creates charts and visualizations for the analysis results
"""

import matplotlib.pyplot as plt
import seaborn as sns
import plotly.graph_objects as go
import plotly.express as px
from plotly.subplots import make_subplots
import pandas as pd
import numpy as np
from pathlib import Path
import sys
import logging

# Add parent directory to path to import config
sys.path.append(str(Path(__file__).parent.parent))
from config import VIZ_CONFIG, CHARTS_DIR

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Set matplotlib and seaborn styles
plt.style.use('default')
sns.set_style(VIZ_CONFIG["style"])
sns.set_palette(VIZ_CONFIG["color_palette"])


class AnalysisVisualizer:
    """
    Main visualizer class for creating charts from analysis results
    """
    
    def __init__(self, analysis_results):
        """
        Initialize visualizer with analysis results
        """
        self.results = analysis_results
        self.charts_dir = CHARTS_DIR
        logger.info("AnalysisVisualizer initialized")
    
    def plot_top_products_revenue(self, top_n=15):
        """
        Create horizontal bar chart for top products by revenue
        """
        if 'top_products_by_revenue' not in self.results:
            logger.warning("Top products by revenue data not available")
            return None
        
        data = self.results['top_products_by_revenue'].head(top_n)
        
        plt.figure(figsize=VIZ_CONFIG["figure_size"])
        
        # Create horizontal bar chart
        bars = plt.barh(range(len(data)), data['total_revenue'], 
                       color=sns.color_palette("viridis", len(data)))
        
        # Customize the plot
        plt.yticks(range(len(data)), [desc[:30] + "..." if len(desc) > 30 else desc 
                                      for desc in data['Description']])
        plt.xlabel('Total Revenue ($)')
        plt.title(f'Top {top_n} Products by Revenue', fontsize=16, fontweight='bold')
        plt.gca().invert_yaxis()  # Highest revenue at top
        
        # Add value labels on bars
        for i, bar in enumerate(bars):
            width = bar.get_width()
            plt.text(width, bar.get_y() + bar.get_height()/2, 
                    f'${width:,.0f}', ha='left', va='center', fontsize=9)
        
        plt.tight_layout()
        
        # Save the plot
        output_path = self.charts_dir / "top_products_revenue.png"
        plt.savefig(output_path, dpi=VIZ_CONFIG["dpi"], bbox_inches='tight')
        plt.close()
        
        logger.info(f"Top products revenue chart saved to {output_path}")
        return output_path
    
    def plot_customer_segments(self):
        """
        Create pie chart for customer segments distribution
        """
        if 'segment_summary' not in self.results:
            logger.warning("Customer segments data not available")
            return None
        
        data = self.results['segment_summary']
        
        # Create subplots: pie chart and bar chart
        fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(16, 8))
        
        # Pie chart for customer count distribution
        colors = sns.color_palette("Set3", len(data))
        wedges, texts, autotexts = ax1.pie(data['customer_count'], 
                                          labels=data['segment'],
                                          autopct='%1.1f%%',
                                          startangle=90,
                                          colors=colors)
        ax1.set_title('Customer Distribution by Segment', fontsize=14, fontweight='bold')
        
        # Bar chart for average spending per segment
        bars = ax2.bar(data['segment'], data['avg_spent'], color=colors)
        ax2.set_title('Average Spending by Customer Segment', fontsize=14, fontweight='bold')
        ax2.set_ylabel('Average Spending ($)')
        ax2.set_xlabel('Customer Segment')
        
        # Add value labels on bars
        for bar in bars:
            height = bar.get_height()
            ax2.text(bar.get_x() + bar.get_width()/2., height,
                    f'${height:,.0f}', ha='center', va='bottom')
        
        plt.tight_layout()
        
        # Save the plot
        output_path = self.charts_dir / "customer_segments.png"
        plt.savefig(output_path, dpi=VIZ_CONFIG["dpi"], bbox_inches='tight')
        plt.close()
        
        logger.info(f"Customer segments chart saved to {output_path}")
        return output_path
    
    def plot_temporal_patterns(self):
        """
        Create multiple charts for temporal patterns
        """
        if not all(key in self.results for key in ['hourly_sales', 'daily_sales', 'monthly_sales']):
            logger.warning("Temporal patterns data not available")
            return None
        
        # Create subplots
        fig, ((ax1, ax2), (ax3, ax4)) = plt.subplots(2, 2, figsize=(20, 16))
        
        # 1. Hourly sales pattern
        hourly_data = self.results['hourly_sales']
        ax1.plot(hourly_data['Hour'], hourly_data['total_revenue'], 
                marker='o', linewidth=2, markersize=6)
        ax1.set_title('Revenue by Hour of Day', fontsize=14, fontweight='bold')
        ax1.set_xlabel('Hour')
        ax1.set_ylabel('Revenue ($)')
        ax1.grid(True, alpha=0.3)
        ax1.set_xticks(range(0, 24, 2))
        
        # 2. Daily sales pattern (day of week)
        daily_data = self.results['daily_sales'].sort_values('DayOfWeek')
        bars = ax2.bar(daily_data['DayName'], daily_data['total_revenue'])
        ax2.set_title('Revenue by Day of Week', fontsize=14, fontweight='bold')
        ax2.set_xlabel('Day of Week')
        ax2.set_ylabel('Revenue ($)')
        ax2.tick_params(axis='x', rotation=45)
        
        # Add value labels on bars
        for bar in bars:
            height = bar.get_height()
            ax2.text(bar.get_x() + bar.get_width()/2., height,
                    f'${height:,.0f}', ha='center', va='bottom', rotation=45)
        
        # 3. Monthly sales trend
        monthly_data = self.results['monthly_sales']
        monthly_data['month_year'] = monthly_data['Year'].astype(str) + '-' + \
                                   monthly_data['Month'].astype(str).str.zfill(2)
        
        ax3.plot(range(len(monthly_data)), monthly_data['total_revenue'], 
                marker='o', linewidth=2)
        ax3.set_title('Monthly Revenue Trend', fontsize=14, fontweight='bold')
        ax3.set_xlabel('Month')
        ax3.set_ylabel('Revenue ($)')
        ax3.grid(True, alpha=0.3)
        
        # Set x-tick labels (show every other month to avoid crowding)
        tick_positions = range(0, len(monthly_data), max(1, len(monthly_data)//6))
        ax3.set_xticks(tick_positions)
        ax3.set_xticklabels([monthly_data.iloc[i]['month_year'] for i in tick_positions], 
                           rotation=45)
        
        # 4. Order count by hour
        ax4.bar(hourly_data['Hour'], hourly_data['order_count'], alpha=0.7)
        ax4.set_title('Order Count by Hour of Day', fontsize=14, fontweight='bold')
        ax4.set_xlabel('Hour')
        ax4.set_ylabel('Number of Orders')
        ax4.set_xticks(range(0, 24, 2))
        
        plt.tight_layout()
        
        # Save the plot
        output_path = self.charts_dir / "temporal_patterns.png"
        plt.savefig(output_path, dpi=VIZ_CONFIG["dpi"], bbox_inches='tight')
        plt.close()
        
        logger.info(f"Temporal patterns chart saved to {output_path}")
        return output_path
    
    def plot_geographic_analysis(self, top_n=15):
        """
        Create charts for geographic sales distribution
        """
        if 'country_analysis' not in self.results:
            logger.warning("Country analysis data not available")
            return None
        
        data = self.results['country_analysis'].head(top_n)
        
        # Create subplots
        fig, (ax1, ax2) = plt.subplots(2, 1, figsize=(14, 12))
        
        # 1. Revenue by country (horizontal bar)
        bars1 = ax1.barh(range(len(data)), data['total_revenue'])
        ax1.set_yticks(range(len(data)))
        ax1.set_yticklabels(data['Country'])
        ax1.set_xlabel('Total Revenue ($)')
        ax1.set_title(f'Top {top_n} Countries by Revenue', fontsize=14, fontweight='bold')
        ax1.invert_yaxis()
        
        # Add value labels
        for i, bar in enumerate(bars1):
            width = bar.get_width()
            ax1.text(width, bar.get_y() + bar.get_height()/2, 
                    f'${width:,.0f}', ha='left', va='center')
        
        # 2. Number of customers by country
        bars2 = ax2.bar(range(len(data)), data['unique_customers'])
        ax2.set_xticks(range(len(data)))
        ax2.set_xticklabels(data['Country'], rotation=45, ha='right')
        ax2.set_ylabel('Number of Customers')
        ax2.set_title(f'Top {top_n} Countries by Customer Count', fontsize=14, fontweight='bold')
        
        # Add value labels
        for i, bar in enumerate(bars2):
            height = bar.get_height()
            ax2.text(bar.get_x() + bar.get_width()/2., height,
                    f'{height:,}', ha='center', va='bottom')
        
        plt.tight_layout()
        
        # Save the plot
        output_path = self.charts_dir / "geographic_analysis.png"
        plt.savefig(output_path, dpi=VIZ_CONFIG["dpi"], bbox_inches='tight')
        plt.close()
        
        logger.info(f"Geographic analysis chart saved to {output_path}")
        return output_path
    
    def plot_customer_retention(self):
        """
        Create charts for customer retention analysis
        """
        if 'retention_summary' not in self.results:
            logger.warning("Customer retention data not available")
            return None
        
        data = self.results['retention_summary']
        
        # Create subplots
        fig, ((ax1, ax2), (ax3, ax4)) = plt.subplots(2, 2, figsize=(16, 12))
        
        # 1. Customer distribution by type (pie chart)
        colors = sns.color_palette("pastel", len(data))
        wedges, texts, autotexts = ax1.pie(data['customer_count'], 
                                          labels=data['customer_type'],
                                          autopct='%1.1f%%',
                                          startangle=90,
                                          colors=colors)
        ax1.set_title('Customer Distribution by Type', fontsize=14, fontweight='bold')
        
        # 2. Average purchases per customer type
        bars = ax2.bar(data['customer_type'], data['avg_purchases'], color=colors)
        ax2.set_title('Average Purchases by Customer Type', fontsize=14, fontweight='bold')
        ax2.set_ylabel('Average Number of Purchases')
        ax2.set_xlabel('Customer Type')
        ax2.tick_params(axis='x', rotation=45)
        
        # Add value labels
        for bar in bars:
            height = bar.get_height()
            ax2.text(bar.get_x() + bar.get_width()/2., height,
                    f'{height:.1f}', ha='center', va='bottom')
        
        # 3. Customer count by type (bar chart)
        bars3 = ax3.bar(data['customer_type'], data['customer_count'], color=colors)
        ax3.set_title('Number of Customers by Type', fontsize=14, fontweight='bold')
        ax3.set_ylabel('Number of Customers')
        ax3.set_xlabel('Customer Type')
        ax3.tick_params(axis='x', rotation=45)
        
        # Add value labels
        for bar in bars3:
            height = bar.get_height()
            ax3.text(bar.get_x() + bar.get_width()/2., height,
                    f'{height:,}', ha='center', va='bottom')
        
        # 4. Average customer lifespan
        bars4 = ax4.bar(data['customer_type'], data['avg_lifespan_days'], color=colors)
        ax4.set_title('Average Customer Lifespan (Days)', fontsize=14, fontweight='bold')
        ax4.set_ylabel('Average Lifespan (Days)')
        ax4.set_xlabel('Customer Type')
        ax4.tick_params(axis='x', rotation=45)
        
        # Add value labels
        for bar in bars4:
            height = bar.get_height()
            ax4.text(bar.get_x() + bar.get_width()/2., height,
                    f'{height:.0f}', ha='center', va='bottom')
        
        plt.tight_layout()
        
        # Save the plot
        output_path = self.charts_dir / "customer_retention.png"
        plt.savefig(output_path, dpi=VIZ_CONFIG["dpi"], bbox_inches='tight')
        plt.close()
        
        logger.info(f"Customer retention chart saved to {output_path}")
        return output_path
    
    def create_kpi_dashboard(self):
        """
        Create a KPI dashboard with key metrics
        """
        if 'kpis' not in self.results:
            logger.warning("KPI data not available")
            return None
        
        kpis = self.results['kpis']
        
        # Create figure with custom layout
        fig, ((ax1, ax2, ax3), (ax4, ax5, ax6)) = plt.subplots(2, 3, figsize=(18, 10))
        
        # Remove axes for text-based KPIs
        for ax in [ax1, ax2, ax3, ax4, ax5, ax6]:
            ax.axis('off')
        
        # KPI 1: Total Revenue
        ax1.text(0.5, 0.5, f"${kpis['total_revenue']:,.0f}", 
                ha='center', va='center', fontsize=36, fontweight='bold', color='green')
        ax1.text(0.5, 0.2, "Total Revenue", ha='center', va='center', fontsize=16)
        ax1.add_patch(plt.Rectangle((0.1, 0.1), 0.8, 0.8, fill=False, edgecolor='green', linewidth=2))
        
        # KPI 2: Total Orders
        ax2.text(0.5, 0.5, f"{kpis['total_orders']:,}", 
                ha='center', va='center', fontsize=36, fontweight='bold', color='blue')
        ax2.text(0.5, 0.2, "Total Orders", ha='center', va='center', fontsize=16)
        ax2.add_patch(plt.Rectangle((0.1, 0.1), 0.8, 0.8, fill=False, edgecolor='blue', linewidth=2))
        
        # KPI 3: Unique Customers
        ax3.text(0.5, 0.5, f"{kpis['unique_customers']:,}", 
                ha='center', va='center', fontsize=36, fontweight='bold', color='purple')
        ax3.text(0.5, 0.2, "Unique Customers", ha='center', va='center', fontsize=16)
        ax3.add_patch(plt.Rectangle((0.1, 0.1), 0.8, 0.8, fill=False, edgecolor='purple', linewidth=2))
        
        # KPI 4: Average Order Value
        ax4.text(0.5, 0.5, f"${kpis['average_order_value']:.2f}", 
                ha='center', va='center', fontsize=36, fontweight='bold', color='orange')
        ax4.text(0.5, 0.2, "Avg Order Value", ha='center', va='center', fontsize=16)
        ax4.add_patch(plt.Rectangle((0.1, 0.1), 0.8, 0.8, fill=False, edgecolor='orange', linewidth=2))
        
        # KPI 5: Orders per Customer
        ax5.text(0.5, 0.5, f"{kpis['orders_per_customer']:.1f}", 
                ha='center', va='center', fontsize=36, fontweight='bold', color='red')
        ax5.text(0.5, 0.2, "Orders per Customer", ha='center', va='center', fontsize=16)
        ax5.add_patch(plt.Rectangle((0.1, 0.1), 0.8, 0.8, fill=False, edgecolor='red', linewidth=2))
        
        # KPI 6: Revenue per Customer
        ax6.text(0.5, 0.5, f"${kpis['revenue_per_customer']:.0f}", 
                ha='center', va='center', fontsize=36, fontweight='bold', color='teal')
        ax6.text(0.5, 0.2, "Revenue per Customer", ha='center', va='center', fontsize=16)
        ax6.add_patch(plt.Rectangle((0.1, 0.1), 0.8, 0.8, fill=False, edgecolor='teal', linewidth=2))
        
        plt.suptitle('Key Performance Indicators Dashboard', fontsize=24, fontweight='bold', y=0.95)
        plt.tight_layout()
        
        # Save the plot
        output_path = self.charts_dir / "kpi_dashboard.png"
        plt.savefig(output_path, dpi=VIZ_CONFIG["dpi"], bbox_inches='tight')
        plt.close()
        
        logger.info(f"KPI dashboard saved to {output_path}")
        return output_path
    
    def create_all_visualizations(self):
        """
        Create all visualizations
        """
        logger.info("Creating all visualizations...")
        
        created_charts = {}
        
        # Create each visualization
        try:
            created_charts['kpi_dashboard'] = self.create_kpi_dashboard()
            created_charts['top_products'] = self.plot_top_products_revenue()
            created_charts['customer_segments'] = self.plot_customer_segments()
            created_charts['temporal_patterns'] = self.plot_temporal_patterns()
            created_charts['geographic_analysis'] = self.plot_geographic_analysis()
            created_charts['customer_retention'] = self.plot_customer_retention()
            
        except Exception as e:
            logger.error(f"Error creating visualizations: {str(e)}")
            raise
        
        # Filter out None values (failed charts)
        created_charts = {k: v for k, v in created_charts.items() if v is not None}
        
        logger.info(f"Successfully created {len(created_charts)} visualizations")
        return created_charts


def create_interactive_plotly_dashboard(analysis_results, output_path=None):
    """
    Create an interactive Plotly dashboard
    """
    if output_path is None:
        output_path = CHARTS_DIR / "interactive_dashboard.html"
    
    # Create subplots
    fig = make_subplots(
        rows=3, cols=2,
        subplot_titles=("Top Products by Revenue", "Customer Segments", 
                       "Monthly Sales Trend", "Geographic Distribution",
                       "Hourly Sales Pattern", "Customer Retention"),
        specs=[[{"type": "bar"}, {"type": "pie"}],
               [{"type": "scatter"}, {"type": "bar"}],
               [{"type": "scatter"}, {"type": "pie"}]]
    )
    
    # Top products (if available)
    if 'top_products_by_revenue' in analysis_results:
        top_products = analysis_results['top_products_by_revenue'].head(10)
        fig.add_trace(
            go.Bar(x=top_products['total_revenue'], 
                  y=[desc[:20] + "..." if len(desc) > 20 else desc for desc in top_products['Description']],
                  orientation='h',
                  name="Revenue"),
            row=1, col=1
        )
    
    # Customer segments pie chart (if available)
    if 'segment_summary' in analysis_results:
        segments = analysis_results['segment_summary']
        fig.add_trace(
            go.Pie(labels=segments['segment'], 
                  values=segments['customer_count'],
                  name="Segments"),
            row=1, col=2
        )
    
    # Monthly trend (if available)
    if 'monthly_sales' in analysis_results:
        monthly_data = analysis_results['monthly_sales']
        fig.add_trace(
            go.Scatter(x=monthly_data.index, 
                      y=monthly_data['total_revenue'],
                      mode='lines+markers',
                      name="Monthly Revenue"),
            row=2, col=1
        )
    
    # Update layout
    fig.update_layout(
        title_text="Customer Purchase Behavior Analysis Dashboard",
        title_x=0.5,
        showlegend=False,
        height=1200
    )
    
    # Save as HTML
    fig.write_html(output_path)
    logger.info(f"Interactive dashboard saved to {output_path}")
    
    return output_path
