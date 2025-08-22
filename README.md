# Customer Purchase Behavior Analysis using PySpark

##  Project Description

This project analyzes E-commerce transaction data using PySpark to uncover purchase patterns, customer preferences, and product trends. The analysis focuses on batch data processing to extract meaningful business insights.

##  Key Insights Generated

- **Most purchased products** and their sales trends
- **Peak shopping hours** and seasonal patterns  
- **Top spending customers** and customer segmentation
- **Average order value** analysis
- **Category-wise sales performance**
- **Geographic sales distribution**
- **Customer retention patterns**

##  Project Structure
```
customer-purchase-analysis/
├── data/
│   ├── raw/                 # Raw dataset files
│   └── processed/           # Cleaned and processed data
├── notebooks/
│   └── analysis.ipynb       # Jupyter notebook for interactive analysis
├── src/
│   ├── data_ingestion.py    # Data loading and preprocessing
│   ├── data_analysis.py     # Main analysis functions
│   └── visualization.py     # Chart generation
├── outputs/
│   ├── reports/             # Generated reports
│   └── charts/              # Visualization outputs
├── requirements.txt         # Python dependencies
└── run_analysis.py          # Main execution script
```

##  Tech Stack

- **PySpark** - Distributed data processing
- **Python** - Programming language
- **Matplotlib/Seaborn** - Data visualization
- **Pandas** - Data manipulation for small results
- **Jupyter Notebook** - Interactive analysis

##  Setup Instructions

### 1. Install Requirements
```bash
pip install -r requirements.txt
```

### 2. Download Dataset
The project uses the Online Retail Dataset. Run:
```bash
python src/data_ingestion.py
```

### 3. Run Analysis
```bash
python run_analysis.py
```

### 4. Interactive Analysis
```bash
jupyter notebook notebooks/analysis.ipynb
```

##  Key Features

- **Scalable Processing**: Uses PySpark for handling large datasets
- **Comprehensive Analysis**: Multiple business metrics and KPIs
- **Visual Reports**: Automated chart generation
- **Modular Code**: Well-structured, reusable components
- **Documentation**: Clear code documentation and README

##  Business Applications

This analysis can help businesses:
- Optimize inventory management
- Plan marketing campaigns
- Improve customer retention
- Identify growth opportunities
- Make data-driven decisions

##  Resume Highlights

- **Big Data Processing** with PySpark
- **ETL Pipeline** development
- **Business Intelligence** insights
- **Data Visualization** and reporting
- **Customer Analytics** expertise

##  Sample Outputs

The project generates:
- Customer segmentation reports
- Product performance dashboards
- Sales trend visualizations
- Geographic analysis maps
- Time-series analysis charts

##  Configuration

Modify `config.py` to adjust:
- Spark cluster settings
- Data file paths
- Analysis parameters
- Output formats
