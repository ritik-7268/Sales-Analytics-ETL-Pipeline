# Product Sales Analytics Pipeline

## Project Overview
A batch processing data pipeline built using **PySpark** that implements the **Medallion Architecture** (Bronze → Silver → Gold) to process product sales data and generate business analytics.

## Business Problem
Process raw sales transaction data to generate actionable business insights including:
- Monthly sales trends
- Top-performing products
- Regional performance analysis
- Customer purchase behavior

## Tech Stack
- **Language**: Python
- **Framework**: PySpark
- **Architecture**: Medallion (Bronze-Silver-Gold)
- **Data Format**: CSV (input), Parquet (processing)

## Project Structure
```
sales-analytics-project/
├── data/
│   └── raw/
│       └── sales_data.csv          # Sample raw sales data
├── scripts/
│   ├── 01_bronze_layer.py          # Data ingestion
│   ├── 02_silver_layer.py          # Data cleaning & transformation
│   ├── 03_gold_layer.py            # Business metrics & aggregations
│   └── run_pipeline.py             # Master pipeline executor
├── docs/
|    ├── ARCHITECTURE.md
|    └── SETUP_GUIDE.md
├── requirements.txt
└── README.md
```

## Pipeline Layers

### Bronze Layer (Data Ingestion)
- Reads raw CSV sales data
- Adds ingestion timestamp
- Saves as Parquet format
- **No transformations** - preserves raw data

### Silver Layer (Data Cleaning)
- Removes duplicate records
- Handles missing values (filters null customer_id)
- Standardizes text columns (uppercase, trim)
- Converts date formats
- Calculates total_amount (quantity × price)

### Gold Layer (Business Analytics)
Generates 4 key analytical datasets:
1. **Monthly Sales Summary** - Revenue trends by month
2. **Top 5 Products** - Best performing products by revenue
3. **Region Performance** - Sales analysis by geographic region
4. **Top 5 Customers** - Highest revenue-generating customers

## Setup Instructions

### Installation

**Install PySpark**
```bash
pip install pyspark
```

## How to Run

### Option 1: Run Complete Pipeline
```bash
cd scripts/
python3 run_pipeline.py
```

### Option 2: Run Individual Layers
```bash
cd scripts/

# Step 1: Bronze Layer
python3 01_bronze_layer.py

# Step 2: Silver Layer
python3 02_silver_layer.py

# Step 3: Gold Layer
python3 03_gold_layer.py
```

## Expected Output Example:

**Starting Bronze Layer Processing**
Reading raw data from: ../data/raw/sales_data.csv
Total Records in Bronze Layer: 30

**Starting Silver Layer Processing**
Records before cleaning: 30
Records after removing duplicates: 30
Records after removing null customer_id: 29

**Starting Gold Layer Processing**
```
Monthly Sales Summary:
+----+-----+------------+-------------+---------------+
|year|month|total_orders|total_revenue|avg_order_value|
+----+-----+------------+-------------+---------------+
|2024|    1|           5|       106000|        21200.0|
|2024|    2|           4|        83000|        20750.0|
+----+-----+------------+-------------+---------------+
```

### Generated Files:
- `output/bronze/sales_raw/` - Parquet files with raw data
- `output/silver/sales_cleaned/` - Parquet files with cleaned data
- `output/gold/monthly_sales_summary/` - Monthly metrics
- `output/gold/top_products/` - Top products report
- `output/gold/region_performance/` - Regional analysis
- `output/gold/top_customers/` - Top customers report

## Data Quality Checks Implemented
- Duplicate removal based on order_id  
- Null value handling for customer_id  
- Data type standardization  
- Date format validation  
- Calculated field validation  

## Key Metrics Tracked
- **Total Revenue**: Sum of all sales
- **Order Count**: Number of transactions
- **Average Order Value**: Mean transaction amount
- **Product Performance**: Revenue by product
- **Regional Distribution**: Sales by geography
- **Customer Segmentation**: Top buyers

## Sample Data Schema

### Bronze/Silver Schema:
```
order_id: integer
customer_id: string
product_id: string
product_name: string
category: string
quantity: integer
price: integer
order_date: date
region: string
total_amount: double
```
