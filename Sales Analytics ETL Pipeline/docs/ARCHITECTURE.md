# PROJECT ARCHITECTURE

## Data Flow Diagram

```
┌─────────────────────────────────────────────────────────────────┐
│                   SALES ANALYTICS PIPELINE                      │
│                   (Medallion Architecture)                      │
└─────────────────────────────────────────────────────────────────┘

INPUT DATA
──────────
┌──────────────────┐
│  sales_data.csv  │
│  (Raw CSV File)  │
│                  │
│ • 30 records     │
│ • 9 columns      │
│ • May have       │
│   duplicates &   │
│   null values    │
└────────┬─────────┘
         │
         ▼
┌─────────────────────────────────────────────────────────────────┐
│                     BRONZE LAYER                                │
│                  (01_bronze_layer.py)                           │
│                                                                 │
│  Purpose: Raw Data Ingestion                                    │
│  ─────────────────────────────────                              │
│  • Read CSV file                                                │
│  • Add ingestion_time timestamp                                 │
│  • Save as Parquet (columnar format)                            │
│  • NO transformations - preserve original data                  │
│                                                                 │
│  Output: output/bronze/sales_raw/                               │
└────────┬────────────────────────────────────────────────────────┘
         │
         ▼
┌────────────────────────────────────────────────────────────────┐
│                     SILVER LAYER                               │
│                  (02_silver_layer.py)                          │
│                                                                │
│  Purpose: Data Cleaning & Transformation                       │
│  ─────────────────────────────────────────                     │
│  • Remove duplicate orders (by order_id)                       │
│  • Filter null customer_id records                             │
│  • Standardize text (UPPERCASE, trim spaces)                   │
│  • Convert date formats (string → date)                        │
│  • Calculate total_amount (quantity × price)                   │
│                                                                │
│  Quality Checks:                                               │
│  Deduplication                                                 │
│  Null handling                                                 │
│  Schema standardization                                        │
│                                                                │
│  Output: output/silver/sales_cleaned/                          │
└────────┬───────────────────────────────────────────────────────┘
         │
         ▼
┌─────────────────────────────────────────────────────────────────┐
│                     GOLD LAYER                                  │
│                  (03_gold_layer.py)                             │
│                                                                 │
│  Purpose: Business Analytics & Aggregations                     │
│  ─────────────────────────────────────────────                  │
│                                                                 │
│  Analytical Datasets Created:                                   │
│  ═══════════════════════════════                                │
│                                                                 │
│    Monthly Sales Summary                                        │
│     ├─ Total orders by month                                    │
│     ├─ Total revenue by month                                   │
│     └─ Average order value by month                             │
│                                                                 │
│    Top 5 Products                                               │
│     ├─ Total revenue per product                                │
│     ├─ Total quantity sold                                      │
│     └─ Order count per product                                  │
│                                                                 │
│    Region Performance                                           │
│     ├─ Orders by region                                         │
│     ├─ Revenue by region                                        │
│     └─ Avg order value by region                                │
│                                                                 │
│    Top 5 Customers                                              │
│     ├─ Total orders per customer                                │
│     ├─ Total revenue per customer                               │
│     └─ Avg order value per customer                             │
│                                                                 │
│  Outputs:                                                       │
│  • output/gold/monthly_sales_summary/                           │
│  • output/gold/top_products/                                    │
│  • output/gold/region_performance/                              │
│  • output/gold/top_customers/                                   │
└─────────────────────────────────────────────────────────────────┘
         │
         ▼
    BUSINESS INTELLIGENCE
    ──────────────────────
    • Dashboards
    • Reports
    • Analytics
    • Decision Making
```

## TECHNOLOGY STACK

Language:        Python 3.x
Framework:       PySpark (Distributed Processing)
Data Format:     CSV (input) → Parquet (processing)
Architecture:    Medallion (Bronze-Silver-Gold)
Processing:      Batch ETL


## KEY FEATURES

• Three-layer separation of concerns
• Data quality validation at each layer
• Automated pipeline orchestration
• Scalable PySpark implementation
• Columnar storage (Parquet) for efficiency
• Comprehensive business analytics
• Clean, commented, production-ready code


## PIPELINE EXECUTION
```
Manual Execution:
$ cd scripts/
$ python3 01_bronze_layer.py    # Ingest raw data
$ python3 02_silver_layer.py    # Clean data
$ python3 03_gold_layer.py      # Generate analytics

Automated Execution:
$ cd scripts/
$ python3 run_pipeline.py       # Run all layers
```
