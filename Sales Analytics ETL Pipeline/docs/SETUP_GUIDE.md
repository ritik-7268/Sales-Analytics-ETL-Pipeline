# SETUP & INSTALLATION GUIDE

## Prerequisites Check

### 1. Check Python Version
```bash
python3 --version
# Should be Python 3.8 or higher
```

### 2. Check if Java is Installed (Required for PySpark)
```bash
java -version
# Should be Java 8 or Java 11
```

---

## Running the Project

### Quick Start 
```bash
# Navigate to project root
cd sales-analytics-project

# Go to scripts folder
cd scripts

# Run complete pipeline
python3 run_pipeline.py
```

### Step-by-Step Execution
```bash
cd sales-analytics-project/scripts

# Step 1: Run Bronze Layer
python3 01_bronze_layer.py

# Step 2: Run Silver Layer
python3 02_silver_layer.py

# Step 3: Run Gold Layer
python3 03_gold_layer.py
```

---

## Expected Runtime
- Bronze Layer: ~5-10 seconds
- Silver Layer: ~5-10 seconds
- Gold Layer: ~10-15 seconds
- **Total Pipeline**: ~20-35 seconds

---

## Verifying Output

### Check Output Files Created:
```bash
# From project root
output/bronze/
output/silver/
output/gold/
```
