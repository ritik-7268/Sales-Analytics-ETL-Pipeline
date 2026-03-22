"""
Master Pipeline - Run All Layers
This script executes the complete ETL pipeline: Bronze -> Silver -> Gold
"""

import subprocess # Run other Python scripts
import sys # Exit pipeline on failure
from datetime import datetime
# (subprocess): Each layer runs as a separate program, Failure in one layer doesn’t corrupt others
# (import + run): All layers run inside one program


print("SALES ANALYTICS PIPELINE - BATCH PROCESSING")

print(f"Pipeline Start Time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
# Gets the current date & time and Converts it into a readable string format

# List of scripts to execute in order
scripts = [
    ("Bronze Layer", "01_bronze_layer.py"),
    ("Silver Layer", "02_silver_layer.py"),
    ("Gold Layer", "03_gold_layer.py")
]

# Execute each script
for layer_name, script_name in scripts:
    print(f"EXECUTING: {layer_name}")
    print(f"Script: {script_name}")
    
    try:
        # Run the Python script using spark-submit
        result = subprocess.run(
            ["python",  script_name],
            capture_output=True, # Captures: stdout (print statements), stderr (warnings/errors)
            text=True, # Output returned as string, Without this → output is bytes
            check=True # 0 = success else Raises CalledProcessError, Pipeline stops
        )
        """
        result contains :
            result.returncode   # 0 = success
            result.stdout       # normal output
            result.stderr       # warnings / errors
        """

        # Print output
        print(result.stdout)
        
        if result.stderr:
            print("Warnings/Info:")
            print(result.stderr)
            
        print(f"\n {layer_name} completed successfully!")
        
    except subprocess.CalledProcessError as e:
        print(f"\n✗ ERROR in {layer_name}")
        print(f"Error Message: {e.stderr}")
        print(f"\nPipeline stopped due to error in {layer_name}")
        sys.exit(1)


print("PIPELINE COMPLETED SUCCESSFULLY!")
print(f"Pipeline End Time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

print("\nAll layers processed:")
print("Bronze Layer - Raw data ingested")
print("Silver Layer - Data cleaned and transformed")
print("Gold Layer - Business metrics generated")
print("\nOutput files saved in ../output/ directory")
