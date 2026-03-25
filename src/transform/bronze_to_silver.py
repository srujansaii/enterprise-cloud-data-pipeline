# Silver Layer Rules
# ------------------
# 1. Read data from Bronze parquet files
# 2. Apply light cleaning and standardization
# 3. Enforce consistent naming and data types
# 4. Remove duplicates where necessary
# 5. Handle nulls in important fields
# 6. Output format: Parquet
# 7. Load strategy: overwrite
from pathlib import Path
import duckdb

BRONZE_PATH = Path("lake/bronze")
SILVER_PATH = Path("lake/silver")

SILVER_PATH.mkdir(parents=True, exist_ok=True)

con = duckdb.connect()
def transform_table(table_name: str):
    print(f"Transforming {table_name}...")

    df = con.execute(f"""
    SELECT * FROM '{(BRONZE_PATH / f"{table_name}.parquet").as_posix()}'""").df()

def run_silver_transformation():
    def run_silver_transformation():
        transform_table("customers")

if __name__ == "__main__":
    run_silver_transformation()