# Gold Layer Rules
# ----------------
# 1. Read data from Silver parquet files
# 2. Build analytics-ready dimension and fact tables
# 3. Use 1 row per order_item as fact grain
# 4. Output format: Parquet
# 5. Load strategy: overwrite

from pathlib import Path
import duckdb

SILVER_PATH = Path("lake/silver")
GOLD_PATH = Path("lake/gold")

GOLD_PATH.mkdir(parents=True, exist_ok=True)

con = duckdb.connect()


def build_dim_customers():
    pass


def build_dim_products():
    pass


def build_dim_stores():
    pass


def build_fact_sales():
    pass


def run_gold_transformation():
    pass


if __name__ == "__main__":
    run_gold_transformation()