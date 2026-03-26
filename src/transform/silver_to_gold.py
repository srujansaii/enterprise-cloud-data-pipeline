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
    print("Building dim_customers...")

    df = con.execute(f"""
        SELECT DISTINCT *
        FROM '{(SILVER_PATH / "customers.parquet").as_posix()}'
    """).df()

    output_path = GOLD_PATH / "dim_customers.parquet"
    df.to_parquet(output_path, index=False)

    print("dim_customers written")


def build_dim_products():
    pass


def build_dim_stores():
    pass


def build_fact_sales():
    pass


def run_gold_transformation():
    print("Running gold transformation...")
    build_dim_customers()


if __name__ == "__main__":
    run_gold_transformation()