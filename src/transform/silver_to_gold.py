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
    print("Building dim_products...")

    df = con.execute(f"""
        SELECT DISTINCT *
        FROM '{(SILVER_PATH / "products.parquet").as_posix()}'
    """).df()

    output_path = GOLD_PATH / "dim_products.parquet"
    df.to_parquet(output_path, index=False)

    print("dim_products written")


def build_dim_stores():
    print("Building dim_stores...")

    df = con.execute(f"""
        SELECT DISTINCT *
        FROM '{(SILVER_PATH / "stores.parquet").as_posix()}'
    """).df()

    output_path = GOLD_PATH / "dim_stores.parquet"
    df.to_parquet(output_path, index=False)

    print("dim_stores written")


def build_fact_sales():
    print("Building fact_sales...")

    df = con.execute(f"""
        SELECT *
        FROM '{(SILVER_PATH / "order_items.parquet").as_posix()}'
    """).df()

    output_path = GOLD_PATH / "fact_sales.parquet"
    df.to_parquet(output_path, index=False)

    print("fact_sales written")


def run_gold_transformation():
    print("Running gold transformation...")
    build_dim_customers()
    build_dim_products()
    build_dim_stores()
    build_fact_sales()


if __name__ == "__main__":
    run_gold_transformation()