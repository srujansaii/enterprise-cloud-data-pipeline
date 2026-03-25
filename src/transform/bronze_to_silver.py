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

print("Script started")


def transform_table(table_name: str):
    print(f"Transforming {table_name}...")

    df = con.execute(f"""
        SELECT *
        FROM '{(BRONZE_PATH / f"{table_name}.parquet").as_posix()}'
    """).df()

    print("Rows loaded:", len(df))
    print("Columns:", list(df.columns))

    df.columns = [col.lower().strip().replace(" ", "_") for col in df.columns]
    df = df.drop_duplicates()

    for col in df.select_dtypes(include="object").columns:
        df[col] = df[col].astype(str).str.strip()

    output_path = SILVER_PATH / f"{table_name}.parquet"
    print("Writing to:", output_path)

    df.to_parquet(output_path, index=False)
    print("Write complete")


def run_silver_transformation():
    print("Running silver transformation")

    tables = ["customers", "orders", "order_items", "products", "stores"]

    for table_name in tables:
        transform_table(table_name)


if __name__ == "__main__":
    print("Main block running")
    run_silver_transformation()