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
        if table_name == "orders":
            if "order_date" in df.columns:
                df["order_date"] = df["order_date"].astype("datetime64[ns]")

        if table_name == "customers":
            for col in ["created_date", "signup_date"]:
                if col in df.columns:
                    df[col] = df[col].astype("datetime64[ns]")

        if table_name == "order_items":
            for col in ["quantity", "unit_price", "price"]:
                if col in df.columns:
                    df[col] = df[col].astype(float)

        if table_name == "products":
            if "price" in df.columns:
                df["price"] = df["price"].astype(float)
        
        if table_name == "customers":
                critical_cols = ["customer_id"]
                df = df.dropna(subset=[col for col in critical_cols if col in df.columns])

        if table_name == "orders":
                critical_cols = ["order_id", "customer_id"]
                df = df.dropna(subset=[col for col in critical_cols if col in df.columns])

        if table_name == "order_items":
                critical_cols = ["order_id", "product_id"]
                df = df.dropna(subset=[col for col in critical_cols if col in df.columns])

        if table_name == "products":
                critical_cols = ["product_id"]
                df = df.dropna(subset=[col for col in critical_cols if col in df.columns])

        if table_name == "stores":
                critical_cols = ["store_id"]
                df = df.dropna(subset=[col for col in critical_cols if col in df.columns])

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