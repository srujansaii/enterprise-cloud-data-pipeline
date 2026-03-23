# Bronze Layer Rules
# ------------------
# 1. One raw source file → one Bronze table
# 2. No transformations applied
# 3. Output format: Parquet
# 4. Load strategy: overwrite (idempotent runs)
# 5. Schema-on-read

from pathlib import Path
import duckdb

RAW_PATH = Path("data/raw_sources")
BRONZE_PATH = Path("lake/bronze")

BRONZE_PATH.mkdir(parents=True, exist_ok=True)

con = duckdb.connect()


def ingest_file(file_path: Path, table_name: str):
    print(f"Ingesting {file_path.name} -> bronze/{table_name}.parquet")

    con.execute(f"""
        CREATE OR REPLACE TABLE "{table_name}" AS
        SELECT *
        FROM read_csv_auto('{file_path.as_posix()}')
    """)

    con.execute(f"""
        COPY "{table_name}"
        TO '{(BRONZE_PATH / f"{table_name}.parquet").as_posix()}'
        (FORMAT 'parquet')
    """)


def run_ingestion():
    print("Running ingestion...")
    files = list(RAW_PATH.glob("*"))

    for file in files:
        if file.suffix.lower() == ".csv":
            table_name = file.stem.lower().replace(" ", "_")
            ingest_file(file, table_name)


if __name__ == "__main__":
    run_ingestion()