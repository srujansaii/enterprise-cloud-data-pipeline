# Bronze Layer Rules
# ------------------
# 1. One raw source file → one Bronze table
# 2. No transformations applied
# 3. Output format: Parquet
# 4. Load strategy: overwrite (idempotent runs)
# 5. Schema-on-read (types inferred at read time)
from pathlib import Path
import duckdb

RAW_PATH = Path("data/raw_sources")
BRONZE_PATH = Path("lake/bronze")
def ingest_file(file_path: Path, table_name: str):
    pass

def run_ingestion():
    pass

if __name__ == "__main__":
    run_ingestion()
BRONZE_PATH.mkdir(parents=True, exist_ok=True)

