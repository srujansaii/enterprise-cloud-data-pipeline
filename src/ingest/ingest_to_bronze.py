# Bronze Layer Rules
# ------------------
# 1. One raw source file → one Bronze table
# 2. No transformations applied
# 3. Output format: Parquet
# 4. Load strategy: overwrite (idempotent runs)
# 5. Schema-on-read (types inferred at read time)

