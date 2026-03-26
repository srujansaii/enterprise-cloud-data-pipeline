import duckdb

con = duckdb.connect()

con.execute("""
SELECT *
FROM 'lake/silver/orders.parquet'
LIMIT 5
""").df()