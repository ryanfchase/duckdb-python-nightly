import duckdb
from duckdb_python.utils.data_converter import generate_parquet_from_csv
from duckdb_python.config import settings

# Convert it for more efficient processing
generate_parquet_from_csv() # Converts csv to parquet

# Seed it
conn = duckdb.connect(database=settings.db_path, read_only=False)
conn.execute(f"drop view if exists {settings.requests}")
conn.execute(f"create view {settings.requests} as select * from '{settings.parquet_data_path}/*.parquet'").fetchall()

# Query it
res = conn.execute(f"select * from requests limit 1").fetchall()

# Output it
print(res)