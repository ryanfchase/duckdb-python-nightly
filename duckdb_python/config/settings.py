# Module name used for relative imports
module_name = 'duckdb_python'

# Paths to local raw data files
data_path = f'./{module_name}/data'
csv_data_path = f'{data_path}/csv'
parquet_data_path = f'{data_path}/parquet'

# Path to persistent duckdb database
db_path = f'./{module_name}/db/db.duckdb'

# SQL tables
requests = 'requests'