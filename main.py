import duckdb
import os
import glob

from utils.generate_parquet_from_csv import generate_parquet_from_csv
from utils.fetch_csv_from_api import fetch_csv_from_api
from utils.delete_files_from_dir import delete_files_from_dir
from config import settings

# Convert it for more efficient processing
# generate_parquet_from_csv() # Converts csv to parquet

prefix = "https://data.lacity.org/resource/"
suffix = ".csv"
resources = [
  {"year": "2023", "name": "4a4x-mna2"}
  ,{"year": "2022", "name": "i5ke-k6by"}
  ,{"year": "2021", "name": "97z7-y5bt"}
  ,{"year": "2020", "name": "rq3b-xjk8"}
  ,{"year": "2019", "name": "pvft-t768"}
  # ,{"year": "2018", "name": "h65r-yf5i"}
  # ,{"year": "2017", "name": "d4vt-q4t5"}
]

def main():
  should_api_call = input("download all years API data? (Y/N): ")
  if should_api_call.lower() == "y":

    max_rows = 2000000
    batch_size = 100000
    offset = 0

    for resource in resources:
      year = resource['year']
      url = f"{prefix}{resource['name']}{suffix}"
      print(f"Downloading {resource['year']} data from {url}...")

      # Delete existing csv files
      curr_csv_dir = f'{settings.csv_data_path}/{year}'
      delete_files_from_dir(curr_csv_dir, "csv")
      fetch_csv_from_api(url, year, max_rows, batch_size, offset)

  should_parquet_transform = input("transform the CSV data to parquet? (Y/N): ")
  if should_parquet_transform.lower() == "y":

    # Delete existing parquet files
    parquet_dir = f'{settings.parquet_data_path}'
    delete_files_from_dir(parquet_dir, "parquet")

    # Generate new parquet files
    for resource in resources:
      year = resource['year']
      generate_parquet_from_csv(year)
      print(f"Data ({year}) transformed to parquet successfully.")

  should_seed_database = input("seed the database? (Y/N): ")
  if should_seed_database.lower() == "y":
    # Seed it
    conn = duckdb.connect(database=settings.db_path, read_only=False)
    conn.execute(f"drop view if exists {settings.requests}")
    conn.execute(f"create view {settings.requests} as select * from '{settings.parquet_data_path}/*.parquet'")

  should_test_database = input("print view? (Y/N): ")
  if should_test_database.lower() == "y":

    # Query it
    conn = duckdb.connect(database=settings.db_path, read_only=False)
    res = conn.execute(f"select * from requests limit 1").fetchall()

    # Output it
    # print(res)
    print(conn.view('requests').show())


  print("Done, exiting the program.")

if __name__ == "__main__":
  main()