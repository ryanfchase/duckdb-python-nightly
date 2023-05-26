import duckdb
import os
import glob

from utils.generate_parquet_from_csv import generate_parquet_from_csv
from utils.fetch_csv_from_api import fetch_csv_from_api
from config import settings

# Convert it for more efficient processing
# generate_parquet_from_csv() # Converts csv to parquet

url_2023 = "https://data.lacity.org/resource/4a4x-mna2.csv"
url_2022 = "https://data.lacity.org/resource/i5ke-k6by.csv"
url_2021 = "https://data.lacity.org/resource/97z7-y5bt.csv"
url_2020 = "https://data.lacity.org/resource/rq3b-xjk8.csv"
url_2019 = "https://data.lacity.org/resource/pvft-t768.csv"
url_2018 = "https://data.lacity.org/resource/h65r-yf5i.csv"
url_2017 = "https://data.lacity.org/resource/d4vt-q4t5.csv"

def main():
  should_api_call = input("get latest API data? (Y/N): ")
  if should_api_call.lower() == "y":

    # Delete existing csv files
    curr_csv_files = glob.glob(f'{settings.csv_data_path}/*.csv')
    for f in curr_csv_files:
      try:
        os.remove(f)
      except Exception as e:
        print(f"Error occurred during file deletion. Status code: {e}")

    url = "https://data.lacity.org/resource/4a4x-mna2.csv"

    max_rows = 2000000
    batch_size = 100000
    offset = 0

    fetch_csv_from_api(url, max_rows, batch_size, offset)

  should_parquet_transform = input("transform the CSV data to parquet? (Y/N): ")
  if should_parquet_transform.lower() == "y":
    generate_parquet_from_csv()
    print(f"Data transformed to parquet successfully.")

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