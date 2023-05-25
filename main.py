import duckdb
import csv
from duckdb_python.utils.data_converter import generate_parquet_from_csv
from duckdb_python.config import settings
from datetime import datetime

# Convert it for more efficient processing
# generate_parquet_from_csv() # Converts csv to parquet


import requests
import concurrent.futures

def make_api_request(url, params, headers):
  response = requests.get(url, params=params, headers=headers)
  return response.text

def main():
  should_api_call = input("get latest API data? (Y/N): ")
  if should_api_call.lower() == "y":

    url_2023 = "https://data.lacity.org/resource/4a4x-mna2.csv"
    url_2022 = "https://data.lacity.org/resource/i5ke-k6by.csv"
    url_2021 = "https://data.lacity.org/resource/97z7-y5bt.csv"
    url_2020 = "https://data.lacity.org/resource/rq3b-xjk8.csv"
    url_2019 = "https://data.lacity.org/resource/pvft-t768.csv"
    url_2018 = "https://data.lacity.org/resource/h65r-yf5i.csv"
    url_2017 = "https://data.lacity.org/resource/d4vt-q4t5.csv"
    url = "https://data.lacity.org/resource/4a4x-mna2.csv"
    # params = {"$limit": 1000}
    params = {}
    headers = {"X-App-Token": "96oEumCRBDNlRk6OCI0VRYknl"}

    with concurrent.futures.ThreadPoolExecutor() as executor:
      future = executor.submit(make_api_request, url, params, headers)
      result = future.result() # Waits for the future to complete

    # filename = "api_data.csv"
    currentTime = datetime.now().strftime("%Y%m%d%H%M%S")
    filename = f'{settings.csv_data_path}/api_data_{currentTime}.csv'
    with open(filename, "w", newline="") as csvfile:
      csvfile.write(result)

    print(f"Data saved to {filename} successfully.")

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
    res = conn.execute(f"select * from requests limit 1").fetchall()

    # Output it
    # print(res)
    print(conn.view('requests').show())


  print("Done, exiting the program.")

if __name__ == "__main__":
  main()


# response = requests.get(url, params=params)

# if response.status_code == 200:
#   # API request successful
#   csv_data = response.text
#   # Process the CSV data as needed
#   print(csv_data)
# else:
#   print("Error occurred during API request. Status code: ", response.status_code)