import requests
import concurrent.futures
import glob
import os
from config import settings

def make_api_request(url, params, headers):
  response = requests.get(url, params=params, headers=headers)
  response.raise_for_status()
  return response.text

def fetch_csv_from_api(url, year, max_rows, batch_size, offset):

  while offset < max_rows:
    limit = min(batch_size, max_rows - offset)
    print(f"{year} :: Getting {batch_size} rows starting from {offset}...")

    try:
      params = {"$limit": batch_size, "$offset": offset}
      headers = {"X-App-Token": "96oEumCRBDNlRk6OCI0VRYknl"}

      with concurrent.futures.ThreadPoolExecutor() as executor:
        future = executor.submit(make_api_request, url, params, headers)
        result = future.result() # Waits for the future to complete
      
      if len(result) > 0:

        filename = f'{settings.csv_data_path}/{year}/data_{offset}_to_{offset + limit}.csv'
        with open(filename, "w", newline="") as csvfile:
          csvfile.write(result)
          row_count = len(result.split("\n")) - 2 # -1 for header, -1 for empty line at end
          print (f"     :: data saved to {filename} successfully. Number of rows: {row_count}")

        offset += row_count

      if row_count < batch_size:
        break

    except Exception as e:
      print(f"Error occurred during API request. Status code: {e}")