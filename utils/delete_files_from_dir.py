import os
import glob

def delete_files_from_dir(path, filetype):
  # Delete existing csv files
  curr_csv_files = glob.glob(f'{path}/*.{filetype}')
  for f in curr_csv_files:
    try:
      os.remove(f)
    except Exception as e:
      print(f"Error occurred during file deletion for year {year}. Status code: {e}")