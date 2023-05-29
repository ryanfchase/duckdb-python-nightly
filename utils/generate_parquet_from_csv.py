import duckdb
import glob
from datetime import datetime
from config import settings

# produces a parquet file in settings.parquet_data_path for each file in settings.csv_data_path
def generate_parquet_from_csv(year):
  conn = duckdb.connect(database=settings.db_path, read_only=False)

  filenames = list(glob.iglob(f'{settings.csv_data_path}/{year}/*.csv'))
  print(f'Files available: {filenames}')
  current_time = datetime.now().strftime("%m-%d-%Y_%H-%M-%S")
  output_path = f'{settings.parquet_data_path}/{year}_data_{current_time}.parquet'

  conn.execute(f"""
  COPY (SELECT * FROM read_csv({filenames}, header=True, columns={{'SRNumber': 'VARCHAR(255)','CreatedDate': 'TIMESTAMP','UpdatedDate': 'TIMESTAMP','ActionTaken': 'VARCHAR(255)','Owner': "VARCHAR(255)",'RequestType': 'VARCHAR(255)','Status': 'VARCHAR(255)','RequestSource': 'VARCHAR(255)','CreatedByUserOrganization': 'VARCHAR(255)','MobileOS': 'VARCHAR(255)','Anonymous': 'VARCHAR(255)','AssignTo': 'VARCHAR(255)','ServiceDate': 'TIMESTAMP','ClosedDate': 'TIMESTAMP','AddressVerified': 'VARCHAR(255)','ApproximateAddress': 'VARCHAR(255)','Address': 'VARCHAR(255)','HouseNumber': 'INTEGER','Direction': 'VARCHAR(255)','StreetName': 'VARCHAR(255)','Suffix': 'VARCHAR(255)','ZipCode': 'VARCHAR(255)','Latitude': 'DECIMAL(8,6)','Longitude': 'DECIMAL(9,6)','Location': 'VARCHAR(255)','TBMPage': 'INTEGER','TBMColumn': 'VARCHAR(255)','TBMRow': 'INTEGER','APC': 'VARCHAR(255)','CD': 'INTEGER','CDMember': 'VARCHAR(255)','NC': 'INTEGER','NCName': 'VARCHAR(255)','PolicePrecinct': 'VARCHAR(255)'}}, filename=True)) 
  TO '{output_path}' (FORMAT 'parquet')""")