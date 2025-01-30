import pandas as pd
from sqlalchemy.orm import Session
from database import engine
from models import Agency
from create_tables import create_tables
from envConfig import GTFS_ROOT_FILE_PATH

# Reference: https://dnmtechs.com/loading-csv-file-into-database-using-sqlalchemy-in-python-3/
# Regerence: https://iifx.dev/en/articles/167606266
# Used this for all the load scripts

def load_agency_data():
  # Read the agency.txt file into a pandas DataFrame
  df = pd.read_csv(GTFS_ROOT_FILE_PATH + '/agency.txt')

  # Create a new session
  with Session(engine) as session:
    # Iterate through the rows in the DataFrame
    for _, row in df.iterrows():
      # Create an Agency object for each row
      agency = Agency(
        id=row['agency_id'],
        agency_name=row['agency_name'],
        agency_url=row['agency_url'] if pd.notna(row['agency_url']) else None,
        agency_timezone=row['agency_timezone'],
        agency_lang=row['agency_lang'] if pd.notna(row['agency_lang']) else None,
        agency_phone=row['agency_phone'] if pd.notna(row['agency_phone']) else None,
        agency_fare_url=row['agency_fare_url'] if pd.notna(row['agency_fare_url']) else None,
        agency_email=row['agency_email'] if pd.notna(row['agency_email']) else None
      )
      # Add the agency to the session
      session.add(agency)
    
    # Commit the session to insert the data into the database
    session.commit()

  print("Data loaded successfully.")

if __name__ == "__main__":
  create_tables()
  load_agency_data()
