import duckdb
import requests
from pathlib import Path

BASE_URL = "https://github.com/DataTalksClub/nyc-tlc-data/releases/download"

def download_csv_gz(taxi_type, year, month):
    filename = f"{taxi_type}_tripdata_{year}-{month:02d}.csv.gz"
    url = f"{BASE_URL}/{taxi_type}/{filename}"
    data_dir = Path("data") / taxi_type
    data_dir.mkdir(exist_ok=True, parents=True)
    filepath = data_dir / filename
    
    if not filepath.exists():
        print(f"Downloading {filename}...")
        response = requests.get(url)
        if response.status_code == 200:
            with open(filepath, 'wb') as f:
                f.write(response.content)
            print(f"Saved {filename}")
            return filepath
        else:
            print(f"Failed to download {filename}")
            return None
    else:
        print(f"{filename} already exists")
        return filepath

def load_to_duckdb():
    con = duckdb.connect('taxi_rides_ny.duckdb')
    
    # Create schema
    con.execute("CREATE SCHEMA IF NOT EXISTS prod")
    
    # Load green taxi data
    print("Loading green taxi data...")
    green_files = []
    for year in [2019]:
        for month in [1]:
            fp = download_csv_gz('green', year, month)
            if fp:
                green_files.append(str(fp))
    
    if green_files:
        con.execute(f"""
            CREATE OR REPLACE TABLE prod.green_tripdata AS
            SELECT * FROM read_csv({green_files}, compression='gzip')
        """)
    
    # Load yellow taxi data
    print("Loading yellow taxi data...")
    yellow_files = []
    for year in [2019]:
        for month in [1]:
            fp = download_csv_gz('yellow', year, month)
            if fp:
                yellow_files.append(str(fp))
    
    if yellow_files:
        con.execute(f"""
            CREATE OR REPLACE TABLE prod.yellow_tripdata AS
            SELECT * FROM read_csv({yellow_files}, compression='gzip')
        """)
    
    con.close()
    print("Data loaded successfully")

if __name__ == "__main__":
    load_to_duckdb()
