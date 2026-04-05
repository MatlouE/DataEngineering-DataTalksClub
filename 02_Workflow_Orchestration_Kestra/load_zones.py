#!/usr/bin/env python
"""Load taxi zones lookup table into PostgreSQL database."""

import pandas as pd
from sqlalchemy import create_engine, text

# Database configuration
db_user = 'root'
db_password = 'root'
db_host = 'localhost'
db_port = '5432'
db_name = 'ny_taxi'

# Create connection
db_url = f'postgresql://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}'
engine = create_engine(db_url)

# Test connection
try:
    with engine.connect() as connection:
        result = connection.execute(text("SELECT 1"))
    print("✓ Database connection successful!")
except Exception as e:
    print(f"✗ Connection failed: {e}")
    raise

# Load zones data
try:
    df_zones = pd.read_csv("taxi_zone_lookup.csv")
    df_zones.to_sql(name='zones', con=engine, if_exists='replace', index=False)
    print(f"✓ Loaded {len(df_zones)} records into 'zones' table")
except Exception as e:
    print(f"✗ Failed to load zones: {e}")
    raise
