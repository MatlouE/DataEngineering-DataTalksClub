-- PostgreSQL version of BigQuery SQL script
-- Adapted to run locally on PostgreSQL instead of GCP BigQuery
-- 
-- CONNECTION INSTRUCTIONS:
-- Connect to the ny_taxi_db database before running this script:
-- 
-- Via command line:
--   psql -U postgres -d ny_taxi_db -h localhost -p 5432
--
-- Via Python (psycopg2):
--   import psycopg2
--   conn = psycopg2.connect(
--       host="localhost",
--       database="ny_taxi_db",
--       user="postgres",
--       password="your_password",
--       port="5432"
--   )
--
-- Via Docker Compose (if Postgres running in container):
--   docker exec -it <postgres_container_name> psql -U postgres -d ny_taxi_db
--
-- ============================================================================

-- Create schema for taxi data
CREATE SCHEMA IF NOT EXISTS nytaxi;

-- Drop tables if they exist (PostgreSQL equivalent of CREATE OR REPLACE)
DROP TABLE IF EXISTS nytaxi.yellow_tripdata_non_partitioned CASCADE;
DROP TABLE IF EXISTS nytaxi.yellow_tripdata_partitioned CASCADE;
DROP TABLE IF EXISTS nytaxi.yellow_tripdata_partitioned_clustered CASCADE;
DROP TABLE IF EXISTS nytaxi.external_yellow_tripdata CASCADE;
DROP TABLE IF EXISTS nytaxi.citibike_stations CASCADE;

-- Create sample citibike_stations table (replacing BigQuery public data)
CREATE TABLE nytaxi.citibike_stations (
  station_id INTEGER PRIMARY KEY,
  name VARCHAR(255)
);

-- Insert sample citibike data
INSERT INTO nytaxi.citibike_stations (station_id, name) VALUES
(1, 'E 31 St & 3 Ave'),
(2, 'W 44 St & 5 Ave'),
(3, 'Central Park S & 6 Ave'),
(4, 'E 40 St & Park Ave'),
(5, 'Broadway & E 22 St');

-- Query sample citibike data
SELECT station_id, name FROM nytaxi.citibike_stations LIMIT 100;

-- Create external table equivalent - a staging table for CSV imports
CREATE TABLE nytaxi.external_yellow_tripdata (
  VendorID INTEGER,
  tpep_pickup_datetime TIMESTAMP,
  tpep_dropoff_datetime TIMESTAMP,
  passenger_count INTEGER,
  trip_distance DECIMAL(10, 2),
  RatecodeID INTEGER,
  store_and_fwd_flag CHAR(1),
  PULocationID INTEGER,
  DOLocationID INTEGER,
  payment_type INTEGER,
  fare_amount DECIMAL(10, 2),
  extra DECIMAL(10, 2),
  mta_tax DECIMAL(10, 2),
  tip_amount DECIMAL(10, 2),
  tolls_amount DECIMAL(10, 2),
  total_amount DECIMAL(10, 2)
);

-- Instead of loading from GCS, you would import local CSV files:
-- COPY nytaxi.external_yellow_tripdata FROM '/path/to/yellow_tripdata_2019-01.csv' WITH (FORMAT csv, HEADER);
-- COPY nytaxi.external_yellow_tripdata FROM '/path/to/yellow_tripdata_2020-01.csv' WITH (FORMAT csv, HEADER);

-- Insert sample data for demonstration
INSERT INTO nytaxi.external_yellow_tripdata VALUES
(1, '2019-06-15 10:30:00', '2019-06-15 10:45:00', 2, 5.5, 1, 'N', 263, 262, 1, 15.50, 0.50, 0.50, 3.00, 0, 19.50),
(2, '2019-06-20 14:20:00', '2019-06-20 14:35:00', 1, 3.2, 1, 'N', 186, 42, 2, 12.00, 1.00, 0.50, 2.50, 0, 16.00),
(1, '2020-01-10 08:15:00', '2020-01-10 08:30:00', 3, 4.1, 1, 'N', 238, 239, 1, 13.00, 0.50, 0.50, 2.75, 0, 16.75),
(2, '2020-06-25 16:45:00', '2020-06-25 17:00:00', 1, 2.8, 1, 'N', 262, 263, 1, 11.50, 0.50, 0.50, 2.00, 0, 14.50);

-- Check yellow trip data
SELECT * FROM nytaxi.external_yellow_tripdata LIMIT 10;

-- Create a non-partitioned table from external table
CREATE TABLE nytaxi.yellow_tripdata_non_partitioned AS
SELECT * FROM nytaxi.external_yellow_tripdata;

-- Create a partitioned table from external table
-- PostgreSQL supports declarative partitioning by range
CREATE TABLE nytaxi.yellow_tripdata_partitioned (
  VendorID INTEGER,
  tpep_pickup_datetime TIMESTAMP,
  tpep_dropoff_datetime TIMESTAMP,
  passenger_count INTEGER,
  trip_distance DECIMAL(10, 2),
  RatecodeID INTEGER,
  store_and_fwd_flag CHAR(1),
  PULocationID INTEGER,
  DOLocationID INTEGER,
  payment_type INTEGER,
  fare_amount DECIMAL(10, 2),
  extra DECIMAL(10, 2),
  mta_tax DECIMAL(10, 2),
  tip_amount DECIMAL(10, 2),
  tolls_amount DECIMAL(10, 2),
  total_amount DECIMAL(10, 2)
) PARTITION BY RANGE (DATE(tpep_pickup_datetime));

-- Create partitions for 2019 and 2020
CREATE TABLE nytaxi.yellow_tripdata_partitioned_2019 
  PARTITION OF nytaxi.yellow_tripdata_partitioned
  FOR VALUES FROM ('2019-01-01') TO ('2020-01-01');

CREATE TABLE nytaxi.yellow_tripdata_partitioned_2020 
  PARTITION OF nytaxi.yellow_tripdata_partitioned
  FOR VALUES FROM ('2020-01-01') TO ('2021-01-01');

-- Insert data into partitioned table
INSERT INTO nytaxi.yellow_tripdata_partitioned
SELECT * FROM nytaxi.external_yellow_tripdata;

-- Impact of partition - demonstrates scanning behavior
-- Query without partition advantage
SELECT DISTINCT(VendorID)
FROM nytaxi.yellow_tripdata_non_partitioned
WHERE DATE(tpep_pickup_datetime) BETWEEN '2019-06-01' AND '2019-06-30';

-- Query with partition advantage
SELECT DISTINCT(VendorID)
FROM nytaxi.yellow_tripdata_partitioned
WHERE DATE(tpep_pickup_datetime) BETWEEN '2019-06-01' AND '2019-06-30';

-- View partition information in PostgreSQL
-- Lists partitions and row counts
SELECT
  schemaname,
  tablename,
  (SELECT COUNT(*) FROM nytaxi.yellow_tripdata_partitioned_2019) as "2019_rows",
  (SELECT COUNT(*) FROM nytaxi.yellow_tripdata_partitioned_2020) as "2020_rows"
FROM pg_tables
WHERE schemaname = 'nytaxi' AND tablename LIKE 'yellow_tripdata_partitioned%';

-- Create a partitioned and indexed table for better performance
CREATE TABLE nytaxi.yellow_tripdata_partitioned_clustered (
  VendorID INTEGER,
  tpep_pickup_datetime TIMESTAMP,
  tpep_dropoff_datetime TIMESTAMP,
  passenger_count INTEGER,
  trip_distance DECIMAL(10, 2),
  RatecodeID INTEGER,
  store_and_fwd_flag CHAR(1),
  PULocationID INTEGER,
  DOLocationID INTEGER,
  payment_type INTEGER,
  fare_amount DECIMAL(10, 2),
  extra DECIMAL(10, 2),
  mta_tax DECIMAL(10, 2),
  tip_amount DECIMAL(10, 2),
  tolls_amount DECIMAL(10, 2),
  total_amount DECIMAL(10, 2)
) PARTITION BY RANGE (DATE(tpep_pickup_datetime));

-- Create index partitions
CREATE TABLE nytaxi.yellow_tripdata_partitioned_clustered_2019
  PARTITION OF nytaxi.yellow_tripdata_partitioned_clustered
  FOR VALUES FROM ('2019-01-01') TO ('2020-01-01');

CREATE TABLE nytaxi.yellow_tripdata_partitioned_clustered_2020
  PARTITION OF nytaxi.yellow_tripdata_partitioned_clustered
  FOR VALUES FROM ('2020-01-01') TO ('2021-01-01');

-- Add indexes for VendorID (clustering effect)
CREATE INDEX idx_yellow_tripdata_clustered_vendor_2019 
  ON nytaxi.yellow_tripdata_partitioned_clustered_2019(VendorID);

CREATE INDEX idx_yellow_tripdata_clustered_vendor_2020 
  ON nytaxi.yellow_tripdata_partitioned_clustered_2020(VendorID);

-- Insert data
INSERT INTO nytaxi.yellow_tripdata_partitioned_clustered
SELECT * FROM nytaxi.external_yellow_tripdata;

-- Query comparison: non-indexed partitioned table
SELECT count(*) as trips
FROM nytaxi.yellow_tripdata_partitioned
WHERE DATE(tpep_pickup_datetime) BETWEEN '2019-06-01' AND '2020-12-31'
  AND VendorID = 1;

-- Query comparison: indexed partitioned table (faster due to index)
SELECT count(*) as trips
FROM nytaxi.yellow_tripdata_partitioned_clustered
WHERE DATE(tpep_pickup_datetime) BETWEEN '2019-06-01' AND '2020-12-31'
  AND VendorID = 1;

-- View table sizes and structure
SELECT
  schemaname,
  tablename,
  pg_size_pretty(pg_total_relation_size(schemaname||'.'||tablename)) as size
FROM pg_tables
WHERE schemaname = 'nytaxi'
ORDER BY pg_total_relation_size(schemaname||'.'||tablename) DESC;
