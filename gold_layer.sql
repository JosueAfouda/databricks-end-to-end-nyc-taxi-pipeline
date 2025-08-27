-- Databricks notebook source
SELECT * FROM silver_db.processed_trips LIMIT 10;

-- COMMAND ----------

CREATE DATABASE IF NOT EXISTS gold_db;

USE gold_db;

SELECT current_database();

-- COMMAND ----------

SELECT * FROM bronze_db.taxi_zones LIMIT 10;

-- COMMAND ----------

CREATE OR REPLACE TABLE processed_trips_with_zones 
USING DELTA 
AS
SELECT
  s.*,
  pu_zone.Borough AS pickup_borough,
  do_zone.Borough AS dropoff_borough,
  pu_zone.Zone AS pickup_zone,
  do_zone.Zone AS dropoff_zone,
  DATE(s.tpep_pickup_datetime) AS pickup_date,
  HOUR(s.tpep_pickup_datetime) AS pickup_hour,
  HOUR(s.tpep_dropoff_datetime) AS dropoff_hour,
  DAYOFWEEK(s.tpep_pickup_datetime) AS pickup_day_of_week
FROM silver_db.processed_trips s
LEFT JOIN bronze_db.taxi_zones pu_zone 
  ON s.PULocationID = pu_zone.LocationID
LEFT JOIN bronze_db.taxi_zones do_zone 
  ON s.DOLocationID = do_zone.LocationID

-- COMMAND ----------

SELECT COUNT(*) FROM processed_trips_with_zones;

-- COMMAND ----------

SELECT * FROM processed_trips_with_zones LIMIT 10;

-- COMMAND ----------

SELECT DISTINCT file_name FROM processed_trips_with_zones ORDER BY file_name;