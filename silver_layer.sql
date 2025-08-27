-- Databricks notebook source
SELECT * FROM bronze_db.raw_trips LIMIT 10;

-- COMMAND ----------

CREATE DATABASE IF NOT EXISTS silver_db;

show databases;

-- COMMAND ----------

USE silver_db;

SELECT current_database();

-- COMMAND ----------

-- MAGIC %md
-- MAGIC - passenger_count > 0
-- MAGIC - trip_distance > 0
-- MAGIC - total_amount > 0
-- MAGIC - Date et heure de demarrage doit être antérieure à date et heure de fin du trajet 
-- MAGIC - considerer uniquement les paiements en carte de crédit

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Pipeline Silver incrémental

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 1. Préparer la table Silver (si elle n'existe pas encore)

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS processed_trips 
USING DELTA
AS
SELECT 
  *
FROM bronze_db.raw_trips
WHERE 
  passenger_count > 0
  AND trip_distance > 0
  AND total_amount > 0
  AND tpep_pickup_datetime < tpep_dropoff_datetime 
  AND tip_amount >= 0
  AND payment_type = 1;

-- COMMAND ----------

SELECT COUNT(*) FROM processed_trips

-- COMMAND ----------

SELECT DISTINCT file_name FROM processed_trips ORDER BY file_name;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 2. Charger seulement les nouvelles données de la table Bronze

-- COMMAND ----------

WITH new_data AS (
  SELECT 
    * 
  FROM bronze_db.raw_trips b  
  WHERE b.ingestion_timestamp > (
    SELECT MAX(ingestion_timestamp) 
    FROM processed_trips
  )
  AND b.passenger_count > 0
  AND b.trip_distance > 0
  AND b.total_amount > 0
  AND b.tpep_pickup_datetime < tpep_dropoff_datetime 
  AND b.tip_amount >= 0
  AND b.payment_type = 1
)

MERGE INTO processed_trips s
USING new_data n
ON s.file_name = n.file_name 
AND s.tpep_pickup_datetime = n.tpep_pickup_datetime
AND s.tpep_dropoff_datetime = n.tpep_dropoff_datetime
AND s.ingestion_timestamp = n.ingestion_timestamp
WHEN MATCHED THEN UPDATE SET *
WHEN NOT MATCHED THEN INSERT *;

-- COMMAND ----------

SELECT COUNT(*) FROM processed_trips;

-- COMMAND ----------

SELECT DISTINCT file_name FROM processed_trips ORDER BY file_name;