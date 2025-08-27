-- Databricks notebook source
create database if not exists bronze_db;

show databases;

-- COMMAND ----------

use bronze_db;

select current_database()

-- COMMAND ----------

-- MAGIC %python
-- MAGIC
-- MAGIC %fs ls /FileStore/tables/nyc_taxi_data/bronze_data/

-- COMMAND ----------

-- MAGIC %python
-- MAGIC
-- MAGIC # Script d'ingestion des données
-- MAGIC
-- MAGIC from pyspark.sql.functions import current_timestamp, col, substring_index
-- MAGIC from pyspark.sql.types import StructType, StructField, IntegerType, LongType, DoubleType, StringType, TimestampType
-- MAGIC import datetime
-- MAGIC
-- MAGIC # 1. Définir explicitement le schema des données
-- MAGIC schema = StructType([
-- MAGIC     StructField("VendorID", IntegerType(), True),
-- MAGIC     StructField("tpep_pickup_datetime", TimestampType(), True),
-- MAGIC     StructField("tpep_dropoff_datetime", TimestampType(), True),
-- MAGIC     StructField("passenger_count", LongType(), True),
-- MAGIC     StructField("trip_distance", DoubleType(), True),
-- MAGIC     StructField("RatecodeID", LongType(), True),
-- MAGIC     StructField("store_and_fwd_flag", StringType(), True),
-- MAGIC     StructField("PULocationID", IntegerType(), True),
-- MAGIC     StructField("DOLocationID", IntegerType(), True),
-- MAGIC     StructField("payment_type", LongType(), True),
-- MAGIC     StructField("fare_amount", DoubleType(), True),
-- MAGIC     StructField("extra", DoubleType(), True),
-- MAGIC     StructField("mta_tax", DoubleType(), True),
-- MAGIC     StructField("tip_amount", DoubleType(), True),
-- MAGIC     StructField("tolls_amount", DoubleType(), True),
-- MAGIC     StructField("improvement_surcharge", DoubleType(), True),
-- MAGIC     StructField("total_amount", DoubleType(), True),
-- MAGIC     StructField("congestion_surcharge", DoubleType(), True),
-- MAGIC     StructField("Airport_fee", DoubleType(), True)
-- MAGIC ])
-- MAGIC
-- MAGIC # 2. Dossier source
-- MAGIC source_folder = "dbfs:/FileStore/tables/nyc_taxi_data/bronze_data/"
-- MAGIC filePath = source_folder + "*.parquet"
-- MAGIC
-- MAGIC # 3. Lister les fichiers parquet
-- MAGIC files =  [f for f in dbutils.fs.ls(source_folder) if f.name.endswith(".parquet")]
-- MAGIC
-- MAGIC if files:
-- MAGIC     print(f"{len(files)} fichiers trouvés, ingestion en cours...")
-- MAGIC     # 4. Lire les fichiers avec schéma forcé
-- MAGIC     df = spark.read.schema(schema).parquet(filePath)
-- MAGIC     display(df)
-- MAGIC
-- MAGIC     # 5. Ajouter une colonne ingestion_timestamp + une colonne nom de fichier
-- MAGIC     df = df.withColumn("ingestion_timestamp", current_timestamp()) \
-- MAGIC             .withColumn("file_name", substring_index(col("_metadata.file_path"), "/", -1))
-- MAGIC
-- MAGIC     # -------------------------------
-- MAGIC     # 6. Calcul des nombres de lignes
-- MAGIC     # -------------------------------
-- MAGIC
-- MAGIC     # Nombre de lignes dans la dataframe pyspark à ingérer
-- MAGIC     new_rows_count = df.count()
-- MAGIC
-- MAGIC     # Vérifier si la table existe dans le Catalogue Spark et compter le nombre de lignes existantes dans cette table
-- MAGIC     if "raw_trips" in [t.name for t in spark.catalog.listTables("bronze_db")]:
-- MAGIC         existing_rows_count = spark.table("bronze_db.raw_trips").count()
-- MAGIC     else:
-- MAGIC         existing_rows_count = 0
-- MAGIC
-- MAGIC     # Affichage des informations
-- MAGIC     print(f"Nombre de lignes à ingérer : {new_rows_count}")
-- MAGIC     print(f"Nombre de lignes existantes dans la table : {existing_rows_count}")
-- MAGIC
-- MAGIC     # 7. Sauvegarde des données dans une table Delta
-- MAGIC     df.write.format("delta").mode("append").option("mergeSchema", True).saveAsTable("raw_trips")
-- MAGIC
-- MAGIC     # 8. Contrôle de cohérence au niveau des nombres de lignes
-- MAGIC     final_rows_count = spark.table("bronze_db.raw_trips").count()
-- MAGIC     expected_rows = existing_rows_count + new_rows_count
-- MAGIC
-- MAGIC     if final_rows_count != expected_rows:
-- MAGIC         raise Exception(f"Contrôle de cohérence échoué : Le nombre de lignes dans la table finale {final_rows_count} ne correspond pas au nombre de lignes attendues {expected_rows}") 
-- MAGIC     else:
-- MAGIC         print(f"Contrôle de cohérence réussi : Le nombre de lignes dans la table finale {final_rows_count} correspond au nombre de lignes attendues {expected_rows}")
-- MAGIC
-- MAGIC     # 9. Archiver les fichiers Parquet traités
-- MAGIC     archive_folder = source_folder + "archive/run_" + datetime.datetime.now().strftime("%Y%m%d_%H%M%S") + "/"
-- MAGIC
-- MAGIC     for f in files:
-- MAGIC         dbutils.fs.mv(f.path, archive_folder + f.name)
-- MAGIC
-- MAGIC     print(f"{len(files)} fichiers archivés dans {archive_folder}")
-- MAGIC
-- MAGIC     # Afficher le contenu du dossier archive
-- MAGIC     display(dbutils.fs.ls(archive_folder))
-- MAGIC
-- MAGIC else:
-- MAGIC     print("Aucun fichier parquet trouvé dans le dossier source donc il n'y a rien à ingérer")

-- COMMAND ----------

SELECT * FROM raw_trips LIMIT 10;

-- COMMAND ----------

SELECT COUNT(*) FROM raw_trips

-- COMMAND ----------

SELECT DISTINCT file_name FROM raw_trips ORDER BY file_name;

-- COMMAND ----------

DESCRIBE EXTENDED raw_trips;

-- COMMAND ----------

-- MAGIC %python
-- MAGIC
-- MAGIC %fs ls /FileStore/tables/nyc_taxi_data/bronze_data/

-- COMMAND ----------

-- MAGIC %python
-- MAGIC zone_file_path = "dbfs:/FileStore/tables/nyc_taxi_data/bronze_data/taxi_zone_lookup.csv"
-- MAGIC df_zones = spark.read.option("header", "true").option("inferSchema", "true").csv(zone_file_path)
-- MAGIC display(df_zones)

-- COMMAND ----------

-- MAGIC %python
-- MAGIC df_zones.write.format("delta").mode("ignore").saveAsTable("taxi_zones")

-- COMMAND ----------

SELECT * FROM taxi_zones LIMIT 10;