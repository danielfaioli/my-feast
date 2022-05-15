# Databricks notebook source
!pip install --upgrade feast

# COMMAND ----------

!pip install feast-azure-provider

# COMMAND ----------

!pip uninstall feast-azure-provider

# COMMAND ----------

import os 
from pyspark.sql.functions import * 

RAW = os.environ["RAW"]
BRONZE = os.environ["BRONZE"]
SILVER = os.environ["SILVER"]
GOLD = os.environ["GOLD"]



# COMMAND ----------

# MAGIC %md # INGEST RAW DATA 

# COMMAND ----------

raw = spark.read.option("header", True).csv(f"{RAW}/driver/yellow_tripdata_2022-01.csv")
display(raw)

# COMMAND ----------

# MAGIC %md ## ADD `created` COLUMN AND SAVE IN DELTA FORMAT

# COMMAND ----------

raw.\
withColumn("created", to_date(col("tpep_dropoff_datetime"))).\
write.\
mode("append").\
partitionBy("created").\
save(f"{BRONZE}/tripdata")

# COMMAND ----------

# MAGIC %md # CREATE SILVER

# COMMAND ----------

brz = spark.read.load(f"{BRONZE}/tripdata")

display(brz)

# COMMAND ----------

svr = brz.\
select(
    "created",
    "VendorID",
    "tpep_dropoff_datetime",
    "passenger_count",
    "trip_distance",
    "fare_amount",
    "tip_amount"
).\
withColumnRenamed("tpep_dropoff_datetime", "dropoff_timestamp").\
write.\
mode("append").\
partitionBy("created").\
save(f"{SILVER}/tripdata")


# COMMAND ----------

# MAGIC %md # CREATE GOLD (FEATURE - OFFLINE STORE)

# COMMAND ----------

gld = spark.read.load(f"{SILVER}/tripdata")
display(gld)

# COMMAND ----------

gld = gld.\
withColumn(
    "dropoff_hour", date_format(
        to_timestamp(
            col("dropoff_timestamp")
        ), "dd-MM-yyyy HH:00:00"        
    )
).\
groupBy("VendorID", "dropoff_hour").\
agg(
    avg(col("passenger_count")),
    avg(col("trip_distance")),
    avg(col("tip_amount")),
    count(col("VendorID")),
    min(col("trip_distance")),
    max(col("trip_distance"))    
).\
withColumn("avg_tip_pass", col("avg(tip_amount)")/col("avg(passenger_count)")).\
withColumn("avg_tip_trip", col("avg(tip_amount)")/col("avg(trip_distance)")).\
withColumn("created", to_date(col("dropoff_hour"),"dd-MM-yyyy HH:00:00"))
display(gld)

# COMMAND ----------

gld.\
select("VendorID", "dropoff_hour", "avg_tip_pass", "avg_tip_trip", "created").\
write.\
mode("overwrite").\
partitionBy("created").\
save(f"{GOLD}/tripdata_hourly")

# COMMAND ----------

# MAGIC %md # CREATE FEATURE VIEW / ENTITY AND APPLY CHANGES TO REGISTRY

# COMMAND ----------

from feast.infra.offline_stores.contrib.spark_offline_store.spark_source import SparkSource
from feast import FeatureStore
from feast import Feature, FeatureView, ValueType
from datetime import timedelta
from feast import Entity

import os 

os.environ["AZURE_TENANT_ID"]="f35cc17d-4ea3-4b5f-9c1e-e6770f7c7603"
os.environ["AZURE_CLIENT_ID"]="5baa3265-c1e8-44fb-bb35-c448ae261d4a"
os.environ["AZURE_CLIENT_SECRET"]="Src8Q~7jJtvkbnsWEzJOu4nS5LnqZOpD4Z_5ia0a"


# Feature Source Definition
trip_stats_source = SparkSource(
    file_format="parquet",
    path=f"{os.environ['GOLD']}/tripdata_hourly",
    timestamp_field="dropoff_hour",
    created_timestamp_column="created",
    name="trip_tip_hourly"
)

# Feature Definition
trip_stats_fv = FeatureView(
    name="trip_tip_hourly",
    entities=["VendorID"],
    features=[
        Feature(name="avg_tip_pass", dtype=ValueType.FLOAT),
        Feature(name="avg_tip_trip", dtype=ValueType.FLOAT),
    ],
    batch_source=trip_stats_source,
    ttl=timedelta(days=1)
)

# Entity definition => entity == primary key 

vendor = Entity(name="VendorID", value_type=ValueType.INT64)

fs = FeatureStore("./feature_repo")
fs.apply([vendor, trip_stats_fv])

# COMMAND ----------

fs_sources = fs.list_data_sources()
fs_sources

# COMMAND ----------

feature_views = fs.list_feature_views()

# COMMAND ----------

for feature in feature_views:
    print("==========================================\n")
    print(feature)

# COMMAND ----------

fs.get_historical_features(
    entity_df=vendor,
    feature_refs=[
        "trip_tip_hourly:avg_tip_pass",
        "driver_hourly_stats:avg_tip_trip",
    ],
).to_df()

# COMMAND ----------


