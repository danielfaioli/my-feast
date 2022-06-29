# Databricks notebook source
# MAGIC %md # FEATURE ENGINEERING

# COMMAND ----------

from pyspark.sql import SparkSession, DataFrame
import pyspark.sql.functions as F
from pyspark.sql.functions import udf
from pyspark.sql.types import *
import os 

spath = os.environ["SILVER"]
gpath = os.environ["GOLD"]

def svr_reader(spath: str) -> DataFrame:
    df = spark.read.load(spath)
    return df

def g_writer(df: DataFrame, path: str, partitionBy: str, mode: str = "overwrite") -> None:
    df.write.mode(mode).partitionBy(partitionBy).save(path)

@udf
def precipitation(_set):
    if "snow" in _set:
        return "snow"
    elif "rain" in _set:
        return "rain"
    else:
        return "no"

df = svr_reader(f"{spath}/chicago/weather/station_reads_hourly")


df = df.\
withColumn("event_timestamp", F.date_format(F.col("timestamp"), "dd-MM-yyyy HH:00:00")).\
withColumn("precipitation_type", F.when(F.col("precipitation_type").isNull(), F.lit("no")).otherwise(F.col("precipitation_type"))).\
withColumn("total_rain", F.when(F.col("total_rain").isNull(), 0).otherwise(F.col("total_rain"))).\
groupBy("event_timestamp").\
agg(
    F.avg("air_temperature").alias("avg_temp"),
    F.collect_set("precipitation_type").alias("precipitation_type"),
    F.sum("total_rain").alias("total_rain")
).\
withColumn("precipitation_type", precipitation(F.col("precipitation_type"))).\
withColumn("read_id", F.unix_timestamp(F.col("event_timestamp"),"dd-MM-yyyy HH:00:00").cast("string")).\
withColumn("created", F.to_date(F.col("event_timestamp"), "dd-MM-yyyy HH:00:00")).\
withColumn("event_timestamp", F.from_unixtime(
    F.unix_timestamp(
        F.col("event_timestamp"),
        "dd-MM-yyyy HH:00:00"
    ), 
"yyyy-MM-dd HH:00:00"
))

g_writer(
    df=df,
    path=f"{gpath}/chicago/weather/station_reads_hourly_fv",
    partitionBy="created"
)

# display(df)

# COMMAND ----------

# MAGIC %md # REGISTER FEATURES TO FEATURE STORE

# COMMAND ----------

from feast.infra.offline_stores.contrib.spark_offline_store.spark_source import SparkSource
from feast import Feature, FeatureView, ValueType
from feast import FeatureStore
from datetime import timedelta, datetime
from feast import Entity

fs = FeatureStore("./station_reads_hourly_fs")

# Feature Source Definition
station_reads_source = SparkSource(
    file_format="parquet",
    path=f"abfss://gold@myfeastadls.dfs.core.windows.net/chicago/weather/station_reads_hourly_fv",
    timestamp_field="event_timestamp",
    created_timestamp_column="created",
    name="chi_station_reads_hourly_fv"
)

# Feature Definition
station_reads_fv = FeatureView(
    name="fv_chi_station_reads_hourly",
    entities=[],
    features=[
        Feature(name="precipitation_type", dtype=ValueType.STRING),
        Feature(name="avg_temp", dtype=ValueType.FLOAT),
        Feature(name="total_rain", dtype=ValueType.FLOAT)
    ],
    batch_source=station_reads_source,
)

# Entity definition => entity == primary key 

read_entity = Entity(name="read_id", value_type=ValueType.STRING)


fs.apply([station_reads_fv])

# COMMAND ----------

# Validate feature metadata

print(fs.get_feature_view("fv_chi_station_reads_hourly"))


# COMMAND ----------

# MAGIC %md # Exploring Feature Store

# COMMAND ----------

from feast.infra.offline_stores.contrib.spark_offline_store.spark_source import SparkSource
from feast import Feature, FeatureView, ValueType
from feast import FeatureStore
from datetime import timedelta, datetime
from feast import Entity

import warnings
warnings.filterwarnings("ignore")

# create FeatureStore object

fs = FeatureStore("./station_reads_hourly_fs")

# COMMAND ----------

fs.list_feature_views()

# COMMAND ----------

spark.conf.set("spark.databricks.delta.formatCheck.enabled", False)

entity_df = df.select("event_timestamp").toPandas()

hist_feat = fs.get_historical_features(
    entity_df=entity_df,
    features=["fv_chi_station_reads_hourly:precipitation_type"]
    
)

# COMMAND ----------

hist_feat.to_spark_df().show()

# COMMAND ----------

entities = fs.list_entities()
print("ENTITIES: \n")
for en in entities:
    print(f"{en.name}")
    print(en)
    print("="*100)
    
print("FEATURE VIEWS: \n")
for f in fs.list_feature_views():
    print(f"{f.name}")
    print(f)
    print("="*100)

# COMMAND ----------


