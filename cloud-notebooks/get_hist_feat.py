# Databricks notebook source
import os 
from feast import FeatureStore

from feast import FeatureStore
os.environ["AZURE_TENANT_ID"]="f35cc17d-4ea3-4b5f-9c1e-e6770f7c7603"
os.environ["AZURE_CLIENT_ID"]="5baa3265-c1e8-44fb-bb35-c448ae261d4a"
os.environ["AZURE_CLIENT_SECRET"]="Src8Q~7jJtvkbnsWEzJOu4nS5LnqZOpD4Z_5ia0a"

RAW = os.environ["RAW"]
BRONZE = os.environ["BRONZE"]
SILVER = os.environ["SILVER"]
GOLD = os.environ["GOLD"]


# connect to FS Registry and apply 
fs = FeatureStore("./feature_repo")

# COMMAND ----------

!feast version

# COMMAND ----------

for feature in fs.list_feature_views():
    print(feature)
    print("="*100+"\n")

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

# spark = SparkSession.\
#         builder.\
#         appName("pyspark-notebook").\
#         master("spark://spark-master:7077").\
#         config("spark.executor.memory", "1g").\
#         config("spark.executor.cores", 1).\
#         getOrCreate()


sdf = spark.read.option("header", True).csv(f"{RAW}/driver/yellow_tripdata_2022-01.csv")

# COMMAND ----------

sdf.show()

# COMMAND ----------

from pyspark.sql import functions as F

sdf_train = sdf.select(
    "VendorID", 
    "tpep_dropoff_datetime", 
    "tip_amount"
).\
withColumn("tpep_dropoff_datetime", F.date_format(
    F.to_timestamp(
        F.col("tpep_dropoff_datetime")
    ), "dd-MM-yyyy HH:00:00"
)).\
filter(F.col("tip_amount") != 0).\
groupBy("VendorID", "tpep_dropoff_datetime").\
agg(F.avg("tip_amount")).\
withColumnRenamed("tpep_dropoff_datetime", "event_timestamp").\
withColumnRenamed("avg(tip_amount)", "tip_target")
# withColumn("created", F.to_date(F.col("event_timestamp"), "dd-MM-yyyy HH:00:00").cast("string")).\

# sdf_train.show()

# COMMAND ----------

sdf_train.show()

# COMMAND ----------

sdf_train.printSchema()

# COMMAND ----------

psdf = sdf_train.to_pandas_on_spark()

# COMMAND ----------

psdf.head()

# COMMAND ----------

pdf = psdf.to_pandas()

# COMMAND ----------

spark.conf.set("spark.databricks.delta.formatCheck.enabled", False)

# COMMAND ----------

train_data = fs.get_historical_features(
    entity_df=pdf,
    features=[
        "trip_tip_hourly:avg_tip_pass",
        "trip_tip_hourly:avg_tip_trip"
    ]
)

# COMMAND ----------

display(train_data.to_spark_df())

# COMMAND ----------


