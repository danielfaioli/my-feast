# Databricks notebook source
from pyspark.sql import SparkSession, DataFrame
import pyspark.sql.functions as F
from pyspark.sql.types import *
import os 

bpath = os.environ["BRONZE"]
spath = os.environ["SILVER"]

def bze_reader(bpath: str) -> DataFrame:
    df = spark.read.load(bpath)
    return df 

def svr_writer(df: DataFrame, spath: str, partitionBy: str, mode: str = "overwrite") -> None:
    df.write.mode(mode).partitionBy(partitionBy).save(spath)
    
df = bze_reader(f"{bpath}/chicago/weather/station_reads").\
    selectExpr("value.*", "created")

df = df.\
    withColumn("timestamp", F.to_timestamp("measurement_timestamp")).\
    withColumn("air_temperature", F.col("air_temperature").cast("double")).\
    withColumn("precipitation_type", 
               F.when(F.col("precipitation_type") == "0", "no")
               .when(F.col("precipitation_type") == "60", "rain")
               .when(F.col("precipitation_type") == "70", "snow")
               .otherwise(F.col("precipitation_type"))).\
    withColumn("humidity", F.col("humidity").cast("double")).\
    withColumn("rain_intensity", F.col("rain_intensity").cast("double")).\
    withColumn("total_rain", F.col("total_rain").cast("double")).\
    select(
        "measurement_id", "station_name", "timestamp", "air_temperature", "precipitation_type",
        "humidity", "rain_intensity", "total_rain", "created"
    )

svr_writer(
    df=df,
    spath=f"{spath}/chicago/weather/station_reads_hourly",
    partitionBy="created"
)


