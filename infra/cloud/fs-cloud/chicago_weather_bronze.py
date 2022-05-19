# Databricks notebook source
from pyspark.sql import SparkSession, DataFrame
import pyspark.sql.functions as F
from pyspark.sql.types import *
import os 

rpath = os.environ["RAW"]
bpath = os.environ["BRONZE"]

def raw_reader(rpath: str) -> DataFrame:
    df = spark.read.format("avro").load(rpath)
    return df 

def bronze_writer(df: DataFrame, bpath: str, partitionBy: str, mode: str = "overwrite") -> None:
    df.write.mode(mode).partitionBy(partitionBy).save(bpath)

schema = StructType([
    StructField("station_name", StringType(), True),
    StructField("measurement_timestamp", StringType(), True),
    StructField("air_temperature", StringType(), True),
    StructField("wet_bulb_temperature", StringType(), True),
    StructField("humidity", StringType(), True),
    StructField("rain_intensity", StringType(), True),
    StructField("interval_rain", StringType(), True),
    StructField("total_rain", StringType(), True),
    StructField("precipitation_type", StringType(), True),
    StructField("wind_direction", StringType(), True),
    StructField("wind_speed", StringType(), True),
    StructField("maximum_wind_speed", StringType(), True),
    StructField("barometric_pressure", StringType(), True),
    StructField("solar_radiation", StringType(), True),
    StructField("heading", StringType(), True),
    StructField("battery_life", StringType(), True),
    StructField("measurement_timestamp_label", StringType(), True),
    StructField("measurement_id", StringType(), True),
])

df = raw_reader(
    rpath=f"{rpath}/chicago/weather/event_hub=myfeaststream/topic=chicago_weather_daily/captured=20220515"
)

df = df.select(F.col("Body").cast("string")).\
    withColumn("value", F.from_json(F.col("Body"), schema)).\
    withColumn("created", F.to_date("value.measurement_timestamp")).\
    select("value", "created")

bronze_writer(
    df=df,
    bpath=f"{bpath}/chicago/weather/station_reads",
    partitionBy="created"
)
