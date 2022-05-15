-- Databricks notebook source
CREATE DATABASE IF NOT EXISTS myfeast

-- COMMAND ----------

USE myfeast;

CREATE TABLE ft_tripdata_hourly USING DELTA LOCATION "abfss://gold@myfeastadls.dfs.core.windows.net/tripdata_hourly/"

-- COMMAND ----------


