from sodapy import Socrata
from pyspark.sql import SparkSession
from tqdm import tqdm
import pyspark.sql.functions as F
from pyspark.sql import Row
from pyspark.sql.types import StructType, StringType, StructField
from datetime import datetime 
from dateutil.relativedelta import relativedelta
import concurrent.futures
import sys

def _helper(res):
    return Row(res["trip_start_timestamp"], res)

class ApiReader:
    def __init__(self, year, month):
        self.sdate = datetime(year, month, 1)
        self.edate = datetime(year, month, 1) + relativedelta(months=+1)
        self.client = Socrata(
            "data.cityofchicago.org",
            "89mr9UrZJHvtFcdGNRbfVZLIg",
            timeout=36000
        )
        results = self._get_results()
        self.data = self._get_data(results)

    def _get_results(self):
        results = self.client.\
            get_all(
                dataset_identifier="wrvz-psew", 
                where=f"trip_start_timestamp >= '2022-{self.sdate.strftime('%m')}-01T00:00:00.000' \
                    AND trip_start_timestamp < '2022-{self.edate.strftime('%m')}-01T00:00:00.000'")
        return results

    @staticmethod
    def _get_data(results):
        res = []

        with concurrent.futures.ProcessPoolExecutor(5) as executor:
            for result in tqdm(results):
                res.append(executor.submit(_helper, result).result())
        return res

if __name__ == "__main__":
    
    params = sys.argv[1].split(",")
    year = int(params[0])
    month = int(params[1])
    
    apiReader = ApiReader(year=year, month=month)
    data = apiReader.data
    
    # spark session creation 
    spark = SparkSession.\
            builder.\
            appName("pyspark-notebook").\
            master("spark://spark-master:7077").\
            config("spark.executor.memory", "1g").\
            config("spark.executor.cores", 2).\
            getOrCreate()
            
    spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")
    
    # create rdd
    _rdd = spark.sparkContext.parallelize(data, numSlices=31)
    # del data
    
    resDF = _rdd.toDF(schema=["created", "Body"])
    
    resDF.\
        withColumn("created", F.to_date(F.col("created"))).\
        repartition("created").\
        write.\
        mode("append").\
        partitionBy("created").\
        save("hdfs://namenode:8020/bronze/chicago/taxi")
