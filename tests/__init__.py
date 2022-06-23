import os
from pyspark.sql import SparkSession

os.environ['PYSPARK_SUBMIT_ARGS'] = '--master local[2] pyspark-shell'

spark_session = SparkSession.builder \
    .appName("test") \
    .getOrCreate()
