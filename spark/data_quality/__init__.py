from pyspark import SparkConf
from pyspark.sql import SQLContext, SparkSession
from spark.data_quality.udf.standard_tools import *

def init_qa_tools(spark):
    spark.udf.register('get_s3_data', get_s3_data)