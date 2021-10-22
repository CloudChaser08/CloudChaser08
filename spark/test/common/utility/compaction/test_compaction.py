import pytest

from spark.common.utility.compaction import compaction


from mock import patch 

SINGLE_FILE_TEST ={ 'Contents': [{'Key': 'test.parquet',   'Size': 36}]}
NON_PARQUET_FILE_TEST = {'Contents': [{'Key': 'test/test.parquet',   'Size': 36}]}
FILES_250 = [{'Contents': [{'Key': 'test/test1.parquet',   'Size': 262400000}]}]
FILES_UNDER = [ {'Contents': [{'Key': 'test/test1.parquet',   'Size': 1000},{'Key': 'test/test2.parquet',   'Size': 1000},{'Key': 'test/test3.parquet',   'Size': 1000}]}]
SINGLE_FILE_UNDER = [{'Contents': [{'Key': 'test/test1.parquet',   'Size': 1000}]}]
UUID_TEST1 = [{'Contents': [{'Key': '2021-07-01_part-00000-1e123c9c-f440-403a-94b8-429fe36f2f7b.c000.gz.parquet', 'Size':1}]}]
UUID_TEST2 = [{'Contents': [{'Key': '2021-07-01_part-00000-1e123c9c-f440-403a-94b8-429fe36f2f7b.c000.gz.parquet'}, {'Key':'2021-08-03_part-00003-a3175e37-e352-4758-844d-ea1cf4bb9533.c000.gz.parquet'}]}]

from pyspark.sql import SparkSession


@patch('spark.helpers.s3_utils.list_path_pages',return_value=UUID_TEST1)
def test_uuid1(test_method):
    spark = SparkSession.builder.master("local[*]") \
                        .appName('abc') \
                        .getOrCreate()
    t = compaction(spark, 's3://mock/test/')

    test = t.get_uuid()[1]
    spark.stop()    
    
    assert test == "1e123c9c-f440-403a-94b8-429fe36f2f7b"

@patch('spark.helpers.s3_utils.list_path_pages',return_value=UUID_TEST2)
def test_uuid2(test_method):
    spark = SparkSession.builder.master("local[*]") \
                    .appName('abc') \
                    .getOrCreate()
    t = compaction(spark, 's3://mock/test/')
    test = t.get_uuid()[1]
    spark.stop()    
    
    assert test == "a3175e37-e352-4758-844d-ea1cf4bb9533"

@patch('spark.helpers.s3_utils.list_path_pages',return_value=FILES_250)
def test_worth(test_method):
    spark = SparkSession.builder.master("local[*]") \
                    .appName('abc') \
                    .getOrCreate()
    t = compaction(spark, 's3://mock/test/')
    test = t.worth_compacting()
    spark.stop()    
    
    assert test == False

@patch('spark.helpers.s3_utils.list_path_pages',return_value=SINGLE_FILE_UNDER)
def test_worth1(test_method):
    spark = SparkSession.builder.master("local[*]") \
                    .appName('abc') \
                    .getOrCreate()
    t = compaction(spark, 's3://mock/test/')
    test = t.worth_compacting()
    spark.stop()    
   
    assert test == False

@patch('spark.helpers.s3_utils.list_path_pages',return_value=FILES_UNDER)
def test_worth2(test_method):
    spark = SparkSession.builder.master("local[*]") \
                    .appName('abc') \
                    .getOrCreate()
    t = compaction(spark, 's3://mock/test/')
    test = t.worth_compacting()
    spark.stop()
        
    assert test == True




