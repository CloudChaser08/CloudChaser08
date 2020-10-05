
import pytest

import datetime
import shutil
import os
import glob
import logging

import spark.reference.ncpdp.sparkNormalizeNCPDP as normalize
import spark.helpers.file_utils as file_utils
from pyspark.sql.types import DateType
from pyspark.sql import SparkSession, SQLContext

# test that a few special columns are of the correct type
# test that a few date columns were parsed correctly
# test that parquet files were created and are not empty
# query from parquet files

# test that top and bottom rows are removed
INPUT_PATH = file_utils.get_abs_path(__file__, './tmp/input/{date}')
OUTPUT_PATH = file_utils.get_abs_path(__file__, './tmp/output/{date}')
TEST_DATE = '2020-09-01'

TABLES = [
    'ref_ncpdp_master_mas', 
    'ref_ncpdp_transactions_trn'
]

def cleanup(spark):
    sql_context: SQLContext = spark['sqlContext'] 
    spark_session: SparkSession = spark["spark"]
    
    for table in TABLES:
        sql_context.dropTempTable(table)

    try:
        shutil.rmtree(file_utils.get_abs_path(__file__, './tmp/output'))
    except:
        logging.warning('No output directory.')


@pytest.mark.usefixtures("spark")
def test_write(spark):
    cleanup(spark)
    sql_context: SQLContext = spark['sqlContext'] 
    spark_session: SparkSession = spark['spark']
    
    normalize.run(spark=spark_session, input_path=INPUT_PATH, output_path=OUTPUT_PATH, date_in=TEST_DATE)
    
    exist_tables = [table.name for table in spark_session.catalog.listTables()]

    for table in TABLES:
        assert table in exist_tables


def test_file_output(spark):
    """
    Test that parquet file is there and has the correct length + types
    """
    master_mas_location = file_utils.get_abs_path(__file__, './tmp/output/20200901/master/mas/')
    
    sql_context: SQLContext = spark['sqlContext'] 
    spark_session: SparkSession = spark["spark"]

    df = None
    try:
        df = spark_session.read.parquet(master_mas_location)
    except:
        logging.error('No output file.')

    assert df is not None
    assert df.count() == 81801
    # Make sure dates are dates
    assert isinstance(df.schema['phys_loc_open_dt'].dataType, DateType)


def test_cleanup(spark):
    cleanup(spark)
    

