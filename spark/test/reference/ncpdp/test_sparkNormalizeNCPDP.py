
import pytest

import datetime
import shutil
import os
import glob
import logging

import spark.reference.ncpdp.sparkNormalizeNCPDP as normalize
import spark.helpers.file_utils as file_utils
from pyspark.sql.types import DateType

# test that a few special columns are of the correct type
# test that a few date columns were parsed correctly
# test that parquet files were created and are not empty
# query from parquet files

# test that top and bottom rows are removed
INPUT_PATH = file_utils.get_abs_path(__file__, './resources/input/{date}')
PREV_MAS_PATH = file_utils.get_abs_path(__file__, './resources/old_master/{date}')
OUTPUT_PATH = file_utils.get_abs_path(__file__, './tmp/output/{date}')
TEST_DATE = datetime.datetime(2020, 9, 1)
PREV_MAS_DATE = datetime.datetime(2020, 8, 1)

TABLES = [
    'ref_ncpdp_master_mas', 
    'ref_ncpdp_transactions_trn'
]

def cleanup(spark):
    sql_context = spark['sqlContext'] 
    spark_session = spark["spark"]
    
    for table in TABLES:
        sql_context.dropTempTable(table)

    try:
        shutil.rmtree(file_utils.get_abs_path(__file__, './tmp'))
    except:
        logging.warning('No output directory.')


@pytest.mark.usefixtures("spark")
def test_write(spark):
    cleanup(spark)
    sql_context = spark['sqlContext'] 
    spark_session = spark['spark']
    
    normalize.run(spark=spark_session, input_path=INPUT_PATH, output_path=OUTPUT_PATH, date=TEST_DATE, date_prev=PREV_MAS_DATE, prev_master_path=PREV_MAS_PATH)
    
    exist_tables = [table.name for table in spark_session.catalog.listTables()]

    for table in TABLES:
        assert table in exist_tables


def test_file_output(spark):
    """
    Test that parquet file is there and has the correct length + types
    """
    master_mas_location = file_utils.get_abs_path(__file__, './tmp/output/20200901/master/mas/')
    
    sql_context = spark['sqlContext'] 
    spark_session = spark["spark"]

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
    

