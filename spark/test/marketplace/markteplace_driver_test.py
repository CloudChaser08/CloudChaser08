import pytest
import gzip
import subprocess
import os
import inspect

import spark.test.marketplace.resources.test_schema as source_table_schemas
from spark.common.marketplace_driver import MarketplaceDriver
from spark.helpers.udf.general_helpers import obfuscate_hvid
from datetime import date


PROVIDER_NAME = 'TEST'
DATA_TYPE = 'TEST_CLAIMS'
OUTPUT_TABLE_NAMES_TO_SCHEMA = {}
PROVIDER_PARTITION_COLUMN = 'test'
DATE_PARTITION_COLUMN = 'test2'

# --------------------------- Common for all providers ---------------------------
INPUT_DATE = '2019-01-02'
SCRIPT_PATH = __file__
PROVIDER_DIRECTORY_PATH = os.path.dirname(inspect.getframeinfo(inspect.stack()[0][0]).filename)
PROVIDER_DIRECTORY_PATH = PROVIDER_DIRECTORY_PATH.replace('spark/target/dewey.zip/', "") + '/'
PROVIDER_DIRECTORY_PATH = PROVIDER_DIRECTORY_PATH + 'resources/sql/'

TEST_DRIVER = MarketplaceDriver(PROVIDER_NAME,
                                DATA_TYPE,
                                INPUT_DATE,
                                SCRIPT_PATH,
                                PROVIDER_DIRECTORY_PATH,
                                source_table_schemas,
                                OUTPUT_TABLE_NAMES_TO_SCHEMA,
                                PROVIDER_PARTITION_COLUMN,
                                DATE_PARTITION_COLUMN,
                                test=True)

E2E_DRIVER = MarketplaceDriver(PROVIDER_NAME,
                               DATA_TYPE,
                               INPUT_DATE,
                               SCRIPT_PATH,
                               PROVIDER_DIRECTORY_PATH,
                               source_table_schemas,
                               OUTPUT_TABLE_NAMES_TO_SCHEMA,
                               PROVIDER_PARTITION_COLUMN,
                               DATE_PARTITION_COLUMN,
                               end_to_end_test=True)

PROD_DRIVER = MarketplaceDriver(PROVIDER_NAME,
                                DATA_TYPE,
                                INPUT_DATE,
                                SCRIPT_PATH,
                                PROVIDER_DIRECTORY_PATH,
                                source_table_schemas,
                                OUTPUT_TABLE_NAMES_TO_SCHEMA,
                                PROVIDER_PARTITION_COLUMN,
                                DATE_PARTITION_COLUMN)


def test_default_paths_templates():
    """
    Ensure that all the various templates are set correctly
    """
    assert TEST_DRIVER.input_path == \
           './test/marketplace/resources/records/'
    assert TEST_DRIVER.matching_path == \
           './test/marketplace/resources/matching/'
    assert TEST_DRIVER.output_path == \
           './test/marketplace/resources/output/'

    assert E2E_DRIVER.input_path == \
        's3://salusv/testing/dewey/airflow/e2e/TEST/TEST_CLAIMS/2019/01/02/records/'
    assert E2E_DRIVER.matching_path == \
        's3://salusv/testing/dewey/airflow/e2e/TEST/TEST_CLAIMS/2019/01/02/matching/'
    assert E2E_DRIVER.output_path == \
        's3://salusv/testing/dewey/airflow/e2e/TEST/TEST_CLAIMS/2019/01/02/output/'

    assert PROD_DRIVER.input_path == \
        's3://salusv/incoming/TEST_CLAIMS/TEST/2019/01/02/'
    assert PROD_DRIVER.matching_path == \
        's3://salusv/input/payload/TEST_CLAIMS/TEST/2019/01/02/'
    assert PROD_DRIVER.output_path == \
        's3://salusv/warehouse/parquet/TEST_CLAIMS/'


def test_load():
    """
    Ensure that a table is created from the files in the
    matching_path directory
    """
    TEST_DRIVER.init_spark_context()
    TEST_DRIVER.load()
    matching_tbl = TEST_DRIVER.spark.table('test_claims')
    assert len(matching_tbl.collect()) == 10
    assert 'hvid' in matching_tbl.columns
    assert 'claimID' in matching_tbl.columns


def test_transform():
    """
    Ensure that the matching_payload table is transformed into one of hvid-rowid
    pairs. hvids should be obfuscated
    """
    TEST_DRIVER.spark.sql("SELECT '1' as hvid, '2' as claimId")\
        .createOrReplaceTempView('matching_payload')

    TEST_DRIVER.transform()

    results = TEST_DRIVER.spark.sql("SELECT * from output_table").collect()

    print(results)
    assert results[0]['hvid'] == '0'
    assert results[0]['claimID'] == '999'
    assert len(results) == 1
