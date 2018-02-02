import pytest

from pyspark.sql.functions import col
import spark.helpers.payload_loader as payload_loader
import spark.helpers.file_utils as file_utils

std_location = file_utils.get_abs_path(
    __file__, '../resources/parentId_test_payload.json'
)

no_hvid_location = file_utils.get_abs_path(
    __file__, '../resources/no_id_test_payload.json'
)


@pytest.fixture(autouse=True)
def setup_teardown(spark):
    spark['sqlContext'].sql('DROP TABLE IF EXISTS test')
    yield


@pytest.mark.usefixtures("spark")
def test_no_hvid_columns(spark):
    """
    Test if hvid column exists despite no hvids in payload
    """
    payload_loader.load(spark['runner'], no_hvid_location)

    spark['sqlContext'].sql('SELECT hvid FROM matching_payload')


def test_hvids_are_null(spark):
    """
    Test that hvids are null when missing
    """
    row_count = spark['sqlContext'].sql('SELECT * FROM matching_payload').count()
    hvid_count = spark['sqlContext'].sql('SELECT * FROM matching_payload').where(col("hvid").isNull()).count()  # TODO: Fix this 

    assert hvid_count == row_count


def test_extra_cols(spark):
    """
    Test that extra columns are in final payload
    """
    extra_cols = ['claimId', 'hvJoinKey']

    payload_loader.load(spark['runner'], std_location, extra_cols, table_name='test')

    spark['sqlContext'].sql('SELECT claimId, hvJoinKey from test')


def test_correct_hvid_used(spark):

    payload_loader.load(spark['runner'], std_location, table_name='test')

    parentId_count = spark['sqlContext'].sql('SELECT hvid FROM test WHERE hvid ="999"').count()
    assert parentId_count == 4  # test that parentId is aliased as hvid where present


def test_custom_table_created(spark):
    payload_loader.load(spark['runner'], std_location, table_name='test')

    tables = spark['sqlContext'].sql('SHOW TABLES')
    for table in tables.collect():
        if table.tableName == "matching_payload":
            table_created = False
        if table.tableName == "test":
            table_created = True
    assert table_created
