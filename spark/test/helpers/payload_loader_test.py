import pytest

from pyspark.sql.functions import col
import spark.helpers.payload_loader as payload_loader
import spark.helpers.file_utils as file_utils

script_path = __file__

std_location = file_utils.get_abs_path(
    script_path, '../resources/parentId_test_payload.json'
)

no_hvid_location = file_utils.get_abs_path(
    script_path, '../resources/no_id_test_payload.json'
)


@pytest.fixture(autouse=True)
def setup_teardown(spark):
    spark['sqlContext'].sql('DROP TABLE IF EXISTS matching_payload')
    spark['sqlContext'].sql('DROP TABLE IF EXISTS test')
    yield


@pytest.mark.usefixtures("spark")
def test_no_hvid_columns(spark):
    """
    Test if hvid column exists despite no hvids in payload
    """
    payload_loader.load(spark['runner'], no_hvid_location)

    assert 'hvid' in spark['sqlContext'].sql('SELECT * FROM matching_payload').columns


def test_hvids_are_null(spark):
    """
    Test that hvids are null when missing
    """
    payload_loader.load(spark['runner'], no_hvid_location)

    hvid_count_not_null = spark['sqlContext'].sql('SELECT * FROM matching_payload').where(col("hvid").isNotNull()).count()

    assert hvid_count_not_null == 0


def test_extra_cols(spark):
    """
    Test that extra columns are in final payload
    """
    extra_cols = ['claimId', 'hvJoinKey']

    payload_loader.load(spark['runner'], std_location, extra_cols, table_name='test')

    assert 'claimId' in spark['sqlContext'].sql('SELECT * FROM test').columns
    assert 'hvJoinKey' in spark['sqlContext'].sql('SELECT * FROM test').columns


def test_correct_hvid_used(spark):

    payload_loader.load(spark['runner'], std_location, table_name='test')

    parentId_count = spark['sqlContext'].sql('SELECT * FROM test').where(col("hvid") == "999").count()
    assert parentId_count == 4  # test that parentId is aliased as hvid where present


def test_return_output(spark):
    output = payload_loader.load(spark['runner'], std_location, return_output=True)
    payload_loader.load(spark['runner'], std_location, table_name='compare_to_output')

    assert output
    assert output.collect() == spark['sqlContext'].sql('select * from compare_to_output').collect()


def test_custom_table_created(spark):
    payload_loader.load(spark['runner'], std_location, table_name='test')

    tables = spark['sqlContext'].sql('SHOW TABLES')
    for table in tables.collect():
        if table.tableName == "matching_payload":
            table_created = False
        if table.tableName == "test":
            table_created = True
    assert table_created

def test_file_name_loaded(spark):
    """
    Test that the file name(s) appear in an 'input_file_name' column in the
    dataframe
    """
    extra_cols = ['claimId', 'hvJoinKey']

    payload_loader.load(spark['runner'], std_location, extra_cols, table_name='test', load_file_name=True)

    assert 'input_file_name' in spark['sqlContext'].table('test').columns
    assert spark['sqlContext'].table('test').collect()[0].input_file_name.endswith('parentId_test_payload.json')
