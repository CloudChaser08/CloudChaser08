import pytest

from pyspark.sql.functions import col
import spark.helpers.payload_loader as payload_loader
import spark.helpers.file_utils as file_utils
import spark.test.resources.one_payload_table as one_payload_table
import spark.test.resources.two_payload_tables as two_payload_tables

script_path = __file__

std_location = file_utils.get_abs_path(
    script_path, '../resources/parentId_test_payload.json'
)

std_location_prefix = file_utils.get_abs_path(
    script_path, '../resources/'
) + '/'

no_hvid_location = file_utils.get_abs_path(
    script_path, '../resources/no_id_test_payload.json'
)

confidence_score_location = file_utils.get_abs_path(
    script_path, '../resources/confidence_score_test_payload.json'
)


@pytest.fixture(autouse=True)
def setup_teardown(spark):
    spark['sqlContext'].sql('DROP TABLE IF EXISTS matching_payload')
    spark['sqlContext'].sql('DROP TABLE IF EXISTS matching_payload_foo')
    spark['sqlContext'].sql('DROP TABLE IF EXISTS matching_payload_bar')
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


def test_confidence_scores_loaded(spark):
    payload_loader.load(spark['runner'], confidence_score_location)

    confidence_not_null = spark['sqlContext'].sql('SELECT * FROM matching_payload').where(col("topCandidatesConfidence").isNotNull()).count()

    assert confidence_not_null == 1


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

def test_load_all_one_table(spark):
    """
    Test that a payload file is loaded as a single table using load_all
    conventions
    """
    payload_loader.load_all(spark['runner'], std_location, one_payload_table)

    assert spark['sqlContext'].table('matching_payload') is not None
    assert len(spark['sqlContext'].table('matching_payload').collect()) == 10
    assert 'my_special_column' in spark['sqlContext'].table('matching_payload').columns

def test_load_all_two_tablse(spark):
    """
    Test that a payload file is loaded as multiple tables using load_all
    conventions
    """
    payload_loader.load_all(spark['runner'], std_location_prefix, two_payload_tables)

    assert spark['sqlContext'].table('matching_payload_foo') is not None
    assert len(spark['sqlContext'].table('matching_payload_foo').collect()) == 10

    assert spark['sqlContext'].table('matching_payload_bar') is not None
    assert len(spark['sqlContext'].table('matching_payload_bar').collect()) == 10
