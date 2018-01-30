import pytest

import spark.helpers.payload_loader as payload_loader
import spark.helpers.file_utils as file_utils


def cleanup(spark):
    spark['sqlContext'].sql('DROP TABLE IF EXISTS test')


@pytest.mark.usefixtures("spark")
def test_no_hvid_columns(spark):
    """
    Test if hvid column exists with null values
    """
    cleanup(spark)
    no_hvid_location = file_utils.get_abs_path(
        __file__, '../resources/no_id_test_payload.json'
    )

    payload_loader.load(spark['runner'], no_hvid_location)

    spark['sqlContext'].sql(
        'create table test as select * from matching_payload'
    ).collect()

    null_hvid_count = spark['sqlContext'].sql('SELECT hvid FROM test').collect()  # ensure hvid column exists

    row_count = spark['sqlContext'].sql('SELECT COUNT(*) FROM TEST').head()[0]

    assert row_count == len(null_hvid_count)  # test that there are null hvids in every row


def test_extra_cols(spark):
    """
    Test that extra columns are in final payload
    """
    cleanup(spark)
    extra_cols = ['claimId', 'hvJoinKey']
    std_location = file_utils.get_abs_path(
        __file__, '../resources/parentId_test_payload.json'
    )
    payload_loader.load(spark['runner'], std_location, extra_cols)
    spark['sqlContext'].sql(
        'create table test as select * from matching_payload'
    ).collect()

    spark['sqlContext'].sql('SELECT claimId, hvJoinKey from test')


def test_correct_hvid_used(spark):
    cleanup(spark)
    std_location = file_utils.get_abs_path(
        __file__, '../resources/parentId_test_payload.json'
    )
    payload_loader.load(spark['runner'], std_location)
    spark['sqlContext'].sql(
        'create table test as select * from matching_payload'
    ).collect()

    parentId_rows = spark['sqlContext'].sql('SELECT hvid FROM test WHERE hvid ="999"').collect()
    assert len(parentId_rows) == 4  # test that parentId is aliased as hvid where present


def test_table_dropped(spark):
    pass
