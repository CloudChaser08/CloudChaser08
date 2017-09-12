import pytest

import spark.helpers.payload_loader as payload_loader
import spark.helpers.file_utils as file_utils

std_payload = []
no_hvid_payload = []

def cleanup(spark):
    spark['sqlContext'].sql('DROP TABLE IF EXISTS test')

@pytest.mark.usefixtures("spark")
def test_init(spark):
    cleanup(spark)
    std_location = file_utils.get_abs_path(
        __file__, '../resources/parentId_test_payload.json'
    )

    no_hvid_location = file_utils.get_abs_path(
        __file__, '../resources/no_id_test_payload.json'
    )

    extra_cols = ['claimId', 'hvJoinKey']

    payload_loader.load(spark['runner'], std_location, extra_cols)

    global std_payload, no_hvid_payload
    std_payload = spark['sqlContext'].sql(
        'select * from matching_payload'
    ).collect()

    payload_loader.load(spark['runner'], no_hvid_location, extra_cols)

    no_hvid_payload = spark['sqlContext'].sql(
        'create table test as select * from matching_payload'
    )

