import pytest

import spark.helpers.payload_loader as payload_loader
import spark.helpers.file_utils as file_utils

payload = []


@pytest.mark.usefixtures("spark")
def test_init(spark):
    location = file_utils.get_rel_path(
        __file__, '../resources/parentId_test_payload.json'
    )

    extra_cols = ['claimId', 'hvJoinKey']

    payload_loader.load(spark['runner'], location, extra_cols)

    global payload
    payload = spark['sqlContext'].sql(
        'select * from matching_payload'
    ).collect()
