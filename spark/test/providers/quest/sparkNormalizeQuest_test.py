import pytest

import spark.providers.quest.sparkNormalizeQuest as quest
import spark.helpers.file_utils as file_utils


@pytest.mark.usefixtures("spark")
def test_init(spark):
    quest.run(spark['spark'], spark['runner'], '2016-12-31',
              file_utils.get_rel_path(__file__, 'resources/output/'), True)


@pytest.mark.usefixtures("spark")
def test_diag_explosion(spark):
    diags = spark['sqlContext'].sql(
        'select distinct diagnosis_code '
        + 'from lab_common_model '
        + 'where claim_id like "2073344012%"'
    ).collect()

    assert diags == ['DIAG1', 'DIAG4', 'DIAG6']
