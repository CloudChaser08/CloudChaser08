import pytest

import spark.helpers.file_utils as file_utils
import spark.providers.treato.sparkNormalizeTreato as treato

results = []


def cleanup(spark):
    spark['sqlContext'].sql('DROP TABLE IF EXISTS emr_diagnosis_common_model')


@pytest.mark.usefixtures("spark")
def test_init(spark):
    cleanup(spark)

    script_path = __file__

    treato.run(
        spark['spark'], spark['runner'], '2016-01-01',
        file_utils.get_abs_path(script_path, './resources/diag_mapfile.txt'), True
    )
    global results
    results = spark['sqlContext'].sql('select hvid, diag_cd from emr_diagnosis_common_model') \
                                 .collect()


def test_correct_output():
    "Ensure that year of birth capping was applied"
    assert sorted([(row.hvid, row.diag_cd) for row in results]) == sorted([
        ('A0000001', 'hash2'),
        ('A0000001', 'hash3'),
        ('A0000002', 'hash4')
    ])


def test_cleanup(spark):
    cleanup(spark)
