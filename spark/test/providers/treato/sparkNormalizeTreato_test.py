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

    with open(file_utils.get_abs_path(script_path, './resources/diag_mapfile.txt'), 'r') as diagnosis_mapfile:
        treato.run(
            spark['spark'], spark['runner'], '2016-01-01', diagnosis_mapfile, True
        )
        global results
        results = spark['sqlContext'].sql('select hvid, diag_cd from emr_diagnosis_common_model') \
                                     .collect()


def test_correct_output():
    "Ensure that correct rollups are applied"
    assert sorted([(row.hvid, row.diag_cd) for row in results]) == [
        ('TREATO-A0000001', 'A50A64'),
        ('TREATO-A0000001', 'HASH2'),
        ('TREATO-A0000001', 'HASH3'),
        ('TREATO-A0000002', 'A201'),
        ('TREATO-A0000002', 'HASH4'),
        ('TREATO-A0000003', 'A01A20'),
        ('TREATO-A0000003', 'HASH7'),
        ('TREATO-A0000004', 'A01B20'),
        ('TREATO-A0000004', 'HASH5'),
        ('TREATO-A0000004', 'HASH6'),
        ('TREATO-A0000005', 'C7BC7B')
    ]


def test_cleanup(spark):
    cleanup(spark)
