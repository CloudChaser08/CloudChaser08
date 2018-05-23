import pytest
import shutil

from datetime import date

import spark.helpers.file_utils as file_utils
import spark.providers.cardinal_pms.medicalclaims.sparkNormalizeCardinalPMS as cardinal_pms

script_path = __file__
results = []


def cleanup(spark):
    spark['sqlContext'].dropTempTable('medicalclaims_common_model')
    spark['sqlContext'].dropTempTable('transactional_cardinal_pms')
    spark['sqlContext'].dropTempTable('service_line_exploder')
    spark['sqlContext'].dropTempTable('claim_exploder')

    try:
        shutil.rmtree(file_utils.get_abs_path(script_path, './resources/delivery'))
    except:
        pass


@pytest.mark.usefixtures("spark")
def test_init(spark):
    cleanup(spark)
    cardinal_pms.run(spark['spark'], spark['runner'], '1990-01-01', None, test=True)
    global results
    results = spark['sqlContext'].sql('select * from medicalclaims_cardinalized') \
                                 .collect()


def test_claim_levels_all_populated():
    assert len(filter(lambda r: r.service_line_number is None, results)) == 5


def test_service_line_levels_all_populated():
    assert len(filter(lambda r: r.service_line_number is not None, results)) == 27


def test_claim_levels_are_unique():
    claim_diags = filter(lambda r: r.service_line_number is None, results)
    unique_claim_diags = set(map(lambda r: (r.claim_id, r.diagnosis_code), claim_diags))

    assert len(claim_diags) == len(unique_claim_diags)


def test_vendor_org_id_is_populated():
    for row in results:
        assert row.vendor_org_id == 'z'


def test_claim_level_date_service():
    assert sorted(set(
        [(res.claim_id, res.date_service, res.date_service_end) for res in results if res.service_line_number is None]
    )) == [
        ('4b9a18a8-9c20-44a2-a5bb-a5c9a2635888',
         date(2017, 8, 5), date(2017, 8, 11)),
        ('58b382b1-f366-405a-ab5d-81a5056890cd',
         date(2017, 8, 1), date(2017, 8, 8))
    ]


def test_cleanup(spark):
    cleanup(spark)
