import datetime
import pytest

import spark.providers.apothecary_by_design.pharmacyclaims.sparkNormalizeApothecaryByDesign as abd

results = None

def cleanup(spark):
    spark['sqlContext'].dropTempTable('pharmacyclaims_common_model')
    spark['sqlContext'].dropTempTable('abd_transaction')
    spark['sqlContext'].dropTempTable('abd_additional_data')


@pytest.mark.usefixturesf('spark')
def test_init(spark):
    abd.run(spark['spark'], spark['runner'], '2017-10-06', test = True)
    global results
    results = spark['sqlContext'] \
                .sql('select * from pharmacyclaims_common_model') \
                .collect()


def test_duplicates_removed():
    unique_claim_ids = set(filter(lambda row: row.claim_id, results))
    assert len(unique_claim_ids) == len(results)


def test_hardcode_values():
    for r in results:
        assert r.model_version == '3'
        assert r.data_feed == '45'
        assert r.data_vendor == '204'


def test_hypens_removed_from_ndc_codes():
    for r in results:
        assert '-' not in r.ndc_code


def test_compound_code_is_1_or_2_or_0():
    for r in results:
        assert r.compound_code in ['0', '1', '2']
        

