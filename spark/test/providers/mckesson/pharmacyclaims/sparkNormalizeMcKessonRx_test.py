import pytest

import datetime

import spark.providers.mckesson.pharmacyclaims.sparkNormalizeMcKessonRx as mckesson

unrestricted_results = []
restricted_results = []


def cleanup(spark):
    spark['sqlContext'].dropTempTable('unrestricted_pharmacyclaims_common_model')
    spark['sqlContext'].dropTempTable('unrestricted_transactions')
    spark['sqlContext'].dropTempTable('restricted_pharmacyclaims_common_model')
    spark['sqlContext'].dropTempTable('restricted_transactions')


@pytest.mark.usefixtures("spark")
def test_init(spark):
    cleanup(spark)
    mckesson.run(spark['spark'], spark['runner'], '2016-12-31', 'both', True)
    global unrestricted_results, restricted_results
    unrestricted_results = spark['sqlContext'].sql('select * from unrestricted_pharmacyclaims_common_model') \
                                              .collect()
    restricted_results = spark['sqlContext'].sql('select * from restricted_pharmacyclaims_common_model') \
                                            .collect()


def test_date_parsing():
    "Ensure that dates are correctly parsed"
    sample_row = filter(lambda r: r.claim_id == 'prescription-key-0', unrestricted_results)[0]

    assert sample_row.date_service == datetime.date(2011, 1, 30)
    assert sample_row.date_authorized == datetime.date(2011, 1, 30)
    assert sample_row.date_authorized == datetime.date(2011, 1, 30)


def test_transaction_code_vendor():
    "Ensure that dates are correctly parsed"
    sample_row_orig = filter(lambda r: r.claim_id == 'prescription-key-1', unrestricted_results)[0]
    sample_row_rebill = filter(lambda r: r.claim_id == 'prescription-key-5', unrestricted_results)[0]

    assert sample_row_orig.transaction_code_vendor == 'Original'
    assert sample_row_rebill.transaction_code_vendor == 'Rebilled'


def test_claim_rejected():
    for k in [
            'prescription-key-6', 'prescription-key-7',
            'prescription-key-8', 'prescription-key-9'
    ]:
        assert filter(lambda r: r.claim_id == k, unrestricted_results)[0] \
            .logical_delete_reason == 'Claim Rejected'

    for k in [
            'prescription-key-0', 'prescription-key-1',
            'prescription-key-2', 'prescription-key-3',
            'prescription-key-4'
    ]:
        assert filter(lambda r: r.claim_id == k, unrestricted_results)[0] \
            .logical_delete_reason != 'Claim Rejected'


def test_ndc_codes_populated():
    for r in unrestricted_results:
        assert r.ndc_code is not None


def test_no_restricted_in_unrestricted():
    assert len(unrestricted_results) == 10
    assert map(lambda r: r.claim_id, unrestricted_results) == ['prescription-key-0', 'prescription-key-1', 'prescription-key-2',
                                                               'prescription-key-3', 'prescription-key-4', 'prescription-key-5',
                                                               'prescription-key-6', 'prescription-key-7', 'prescription-key-8',
                                                               'prescription-key-9']


def test_unrestricted_removed_from_restricted():
    assert len(restricted_results) == 2
    assert map(lambda r: r.claim_id, restricted_results) == ['res-prescription-key-10', 'res-prescription-key-11']


def test_unrestricted_mode(spark):
    cleanup(spark)
    mckesson.run(spark['spark'], spark['runner'], '2016-12-31', 'unrestricted', True)
    assert unrestricted_results == spark['sqlContext'].sql('select * from unrestricted_pharmacyclaims_common_model') \
                                                      .collect()


def test_restricted_mode(spark):
    cleanup(spark)
    mckesson.run(spark['spark'], spark['runner'], '2016-12-31', 'restricted', True)
    assert restricted_results == spark['sqlContext'].sql('select * from restricted_pharmacyclaims_common_model') \
                                                    .collect()


def test_cleanup(spark):
    cleanup(spark)
