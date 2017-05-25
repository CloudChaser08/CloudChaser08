import pytest

import datetime

import spark.providers.mckesson.pharmacyclaims.sparkNormalizeMcKessonRx as mckesson

results = []


def cleanup(spark):
    spark['sqlContext'].dropTempTable('pharmacyclaims_common_model')


@pytest.mark.usefixtures("spark")
def test_init(spark):
    cleanup(spark)
    mckesson.run(spark['spark'], spark['runner'], '2016-12-31', True)
    global results
    results = spark['sqlContext'].sql('select * from pharmacyclaims_common_model') \
                                 .collect()


def test_date_parsing():
    "Ensure that dates are correctly parsed"
    sample_row = filter(lambda r: r.claim_id == 'prescription-key-0', results)[0]

    assert sample_row.date_service == datetime.date(2011, 1, 30)
    assert sample_row.date_authorized == datetime.date(2011, 1, 30)
    assert sample_row.date_authorized == datetime.date(2011, 1, 30)
    assert sample_row.time_authorized == datetime.datetime(2011, 1, 30, 15, 34, 52)


def test_transaction_code_vendor():
    "Ensure that dates are correctly parsed"
    sample_row_orig = filter(lambda r: r.claim_id == 'prescription-key-1', results)[0]
    sample_row_rebill = filter(lambda r: r.claim_id == 'prescription-key-5', results)[0]

    assert sample_row_orig.transaction_code_vendor == 'Original'
    assert sample_row_rebill.transaction_code_vendor == 'Rebilled'


def test_claim_rejected():
    for k in [
            'prescription-key-6', 'prescription-key-7',
            'prescription-key-8', 'prescription-key-9'
    ]:
        assert filter(lambda r: r.claim_id == k, results)[0] \
            .logical_delete_reason == 'Claim Rejected'

    for k in [
            'prescription-key-0', 'prescription-key-1',
            'prescription-key-2', 'prescription-key-3',
            'prescription-key-4'
    ]:
        assert filter(lambda r: r.claim_id == k, results)[0] \
            .logical_delete_reason != 'Claim Rejected'


def test_cleanup(spark):
    cleanup(spark)
