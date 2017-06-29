import pytest

import datetime
import shutil
import os
import logging

import spark.providers.mckesson.pharmacyclaims.sparkNormalizeMcKessonRx as mckesson
import spark.helpers.file_utils as file_utils

unrestricted_results = []
restricted_results = []


def cleanup(spark):
    spark['sqlContext'].dropTempTable('unrestricted_pharmacyclaims_common_model')
    spark['sqlContext'].dropTempTable('unrestricted_transactions')
    spark['sqlContext'].dropTempTable('restricted_pharmacyclaims_common_model')
    spark['sqlContext'].dropTempTable('restricted_transactions')

    try:
        shutil.rmtree(file_utils.get_abs_path(__file__, './resources/output/'))
    except:
        logging.warn('No output directory.')


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


def test_unrestricted_count():
    assert len(unrestricted_results) == 10
    assert map(lambda r: r.claim_id, unrestricted_results) == ['prescription-key-0', 'prescription-key-1', 'prescription-key-2',
                                                               'prescription-key-3', 'prescription-key-4', 'prescription-key-5',
                                                               'prescription-key-6', 'prescription-key-7', 'prescription-key-8',
                                                               'prescription-key-9']


def test_restricted_count():
    assert len(restricted_results) == 12
    assert map(lambda r: r.claim_id, restricted_results) == ['prescription-key-0', 'prescription-key-1', 'prescription-key-2',
                                                             'prescription-key-3', 'prescription-key-4', 'prescription-key-5',
                                                             'prescription-key-6', 'prescription-key-7', 'prescription-key-8',
                                                             'prescription-key-9', 'res-prescription-key-10', 'res-prescription-key-11']


def test_output():
    # ensure both provider dirs are created (filtering out hive staging dirs)
    assert filter(lambda x: not x.startswith('.hive-staging'), os.listdir(file_utils.get_abs_path(__file__, './resources/output/'))) \
        == ['part_provider=mckesson', 'part_provider=mckesson_res']


# After this point, the environment created by running the script in
# 'both' mode will be replaced by the other modes
def test_unrestricted_mode(spark):
    cleanup(spark)
    mckesson.run(spark['spark'], spark['runner'], '2016-12-31', 'unrestricted', True)
    new_results = spark['sqlContext'].sql('select * from unrestricted_pharmacyclaims_common_model').collect()
    for field in unrestricted_results[0].asDict().keys():
        if field != 'record_id':
            assert map(lambda res: res[field], unrestricted_results) == \
                map(lambda res: res[field], new_results)


def test_restricted_mode(spark):
    cleanup(spark)
    mckesson.run(spark['spark'], spark['runner'], '2016-12-31', 'restricted', True)
    new_results = spark['sqlContext'].sql('select * from restricted_pharmacyclaims_common_model').collect()
    for field in restricted_results[0].asDict().keys():
        if field != 'record_id':
            assert map(lambda res: res[field], restricted_results) == \
                map(lambda res: res[field], new_results)


def test_cleanup(spark):
    cleanup(spark)
