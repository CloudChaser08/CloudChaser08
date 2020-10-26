import pytest

import datetime
import shutil
import logging

import spark.providers.mckesson_res.pharmacyclaims.normalize as mckesson
import spark.helpers.file_utils as file_utils

from pyspark.sql import Row

restricted_results = []


def cleanup(spark):
    spark['sqlContext'].dropTempTable('mckesson_res_pharmacyclaims')
    spark['sqlContext'].dropTempTable('txn')

    try:
        shutil.rmtree(file_utils.get_abs_path(__file__, './resources/output/'))
    except:
        logging.warning('No output directory.')


@pytest.mark.usefixtures("spark")
def test_init(spark):
    cleanup(spark)
    spark['spark'].sparkContext.parallelize([
        Row(
            hvm_vdr_feed_id='36',
            gen_ref_domn_nm='EARLIEST_VALID_SERVICE_DATE',
            gen_ref_cd='',
            whtlst_flg='',
            gen_ref_1_dt=datetime.date(2010, 3, 1),
            gen_ref_2_dt=''
        ),
        Row(
            hvm_vdr_feed_id='36',
            gen_ref_domn_nm='HVM_AVAILABLE_HISTORY_START_DATE',
            gen_ref_cd='',
            whtlst_flg='',
            gen_ref_1_dt=datetime.date(2016, 12, 31),
            gen_ref_2_dt=''
        )
    ]).toDF().createOrReplaceTempView('ref_gen_ref')
    
    mckesson.run(spark_in=spark, date_input='2016-12-31', test=True)
    global restricted_results
 
    restricted_results = spark['sqlContext'].sql('select * from mckesson_res_pharmacyclaims') \
                                            .collect()


def test_date_parsing():
    "Ensure that dates are correctly parsed"
    sample_row = [r for r in restricted_results if r.claim_id == 'prescription-key-0'][0]

    assert sample_row.date_service == datetime.date(2011, 1, 30)
    assert sample_row.date_authorized == datetime.date(2011, 1, 30)
    assert sample_row.date_authorized == datetime.date(2011, 1, 30)


def test_transaction_code_vendor():
    "Ensure that dates are correctly parsed"
    sample_row_orig = [r for r in restricted_results if r.claim_id == 'prescription-key-1'][0]
    sample_row_rebill = [r for r in restricted_results if r.claim_id == 'prescription-key-5'][0]

    assert sample_row_orig.transaction_code_vendor == 'Original'
    assert sample_row_rebill.transaction_code_vendor == 'Rebilled'


def test_claim_rejected():
    
    claim_rejected = [
        'prescription-key-6', 'prescription-key-7',
        'prescription-key-8', 'prescription-key-9',
        'res-prescription-key-10', 'res-prescription-key-11',
        
    ]

    claim_not_rejected = [
        'prescription-key-0', 'prescription-key-1',
        'prescription-key-2', 'prescription-key-3',
        'prescription-key-4', 'prescription-key-5'
    ]

    claim_other = []

    for res in restricted_results:
        if res.claim_id in claim_rejected:
            assert res.logical_delete_reason == 'Claim Rejected'
        elif res.claim_id in claim_not_rejected:
            assert res.logical_delete_reason != 'Claim Rejected'
        elif res.claim_id not in claim_other:
            raise Exception(f"Claim id not in rejected or not rejected groups: {res.claim_id}")


def test_ndc_codes_populated():
    for r in restricted_results:
        assert r.ndc_code is not None


def test_restricted_count():
    assert len(restricted_results) == 12
    assert [r.claim_id for r in restricted_results] == ['prescription-key-0', 'prescription-key-1', 'prescription-key-2',
                                                        'prescription-key-3', 'prescription-key-4', 'prescription-key-5',
                                                        'prescription-key-6', 'prescription-key-7', 'prescription-key-8',
                                                        'prescription-key-9', 'res-prescription-key-10', 'res-prescription-key-11']


def test_prescription_number_hash():
    # assert that prescription-key-5 had the correct rx_number
    # MD5(PRESCRIPTIONNUMBER) == '2eef6c6aa75adcbd0c2df418c5838d91'
    assert [r for r in restricted_results if r.claim_id == 'prescription-key-5'][0].rx_number == '2eef6c6aa75adcbd0c2df418c5838d91'

    # assert that all of the other rx_number values are null
    for r in [r for r in restricted_results if r.claim_id != 'prescription-key-5']:
        assert not r.rx_number


def test_no_empty_hvjoinkeys():
    # prescription-key-12 has a newline in it
    assert len([r for r in restricted_results if r.claim_id == 'prescription-key-12' or r.claim_id == 'line']) == 0


def test_cleanup(spark):
    cleanup(spark)
