import pytest

import datetime as datetime
import spark.providers.pdx.pharmacyclaims.sparkNormalizePDX as pdx
import spark.helpers.file_utils as file_utils
from pyspark.sql import Row

results = []

def cleanup(spark):
    spark['sqlContext'].dropTempTable('pdx_transactions')
    spark['sqlContext'].dropTempTable('pharmacyclaims_common_model')
    spark['sqlContext'].dropTempTable('normalized_claims')
    spark['sqlContext'].dropTempTable('pharmacyclaims_common_model_final')


@pytest.mark.usefixtures('spark')
def test_init(spark):
    spark['spark'].sparkContext.parallelize([
        Row(
            hvm_vdr_feed_id='65',
            gen_ref_domn_nm='EARLIEST_VALID_SERVICE_DATE',
            gen_ref_itm_nm='',
            gen_ref_1_dt=datetime.date(1901, 1, 1),
            whtlst_flg=''
        )
    ]).toDF().createOrReplaceTempView('ref_gen_ref')

    pdx.run(spark['spark'], spark['runner'], '2018-03-20', test=True)
    global results

    results = spark['sqlContext'].sql('select * from pharmacyclaims_common_model_final').collect()


def test_row_count():
    assert len(results) == 11


def test_reversals_are_tagged_properly():
    assert len(filter(lambda x: x.logical_delete_reason == 'Reversal', results)) == 1


def test_only_one_claim_is_reversed_for_the_reversal():
    assert len(filter(lambda x: x.logical_delete_reason == 'Reversed Claim', results)) == 1


def test_cleanup(spark):
    cleanup(spark)
