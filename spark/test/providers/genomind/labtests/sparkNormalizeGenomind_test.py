import datetime
import pytest

from pyspark.sql import Row
import spark.providers.genomind.labtests.sparkNormalizeGenomind as gm

results = None

def cleanup(spark):
    spark['sqlContext'].dropTempTable('labtests_common_model')
    spark['sqlContext'].dropTempTable('matching_payload')
    spark['sqlContext'].dropTempTable('ref_gen_ref')

@pytest.mark.usefixtures('spark')
def test_init(spark):
    spark['spark'].sparkContext.parallelize([
        Row(
            hvm_vdr_Feed_id = gm.FEED_ID,
            gen_ref_domn_nm = 'EARLIEST_VALID_SERVICE_DATE',
            gen_ref_itm_nm = '',
            gen_ref_1_dt = datetime.date(1900, 1, 1),
            whtlst_flg = ''
        )
    ]).toDF().createOrReplaceTempView('ref_gen_ref')

    gm.run(spark['spark'], spark['runner'], '2018-06-20', test=True)
    global results
    results = spark['sqlContext'].sql('select * from labtests_common_model').collect()


def test_expected_row_count_matches():
    assert len(results) == 13


def test_expected_amount_of_ordering_npis():
    assert len(set(([x.ordering_npi for x in results]))) == 5


def test_expected_amount_of_medications():
    assert len(set([x.test_ordered_name for x in [x for x in results if x.hvid == '113114247']])) == 4


def test_cleanup(spark):
    cleanup(spark)
