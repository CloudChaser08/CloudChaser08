import datetime
import pytest

from pyspark.sql import Row
import spark.providers.mckesson_macrohelix.pharmacy_claims.sparkNormalizeMckessonMacrohelix as mmh

results = None

def cleanup(spark):
    spark['sqlContext'].dropTempTable('pharmacyclaims_common_model')
    spark['sqlContext'].dropTempTable('mckesson_macrohelix_transactions')
    spark['sqlContext'].dropTempTable('exploder')
    spark['sqlContext'].dropTempTable('ref_gen_ref')


@pytest.mark.usefixtures('spark')
def test_init(spark):
    spark['spark'].sparkContext.parallelize([
        Row(
            hvm_vdr_feed_id = '48',
            gen_ref_domn_nm = 'EARLIEST_VALID_SERVICE_DATE',
            gen_ref_itm_nm = '',
            gen_ref_1_dt = datetime.date(1901, 1, 1),
            whtlst_flg = '' 
        ),
        Row(
            hvm_vdr_feed_id = '48',
            gen_ref_domn_nm = 'HVM_AVAILABLE_HISTORY_START_DATE',
            gen_ref_itm_nm = '',
            gen_ref_1_dt = datetime.date(1901, 1, 1),
            whtlst_flg = '' 
        )
    ]).toDF().createOrReplaceTempView('ref_gen_ref')

    mmh.run(spark['spark'], spark['runner'], '2017-10-06', test = True)
    global results
    results = spark['sqlContext'] \
                .sql('select * from pharmacyclaims_common_model') \
                .collect()


def test_something():
    print results


def test_cleanup(spark):
    cleanup(spark)


