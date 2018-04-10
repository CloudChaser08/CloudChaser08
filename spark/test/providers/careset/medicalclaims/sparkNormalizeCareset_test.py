import datetime
import pytest

from pyspark.sql import Row
import spark.providers.careset.medicalclaims.sparkNormalizeCareset as cs

source = None
results = None

def cleanup(spark):
    spark['sqlContext'].dropTempTable('medicalclaims_common_model')
    spark['sqlContext'].dropTempTable('careset_transactions')
    spark['sqlContext'].dropTempTable('ref_gen_ref')


@pytest.mark.usefixtures('spark')
def test_init(spark):
    spark['spark'].sparkContext.parallelize([
        Row(
            hvm_vdr_feed_id=cs.FEED_ID,
            gen_ref_domn_nm='EARLIEST_VALID_SERVICE_DATE',
            gen_ref_itm_nm='',
            gen_ref_1_dt=datetime.date(1901, 1, 1),
            whtlst_flg=''
        ),
        Row(
            hvm_vdr_feed_id=cs.FEED_ID,
            gen_ref_domn_nm='HVM_AVAILABLE_HISTORY_START_DATE',
            gen_ref_itm_nm='',
            gen_ref_1_dt=datetime.date(1901, 1, 1),
            whtlst_flg=''
        )
    ]).toDF().createOrReplaceTempView('ref_gen_ref')

    cs.run(spark['spark'], spark['runner'], '2018-04-10', test=True)
    global source, results
    source = spark['sqlContext'] \
                .sql('select * from careset_transactions') \
                .collect()
    results = spark['sqlContext'] \
                .sql('select * from medicalclaims_common_model') \
                .collect()


def test_something():
    print source
    print results


def test_cleanup(spark):
    cleanup(spark)
