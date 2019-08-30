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


# 10 rows in the source
# 1 row with both patient and claim counts as 0
# 1 row with patient count 0
# 1 row with claim count 0
# Expected: 9 rows in target
def test_rows_with_zero_claims_and_patients_are_dropped():
    assert len(results) == 9


def test_procedure_code_is_not_null():
    for r in results:
        assert r.procedure_code is not None


def test_bad_npi_code_is_null_in_target():
    bad_row = [x for x in results if x.procedure_code == '0001E'][0]
    assert bad_row.prov_rendering_npi is None


def test_prov_rendering_npi_is_not_null():
    for r in [x for x in results if x.procedure_code != '0001E']:
        assert r.prov_rendering_npi is not None


def test_cleanup(spark):
    cleanup(spark)
