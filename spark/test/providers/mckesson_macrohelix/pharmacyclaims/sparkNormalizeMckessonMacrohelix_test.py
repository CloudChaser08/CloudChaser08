import datetime
import pytest

from pyspark.sql import Row
import spark.providers.mckesson_macrohelix.pharmacy_claims.sparkNormalizeMckessonMacrohelix as mmh

source = None
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
    global source, results
    source = spark['sqlContext'] \
                .sql('select * from mckesson_macrohelix_transactions') \
                .collect()
    results = spark['sqlContext'] \
                .sql('select * from pharmacyclaims_common_model') \
                .collect()


# 4 rows with 11 diagnoses + 6 rows with 13 diagnoses = 122 rows in results
def test_results_exploded_properly():
    assert len(results) == 122


# Each row in source has a blacklisted icd9/10 code, check they were nulled out
def test_exploded_diagnosis_codes_nulled_out():
    assert len([r for r in results if r.diagnosis_code is None]) == 10


def test_model_version_inserted_for_each_row():
    for r in results:
        assert r.model_version == '4'


def test_cleanup(spark):
    cleanup(spark)


