import datetime
import pytest

from pyspark.sql import Row
import spark.providers.mckesson_macro_helix.pharmacyclaims.normalize as mmh

source = None
results = None

def cleanup(spark):
    spark['sqlContext'].dropTempTable('pharmacyclaims_common_model')
    spark['sqlContext'].dropTempTable('mckesson_macro_helix_transactions')
    spark['sqlContext'].dropTempTable('exploder')
    spark['sqlContext'].dropTempTable('ref_gen_ref')


@pytest.mark.usefixtures('spark')
def test_init(spark):
    spark['spark'].sparkContext.parallelize([
        Row(
            hvm_vdr_feed_id = mmh.FEED_ID,
            gen_ref_domn_nm = 'EARLIEST_VALID_SERVICE_DATE',
            gen_ref_itm_nm = '',
            gen_ref_1_dt = datetime.date(1901, 1, 1),
            whtlst_flg = ''
        ),
        Row(
            hvm_vdr_feed_id = mmh.FEED_ID,
            gen_ref_domn_nm = 'HVM_AVAILABLE_HISTORY_START_DATE',
            gen_ref_itm_nm = '',
            gen_ref_1_dt = datetime.date(1901, 1, 1),
            whtlst_flg = ''
        )
    ]).toDF().createOrReplaceTempView('ref_gen_ref')

    mmh.run(spark['spark'], spark['runner'], '2017-10-06', test = True)
    global source, results
    source = spark['sqlContext'] \
                .sql('select * from mckesson_macro_helix_transactions') \
                .collect()
    results = spark['sqlContext'] \
                .sql('select * from pharmacyclaims_common_model') \
                .collect()


# 4 rows with 11 diagnoses + 6 rows with 13 diagnoses + 1 row no diagnoses = 123 rows in results
# (4 * 11)                 + (6 * 13)                 + (1 * 1)            = 123
def test_results_exploded_properly():
    assert len(results) == 123


# Each row in source has a blacklisted icd9/10 code, check they were nulled out
def test_exploded_diagnosis_codes_nulled_out():
    assert len([r for r in results if r.diagnosis_code is None and r.claim_id != '10']) == 5


def test_diagnosis_codes_with_prefix_a():
    assert sorted([r.diagnosis_code for r in results if r.claim_id == '8']) \
        == [
            '2449', '2724', '4019', '4280', '4580', '49390', '53081', '7802', '78630', '92401',
            'A41401', # this code had the '.' in the wrong place
            'E8889', 'V4582'
        ]


def test_model_version_inserted_for_each_row():
    for r in results:
        assert r.model_version == '06'


def test_cleanup(spark):
    cleanup(spark)
