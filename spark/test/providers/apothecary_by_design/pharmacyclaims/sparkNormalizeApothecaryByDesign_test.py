import datetime
import pytest

from pyspark.sql import Row
import spark.providers.apothecary_by_design.pharmacyclaims.sparkNormalizeApothecaryByDesign as abd

results = None

def cleanup(spark):
    spark['sqlContext'].dropTempTable('pharmacyclaims_common_model')
    spark['sqlContext'].dropTempTable('abd_transaction')
    spark['sqlContext'].dropTempTable('abd_additional_data')
    spark['sqlContext'].dropTempTable('ref_gen_ref')


@pytest.mark.usefixtures('spark')
def test_init(spark):
    spark['spark'].sparkContext.parallelize([
        Row(
            hvm_vdr_feed_id = '45',
            gen_ref_domn_nm = 'EARLIEST_VALID_SERVICE_DATE',
            gen_ref_itm_nm = '',
            gen_ref_1_dt = datetime.date(1901, 1, 1),
            whtlst_flg = ''
        )
    ]).toDF().createOrReplaceTempView('ref_gen_ref')

    abd.run(spark['spark'], spark['runner'], '2017-10-06', test = True)
    global results
    results = spark['sqlContext'] \
                .sql('select * from pharmacyclaims_common_model') \
                .collect()


def test_duplicates_removed():
    unique_claim_ids = set([row for row in results if row.claim_id])
    assert len(unique_claim_ids) == len(results)


def test_hardcode_values():
    for r in results:
        assert r.model_version == '6'
        assert r.data_feed == '45'
        assert r.data_vendor == '204'


def test_hypens_removed_from_ndc_codes():
    for r in results:
        assert '-' not in r.ndc_code


def test_compound_code_is_1_or_2_or_0():
    for r in results:
        assert r.compound_code in ['0', '1', '2']


def test_states_are_all_upper_case():
    for r in results:
        assert r.patient_state.isupper()


def test_cleanup(spark):
    cleanup(spark)
