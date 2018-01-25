import datetime
import pytest

from pyspark.sql import Row
import spark.providers.genoa.pharmacyclaims.sparkNormalizeGenoaRX as genoa

results_old = None
results_bad = None
results_new = None

def cleanup(spark):
    spark['sqlContext'].dropTempTable('pharmacyclaims_common_model')
    spark['sqlContext'].dropTempTable('genoa_rx_raw')
    spark['sqlContext'].dropTempTable('ref_gen_ref')


@pytest.mark.usefixtures('spark')
def test_init(spark):
    spark['spark'].sparkContext.parallelize([
        Row(
            hvm_vdr_feed_id = '21',
            gen_ref_domn_nm = 'EARLIEST_VALID_SERVICE_DATE',
            gen_ref_itm_nm = '',
            gen_ref_1_dt = datetime.date(1901, 1, 1),
            whtlst_flg = '' 
        )
    ]).toDF().createOrReplaceTempView('ref_gen_ref')

    global results_old, results_new, results_bad
    genoa.run(spark['spark'], spark['runner'], '2015-02-01', test = True)
    results_old = spark['sqlContext'] \
                .sql('select * from pharmacyclaims_common_model') \
                .collect()
    genoa.run(spark['spark'], spark['runner'], '2017-11-01', test = True)
    results_new = spark['sqlContext'] \
                .sql('select * from pharmacyclaims_common_model') \
                .collect()
    # Genoa sent a restatement of all the data between 2015-05-31 and 2017-11-01
    # Any batches between those dates should have data removed if it falls in
    # the restated range
    genoa.run(spark['spark'], spark['runner'], '2016-05-01', test = True)
    results_bad = spark['sqlContext'] \
                .sql('select * from pharmacyclaims_common_model') \
                .collect()

def test_hardcode_values():
    for r in results_new:
        assert r.model_version == '04'
        assert r.data_feed == '21'
        assert r.data_vendor == '20'
        assert r.data_set == "HealthVerity_DeID_Payload_20171101_110404.txt"

    for r in results_old:
        assert r.model_version == '04'
        assert r.data_feed == '21'
        assert r.data_vendor == '20'
        assert r.data_set == "HealthVerity_DeID_Payload_20150201_110404.txt"

def test_new_fields_mapped():
    for r in results_new:
        assert '50' in r.claim_id
        assert r.transaction_code_std == 'B1'
        assert r.response_code_std == 'P'

def test_new_fields_exist_null():
    for r in results_old:
        assert r.claim_id is None
        assert r.transaction_code_std is None
        assert r.response_code_std is None

def test_bad_data_deleted():
    len(results_bad) == 0
