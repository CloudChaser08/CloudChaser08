import datetime
import pytest

from pyspark.sql import Row
import spark.providers.genoa.pharmacyclaims.sparkNormalizeGenoaRX as genoa

def cleanup(spark):
    spark['sqlContext'].dropTempTable('pharmacyclaims_common_model')
    spark['sqlContext'].dropTempTable('genoa_rx_raw')
    spark['sqlContext'].dropTempTable('ref_gen_ref')


@pytest.fixture(scope='module')
@pytest.mark.usefixtures('spark')
def normalization_results(spark):
    spark['spark'].sparkContext.parallelize([
        Row(
            hvm_vdr_feed_id = '21',
            gen_ref_domn_nm = 'EARLIEST_VALID_SERVICE_DATE',
            gen_ref_itm_nm = '',
            gen_ref_1_dt = datetime.date(1901, 1, 1),
            whtlst_flg = '' 
        )
    ]).toDF().createOrReplaceTempView('ref_gen_ref')

    genoa.run(spark['spark'], spark['runner'], '2017-11-01', test = True)
    return (
                spark['sqlContext']
                    .sql('select * from pharmacyclaims_common_model')
                    .collect()
           )

@pytest.mark.usefixtures('normalization_results')
def test_hardcode_values(normalization_results):
    for r in normalization_results:
        assert r.model_version == '06'
        assert r.data_feed == '21'
        assert r.data_vendor == '20'
        assert r.data_set in {"HealthVerity_DeID_Payload_20171101_110404.txt",
                              "HealthVerity_DeID_Payload_20150201_110404.txt"}

@pytest.mark.usefixtures('normalization_results')
def test_new_fields_mapped(normalization_results):
    for r in normalization_results:
        if r.data_set == "HealthVerity_DeID_Payload_20171101_110404.txt":
            assert '50' in r.claim_id
            assert r.transaction_code_std == 'B1'
            assert r.response_code_std == 'P'

@pytest.mark.usefixtures('normalization_results')
def test_new_fields_exist_null(normalization_results):
    for r in normalization_results:
        if r.data_set == "HealthVerity_DeID_Payload_20150201_110404.txt":
            assert r.claim_id is None
            assert r.transaction_code_std is None
            assert r.response_code_std is None

@pytest.mark.usefixtures('normalization_results')
def test_bad_data_deleted(normalization_results):
    # Genoa sent a restatement of all the data between 2015-05-31 and 2017-11-01
    # Any batches between those dates should have data removed if it falls in
    # the restated range
    bad_results = [
        r for r in normalization_results
        if r.data_set == "HealthVerity_DeID_Payload_20160501_110404.txt"
    ]
    len(bad_results) == 0

@pytest.mark.usefixtures('normalization_results')
def test_duplicate_data_removed(normalization_results):
    # All the data in the 2017-12-01 batch also exists in the 2017-11-01 batch,
    # so nothing of the 2017-12-01 batch will remain
    duplicate_results = [
        r for r in normalization_results
        if r.data_set == "HealthVerity_DeID_Payload_20171201_110404.txt"
    ]
    len(duplicate_results) == 0
