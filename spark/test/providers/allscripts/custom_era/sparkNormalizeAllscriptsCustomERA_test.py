import pytest

import spark.providers.allscripts.custom_era.sparkNormalizeAllscriptsCustomERA as asce

svc_results = None
ts3_results = None
hdr_results = None
plb_results = None
clp_results = None
payload_results = None

def cleanup(spark):
    spark['sqlContext'].dropTempTable('svc')
    spark['sqlContext'].dropTempTable('ts3')
    spark['sqlContext'].dropTempTable('hdr')
    spark['sqlContext'].dropTempTable('plb')
    spark['sqlContext'].dropTempTable('clp')
    spark['sqlContext'].dropTempTable('payload')


@pytest.mark.usefixtures('spark')
def test_init(spark):
    asce.run(spark['spark'], spark['runner'], '2018-08-14', test=True)
    global svc_results, ts3_results, hdr_results, plb_results, clp_results, payload_results
    svc_results = spark['sqlContext'].sql('select * from svc').collect()
    ts3_results = spark['sqlContext'].sql('select * from ts3').collect()
    hdr_results = spark['sqlContext'].sql('select * from hdr').collect()
    plb_results = spark['sqlContext'].sql('select * from plb').collect()
    clp_results = spark['sqlContext'].sql('select * from clp').collect()
    payload_results = spark['sqlContext'].sql('select * from payload').collect()


def test_all_rows_read():
    assert len(svc_results) == 10
    assert len(ts3_results) == 10
    assert len(hdr_results) == 10
    assert len(plb_results) == 2
    assert len(clp_results) == 10
    assert len(payload_results) == 10
