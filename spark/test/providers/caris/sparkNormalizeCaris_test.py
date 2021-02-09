import pytest

import datetime

import spark.providers.caris.labtests.normalize as caris

legacy_results = []
results = []


def cleanup(spark):
    spark['sqlContext'].dropTempTable('lab_common_model')
    spark['sqlContext'].dropTempTable('raw_transactional')


@pytest.mark.usefixtures("spark")
def test_init(spark):
    # new model run
    caris.run(spark['spark'], spark['runner'], '2017-04-01', True)
    global legacy_results
    legacy_results = spark['sqlContext'].sql('select * from lab_common_model') \
                                        .collect()

    cleanup(spark)

    caris.run(spark['spark'], spark['runner'], '2017-05-01', True)
    global results
    results = spark['sqlContext'].sql('select * from lab_common_model') \
                                 .collect()


def test_legacy_date_parsing():
    assert [r for r in legacy_results if r.claim_id == 'patient-6_ods-6'][0] \
        .date_service == datetime.date(2005, 4, 20)
    assert [r for r in legacy_results if r.claim_id == 'patient-7_ods-7'][0] \
        .date_service == datetime.date(2017, 3, 17)


def test_date_parsing():
    assert [r for r in results if r.claim_id == 'patient-0_deid-0'][0] \
        .date_service == datetime.date(2017, 3, 25)


def test_labtests_translated():
    assert len([
        r for r in results
        if r.claim_id == 'patient-1_deid-1' and r.test_ordered_name == 'IHC_ERCC1'
    ]) == 1


def test_ngs_offering():
    assert len([
        r for r in results
        if r.claim_id == 'patient-0_deid-0' and r.test_ordered_name == 'NGS_OFFERING_X'
    ]) == 1
    assert len([
        r for r in results
        if r.claim_id == 'patient-6_deid-6' and r.test_ordered_name == 'NGS_OFFERING_A'
    ]) == 1


def test_cleanup(spark):
    cleanup(spark)
