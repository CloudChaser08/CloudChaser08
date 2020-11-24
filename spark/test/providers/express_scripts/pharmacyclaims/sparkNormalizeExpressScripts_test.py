import pytest
from datetime import datetime
import spark.providers.express_scripts.pharmacyclaims.normalize as esi

results_reversal = []
results = []
transactions = []
matching = []
normalized_claims = []
table = []


def cleanup(spark):
    spark['sqlContext'].dropTempTable('esi_06_norm_final')


@pytest.mark.usefixtures("spark")
def test_init(spark):
    cleanup(spark)

    esi.run('2017-05-06', first_run=False, reversal_apply_hist_months=2
            , end_to_end_test=False, test=True, spark=spark['spark'], runner=spark['runner'])

    global results_reversal, results, transactions, matching, normalized_claims
    transactions = spark['sqlContext'].sql('select * from transaction').collect()
    matching = spark['sqlContext'].sql('select * from matching_payload').collect()
    normalized_claims = spark['sqlContext'].sql('select * from esi_03_norm_cf').collect()
    results = spark['sqlContext'].sql("select * from esi_06_norm_final").collect()
    results_reversal = spark['sqlContext'].sql("select * from esi_06_norm_final "
                                               "where logical_delete_reason != '' ").collect()


def text_fixed_width_parsing():
    """Check that that fixed width transaction data was parsed correctly"""
    row = [r for r in transactions if r.pharmacy_claim_id == '__________________________________________10'][0]
    assert row.creation_date == '20170502'
    assert row.patient_gender_code == 'F'
    assert row.hv_join_key == '0262b769-2a33-4d84-8532-2c832bf33528'


def test_date_of_service_parsing():
    """Check that date_of_service is parsed correctly"""
    row = [r for r in results if r.claim_id == '___________________________________________9'][0]
    # assert row.date_service.strftime('%Y-%m-%d') == datetime.date(2017, 4, 30)
    assert row.date_service == '2017-04-30'


def test_union_of_old_results_and_new():
    """Check that the final output table contains all the records from the latest
    as well as some records from previously normalized batches (1 additional
    record in this case)"""
    assert len(transactions) == 10
    assert len(transactions) <= len(results)
    assert len(results) == 12


def test_normalized_claims_counts():
    """Check that the normalized output table counts should match with transactions table counts"""
    assert len([r for r in normalized_claims]) == len([r for r in transactions])


def test_reversed_claims_logical_deleted():
    """Check that the final output table contain claims that were reversed"""
    assert len([r for r in results if r.claim_id == '__________________________________________15']) == 1
    assert len([r for r in results if r.claim_id != '__________________________________________15']) == 11


def test_reversed_claims_counts():
    """Check that the final output table contain counts of claims that were reversed"""
    assert len([r for r in results_reversal]) == 2


def test_cleanup(spark):
    cleanup(spark)
