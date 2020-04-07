import pytest
import datetime
import spark.providers.express_scripts.pharmacyclaims.sparkNormalizeExpressScripts as esi

results = []
transactions = []
matching = []
normalized_claims = []
table = []

def cleanup(spark):
    spark['sqlContext'].dropTempTable('pharmacyclaims_common_model_final')

@pytest.mark.usefixtures("spark")
def test_init(spark):
    cleanup(spark)

    esi.run(spark['spark'], spark['runner'], '2017-05-06', True)

    global results, transactions, matching, normalized_claims
    transactions = spark['sqlContext'].sql('select * from transactions').collect()
    matching = spark['sqlContext'].sql('select * from matching_payload').collect()
    normalized_claims = spark['sqlContext'].sql('select * from normalized_claims').collect()
    results = spark['sqlContext'].sql('select * from pharmacyclaims_common_model_final') \
                                 .collect()

def text_fixed_width_parsing():
    "Check that that fixed width transaction data was parsed correctly"
    row = [r for r in transactions if r.pharmacy_claim_id == '__________________________________________10'][0]
    assert row.creation_date == '20170502'
    assert row.patient_gender_code == 'F'
    assert row.hv_join_key == '0262b769-2a33-4d84-8532-2c832bf33528'

def test_date_of_service_parsing():
    "Check that date_of_service is parsed correctly"
    row = [r for r in results if r.claim_id == '___________________________________________9'][0]
    assert row.date_service == datetime.date(2017, 4, 30)

def test_union_of_old_results_and_new():
    """Check that the final output table contains all the records from the latest
    as well as some records from previously normalized batches (1 additional
    record in this case)"""
    assert len(transactions) == 10
    assert len(transactions) < len(results)
    assert len(results) == 11

def test_reversed_claims_deleted():
    "Check that the final output table does not contain claims that were reversed"
    assert len([r for r in results if r.claim_id == '__________________________________________15']) == 0
    assert len([r for r in results if r.claim_id != '__________________________________________15']) == 11

def test_cleanup(spark):
    cleanup(spark)
