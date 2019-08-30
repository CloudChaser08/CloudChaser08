import datetime
import hashlib
import pytest
import spark.providers.cardinal_pds.pharmacyclaims.sparkNormalizeCardinalRx as cardinal_pds

hv_results = []
cardinal_results = []


def cleanup(spark):
    spark['sqlContext'].dropTempTable('pharmacyclaims_common_model')

@pytest.mark.usefixtures("spark")
def test_init(spark):
    cleanup(spark)
    cardinal_pds.run(spark['spark'], spark['runner'], '2017-08-29', True)
    global hv_results, cardinal_results
    hv_results = spark['sqlContext'].sql('select * from pharmacyclaims_common_model_final') \
        .collect()
    cardinal_results = spark['sqlContext'].sql('select * from cardinal_deliverable') \
        .collect()


def test_patient_ids():
    "Ensure that patient ids and hvids are populated correctly"
    sample_row = [r for r in cardinal_results if r.rx_number == '0817b8e97a88844fa6fa894450923ca7'][0]
    assert sample_row.hvid == '0003660202I'

    sample_row = [r for r in cardinal_results if r.rx_number == '2e531da0548a838e2ba3497e432b87f1'][0]
    assert sample_row.hvid == '1505255655'

    sample_row = [r for r in hv_results if r.rx_number == '0817b8e97a88844fa6fa894450923ca7'][0]
    assert sample_row.hvid is None

    sample_row = [r for r in hv_results if r.rx_number == '2e531da0548a838e2ba3497e432b87f1'][0]
    assert sample_row.hvid == '30040263'


def test_date_parsing():
    "Ensure that dates are correctly parsed"
    sample_row = [r for r in hv_results if r.rx_number == '0817b8e97a88844fa6fa894450923ca7'][0]
    assert sample_row.date_service == datetime.date(2017, 8, 22)
    assert sample_row.date_written == datetime.date(2016, 8, 17)


def test_product_code():
    "Ensure that NDC and non-NDC product codes are identified correctly"
    sample_row_ndc = [r for r in hv_results if r.rx_number == '0817b8e97a88844fa6fa894450923ca7'][0]
    sample_row_nonndc = [r for r in hv_results if r.rx_number == 'ab572511f8dfb0b8aac3e3e742e99f60'][0]

    assert sample_row_ndc.ndc_code == '00517003125'
    assert sample_row_ndc.product_service_id is None
    assert sample_row_ndc.product_service_id_qual is None
    assert sample_row_nonndc.ndc_code is None
    assert sample_row_nonndc.product_service_id == '59572021015'
    assert sample_row_nonndc.product_service_id_qual == '3'


def test_pharmacy_ncpdp():
    "Ensure that pharmacy NCPDP numbers are 0 left-padded to 7 characters"
    sample_row_ndc = [r for r in cardinal_results if r.rx_number == '0817b8e97a88844fa6fa894450923ca7'][0]

    assert sample_row_ndc.pharmacy_other_id == '0134635'


def test_logical_delete_reason(spark):
    "Ensure that logical delete reason is being correctly applied"

    # The group of claims with this RX number should have 1 final paid claim, 1 rejected claims
    # 1 reversal, and 1 reversed claim
    res = [r for r in hv_results if r.rx_number == 'ab572511f8dfb0b8aac3e3e742e99f60']

    assert len([r for r in res if r.logical_delete_reason == 'Claim Rejected']) == 1
    assert len([r for r in res if r.logical_delete_reason == 'Reversed Claim']) == 1
    assert len([r for r in res if r.logical_delete_reason == 'Reversal']) == 1
    assert len([r for r in res if r.logical_delete_reason is None]) == 1


def test_pharmacy_other_id_hashed_in_hv_results_but_not_cardinal_results():
    hv_sample_row = [r for r in hv_results if r.rx_number == '0817b8e97a88844fa6fa894450923ca7'][0]
    cardinal_sample_row = [r for r in cardinal_results if r.rx_number == '0817b8e97a88844fa6fa894450923ca7'][0]
    assert hv_sample_row.pharmacy_other_id == hashlib.md5('0134635'.encode('UTF-8')).hexdigest()
    assert cardinal_sample_row.pharmacy_other_id == '0134635'


def test_hvm_approved_filtering():
    """Ensure hvm_approved fields are filtered out for hv_results, but not cardinal_results"""
    assert len(cardinal_results) == 13
    assert len(hv_results) == 9


def test_hvm_approved_columns():
    """Ensure that the hvm_approved column doesn't exist in cardinal_results or hv_results"""
    assert not hasattr(cardinal_results[0], 'hvm_approved')
    assert not hasattr(hv_results[0], 'hvm_approved')


def test_cleanup(spark):
    cleanup(spark)
