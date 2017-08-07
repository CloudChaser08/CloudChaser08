import pytest

import datetime

import spark.providers.cardinal_pds.pharmacyclaims.sparkNormalizeCardinalRx as cardinal_pds

results = []


def cleanup(spark):
    spark['sqlContext'].dropTempTable('pharmacyclaims_common_model')


@pytest.mark.usefixtures("spark")
def test_init(spark):
    cleanup(spark)
    cardinal_pds.run(spark['spark'], spark['runner'], '2017-05-24', True)
    global results
    results = spark['sqlContext'].sql('select * from pharmacyclaims_common_model_final') \
                                 .collect()


def test_date_parsing():
    "Ensure that dates are correctly parsed"
    sample_row = filter(lambda r: r.rx_number == '0817b8e97a88844fa6fa894450923ca7', results)[0]

    assert sample_row.date_service == datetime.date(2017, 4, 12)
    assert sample_row.date_written == datetime.date(2016, 11, 7)


def test_product_code():
    "Ensure that NDC and non-NDC product codes are identified correctly"
    sample_row_ndc = filter(lambda r: r.rx_number == '0817b8e97a88844fa6fa894450923ca7', results)[0]
    sample_row_nonndc = filter(lambda r: r.rx_number == 'ab572511f8dfb0b8aac3e3e742e99f60', results)[0]

    assert sample_row_ndc.ndc_code == '00517003125'
    assert sample_row_ndc.product_service_id is None
    assert sample_row_ndc.product_service_id_qual is None
    assert sample_row_nonndc.ndc_code is None
    assert sample_row_nonndc.product_service_id == '59572021015'
    assert sample_row_nonndc.product_service_id_qual == '3'

def test_pharmacy_ncpdp():
    "Ensure that pharmacy NCPDP numbers are 0 left-padded to 7 characters"
    sample_row_ndc = filter(lambda r: r.rx_number == '0817b8e97a88844fa6fa894450923ca7', results)[0]

    assert sample_row_ndc.pharmacy_other_id == '0134635'


def test_logical_delete_reason(spark):
    "Ensure that logical delete reason is being correctly applied"

    # The group of claims with this RX number should have 1 final paid claim, 1 rejected claims
    # 1 reversal, and 1 reversed claim
    res = filter(lambda r: r.rx_number == 'ab572511f8dfb0b8aac3e3e742e99f60', results)

    assert len(filter(lambda r: r.logical_delete_reason == 'Claim Rejected', res)) == 1
    assert len(filter(lambda r: r.logical_delete_reason == 'Reversed Claim', res)) == 1
    assert len(filter(lambda r: r.logical_delete_reason == 'Reversal', res)) == 1
    assert len(filter(lambda r: r.logical_delete_reason is None, res)) == 1

def test_cleanup(spark):
    cleanup(spark)