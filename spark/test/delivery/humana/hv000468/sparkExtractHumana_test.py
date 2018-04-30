import pytest
from datetime import datetime, date
import hashlib
import spark.delivery.humana_000468.sparkExtractHumana as humana_extract
import spark.helpers.file_utils as file_utils
import os

today = date(2018, 4, 26)
ts = 1524690702.12345
group_id = '87654321'
pharmacy_extract = None
medical_extract = None
summary = None
@pytest.mark.usefixtures("spark")
def test_init(spark):
    test_cleanup(spark)
    
    spark['spark'].read.json(file_utils.get_abs_path(__file__, 'resources/ref_vdr_feed.json')).createOrReplaceTempView('ref_vdr_feed')
    spark['spark'].sql('CREATE SCHEMA dw')
    spark['spark'].sql('CREATE TABLE dw.ref_vdr_feed AS SELECT * FROM ref_vdr_feed')
    spark['spark'].read.json(file_utils.get_abs_path(__file__, 'resources/pharma_sample.json')).createOrReplaceTempView('pharmacyclaims')
    spark['spark'].read.json(file_utils.get_abs_path(__file__, 'resources/med_sample.json')).createOrReplaceTempView('medicalclaims')
    humana_extract.run(spark['spark'], spark['runner'], group_id, test=True)

    global pharmacy_extract, medical_extract, summary
    pharmacy_extract = spark['spark'].table('pharmacy_extract').collect()
    medical_extract = spark['spark'].table('medical_extract').collect()
    summary = spark['spark'].table('summary').collect()

def test_hashing():
    med_row = [r for r in medical_extract if r['claim_id'] == '365255892'][0]
    assert med_row['hvid'] == '62af2ceacf0e9c6784b5ddbc451beb6f'
    assert med_row['prov_rendering_npi'] == '6068df8277a5b577e5529bacea97a95b'
    assert med_row['prov_billing_npi'] == '83369192da83b9d2995f9395e668dd76'
    assert med_row['prov_referring_npi'] == '794a2df821a83381288ee908e8638490'
    assert med_row['prov_facility_npi'] == '83369192da83b9d2995f9395e668dd76'

    pharma_row = [r for r in pharmacy_extract if r['claim_id'] == '749993504'][0]
    assert pharma_row['hvid'] == '727745e423bda12382d7cccc2db1f6d3'
    assert pharma_row['pharmacy_npi'] == '6797becb38c81200306e170bfa295244'
    assert pharma_row['prov_prescribing_npi'] == 'b8cc6f58a038b752e5db275509c67dcd'

def test_nulling():
    for r in pharmacy_extract + medical_extract:
        assert len(r['patient_year_of_birth'] or '') == 0

def test_record_count():
    assert [r['count'] for r in summary if r['data_vendor'] == 'Private Source 14'][0] == 26
    assert [r['count'] for r in summary if r['data_vendor'] == 'Private Source 22'][0] == 29

def test_cleanup(spark):
    try:
        spark['spark'].sql('DROP TABLE IF EXISTS dw.ref_vdr_feed')
    except:
        pass
    spark['spark'].sql('DROP SCHEMA IF EXISTS dw')
