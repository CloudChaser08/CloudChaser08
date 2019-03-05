import pytest
from datetime import datetime, date
import hashlib
import spark.delivery.humana_000468.sparkExtractHumana as humana_extract
import spark.helpers.file_utils as file_utils
import os
from pyspark.sql import Row

GROUP1 = 'test1234'
GROUP2 = 'test0000' # Invalid group, less than 10 matches patients
GROUP3 = 'test4321'
pharmacy_extract = None
medical_extract = medical_extract2 = None
summary = None
summary2 = None
DW_TABLES = ['hvm_emr_diag', 'hvm_emr_proc', 'hvm_emr_medctn', 'hvm_emr_enc', 'ref_vdr_feed',
             'ref_gen_ref', 'hvm_pharmacyclaims_v07', 'hvm_medicalclaims_v08']
@pytest.mark.usefixtures("spark")
def test_init(spark):
    test_cleanup(spark)

    spark['spark'].sparkContext.parallelize([
        Row(
            hvm_vdr_feed_id=1,
            gen_ref_domn_nm='EARLIEST_VALID_SERVICE_DATE',
            gen_ref_itm_nm='',
            gen_ref_cd='',
            gen_ref_1_dt=date(2010, 1, 1),
            whtlst_flg=''
        )
    ]).toDF().createOrReplaceTempView('ref_gen_ref')
    
    spark['spark'].read.json(file_utils.get_abs_path(__file__, 'resources/ref_vdr_feed.json')).createOrReplaceTempView('ref_vdr_feed')
    spark['spark'].read.json(file_utils.get_abs_path(__file__, 'resources/pharma_sample.json')).createOrReplaceTempView('hvm_pharmacyclaims_v07')
    spark['spark'].read.json(file_utils.get_abs_path(__file__, 'resources/med_sample.json')).createOrReplaceTempView('hvm_medicalclaims_v08')
    spark['spark'].read.json(file_utils.get_abs_path(__file__, 'resources/enroll_sample.json')).createOrReplaceTempView('enrollmentrecords')
    spark['spark'].read.json(file_utils.get_abs_path(__file__, 'resources/emr_diag_sample.json')).createOrReplaceTempView('hvm_emr_diag')
    spark['spark'].read.json(file_utils.get_abs_path(__file__, 'resources/emr_proc_sample.json')).createOrReplaceTempView('hvm_emr_proc')
    spark['spark'].read.json(file_utils.get_abs_path(__file__, 'resources/emr_medctn_sample.json')).createOrReplaceTempView('hvm_emr_medctn')
    spark['spark'].read.json(file_utils.get_abs_path(__file__, 'resources/emr_enc_sample.json')).createOrReplaceTempView('hvm_emr_enc')
    spark['spark'].sql('CREATE SCHEMA dw')
    for table_name in DW_TABLES:
        spark['spark'].sql('CREATE TABLE dw.{0} AS SELECT * FROM {0}'.format(table_name))

    # Test that a run with only invalid groups works
    humana_extract.run(spark['spark'], spark['runner'], [GROUP2], test=True)
    # Test that a run with mixed groups works
    humana_extract.run(spark['spark'], spark['runner'], [GROUP1, GROUP2, GROUP3], test=True)

    global pharmacy_extract, medical_extract, medical_extract2, summary, summary2
    pharmacy_extract = spark['spark'].table(GROUP1 + '_pharmacy_extract').collect()
    medical_extract = spark['spark'].table(GROUP1 + '_medical_extract').collect()
    medical_extract2 = spark['spark'].table(GROUP3 + '_medical_extract').collect()
    summary  = spark['spark'].table(GROUP1 + '_summary').collect()
    summary2 = spark['spark'].table(GROUP2 + '_summary').collect()

def test_hashing():
    med_row = [r for r in medical_extract if r['claim_id'] == '365255892'][0]
    assert med_row['hvid'] == '082f3afe093cf65aad8e29a13e044173'
    assert med_row['prov_rendering_npi'] == '78ec74e4afe5e3f9d1bdab31900f0d68'
    assert med_row['prov_billing_npi'] == '3cf098bee8b3a620a618380c197cbf2a'
    assert med_row['prov_referring_npi'] == 'f3a0090792db47e0a9e72d8b407a9b09'
    assert med_row['prov_facility_npi'] == '3cf098bee8b3a620a618380c197cbf2a'

    pharma_row = [r for r in pharmacy_extract if r['claim_id'] == '749993504'][0]
    assert pharma_row['hvid'] == '1996efad03e9ee4320042c52f9f2c353'
    assert pharma_row['pharmacy_npi'] == '75d1d410d22ff291a33959c30a75cc38'
    assert pharma_row['prov_prescribing_npi'] == '810e939fe8a15b58877bb86e2a57db72'

def test_different_hashing_differnt_groups():
    """Ensure that two identical groups have identical data except for the
    hashed values"""

    # The HVIDs are the same for groups 1 and 3, but the hashed values should be different
    med_row  = [r for r in medical_extract if r['claim_id'] == '365255892'][0]
    med_row2 = [r for r in medical_extract2 if r['claim_id'] == '365255892'][0]

    HASHED_FIELDS = ['hvid', 'prov_rendering_npi', 'prov_billing_npi', 'prov_referring_npi', 'prov_facility_npi']
    for field in HASHED_FIELDS:
        assert med_row[field] != med_row2[field]

    for field in med_row.asDict().keys():
        if field not in HASHED_FIELDS:
            assert med_row[field] == med_row2[field]

def test_nulling():
    for r in pharmacy_extract + medical_extract:
        assert len(r['patient_year_of_birth'] or '') == 0

def test_record_count():
    assert [r['count'] for r in summary if r['data_vendor'] == 'Private Source 14'][0] == 26
    assert [r['count'] for r in summary if r['data_vendor'] == 'Private Source 22'][0] == 29

def test_synthetic_record_count():
    assert not [r['count'] for r in summary if r['data_vendor'] == 'Allscripts']

def test_few_patients():
    assert [r['count'] for r in summary2 if r['data_vendor'] == '-'][0] == 0

def test_cleanup(spark):
    for table_name in DW_TABLES:
        try:
            spark['spark'].sql('DROP TABLE IF EXISTS dw.' + table_name)
        except:
            pass
    spark['spark'].sql('DROP SCHEMA IF EXISTS dw')
