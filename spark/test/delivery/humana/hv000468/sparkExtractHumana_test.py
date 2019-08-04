from datetime import date

import pytest
from pyspark.sql import Row
from pyspark.sql.utils import AnalysisException

import spark.delivery.humana_000468.sparkExtractHumana as humana_extract
import spark.helpers.file_utils as file_utils

GROUP1 = 'test1234'
GROUP2 = 'test0000' # Invalid group, 20 patients, 1 valid
GROUP3 = 'test4321'
GROUP4 = 'test0001' # Invalid group, 5 patients, 5 valid
DW_TABLES = ['hvm_emr_diag_v08', 'hvm_emr_proc_v10', 'hvm_emr_medctn_v09', 'hvm_emr_enc_v08', 'ref_vdr_feed',
             'ref_gen_ref', 'hvm_pharmacyclaims_v07', 'hvm_medicalclaims_v08']

HASHED_FIELDS = ['hvid', 'prov_rendering_npi', 'prov_billing_npi', 'prov_referring_npi', 'prov_facility_npi']

@pytest.fixture(scope="module")
@pytest.mark.usefixtures("spark")
def _transformation_results(spark):
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
    spark['spark'].read.json(file_utils.get_abs_path(__file__, 'resources/emr_diag_sample.json')).createOrReplaceTempView('hvm_emr_diag_v08')
    spark['spark'].read.json(file_utils.get_abs_path(__file__, 'resources/emr_proc_sample.json')).createOrReplaceTempView('hvm_emr_proc_v10')
    spark['spark'].read.json(file_utils.get_abs_path(__file__, 'resources/emr_medctn_sample.json')).createOrReplaceTempView('hvm_emr_medctn_v09')
    spark['spark'].read.json(file_utils.get_abs_path(__file__, 'resources/emr_enc_sample.json')).createOrReplaceTempView('hvm_emr_enc_v08')
    spark['spark'].sql('CREATE SCHEMA dw')
    for table_name in DW_TABLES:
        spark['spark'].sql('CREATE TABLE dw.{0} AS SELECT * FROM {0}'.format(table_name))

    # Test that a run with only invalid groups works
    humana_extract.run(spark['spark'], spark['runner'], [GROUP2], test=True)
    # Test that a run with mixed groups works
    humana_extract.run(spark['spark'], spark['runner'], [GROUP1, GROUP2, GROUP3, GROUP4], test=True)

    yield {
        'pharmacy_extract' : spark['spark'].table(GROUP1 + '_pharmacy_extract').collect(),
        'medical_extract' : spark['spark'].table(GROUP1 + '_medical_extract').collect(),
        'medical_extract3' : spark['spark'].table(GROUP3 + '_medical_extract').collect(),
        'summary' : spark['spark'].table(GROUP1 + '_summary').collect(),
        'summary2' : spark['spark'].table(GROUP2 + '_summary').collect(),
        'summary4' : spark['spark'].table(GROUP4 + '_summary').collect()
    }

def test_hashing(_transformation_results):
    """Ensure that hvid and all NPI fields are hashed correct"""
    medical_extract = _transformation_results['medical_extract']
    pharmacy_extract = _transformation_results['pharmacy_extract']

    med_row = [r for r in medical_extract if r['claim_id'] == '365255892'][0]
    assert med_row['hvid'] == '137cceaa255a472621476d361c8cc040'
    assert med_row['prov_rendering_npi'] == 'c596079048b796c7f4443662ea0470b9'
    assert med_row['prov_billing_npi'] == 'c1128648fadc3692af85544eb6115729'
    assert med_row['prov_referring_npi'] == '63d0f298ddf0a528b84f99e9e01c425a'
    assert med_row['prov_facility_npi'] == 'c1128648fadc3692af85544eb6115729'

    pharma_row = [r for r in pharmacy_extract if r['claim_id'] == '749993504'][0]
    assert pharma_row['hvid'] == 'c37a8baebde81fcce7b0bb79925fbdb4'
    assert pharma_row['pharmacy_npi'] == '04fe3b9cd69c583baab0936f4317bdee'
    assert pharma_row['prov_prescribing_npi'] == '7d69a4611b5a96ee5dcd8db96f855fa1'

def test_different_hashing_differnt_groups(_transformation_results):
    """Ensure that two identical groups have identical data except for the
    hashed values"""
    group1_extract = _transformation_results['medical_extract']
    group3_extract = _transformation_results['medical_extract3']

    # The HVIDs are the same for groups 1 and 3, but the hashed values should be different
    group1_row = [r for r in group1_extract if r['claim_id'] == '365255892'][0]
    group3_row = [r for r in group3_extract if r['claim_id'] == '365255892'][0]

    for field in HASHED_FIELDS:
        assert group1_row[field] != group3_row[field]

    for field in group1_row.asDict().keys():
        if field not in HASHED_FIELDS:
            assert group1_row[field] == group3_row[field]

def test_nulling(_transformation_results):
    """Ensure that demographics data is being nulled out"""
    for r in _transformation_results['pharmacy_extract'] + _transformation_results['medical_extract']:
        assert r['patient_year_of_birth'] is None

def test_record_count(_transformation_results):
    """Ensure that records counts for suppliers are included in the summary data"""
    summary = _transformation_results['summary']

    assert [r['count'] for r in summary if r['data_vendor'] == 'Private Source 14'][0] == 26
    assert [r['count'] for r in summary if r['data_vendor'] == 'Private Source 22'][0] == 29

def test_few_valid_patients(_transformation_results):
    """Ensure that no records are extracted for a group that contains 20 patients,
    but only 1 of them is valid"""
    summary = _transformation_results['summary2']

    assert [r['count'] for r in summary if r['data_vendor'] == '-'][0] == 0

def test_few_patients(_transformation_results):
    """Ensure that no records are extracted for a group that contains only 5 patients
    even though all of them are valid"""
    summary = _transformation_results['summary4']

    assert [r['count'] for r in summary if r['data_vendor'] == '-'][0] == 0

def test_cleanup(spark):
    for table_name in DW_TABLES:
        try:
            spark['spark'].sql('DROP TABLE IF EXISTS dw.' + table_name)
        except AnalysisException as exp:
            if 'not found' not in exp.desc:
                raise exp

    spark['spark'].sql('DROP SCHEMA IF EXISTS dw')
