"""
medicalclaims v9 schema
"""
# pylint: disable=duplicate-code
from pyspark.sql.types import LongType, StringType, DateType, FloatType, StructType, StructField
from spark.common.schema import Schema

schema = Schema(
    name='schema_v9',
    data_type='medicalclaims',
    output_directory='medicalclaims/2018-06-06/',
    date_partition_column='part_best_date',
    provider_partition_column='part_provider',
    schema_structure=StructType([
        StructField('record_id', LongType(), True),
        StructField('claim_id', StringType(), True),
        StructField('hvid', StringType(), True),
        StructField('created', DateType(), True),
        StructField('model_version', StringType(), True),
        StructField('data_set', StringType(), True),
        StructField('data_feed', StringType(), True),
        StructField('data_vendor', StringType(), True),
        StructField('source_version', StringType(), True),
        StructField('vendor_org_id', StringType(), True),
        StructField('patient_gender', StringType(), True),
        StructField('patient_age', StringType(), True),
        StructField('patient_year_of_birth', StringType(), True),
        StructField('patient_zip3', StringType(), True),
        StructField('patient_state', StringType(), True),
        StructField('claim_type', StringType(), True),
        StructField('date_received', DateType(), True),
        StructField('date_service', DateType(), True),
        StructField('date_service_end', DateType(), True),
        StructField('inst_date_admitted', DateType(), True),
        StructField('inst_date_discharged', DateType(), True),
        StructField('inst_admit_type_std_id', StringType(), True),
        StructField('inst_admit_type_vendor_id', StringType(), True),
        StructField('inst_admit_type_vendor_desc', StringType(), True),
        StructField('inst_admit_source_std_id', StringType(), True),
        StructField('inst_admit_source_vendor_id', StringType(), True),
        StructField('inst_admit_source_vendor_desc', StringType(), True),
        StructField('inst_discharge_status_std_id', StringType(), True),
        StructField('inst_discharge_status_vendor_id', StringType(), True),
        StructField('inst_discharge_status_vendor_desc', StringType(), True),
        StructField('inst_type_of_bill_std_id', StringType(), True),
        StructField('inst_type_of_bill_vendor_id', StringType(), True),
        StructField('inst_type_of_bill_vendor_desc', StringType(), True),
        StructField('inst_drg_std_id', StringType(), True),
        StructField('inst_drg_vendor_id', StringType(), True),
        StructField('inst_drg_vendor_desc', StringType(), True),
        StructField('place_of_service_std_id', StringType(), True),
        StructField('place_of_service_vendor_id', StringType(), True),
        StructField('place_of_service_vendor_desc', StringType(), True),
        StructField('service_line_number', StringType(), True),
        StructField('service_line_id', StringType(), True),
        StructField('diagnosis_code', StringType(), True),
        StructField('diagnosis_code_qual', StringType(), True),
        StructField('diagnosis_priority', StringType(), True),
        StructField('admit_diagnosis_ind', StringType(), True),
        StructField('procedure_code', StringType(), True),
        StructField('procedure_code_qual', StringType(), True),
        StructField('principal_proc_ind', StringType(), True),
        StructField('procedure_units', FloatType(), True),
        StructField('procedure_units_billed', FloatType(), True),
        StructField('procedure_units_paid', FloatType(), True),
        StructField('procedure_modifier_1', StringType(), True),
        StructField('procedure_modifier_2', StringType(), True),
        StructField('procedure_modifier_3', StringType(), True),
        StructField('procedure_modifier_4', StringType(), True),
        StructField('revenue_code', StringType(), True),
        StructField('ndc_code', StringType(), True),
        StructField('medical_coverage_type', StringType(), True),
        StructField('line_charge', FloatType(), True),
        StructField('line_allowed', FloatType(), True),
        StructField('total_charge', FloatType(), True),
        StructField('total_allowed', FloatType(), True),
        StructField('prov_rendering_npi', StringType(), True),
        StructField('prov_billing_npi', StringType(), True),
        StructField('prov_referring_npi', StringType(), True),
        StructField('prov_facility_npi', StringType(), True),
        StructField('payer_vendor_id', StringType(), True),
        StructField('payer_name', StringType(), True),
        StructField('payer_parent_name', StringType(), True),
        StructField('payer_org_name', StringType(), True),
        StructField('payer_plan_id', StringType(), True),
        StructField('payer_plan_name', StringType(), True),
        StructField('payer_type', StringType(), True),
        StructField('prov_rendering_vendor_id', StringType(), True),
        StructField('prov_rendering_tax_id', StringType(), True),
        StructField('prov_rendering_dea_id', StringType(), True),
        StructField('prov_rendering_ssn', StringType(), True),
        StructField('prov_rendering_state_license', StringType(), True),
        StructField('prov_rendering_upin', StringType(), True),
        StructField('prov_rendering_commercial_id', StringType(), True),
        StructField('prov_rendering_name_1', StringType(), True),
        StructField('prov_rendering_name_2', StringType(), True),
        StructField('prov_rendering_address_1', StringType(), True),
        StructField('prov_rendering_address_2', StringType(), True),
        StructField('prov_rendering_city', StringType(), True),
        StructField('prov_rendering_state', StringType(), True),
        StructField('prov_rendering_zip', StringType(), True),
        StructField('prov_rendering_std_taxonomy', StringType(), True),
        StructField('prov_rendering_vendor_specialty', StringType(), True),
        StructField('prov_billing_vendor_id', StringType(), True),
        StructField('prov_billing_tax_id', StringType(), True),
        StructField('prov_billing_dea_id', StringType(), True),
        StructField('prov_billing_ssn', StringType(), True),
        StructField('prov_billing_state_license', StringType(), True),
        StructField('prov_billing_upin', StringType(), True),
        StructField('prov_billing_commercial_id', StringType(), True),
        StructField('prov_billing_name_1', StringType(), True),
        StructField('prov_billing_name_2', StringType(), True),
        StructField('prov_billing_address_1', StringType(), True),
        StructField('prov_billing_address_2', StringType(), True),
        StructField('prov_billing_city', StringType(), True),
        StructField('prov_billing_state', StringType(), True),
        StructField('prov_billing_zip', StringType(), True),
        StructField('prov_billing_std_taxonomy', StringType(), True),
        StructField('prov_billing_vendor_specialty', StringType(), True),
        StructField('prov_referring_vendor_id', StringType(), True),
        StructField('prov_referring_tax_id', StringType(), True),
        StructField('prov_referring_dea_id', StringType(), True),
        StructField('prov_referring_ssn', StringType(), True),
        StructField('prov_referring_state_license', StringType(), True),
        StructField('prov_referring_upin', StringType(), True),
        StructField('prov_referring_commercial_id', StringType(), True),
        StructField('prov_referring_name_1', StringType(), True),
        StructField('prov_referring_name_2', StringType(), True),
        StructField('prov_referring_address_1', StringType(), True),
        StructField('prov_referring_address_2', StringType(), True),
        StructField('prov_referring_city', StringType(), True),
        StructField('prov_referring_state', StringType(), True),
        StructField('prov_referring_zip', StringType(), True),
        StructField('prov_referring_std_taxonomy', StringType(), True),
        StructField('prov_referring_vendor_specialty', StringType(), True),
        StructField('prov_facility_vendor_id', StringType(), True),
        StructField('prov_facility_tax_id', StringType(), True),
        StructField('prov_facility_dea_id', StringType(), True),
        StructField('prov_facility_ssn', StringType(), True),
        StructField('prov_facility_state_license', StringType(), True),
        StructField('prov_facility_upin', StringType(), True),
        StructField('prov_facility_commercial_id', StringType(), True),
        StructField('prov_facility_name_1', StringType(), True),
        StructField('prov_facility_name_2', StringType(), True),
        StructField('prov_facility_address_1', StringType(), True),
        StructField('prov_facility_address_2', StringType(), True),
        StructField('prov_facility_city', StringType(), True),
        StructField('prov_facility_state', StringType(), True),
        StructField('prov_facility_zip', StringType(), True),
        StructField('prov_facility_std_taxonomy', StringType(), True),
        StructField('prov_facility_vendor_specialty', StringType(), True),
        StructField('cob_payer_vendor_id_1', StringType(), True),
        StructField('cob_payer_seq_code_1', StringType(), True),
        StructField('cob_payer_hpid_1', StringType(), True),
        StructField('cob_payer_claim_filing_ind_code_1', StringType(), True),
        StructField('cob_ins_type_code_1', StringType(), True),
        StructField('cob_payer_vendor_id_2', StringType(), True),
        StructField('cob_payer_seq_code_2', StringType(), True),
        StructField('cob_payer_hpid_2', StringType(), True),
        StructField('cob_payer_claim_filing_ind_code_2', StringType(), True),
        StructField('cob_ins_type_code_2', StringType(), True),
        StructField('vendor_test_id', StringType(), True),
        StructField('vendor_test_name', StringType(), True),
        StructField('claim_transaction_date', DateType(), True),
        StructField('claim_transaction_date_qual', StringType(), True),
        StructField('claim_transaction_amount', FloatType(), True),
        StructField('claim_transaction_amount_qual', StringType(), True),
        StructField('medical_claim_link_text', StringType(), True),
        StructField('emr_link_text', StringType(), True),
        StructField('logical_delete_reason', StringType(), True)
    ])
)
