from pyspark.sql.types import StructType, StructField, StringType, LongType, DateType

schema = StructType([
    StructField('record_id', LongType(), True),
    StructField('hvid', StringType(), True),
    StructField('created', DateType(), True),
    StructField('model_version', StringType(), True),
    StructField('data_set', StringType(), True),
    StructField('data_feed', StringType(), True),
    StructField('data_vendor', StringType(), True),
    StructField('source_version', StringType(), True),
    StructField('claim_type', StringType(), True),
    StructField('claim_id', StringType(), True),
    StructField('claim_qual', StringType(), True),
    StructField('claim_date', DateType(), True),
    StructField('claim_error_ind', StringType(), True),
    StructField('patient_age', StringType(), True),
    StructField('patient_year_of_birth', StringType(), True),
    StructField('patient_zip', StringType(), True),
    StructField('patient_state', StringType(), True),
    StructField('patient_deceased_flag', StringType(), True),
    StructField('patient_gender', StringType(), True),
    StructField('patient_race', StringType(), True),
    StructField('patient_ethnicity', StringType(), True),
    StructField('provider_client_id_qual', StringType(), True),
    StructField('provider_client_id', StringType(), True),
    StructField('provider_rendering_id_qual', StringType(), True),
    StructField('provider_rendering_id', StringType(), True),
    StructField('provider_referring_id_qual', StringType(), True),
    StructField('provider_referring_id', StringType(), True),
    StructField('provider_billing_id_qual', StringType(), True),
    StructField('provider_billing_id', StringType(), True),
    StructField('provider_facility_id_qual', StringType(), True),
    StructField('provider_facility_id', StringType(), True),
    StructField('provider_ordering_id_qual', StringType(), True),
    StructField('provider_ordering_id', StringType(), True),
    StructField('provider_lab_id_qual', StringType(), True),
    StructField('provider_lab_id', StringType(), True),
    StructField('provider_pharmacy_id_qual', StringType(), True),
    StructField('provider_pharmacy_id', StringType(), True),
    StructField('provider_prescriber_id_qual', StringType(), True),
    StructField('provider_prescriber_id', StringType(), True),
    StructField('payer_id_qual', StringType(), True),
    StructField('payer_id', StringType(), True),
    StructField('payer_type', StringType(), True),
    StructField('payer_parent', StringType(), True),
    StructField('payer_name', StringType(), True),
    StructField('plan_name', StringType(), True),
    StructField('encounter_id', StringType(), True),
    StructField('encounter_id_qual', StringType(), True),
    StructField('description', StringType(), True),
    StructField('description_qual', StringType(), True),
    StructField('date_start', DateType(), True),
    StructField('date_end', DateType(), True),
    StructField('date_qual', StringType(), True),
    StructField('diagnosis_code', StringType(), True),
    StructField('diagnosis_code_qual', StringType(), True),
    StructField('diagnosis_code_priority', StringType(), True),
    StructField('procedure_code', StringType(), True),
    StructField('procedure_code_qual', StringType(), True),
    StructField('procedure_code_modifier', StringType(), True),
    StructField('procedure_code_priority', StringType(), True),
    StructField('ndc_code', StringType(), True),
    StructField('ndc_code_qual', StringType(), True),
    StructField('loinc_code', StringType(), True),
    StructField('other_code', StringType(), True),
    StructField('other_code_qual', StringType(), True),
    StructField('other_code_modifier', StringType(), True),
    StructField('other_code_mod_qual', StringType(), True),
    StructField('other_code_priority', StringType(), True),
    StructField('type', StringType(), True),
    StructField('type_qual', StringType(), True),
    StructField('category', StringType(), True),
    StructField('category_qual', StringType(), True),
    StructField('panel', StringType(), True),
    StructField('panel_qual', StringType(), True),
    StructField('specimen', StringType(), True),
    StructField('specimen_qual', StringType(), True),
    StructField('method', StringType(), True),
    StructField('method_qual', StringType(), True),
    StructField('result', StringType(), True),
    StructField('result_qual', StringType(), True),
    StructField('reason', StringType(), True),
    StructField('reason_qual', StringType(), True),
    StructField('ref_range', StringType(), True),
    StructField('ref_range_qual', StringType(), True),
    StructField('abnormal', StringType(), True),
    StructField('abnormal_qual', StringType(), True),
    StructField('uom', StringType(), True),
    StructField('uom_qual', StringType(), True),
    StructField('severity', StringType(), True),
    StructField('severity_qual', StringType(), True),
    StructField('status', StringType(), True),
    StructField('status_qual', StringType(), True),
    StructField('units', StringType(), True),
    StructField('units_qual', StringType(), True),
    StructField('qty_dispensed', StringType(), True),
    StructField('qty_dispensed_qual', StringType(), True),
    StructField('days_supply', StringType(), True),
    StructField('elapsed_days', StringType(), True),
    StructField('num_refills', StringType(), True),
    StructField('route', StringType(), True),
    StructField('sig', StringType(), True),
    StructField('frequency_units', StringType(), True),
    StructField('frequency_times', StringType(), True),
    StructField('frequency_uom', StringType(), True),
    StructField('dose', StringType(), True),
    StructField('dose_qual', StringType(), True),
    StructField('strength', StringType(), True),
    StructField('form', StringType(), True),
    StructField('sample', StringType(), True),
    StructField('unverified', StringType(), True),
    StructField('electronicrx', StringType(), True)
])
