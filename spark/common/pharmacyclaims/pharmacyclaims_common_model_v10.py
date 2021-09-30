"""pharmacyclaims common model v10"""
from pyspark.sql.types import *
from spark.common.schema import Schema

schema = Schema(
    name='schema_v10',
    data_type='pharmacyclaims',
    output_directory='pharmacyclaims/2018-11-26',
    distribution_key='record_id',
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
        StructField('patient_gender', StringType(), True),
        StructField('patient_age', StringType(), True),
        StructField('patient_year_of_birth', StringType(), True),
        StructField('patient_zip3', StringType(), True),
        StructField('patient_state', StringType(), True),
        StructField('date_service', DateType(), True),
        StructField('date_written', DateType(), True),
        StructField('year_of_injury', StringType(), True),
        StructField('date_authorized', DateType(), True),
        StructField('time_authorized', StringType(), True),
        StructField('discharge_date', DateType(), True),
        StructField('transaction_code_std', StringType(), True),
        StructField('transaction_code_vendor', StringType(), True),
        StructField('response_code_std', StringType(), True),
        StructField('response_code_vendor', StringType(), True),
        StructField('reject_reason_code_1', StringType(), True),
        StructField('reject_reason_code_2', StringType(), True),
        StructField('reject_reason_code_3', StringType(), True),
        StructField('reject_reason_code_4', StringType(), True),
        StructField('reject_reason_code_5', StringType(), True),
        StructField('diagnosis_code', StringType(), True),
        StructField('diagnosis_code_qual', StringType(), True),
        StructField('procedure_code', StringType(), True),
        StructField('procedure_code_qual', StringType(), True),
        StructField('ndc_code', StringType(), True),
        StructField('product_service_id', StringType(), True),
        StructField('product_service_id_qual', StringType(), True),
        StructField('rx_number', StringType(), True),
        StructField('rx_number_qual', StringType(), True),
        StructField('bin_number', StringType(), True),
        StructField('processor_control_number', StringType(), True),
        StructField('fill_number', IntegerType(), True),
        StructField('refill_auth_amount', StringType(), True),
        StructField('dispensed_quantity', IntegerType(), True),
        StructField('unit_of_measure', StringType(), True),
        StructField('days_supply', IntegerType(), True),
        StructField('pharmacy_npi', StringType(), True),
        StructField('prov_dispensing_npi', StringType(), True),
        StructField('payer_id', StringType(), True),
        StructField('payer_id_qual', StringType(), True),
        StructField('payer_name', StringType(), True),
        StructField('payer_parent_name', StringType(), True),
        StructField('payer_org_name', StringType(), True),
        StructField('payer_plan_id', StringType(), True),
        StructField('payer_plan_name', StringType(), True),
        StructField('payer_type', StringType(), True),
        StructField('compound_code', StringType(), True),
        StructField('unit_dose_indicator', StringType(), True),
        StructField('dispensed_as_written', StringType(), True),
        StructField('prescription_origin', StringType(), True),
        StructField('submission_clarification', StringType(), True),
        StructField('orig_prescribed_product_service_code', StringType(), True),
        StructField('orig_prescribed_product_service_code_qual', StringType(), True),
        StructField('orig_prescribed_quantity', StringType(), True),
        StructField('prior_auth_type_code', StringType(), True),
        StructField('level_of_service', StringType(), True),
        StructField('reason_for_service', StringType(), True),
        StructField('professional_service_code', StringType(), True),
        StructField('result_of_service_code', StringType(), True),
        StructField('prov_prescribing_npi', StringType(), True),
        StructField('prov_primary_care_npi', StringType(), True),
        StructField('cob_count', StringType(), True),
        StructField('usual_and_customary_charge', DoubleType(), True),
        StructField('product_selection_attributed', DoubleType(), True),
        StructField('other_payer_recognized', DoubleType(), True),
        StructField('periodic_deductible_applied', DoubleType(), True),
        StructField('periodic_benefit_exceed', DoubleType(), True),
        StructField('accumulated_deductible', DoubleType(), True),
        StructField('remaining_deductible', DoubleType(), True),
        StructField('remaining_benefit', DoubleType(), True),
        StructField('copay_coinsurance', DoubleType(), True),
        StructField('basis_of_cost_determination', DoubleType(), True),
        StructField('submitted_ingredient_cost', DoubleType(), True),
        StructField('submitted_dispensing_fee', DoubleType(), True),
        StructField('submitted_incentive', DoubleType(), True),
        StructField('submitted_gross_due', DoubleType(), True),
        StructField('submitted_professional_service_fee', DoubleType(), True),
        StructField('submitted_patient_pay', DoubleType(), True),
        StructField('submitted_other_claimed_qual', DoubleType(), True),
        StructField('submitted_other_claimed', DoubleType(), True),
        StructField('basis_of_reimbursement_determination', DoubleType(), True),
        StructField('paid_ingredient_cost', DoubleType(), True),
        StructField('paid_dispensing_fee', DoubleType(), True),
        StructField('paid_incentive', DoubleType(), True),
        StructField('paid_gross_due', DoubleType(), True),
        StructField('paid_professional_service_fee', DoubleType(), True),
        StructField('paid_patient_pay', DoubleType(), True),
        StructField('paid_other_claimed_qual', DoubleType(), True),
        StructField('paid_other_claimed', DoubleType(), True),
        StructField('tax_exempt_indicator', StringType(), True),
        StructField('coupon_type', StringType(), True),
        StructField('coupon_number', StringType(), True),
        StructField('coupon_value', DoubleType(), True),
        StructField('pharmacy_other_id', StringType(), True),
        StructField('pharmacy_other_qual', StringType(), True),
        StructField('pharmacy_postal_code', StringType(), True),
        StructField('prov_dispensing_id', StringType(), True),
        StructField('prov_dispensing_qual', StringType(), True),
        StructField('prov_prescribing_id', StringType(), True),
        StructField('prov_prescribing_qual', StringType(), True),
        StructField('prov_primary_care_id', StringType(), True),
        StructField('prov_primary_care_qual', StringType(), True),
        StructField('other_payer_coverage_type', StringType(), True),
        StructField('other_payer_coverage_id', StringType(), True),
        StructField('other_payer_coverage_qual', StringType(), True),
        StructField('other_payer_date', DateType(), True),
        StructField('other_payer_coverage_code', StringType(), True),
        StructField('logical_delete_reason', StringType(), True)
    ])
)
