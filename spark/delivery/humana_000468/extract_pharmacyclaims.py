import pyspark.sql.functions as F

def extract(runner, hvids, timestamp, start_dt, end_dt):
    t1 = runner.sqlContext.table('pharmacyclaims')
    t2 = runner.sqlContext.table('dw.ref_vdr_feed')
    t2 = t2[t2.hvm_tile_nm.isin(*SUPPLIERS)]
    
    # Extract conditions
    ext = t1.join(t2, t1['data_feed'] == t2['hvm_vdr_feed_id'], 'inner') \
        .join(hvids, t1['hvid'] == hvids['hvid'], 'left_semi') \
        .where((t1['date_service'] <= end_dt.isoformat()) & (t1['date_service'] >= start_dt.isoformat()))

    # Hashing
    ext = ext.withColumn('hvid', F.md5(F.concat(F.col('hvid'), F.lit('hvid'), F.lit('hv000468'), F.lit(str(timestamp))))) \
            .withColumn('pharmacy_npi', F.md5(F.concat(F.col('pharmacy_npi'), F.lit('npi'), F.lit('hv000468'), F.lit(str(timestamp))))) \
            .withColumn('prov_dispensing_npi', F.md5(F.concat(F.col('prov_dispensing_npi'), F.lit('npi'), F.lit('hv000468'), F.lit(str(timestamp))))) \
            .withColumn('prov_prescribing_npi', F.md5(F.concat(F.col('prov_prescribing_npi'), F.lit('npi'), F.lit('hv000468'), F.lit(str(timestamp))))) \
            .withColumn('prov_primary_care_npi', F.md5(F.concat(F.col('prov_primary_care_npi'), F.lit('npi'), F.lit('hv000468'), F.lit(str(timestamp)))))

    # Rename columns
    ext = ext.withColumn('data_feed', F.col('hvm_vdr_feed_id')) \
            .withColumn('data_vendor', F.col('hvm_tile_nm'))

    # NULL columns
    for c in NULL_COLUMNS:
        ext = ext.withColumn(c, F.lit(None).cast('string'))

    # Reorder
    return ext.select(*EXTRACT_COLUMNS)

EXTRACT_COLUMNS = [
    'record_id',
    'claim_id',
    'hvid',                     # Hashed
    'created',
    'model_version',
    'data_set',
    'data_feed',                # Feed ID#
    'data_vendor',              # Marketplace tile name
    'source_version',
    'patient_gender',           # NULL per Austin
    'patient_age',              # NULL, was age-group
    'patient_year_of_birth',    # NULL per Austin
    'patient_zip3',             # NULL per Austin
    'patient_state',            # NULL per Austin
    'date_service',
    'date_written',
    'date_authorized',
    'time_authorized',
    'transaction_code_std',
    'transaction_code_vendor',
    'response_code_std',
    'response_code_vendor',
    'reject_reason_code_1',
    'reject_reason_code_2',
    'reject_reason_code_3',
    'reject_reason_code_4',
    'reject_reason_code_5',
    'diagnosis_code',
    'diagnosis_code_qual',
    'procedure_code',
    'procedure_code_qual',
    'ndc_code',
    'product_service_id',
    'product_service_id_qual',
    'rx_number',
    'rx_number_qual',
    'bin_number',               # NULL per AG
    'processor_control_number', # NULL per AG
    'fill_number',
    'refill_auth_amount',
    'dispensed_quantity',
    'unit_of_measure',
    'days_supply',
    'pharmacy_npi',             # Hashed per Austin
    'prov_dispensing_npi',      # Hashed per Austin
    'payer_id',
    'payer_id_qual',            # NULL per Austin
    'payer_name',               # NULL per Austin
    'payer_parent_name',        # NULL per Austin
    'payer_org_name',           # NULL per Austin
    'payer_plan_id',            # NULL per AG (per McKesson)
    'payer_plan_name',          # NULL per Austin
    'payer_type',               # NULL per AG (per McKesson)
    'compound_code',
    'unit_dose_indicator',
    'dispensed_as_written',
    'prescription_origin',
    'submission_clarification',
    'orig_prescribed_product_service_code',
    'orig_prescribed_product_service_code_qual',
    'orig_prescribed_quantity',
    'prior_auth_type_code',
    'level_of_service',
    'reason_for_service',
    'professional_service_code',
    'result_of_service_code',
    'prov_prescribing_npi',     # Hashed per AG
    'prov_primary_care_npi',    # Hashed per AG
    'cob_count',
    'usual_and_customary_charge',
    'product_selection_attributed',
    'other_payer_recognized',
    'periodic_deductible_applied',
    'periodic_benefit_exceed',
    'accumulated_deductible',
    'remaining_deductible',
    'remaining_benefit',
    'copay_coinsurance',
    'basis_of_cost_determination',
    'submitted_ingredient_cost',
    'submitted_dispensing_fee',
    'submitted_incentive',
    'submitted_gross_due',
    'submitted_professional_service_fee',
    'submitted_patient_pay',
    'submitted_other_claimed_qual',
    'submitted_other_claimed',
    'basis_of_reimbursement_determination',
    'paid_ingredient_cost',
    'paid_dispensing_fee',
    'paid_incentive',
    'paid_gross_due',
    'paid_professional_service_fee',
    'paid_patient_pay',
    'paid_other_claimed_qual',
    'paid_other_claimed',
    'tax_exempt_indicator',
    'coupon_type',
    'coupon_number',
    'coupon_value',
    'pharmacy_other_id',        # NULL per Austin
    'pharmacy_other_qual',      # NULL per Austin
    'pharmacy_postal_code',     # NULL per Austin
    'prov_dispensing_id',       # NULL per Austin
    'prov_dispensing_qual',     # NULL per Austin
    'prov_prescribing_id',      # NULL per Austin
    'prov_prescribing_qual',    # NULL per Austin
    'prov_primary_care_id',     # NULL per Austin
    'prov_primary_care_qual',   # NULL per Austin
    'other_payer_coverage_type',# NULL per AG
    'other_payer_coverage_id',  # NULL per AG
    'other_payer_coverage_qual',# NULL per AG
    'other_payer_date',         # NULL per AG
    'other_payer_coverage_code',# NULL per AG
    'logical_delete_reason'
]

SUPPLIERS = [
    'Private Source 17',
    'Private Source 22',
    'PDX, Inc.'
]

NULL_COLUMNS = [
    'patient_gender',
    'patient_age',
    'patient_year_of_birth',
    'patient_zip3',
    'patient_state',
    'bin_number',
    'processor_control_number',
    'payer_id_qual',
    'payer_name',
    'payer_parent_name',
    'payer_org_name',
    'payer_plan_id',
    'payer_plan_name',
    'payer_type',
    'pharmacy_other_id',
    'pharmacy_other_qual',
    'pharmacy_postal_code',
    'prov_dispensing_id',
    'prov_dispensing_qual',
    'prov_prescribing_id',
    'prov_prescribing_qual',
    'prov_primary_care_id',
    'prov_primary_care_qual',
    'other_payer_coverage_type',
    'other_payer_coverage_id',
    'other_payer_coverage_qual',
    'other_payer_date',
    'other_payer_coverage_code'
]