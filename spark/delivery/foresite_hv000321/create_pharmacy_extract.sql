set hive.exec.compress.output=true;
set mapreduce.output.fileoutputformat.compress=true;
set mapreduce.output.fileoutputformat.compress.codec=org.apache.hadoop.io.compress.GzipCodec; 
set mapreduce.output.fileoutputformat.compress.type=BLOCK;
set spark.sql.shuffle.partitions=20;

DROP TABLE IF EXISTS {analyticsdb_schema}.pharmacy_claims_t2d;
CREATE TABLE {analyticsdb_schema}.pharmacy_claims_t2d
    AS SELECT record_id, claim_id,
    
    /* hash hvid */
    MD5(CONCAT(a.hvid, 'hvid321')) AS hvid, 
     
    created, model_version, data_set, marketplace_feed_id AS data_feed, marketplace_feed_name AS data_vendor,
    source_version, patient_gender, patient_age, patient_year_of_birth, patient_zip3, patient_state,
    date_service, date_written, date_authorized, time_authorized, transaction_code_std, transaction_code_vendor,
    response_code_std, response_code_vendor, reject_reason_code_1, reject_reason_code_2, reject_reason_code_3,
    reject_reason_code_4, reject_reason_code_5, diagnosis_code, diagnosis_code_qual, procedure_code, procedure_code_qual,
    a.ndc_code, product_service_id, product_service_id_qual, rx_number, rx_number_qual, bin_number, processor_control_number,
    fill_number, refill_auth_amount, dispensed_quantity, unit_of_measure, days_supply, 
    
    /* provider npi */
    pharmacy_npi, prov_dispensing_npi,
    
    /* payer */
    payer_id, payer_id_qual, payer_name, payer_parent_name, payer_org_name, payer_plan_id, payer_plan_name, payer_type,
    
    compound_code, unit_dose_indicator, dispensed_as_written, prescription_origin, submission_clarification,
    orig_prescribed_product_service_code, orig_prescribed_product_service_code_qual, orig_prescribed_quantity,
    prior_auth_type_code, level_of_service, reason_for_service, professional_service_code, result_of_service_code,
    
    /* provider npi */
    prov_prescribing_npi, prov_primary_care_npi, 

    cob_count, 
    
    /* cost data */
    usual_and_customary_charge, product_selection_attributed, other_payer_recognized, periodic_deductible_applied,
    periodic_benefit_exceed, accumulated_deductible, remaining_deductible, remaining_benefit, copay_coinsurance, 
    basis_of_cost_determination, submitted_ingredient_cost, submitted_dispensing_fee, submitted_incentive, submitted_gross_due,
    submitted_professional_service_fee, submitted_patient_pay, submitted_other_claimed_qual, submitted_other_claimed, 
    basis_of_reimbursement_determination, paid_ingredient_cost, paid_dispensing_fee, paid_incentive, paid_gross_due,
    paid_professional_service_fee, paid_patient_pay, paid_other_claimed_qual, paid_other_claimed, tax_exempt_indicator,
    coupon_type, coupon_number, coupon_value, pharmacy_other_id, pharmacy_other_qual, pharmacy_postal_code, prov_dispensing_id,
    prov_dispensing_qual, prov_prescribing_id, prov_prescribing_qual, prov_primary_care_id, prov_primary_care_qual,
    other_payer_coverage_type, other_payer_coverage_id, other_payer_coverage_qual, other_payer_date, other_payer_coverage_code,
    logical_delete_reason
    
FROM pharmacyclaims a
    INNER JOIN (
    SELECT *
    FROM ref_marketplace_to_warehouse
    WHERE data_type = 'pharmacy'
        ) b ON a.part_provider = b.warehouse_feed_name
    INNER JOIN {analyticsdb_schema}.mkt_def_ndc c ON a.ndc_code = c.ndc_code
    INNER JOIN (
    SELECT * 
    FROM {analyticsdb_schema}.mkt_def_calendar 
    WHERE delivery_date = {delivery_date}
        ) d ON a.date_service BETWEEN d.start_date AND d.end_date
WHERE a.hvid IS NOT NULL AND a.hvid NOT IN ('',' ') AND 
    a.part_provider IN ('express_scripts') AND 
    '2017/01/01' <= part_processdate
    ;