SELECT
    sk_utilization_key               AS claim_id,
    '6'                              AS model_version,
    dispense_dttm                    AS date_service,
    effective_start_date             AS date_written,
    discharge_dttm                   AS discharge_date,
    CASE
        WHEN drg_code="-1" THEN NULL
        ELSE drg_code
    END                              AS diagnosis_code,
    '99'                             AS diagnosis_code_qual,
    ndc                              AS ndc_code,
    hdc                              AS product_service_id,
    '99'                             AS product_service_id_qual,
    qty                              AS dispensed_quantity,
    uom_qty                          AS unit_of_measure,
    acq_cost                         AS usual_and_customary_charge,
    extended_cost                    AS submitted_ingredient_cost,
    revenue                          AS submitted_gross_due,
    sk_client_type_key               AS pharmacy_other_id,
    '99'                             AS pharmacy_other_qual,
    prescribing_physician_code       AS prov_prescribing_id,
    '99'                             AS prov_prescribing_qual,
    physician_code                   AS prov_primary_care_id,
    '99'                             AS prov_primary_care_qual,
    patient_type                     AS patient_type_vendor,
    outlier                          AS outlier_vendor,
    monthly_patient_days             AS monthly_patient_days_vendor,
    extended_fee                     AS extended_fee_vendor,
    discharge                        AS discharges_vendor,
    discharge_patient_days           AS discharge_patient_days_vendor,
    total_patient_days               AS total_patient_days_vendor,
    client_name                      AS pharmacy_name,
    address1                         AS pharmacy_address,
    service_area_description         AS pharmacy_service_area_vendor,
    master_service_area_description  AS pharmacy_master_service_area_vendor
FROM cardinal_dcoa_transactions
