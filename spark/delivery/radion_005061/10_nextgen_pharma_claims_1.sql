SELECT 
    CAST(med.row_id AS BIGINT)                                                              AS record_id,
    med.hv_medctn_id                                                                        AS claim_id,
    med.hvid                                                                                AS hvid,
    med.crt_dt                                                                              AS created,
    '7'                                                                                     AS model_version,
    med.data_set_nm                                                                         AS data_set,
    CAST(med.hvm_vdr_feed_id AS STRING)                                                     AS data_feed,
    CAST(med.hvm_vdr_id AS STRING)                                                          AS data_vendor,
    med.src_vrsn_id                                                                         AS source_version,
    med.ptnt_gender_cd                                                                      AS patient_gender,
    med.ptnt_age_num                                                                        AS patient_age,
    CAST(med.ptnt_birth_yr AS STRING)                                                       AS patient_year_of_birth,
    med.ptnt_zip3_cd                                                                        AS patient_zip3,
    med.ptnt_state_cd                                                                       AS patient_state, 
    CAST(NULL AS DATE)                                                                      AS date_service,
    /* COALESCE the calculated Medication date with calculated Encounter date. */
    COALESCE
        (
            med.hv_medctn_dt,
            enc.hv_enc_dt
        )                                                                                   AS date_written,
    CAST(NULL AS STRING)                                                                    AS year_of_injury,
    CAST(NULL AS DATE)                                                                      AS date_authorized,
    CAST(NULL AS STRING)                                                                    AS time_authorized,
    CAST(NULL AS DATE)                                                                      AS discharge_date,
    CAST(NULL AS STRING)                                                                    AS transaction_code_std,
    CAST(NULL AS STRING)                                                                    AS transaction_code_vendor,
    CAST(NULL AS STRING)                                                                    AS response_code_std,
    CAST(NULL AS STRING)                                                                    AS response_code_vendor,
    CAST(NULL AS STRING)                                                                    AS reject_reason_code_1,
    CAST(NULL AS STRING)                                                                    AS reject_reason_code_2,
    CAST(NULL AS STRING)                                                                    AS reject_reason_code_3,
    CAST(NULL AS STRING)                                                                    AS reject_reason_code_4,
    CAST(NULL AS STRING)                                                                    AS reject_reason_code_5,
    med.medctn_diag_cd                                                                      AS diagnosis_code,
    CAST(NULL AS STRING)                                                                    AS diagnosis_code_qual,
    CAST(NULL AS STRING)                                                                    AS procedure_code,
    CAST(NULL AS STRING)                                                                    AS procedure_code_qual,
    med.medctn_ndc                                                                          AS ndc_code,
    med.hv_enc_id                                                                           AS product_service_id,
    'HV_ENC_ID'                                                                             AS product_service_id_qual,
    CAST(NULL AS STRING)                                                                    AS rx_number,
    CAST(NULL AS STRING)                                                                    AS rx_number_qual,
    CAST(NULL AS STRING)                                                                    AS bin_number,
    CAST(NULL AS STRING)                                                                    AS processor_control_number,
    CAST(NULL AS INTEGER)                                                                   AS fill_number,
    med.medctn_remng_rfll_qty                                                               AS refill_auth_amount,
    CAST(med.medctn_qty AS FLOAT)                                                           AS dispensed_quantity,
    CAST(NULL AS STRING)                                                                    AS unit_of_measure,
    CAST(med.medctn_days_supply_cnt AS INTEGER)                                             AS days_supply,
    CAST(NULL AS STRING)                                                                    AS pharmacy_npi,
    CAST(NULL AS STRING)                                                                    AS prov_dispensing_npi,
    CAST(NULL AS STRING)                                                                    AS payer_id,
    CAST(NULL AS STRING)                                                                    AS payer_id_qual,
    CAST(NULL AS STRING)                                                                    AS payer_name,
    CAST(NULL AS STRING)                                                                    AS payer_parent_name,
    CAST(NULL AS STRING)                                                                    AS payer_org_name,
    CAST(NULL AS STRING)                                                                    AS payer_plan_id,
    CAST(NULL AS STRING)                                                                    AS payer_plan_name,
    CAST(NULL AS STRING)                                                                    AS payer_type,
    CAST(NULL AS STRING)                                                                    AS compound_code,
    /* As per Reyna, do not map to unit_dose_indicator. It's an NCPDP code. */
    CAST(NULL AS STRING)                                                                    AS unit_dose_indicator, 
    CAST(NULL AS STRING)                                                                    AS dispensed_as_written,
    CAST(NULL AS STRING)                                                                    AS prescription_origin,
    CAST(NULL AS STRING)                                                                    AS submission_clarification,
    CAST(NULL AS STRING)                                                                    AS orig_prescribed_product_service_code,
    CAST(NULL AS STRING)                                                                    AS orig_prescribed_product_service_code_qual,
    CAST(NULL AS STRING)                                                                    AS orig_prescribed_quantity,
    CAST(NULL AS STRING)                                                                    AS prior_auth_type_code,
    CAST(NULL AS STRING)                                                                    AS level_of_service,
    CAST(NULL AS STRING)                                                                    AS reason_for_service,
    CAST(NULL AS STRING)                                                                    AS professional_service_code,
    CAST(NULL AS STRING)                                                                    AS result_of_service_code,
    CAST(NULL AS STRING)                                                                    AS prov_prescribing_npi,
    CAST(NULL AS STRING)                                                                    AS prov_prescribing_tax_id,
    CAST(NULL AS STRING)                                                                    AS prov_prescribing_dea_id,
    CAST(NULL AS STRING)                                                                    AS prov_prescribing_ssn,
    CAST(NULL AS STRING)                                                                    AS prov_prescribing_state_license,
    CAST(NULL AS STRING)                                                                    AS prov_prescribing_upin,
    CAST(NULL AS STRING)                                                                    AS prov_prescribing_commercial_id,
    CAST(NULL AS STRING)                                                                    AS prov_prescribing_name_1,
    CAST(NULL AS STRING)                                                                    AS prov_prescribing_name_2,
    CAST(NULL AS STRING)                                                                    AS prov_prescribing_address_1,
    CAST(NULL AS STRING)                                                                    AS prov_prescribing_address_2,
    CAST(NULL AS STRING)                                                                    AS prov_prescribing_city,
    /* prov_prescribing_state */
    CASE
        WHEN med.medctn_prov_state_cd_qual = 'ORDERING_PROVIDER'
            THEN med.medctn_prov_state_cd
        ELSE CAST(NULL AS STRING)
    END                                                                                     AS prov_prescribing_state,
    /* prov_prescribing_zip */
    CASE
        WHEN med.medctn_prov_zip_cd_qual = 'ORDERING_PROVIDER'
            THEN med.medctn_prov_zip_cd
        ELSE CAST(NULL AS STRING)
    END                                                                                     AS prov_prescribing_zip,
    /* prov_prescribing_std_taxonomy */
    CASE
        WHEN med.medctn_prov_taxnmy_id_qual = 'ORDERING_PROVIDER'
            THEN med.medctn_prov_taxnmy_id
        ELSE CAST(NULL AS STRING)
    END                                                                                     AS prov_prescribing_std_taxonomy,
    /* prov_prescribing_vendor_specialty */
    CASE
        WHEN med.medctn_prov_speclty_id_qual = 'ORDERING_PROVIDER'
            THEN med.medctn_prov_speclty_id
        ELSE CAST(NULL AS STRING)
    END                                                                                     AS prov_prescribing_vendor_specialty,
    CAST(NULL AS STRING)                                                                    AS prov_primary_care_npi,
    CAST(NULL AS STRING)                                                                    AS cob_count,
    CAST(NULL AS FLOAT)                                                                     AS usual_and_customary_charge,
    CAST(NULL AS FLOAT)                                                                     AS product_selection_attributed,
    CAST(NULL AS FLOAT)                                                                     AS other_payer_recognized,
    CAST(NULL AS FLOAT)                                                                     AS periodic_deductible_applied,
    CAST(NULL AS FLOAT)                                                                     AS periodic_benefit_exceed,
    CAST(NULL AS FLOAT)                                                                     AS accumulated_deductible,
    CAST(NULL AS FLOAT)                                                                     AS remaining_deductible,
    CAST(NULL AS FLOAT)                                                                     AS remaining_benefit,
    CAST(NULL AS FLOAT)                                                                     AS copay_coinsurance,
    CAST(NULL AS STRING)                                                                    AS basis_of_cost_determination,
    CAST(NULL AS FLOAT)                                                                     AS submitted_ingredient_cost,
    CAST(NULL AS FLOAT)                                                                     AS submitted_dispensing_fee,
    CAST(NULL AS FLOAT)                                                                     AS submitted_incentive,
    CAST(NULL AS FLOAT)                                                                     AS submitted_gross_due,
    CAST(NULL AS FLOAT)                                                                     AS submitted_professional_service_fee,
    CAST(NULL AS FLOAT)                                                                     AS submitted_patient_pay,
    CAST(NULL AS FLOAT)                                                                     AS submitted_other_claimed_qual,
    CAST(NULL AS FLOAT)                                                                     AS submitted_other_claimed,
    CAST(NULL AS STRING)                                                                    AS basis_of_reimbursement_determination,
    CAST(NULL AS FLOAT)                                                                     AS paid_ingredient_cost,
    CAST(NULL AS FLOAT)                                                                     AS paid_dispensing_fee,
    CAST(NULL AS FLOAT)                                                                     AS paid_incentive,
    CAST(NULL AS FLOAT)                                                                     AS paid_gross_due,
    CAST(NULL AS FLOAT)                                                                     AS paid_professional_service_fee,
    CAST(NULL AS FLOAT)                                                                     AS paid_patient_pay,
    CAST(NULL AS FLOAT)                                                                     AS paid_other_claimed_qual,
    CAST(NULL AS FLOAT)                                                                     AS paid_other_claimed,
    CAST(NULL AS STRING)                                                                    AS tax_exempt_indicator,
    CAST(NULL AS STRING)                                                                    AS coupon_type,
    CAST(NULL AS STRING)                                                                    AS coupon_number,
    CAST(NULL AS FLOAT)                                                                     AS coupon_value,
    CAST(NULL AS STRING)                                                                    AS pharmacy_other_id,
    CAST(NULL AS STRING)                                                                    AS pharmacy_other_qual,
    CAST(NULL AS STRING)                                                                    AS pharmacy_postal_code,
    CAST(NULL AS STRING)                                                                    AS prov_dispensing_id,
    CAST(NULL AS STRING)                                                                    AS prov_dispensing_qual,
    CAST(NULL AS STRING)                                                                    AS prov_prescribing_id,
    CAST(NULL AS STRING)                                                                    AS prov_prescribing_qual,
    CAST(NULL AS STRING)                                                                    AS prov_primary_care_id,
    CAST(NULL AS STRING)                                                                    AS prov_primary_care_qual,
    CAST(NULL AS STRING)                                                                    AS other_payer_coverage_type,
    CAST(NULL AS STRING)                                                                    AS other_payer_coverage_id,
    CAST(NULL AS STRING)                                                                    AS other_payer_coverage_qual,
    CAST(NULL AS DATE)                                                                      AS other_payer_date,
    CAST(NULL AS STRING)                                                                    AS other_payer_coverage_code,
    CAST(NULL AS STRING)                                                                    AS logical_delete_reason, 
    'nextgen'                                                                               AS part_provider,
    med.part_mth                                                                            AS part_best_date
 FROM hvm_emr_medctn_v09 med
 LEFT OUTER JOIN
(
    SELECT *
     FROM hvm_emr_enc_v08 enc1
    WHERE enc1.part_hvm_vdr_feed_id = '35'
      AND enc1.hvid IS NOT NULL
      AND enc1.enc_start_dt IS NOT NULL
      /* Reduce the encounter data to one row per hv_enc_id - the most recent.
         Eliminate rows where another row exists with a later create date,
         with a "larger" dataset name, or the same dataset name and a larger row ID. */
      AND NOT EXISTS
        (
            SELECT 1
             FROM hvm_emr_enc_v08 enc2
            WHERE enc2.part_hvm_vdr_feed_id = '35'
              AND enc2.hvid IS NOT NULL
              AND enc2.enc_start_dt IS NOT NULL
              
              AND enc1.part_hvm_vdr_feed_id = '35'
              AND enc1.hvid IS NOT NULL
              AND enc1.enc_start_dt IS NOT NULL
              
              AND enc1.hv_enc_id = enc2.hv_enc_id
              
              AND
                /* Later create date, larger data_set_nm. */
                (
                    enc2.crt_dt > enc1.crt_dt
                 OR enc2.data_set_nm > enc1.data_set_nm
                 OR
                 /* Same dataset, but a larger row_id. */
                    (
                        enc2.data_set_nm = enc1.data_set_nm
                    AND enc2.row_id > enc1.row_id
                    )
                )
        )
) enc
   ON med.part_hvm_vdr_feed_id = '35'
  AND med.hvid IS NOT NULL
  AND med.medctn_ndc IS NOT NULL
  AND COALESCE(med.hv_enc_id, 'EMPTY') = COALESCE(enc.hv_enc_id, 'UNPOPULATED')
WHERE med.part_hvm_vdr_feed_id = '35'
  AND med.hvid IS NOT NULL
  AND med.medctn_ndc IS NOT NULL
