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
    NULL                                                                                    AS date_service,
    -- COALESCE the primary date with other dates available.
    COALESCE
        (
            med.medctn_admin_dt,
            med.medctn_start_dt,
            med.medctn_end_dt,
            med.enc_dt,
            enc.enc_start_dt, 
            enc.enc_end_dt
        )                                                                                   AS date_written,
    NULL                                                                                    AS year_of_injury,
    NULL                                                                                    AS date_authorized,
    NULL                                                                                    AS time_authorized,
    NULL                                                                                    AS discharge_date,
    NULL                                                                                    AS transaction_code_std,
    NULL                                                                                    AS transaction_code_vendor,
    NULL                                                                                    AS response_code_std,
    NULL                                                                                    AS response_code_vendor,
    NULL                                                                                    AS reject_reason_code_1,
    NULL                                                                                    AS reject_reason_code_2,
    NULL                                                                                    AS reject_reason_code_3,
    NULL                                                                                    AS reject_reason_code_4,
    NULL                                                                                    AS reject_reason_code_5,
    NULL                                                                                    AS diagnosis_code,
    NULL                                                                                    AS diagnosis_code_qual,
    NULL                                                                                    AS procedure_code,
    NULL                                                                                    AS procedure_code_qual,
    med.medctn_ndc                                                                          AS ndc_code,
    med.hv_enc_id                                                                           AS product_service_id,
    'HV_ENC_ID'                                                                             AS product_service_id_qual,
    NULL                                                                                    AS rx_number,
    NULL                                                                                    AS rx_number_qual,
    NULL                                                                                    AS bin_number,
    NULL                                                                                    AS processor_control_number,
    NULL                                                                                    AS fill_number,
    NULL                                                                                    AS refill_auth_amount,
    med.medctn_dispd_qty                                                                    AS dispensed_quantity,
    NULL                                                                                    AS unit_of_measure,
    med.medctn_days_supply_qty                                                              AS days_supply,
    NULL                                                                                    AS pharmacy_npi,
    NULL                                                                                    AS prov_dispensing_npi,
    NULL                                                                                    AS payer_id,
    NULL                                                                                    AS payer_id_qual,
    NULL                                                                                    AS payer_name,
    NULL                                                                                    AS payer_parent_name,
    NULL                                                                                    AS payer_org_name,
    NULL                                                                                    AS payer_plan_id,
    NULL                                                                                    AS payer_plan_name,
    NULL                                                                                    AS payer_type,
    NULL                                                                                    AS compound_code,
    -- As per Reyna, do not map to unit_dose_indicator. It's an NCPDP code.
    NULL                                                                                    AS unit_dose_indicator, 
    NULL                                                                                    AS dispensed_as_written,
    NULL                                                                                    AS prescription_origin,
    NULL                                                                                    AS submission_clarification,
    NULL                                                                                    AS orig_prescribed_product_service_code,
    NULL                                                                                    AS orig_prescribed_product_service_code_qual,
    NULL                                                                                    AS orig_prescribed_quantity,
    NULL                                                                                    AS prior_auth_type_code,
    NULL                                                                                    AS level_of_service,
    NULL                                                                                    AS reason_for_service,
    NULL                                                                                    AS professional_service_code,
    NULL                                                                                    AS result_of_service_code,
    NULL                                                                                    AS prov_prescribing_npi,
    NULL                                                                                    AS prov_prescribing_tax_id,
    NULL                                                                                    AS prov_prescribing_dea_id,
    NULL                                                                                    AS prov_prescribing_ssn,
    NULL                                                                                    AS prov_prescribing_state_license,
    NULL                                                                                    AS prov_prescribing_upin,
    NULL                                                                                    AS prov_prescribing_commercial_id,
    NULL                                                                                    AS prov_prescribing_name_1,
    NULL                                                                                    AS prov_prescribing_name_2,
    NULL                                                                                    AS prov_prescribing_address_1,
    NULL                                                                                    AS prov_prescribing_address_2,
    NULL                                                                                    AS prov_prescribing_city,
    med.medctn_ordg_prov_state_cd                                                           AS prov_prescribing_state,
    NULL                                                                                    AS prov_prescribing_zip, 
    med.medctn_ordg_prov_taxnmy_id                                                          AS prov_prescribing_std_taxonomy,
    med.medctn_ordg_prov_speclty_id                                                         AS prov_prescribing_vendor_specialty,
    NULL                                                                                    AS prov_primary_care_npi,
    NULL                                                                                    AS cob_count,
    NULL                                                                                    AS usual_and_customary_charge,
    NULL                                                                                    AS product_selection_attributed,
    NULL                                                                                    AS other_payer_recognized,
    NULL                                                                                    AS periodic_deductible_applied,
    NULL                                                                                    AS periodic_benefit_exceed,
    NULL                                                                                    AS accumulated_deductible,
    NULL                                                                                    AS remaining_deductible,
    NULL                                                                                    AS remaining_benefit,
    NULL                                                                                    AS copay_coinsurance,
    NULL                                                                                    AS basis_of_cost_determination,
    NULL                                                                                    AS submitted_ingredient_cost,
    NULL                                                                                    AS submitted_dispensing_fee,
    NULL                                                                                    AS submitted_incentive,
    NULL                                                                                    AS submitted_gross_due,
    NULL                                                                                    AS submitted_professional_service_fee,
    NULL                                                                                    AS submitted_patient_pay,
    NULL                                                                                    AS submitted_other_claimed_qual,
    NULL                                                                                    AS submitted_other_claimed,
    NULL                                                                                    AS basis_of_reimbursement_determination,
    NULL                                                                                    AS paid_ingredient_cost,
    NULL                                                                                    AS paid_dispensing_fee,
    NULL                                                                                    AS paid_incentive,
    NULL                                                                                    AS paid_gross_due,
    NULL                                                                                    AS paid_professional_service_fee,
    NULL                                                                                    AS paid_patient_pay,
    NULL                                                                                    AS paid_other_claimed_qual,
    NULL                                                                                    AS paid_other_claimed,
    NULL                                                                                    AS tax_exempt_indicator,
    NULL                                                                                    AS coupon_type,
    NULL                                                                                    AS coupon_number,
    NULL                                                                                    AS coupon_value,
    NULL                                                                                    AS pharmacy_other_id,
    NULL                                                                                    AS pharmacy_other_qual,
    NULL                                                                                    AS pharmacy_postal_code,
    NULL                                                                                    AS prov_dispensing_id,
    NULL                                                                                    AS prov_dispensing_qual,
    NULL                                                                                    AS prov_prescribing_id,
    NULL                                                                                    AS prov_prescribing_qual,
    NULL                                                                                    AS prov_primary_care_id,
    NULL                                                                                    AS prov_primary_care_qual,
    NULL                                                                                    AS other_payer_coverage_type,
    NULL                                                                                    AS other_payer_coverage_id,
    NULL                                                                                    AS other_payer_coverage_qual,
    NULL                                                                                    AS other_payer_date,
    NULL                                                                                    AS other_payer_coverage_code,
    (CASE WHEN med.rec_stat_cd IS NOT NULL
               THEN CONCAT('rec_stat_cd: ', med.rec_stat_cd)
          ELSE NULL
      END)                                                                                  AS logical_delete_reason, 
    'allscripts'                                                                            AS part_provider,
    CONCAT(med.part_mth, '-01')                                                             AS part_processdate
 FROM hvm_emr_medctn med
 LEFT OUTER JOIN
(
    SELECT *
     FROM hvm_emr_enc enc1
    WHERE enc1.part_hvm_vdr_feed_id = '25'
      AND enc1.hvid IS NOT NULL
      AND enc1.enc_start_dt IS NOT NULL
      AND enc1.prmy_src_tbl_nm <> 'appointments'
      -- Reduce the encounter data to one row per hv_enc_id --  the most recent.
      -- Eliminate rows where another row exists with a later create date,
      -- with a "larger" dataset name, or the same dataset name and a larger row ID.
      AND NOT EXISTS
        (
            SELECT 1
             FROM hvm_emr_enc enc2
            WHERE enc2.part_hvm_vdr_feed_id = '25'
              AND enc2.hvid IS NOT NULL
              AND enc2.enc_start_dt IS NOT NULL
              AND enc2.prmy_src_tbl_nm <> 'appointments'
              
              AND enc1.part_hvm_vdr_feed_id = '25'
              AND enc1.hvid IS NOT NULL
              AND enc1.enc_start_dt IS NOT NULL
              AND enc1.prmy_src_tbl_nm <> 'appointments'
              
              AND enc1.hv_enc_id = enc2.hv_enc_id
              
              AND
                -- Later create date, larger data_set_nm.
                (
                    enc2.crt_dt > enc1.crt_dt
                 OR enc2.data_set_nm > enc1.data_set_nm
                 OR
                 -- Same dataset, but a larger row_id.
                    (
                        enc2.data_set_nm = enc1.data_set_nm
                    AND enc2.row_id > enc1.row_id
                    )
                )
        )
) enc
   ON med.part_hvm_vdr_feed_id = '25'
  AND med.hvid IS NOT NULL
  AND med.medctn_ndc IS NOT NULL
  AND COALESCE(med.hv_enc_id, 'EMPTY') = COALESCE(enc.hv_enc_id, 'UNPOPULATED')
WHERE med.part_hvm_vdr_feed_id = '25'
  AND med.hvid IS NOT NULL
  AND med.medctn_ndc IS NOT NULL
