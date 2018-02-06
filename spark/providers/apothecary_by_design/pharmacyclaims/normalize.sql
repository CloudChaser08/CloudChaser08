SELECT
    ad.sales_cd                                 AS claim_id,
    pay.hvid                                    AS hvid,
    '6'                                         AS model_version,
    COALESCE(pay.gender, txn.patient_sex_cd)    AS patient_gender,
    COALESCE(pay.age, txn.patient_age_nbr)      AS patient_age,
    COALESCE(pay.yearOfBirth, txn.patient_dob)  AS patient_year_of_birth,
    COALESCE(pay.threeDigitZip,
        SUBSTR(txn.patient_ship_zip_cd, 1, 3))  AS patient_zip3,
    TRIM(UPPER(COALESCE(pay.state,
        txn.patient_ship_state_cd)))            AS patient_state,
    extract_date(
        COALESCE(ad.fill_dt, ad.ticket_dt),
        '%Y%m%d'
    )                                           AS date_service,
    ad.national_drug_cd                         AS ndc_code,
    ad.rx_nbr                                   AS rx_number,
    CASE
        WHEN ad.rx_nbr IS NULL THEN NULL
        ELSE 'VENDOR'
    END                                         AS rx_number_qual,
    ad.ins_ref_bin_nbr                          AS bin_number,
    CASE
        WHEN UPPER(txn.ins1_proc_cntrl_cd) = 'UNKNOWN' THEN NULL
        ELSE txn.ins1_proc_cntrl_cd
    END                                         AS processor_control_number,
    ad.fill_nbr                                 AS fill_number,
    ad.refill_remain_qty                        AS refill_auth_amount,
    ad.fill_qty                                 AS dispensed_quantity,
    ad.days_supply_qty                          AS days_supply,
    TRIM(UPPER(ad.ins_ref_nam))                 AS payer_name,
    TRIM(UPPER(ad.ins1_plan_type_cd))           AS payer_type,
    CASE
        WHEN ad.is_compound_flg = 'True' THEN '2'
        WHEN ad.is_compound_flg = 'False' THEN '1'
        ELSE '0'
    END                                         AS compound_code,
    CASE
        WHEN txn.doctor_natl_provider_id = '0' THEN NULL
        ELSE txn.doctor_natl_provider_id
    END                                         AS prov_prescribing_npi,
    ad.abd_location_id                          AS pharmacy_other_id,
    CASE
        WHEN ad.abd_location_id = NULL THEN NULL
        ELSE 'VENDOR'
    END                                         AS pharmacy_other_qual
FROM abd_additional_data ad
    LEFT OUTER JOIN abd_transactions txn ON txn.sales_id = ad.sales_cd
    LEFT OUTER JOIN matching_payload pay ON txn.hvjoinkey = pay.hvJoinKey
WHERE
    is_active_flg = 'True'
    AND ad.sales_cd <> 'sales_cd'
