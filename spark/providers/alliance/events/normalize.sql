SELECT
    p.hvid                                  AS hvid,
    p.age                                   AS patient_age,
    p.yearOfBirth                           AS patient_year_of_birth,
    SUBSTR(TRIM(p.threeDigitZip), 1, 3)     AS patient_zip3,
    TRIM(UPPER(p.state))                    AS patient_state,
    p.gender                                AS patient_gender,
    CASE
        WHEN SUBSTR(TRIM(t.naics_code), 1, 3) = '813' THEN NULL
        ELSE t.merchant_name
    END                                     AS event,
    extract_date(
        t.transaction_date,
        '%Y-%m-%d'
    )                                       AS event_date,
    SUBSTR(TRIM(t.zip5), 1, 3)              AS event_zip,
    CASE
        WHEN CAST(ROUND(t.transaction_usd, 0) AS INTEGER) < -1000 THEN -1000
        WHEN CAST(ROUND(t.transaction_usd, 0) AS INTEGER) > 1000 THEN 1000
        ELSE CAST(ROUND(t.transaction_usd, 0) AS INTEGER)
    END                                     AS event_revenue,
    CASE
        WHEN SUBSTR(TRIM(t.naics_code), 1, 3) = '813' THEN NULL
        ELSE t.naics_code
    END                                     AS event_category_code,
    CASE
        WHEN SUBSTR(TRIM(t.naics_code), 1, 3) = '813' THEN NULL
        WHEN t.naics_code IS NULL THEN NULL
        ELSE 'NAICS_CODE'
    END                                     AS event_category_code_qual,
    CASE
        WHEN SUBSTR(TRIM(t.naics_code), 1, 3) = '813' THEN NULL
        WHEN t.naics_code IS NULL THEN NULL
        ELSE t.naics_desc
    END                                     AS event_category_name,
    CASE
        WHEN SUBSTR(TRIM(t.cnp_ind), 1, 1) IN ('0', 'N') THEN 'N'
        WHEN SUBSTR(TRIM(t.cnp_ind), 1, 1) IN ('1', 'Y') THEN 'Y'
        ELSE NULL
    END                                     AS event_category_flag,
    CASE
        WHEN SUBSTR(TRIM(t.cnp_ind), 1, 1) IN ('0', '1', 'Y', 'N') THEN 'CNP_IND'
        ELSE NULL
    END                                     AS event_category_flag_qual
FROM alliance_transactions t
INNER JOIN matching_payload p ON t.agility_id = p.personId
