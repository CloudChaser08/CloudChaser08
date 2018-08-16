SELECT
    CASE WHEN matchStatus = 'exact_match' OR matchStatus = 'inexact_match' OR matchStatus = 'multi_match'
        THEN LOWER(obfuscate_hvid(hvid, 'hvid265'))
    END                                         AS hvid,
    CASE WHEN (matchStatus != 'exact_match' AND matchStatus != 'inexact_match' AND matchStatus != 'multi_match')
            OR matchStatus IS NULL
        THEN gen_uuid()
    END                                         AS temporary_id,
    matchScore                                  AS match_score,
    yearOfBirth                                 AS year_of_birth,
    p.age                                       AS age,
    CASE WHEN UPPER(p.gender) IN ('M', 'F', 'U')
        THEN UPPER(p.gender)
    END                                         AS gender,
    threeDigitZip                               AS zip3,
    p.state                                     AS state,
    status                                      AS status,
    symptoms                                    AS symptoms,
    hcp_1_npi                                   AS hcp_1_npi,
    hcp_1_first_name                            AS hcp_1_first_name,
    hcp_1_last_name                             AS hcp_1_last_name,
    hcp_1_address                               AS hcp_1_address,
    hcp_1_city                                  AS hcp_1_city,
    hcp_1_state                                 AS hcp_1_state,
    hcp_1_zip_code                              AS hcp_1_zip_code,
    hcp_1_email                                 AS hcp_1_email,
    hcp_1_role                                  AS hcp_1_role,
    hcp_2_npi                                   AS hcp_2_npi,
    hcp_2_first_name                            AS hcp_2_first_name,
    hcp_2_last_name                             AS hcp_2_last_name,
    hcp_2_address                               AS hcp_2_address,
    hcp_2_city                                  AS hcp_2_city,
    hcp_2_state                                 AS hcp_2_state,
    hcp_2_zip_code                              AS hcp_2_zip_code,
    hcp_2_email                                 AS hcp_2_email,
    hcp_2_role                                  AS hcp_2_role,
    treating_site_npi                           AS treating_site_npi,
    treating_site_name                          AS treating_site_name,
    payer_id                                    AS payer_id,
    payer_name                                  AS payer_name,
    payer_plan_id                               AS payer_plan_id,
    payer_plan_name                             AS payer_plan_name,
    data_source                                 AS data_source,
    crm_id_1                                    AS crm_id_1,
    crm_id_2                                    AS crm_id_2,
    activity_date                               AS activity_date
FROM matching_payload p
    LEFT JOIN haystack_raw USING (hvJoinKey)
