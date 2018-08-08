SELECT
    t.ediclaim_id                              AS claim_id,
    t.hvid                                     AS hvid,
    '08'                                       AS model_version,
    t.tenant_id                                AS vendor_org_id,
    COALESCE(p.gender, t.patientgender)        AS patient_gender,
    COALESCE(p.yearOfBirth,
             YEAR(t.patientdob)
    )                                          AS patient_year_of_birth,
    'P'                                        AS claim_type,
    extract_date(
        t.dateservicestart,
        '%Y%m%d'
    )                                          AS date_service,
    extract_date(
        t.dateservicestart,
        '%Y%m%d'
    )                                          AS date_service_end,
    t.facilitycode                             AS place_of_service_std_id,
    ARRAY(t.principaldiagnosis, t.diagnosistwo,
        t.diagnosisthree, t.diagnosisfour, t.diagnosisfive,
        t.diagnosissix, t.diagnosisseven, t.diagnosiseight,
        t.diagnosisnine, t.diagnosisten, t.diagnosiseleven,
        t.diagnosistwelve
        )[c_explode.n]                         AS diagnosis_code,
    t.submittedchargetotal                     AS total_charge,
    CASE
        WHEN t.billprovideridqualifier = 'XX'
             AND 11 = LENGTH(TRIM(COALESCE(t.billprovidernpid, '')))
             THEN COALESCE(t.billproviderid, t.billprovidernpid)
        WHEN t.billprovideridqualifier = 'XX'
             THEN t.billproviderid
        ELSE NULL
    END                                        AS prov_billing_npi,
    t.payerid                                  AS payer_vendor_id,
    t.payername                                AS payer_name,
    CASE
        WHEN t.billprovideridqualifier <> 'XX'
             AND 0 <> LENGTH(TRIM(COALESCE(t.billproviderid, '')))
             THEN t.billproviderid
        ELSE NULL
    END                                        AS prov_billing_vendor_id,
    t.billprovidername                         AS prov_billing_name_1,
    t.billprovidertaxonomycode                 AS prov_billing_std_taxonomy
FROM limited_transactional_cardinal_pms t
    LEFT OUTER JOIN matching_payload p
    ON t.hvJoinKey = p.hvJoinKey
    CROSS JOIN claim_exploder c_explode
    LEFT OUTER JOIN service_line_diags d
    ON t.ediclaim_id = d.claim_id AND 
       ARRAY(t.principaldiagnosis, t.diagnosistwo, t.diagnosisthree, t.diagnosisfour,
            t.diagnosisfive, t.diagnosissix, t.diagnosisseven, t.diagnosiseight,
            t.diagnosisnine, t.diagnosisten, t.diagnosiseleven, t.diagnosistwelve
            )[c_explode.n] = d.diagnosis_code
WHERE
    -- Filter out cases from explosion where diagnosis_code would be null
    (
        ARRAY(t.principaldiagnosis, t.diagnosistwo, t.diagnosisthree, t.diagnosisfour,
            t.diagnosisfive, t.diagnosissix, t.diagnosisseven, t.diagnosiseight,
            t.diagnosisnine, t.diagnosisten, t.diagnosiseleven, t.diagnosistwelve
            )[c_explode.n] IS NOT NULL
    )
    AND
    -- Only include the row if the diagnosis is not a service-line diagnosis.
    -- This would mean that doing a LEFT OUTER JOIN on service_line_diags temp
    -- table would have null values for d.diagnosis_code and d.claim_id 
    -- (b/c no service line diagnosis existed)
    (
        d.diagnosis_code IS NULL AND d.claim_id IS NULL
    )
DISTRIBUTE BY claim_id
