SELECT
    CASE
        WHEN ptn_pay.hvid IS NOT NULL
            THEN ptn_pay.hvid
        WHEN dgn_pay.patientid IS NOT NULL
            THEN CONCAT('493_', dgn_pay.patientid)
        ELSE NULL
    END                                                                                     AS hvid,
    dgn.claim_id,
    dgn.diagnosis_code,
    ptn_pay.yearofbirth,
    ptn_pay.age,
    ptn_pay.gender,
    ptn_pay.state,
    ptn_pay.threedigitzip,
    MAX(dgn.hvjoinkey)                                                                      AS hvjoinkey,
    MAX(dgn.input_file_name)                                                                AS input_file_name
 FROM dgn
 LEFT OUTER JOIN dgn_pay
              ON dgn.hvjoinkey = dgn_pay.hvjoinkey
 LEFT OUTER JOIN sentry_temp00_patient_payload_dedup ptn_pay
              ON dgn_pay.patientid = ptn_pay.patientid
/* Eliminate column headers. */
WHERE UPPER(COALESCE(dgn.claim_id, '')) <> 'CLAIM_ID'
GROUP BY 1, 2, 3,4, 5, 6, 7, 8
