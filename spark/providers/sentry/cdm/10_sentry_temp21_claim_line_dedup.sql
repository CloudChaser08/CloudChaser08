SELECT
    CASE
        WHEN ptn_pay.hvid IS NOT NULL
            THEN ptn_pay.hvid
        WHEN lin_pay.patientid IS NOT NULL
            THEN CONCAT('493_', lin_pay.patientid)
        ELSE NULL
    END                                                                             AS hvid,
    lin.claim_id,
    lin.charge_date,
    lin.charge_type,
    lin.charge_amount,
    lin.charge_quantity,
    lin.code,
    ptn_pay.yearofbirth,
    ptn_pay.age,
    ptn_pay.gender,
    ptn_pay.state,
    ptn_pay.threedigitzip,
    MAX(lin.input_file_name)                                                        AS input_file_name
 FROM lin
 LEFT OUTER JOIN lin_pay
              ON lin.hvjoinkey = lin_pay.hvjoinkey
 LEFT OUTER JOIN sentry_temp00_patient_payload_dedup ptn_pay
              ON lin_pay.patientid = ptn_pay.patientid
WHERE UPPER(COALESCE(lin.claim_id, '')) <> 'CLAIM_ID'
GROUP BY  1,  2,  3,  4,  5,  6,  7,  8,  9,  10,
         11, 12
