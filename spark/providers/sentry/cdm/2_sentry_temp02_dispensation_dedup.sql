SELECT
    hvid,
    dispensation_id,
    service_date,
    charge_amount,
    ndc,
    brand_generic_name,
    dispensation_quantity,
    hcpcs_code,
    hcpcs_quantity,
    yearofbirth,
    age,
    gender,
    state,
    threedigitzip,
    hvjoinkey,
    input_file_name,
    CONCAT_WS(', ', COLLECT_LIST(molecule))                                                 AS molecule
 FROM
(
    SELECT
        CASE
            WHEN ptn_pay.hvid IS NOT NULL
                THEN ptn_pay.hvid
            WHEN dsp_pay.patientid IS NOT NULL
                THEN CONCAT('493_', dsp_pay.patientid)
            ELSE NULL
        END                                                                                     AS hvid,
        dsp.dispensation_id,
        dsp.service_date,
        dsp.charge_amount,
        dsp.ndc,
        dsp.brand_generic_name,
        dsp.dispensation_quantity,
        dsp.hcpcs_code,
        dsp.hcpcs_quantity,
        ptn_pay.yearofbirth,
        ptn_pay.age,
        ptn_pay.gender,
        ptn_pay.state,
        ptn_pay.threedigitzip,
        dsp.molecule,
        MAX(dsp.hvjoinkey)                                                                      AS hvjoinkey,
        MAX(dsp.input_file_name)                                                                AS input_file_name
     FROM dsp
     LEFT OUTER JOIN dsp_pay
                  ON dsp.hvjoinkey = dsp_pay.hvjoinkey
     LEFT OUTER JOIN sentry_temp00_patient_payload_dedup ptn_pay
                  ON dsp_pay.patientid = ptn_pay.patientid
    /* Eliminate column headers. */
    WHERE UPPER(COALESCE(dsp.dispensation_id, '')) <> 'DISPENSATION_ID'
    GROUP BY  1,  2,  3,  4,  5,  6,  7,  8,  9, 10,
             11, 12, 13, 14, 15
)
GROUP BY  1,  2,  3,  4,  5,  6,  7,  8,  9, 10,
         11, 12, 13, 14, 15, 16
