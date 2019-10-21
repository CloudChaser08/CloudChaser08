SELECT
    SUM(1) OVER 
            (
                ORDER BY
                    hvid,
                    clm_attributes, 
                    admit_date_of_service, 
                    discharge_date, 
                    imported_on,
                    -- Since the above combination of columns may not be unique, 
                    -- it's important to include claim_id to ensure the sort is unique.
                    claim_id
            )                                                                               AS row_num,
    hvid,
    patientid,
    admit_date_of_service                                                                   AS admit_dt,
    discharge_date                                                                          AS disch_dt,
    imported_on,
    clm_attributes,
    claim_id,
    patient_status_code,
    patient_status,
    emergent_status,
    facility_id,
    facility_zip,
    medicare_provider_number,
    admission_source_code,
    admission_type_code,
    bill_type,
    340b_id,
    input_file_name,
    hvjoinkey,
    yearofbirth,
    age,
    gender,
    state,
    threedigitzip
 FROM
(
    SELECT
        CASE
            WHEN ptn_pay.hvid IS NOT NULL
                THEN ptn_pay.hvid
            WHEN clm_pay.patientid IS NOT NULL
                THEN CONCAT('493_', clm_pay.patientid)
            ELSE NULL
        END                                                                                 AS hvid,
        clm_pay.patientid,
        clm.claim_id,
        admit_date_of_service,
        COALESCE(discharge_date, admit_date_of_service)                                     AS discharge_date,
        imported_on,
        CONCAT
            (
                COALESCE(patient_status_code, 'na'), '_',
                COALESCE(patient_status, 'na'), '_',
                COALESCE(emergent_status, 'na'), '_',
                COALESCE(facility_id, 'na'), '_',
                COALESCE(facility_zip, 'na'), '_',
                COALESCE(medicare_provider_number, 'na'), '_',
                COALESCE(admission_source_code, 'na'), '_',
                COALESCE(admission_type_code, 'na'), '_',
                COALESCE(bill_type, 'na'), '_',
                COALESCE(340b_id, 'na')
            )                                                                               AS clm_attributes,
        patient_status_code,
        patient_status,
        emergent_status,
        facility_id,
        facility_zip,
        medicare_provider_number,
        admission_source_code,
        admission_type_code,
        bill_type,
        340b_id,
        clm.input_file_name,
        clm.hvjoinkey,
        ptn_pay.yearofbirth,
        ptn_pay.age,
        ptn_pay.gender,
        ptn_pay.state,
        ptn_pay.threedigitzip,
        ROW_NUMBER() OVER
                        (
                            PARTITION BY claim_id
                            ORDER BY imported_on DESC
                        )                                                   AS dedup_row_num
     FROM clm
     LEFT OUTER JOIN clm_pay
                  ON clm.hvjoinkey = clm_pay.hvjoinkey
     LEFT OUTER JOIN sentry_temp00_patient_payload_dedup ptn_pay
                  ON clm_pay.patientid = ptn_pay.patientid
    /* Eliminate column headers. */
    WHERE UPPER(COALESCE(clm.claim_id, '')) <> 'CLAIM_ID'
)
WHERE dedup_row_num = 1
