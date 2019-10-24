SELECT
    clm_prt.hvid,
    enc.parent                                                                          AS enc_id,
    enc.admit_dt,
    enc.disch_dt,
    enc.imported_on,
    clm_prt.patientid,
    clm_prt.claim_id,  -- claim_id added to populate provider table for 340B and Medicare Provider Number
    clm_prt.patient_status_code,
    clm_prt.patient_status,
    clm_prt.emergent_status,
    clm_prt.facility_id,
    clm_prt.facility_zip,
    clm_prt.medicare_provider_number,
    clm_prt.admission_source_code,
    clm_prt.admission_type_code,
    clm_prt.bill_type,
    clm_prt.340b_id,
    clm_prt.input_file_name,
    clm_prt.yearofbirth,
    clm_prt.age,
    clm_prt.gender,
    clm_prt.state,
    clm_prt.threedigitzip
 FROM
(
    -- Retrieve the earliest admit date, latest discharge date, and latest
    -- imported_on timestamp from all of the claims in each encounter.
    SELECT
        prt.parent,
        MIN(clm_chd.admit_dt)                                                           AS admit_dt,
        MAX(clm_chd.disch_dt)                                                           AS disch_dt,
        MAX(clm_chd.imported_on)                                                        AS imported_on
     FROM sentry_temp14_parent_child prt 
    INNER JOIN sentry_temp11_claim clm_chd
            ON clm_chd.row_num = prt.child
    GROUP BY prt.parent
) enc
INNER JOIN sentry_temp11_claim clm_prt
        ON enc.parent = clm_prt.row_num
