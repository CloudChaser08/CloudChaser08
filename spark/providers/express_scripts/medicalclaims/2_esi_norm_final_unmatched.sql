/** This table will overwrite the existing unmatched reference data */

/** New unmatched data */
SELECT split(hvid, '_')[1] as patient_id,
    '{VDR_FILE_DT}'     as file_date,
    *
FROM esi_norm_cf
WHERE instr(hvid, '_') > 0
