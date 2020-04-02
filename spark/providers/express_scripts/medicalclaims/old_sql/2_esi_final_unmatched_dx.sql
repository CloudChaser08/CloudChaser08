/** This table will overwrite the existing unmatched reference data */

 /** New unmatched data */
 SELECT split(hvid, '_')[1] as patient_id,
        '{VDR_FILE_DT}'     as file_date,
        *
 FROM esi_join_transactions_and_payload_dx
 WHERE instr(hvid, '_') > 0

UNION ALL

/** Reference data that is still unmatched */
SELECT *
FROM esi_join_historic_and_payload_dx
WHERE instr(hvid, '_') > 0