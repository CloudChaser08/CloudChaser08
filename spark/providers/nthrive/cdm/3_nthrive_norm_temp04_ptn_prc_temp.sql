SELECT DISTINCT
    COALESCE(ptn_prc.record_id, 'EMPTY')                    AS record_id,
    COALESCE(ptn_prc.procedure_day, 'EMPTY')                AS procedure_day,
    COALESCE(ptn_prc.icd_procedure_code, 'EMPTY')           AS icd_procedure_code
 FROM patient_procedure ptn_prc
