SELECT DISTINCT
    COALESCE(ptn_chg.record_id, 'EMPTY')            AS record_id,
    COALESCE(ptn_chg.service_day, 'EMPTY')          AS service_day,
    COALESCE(ptn_chg.cpt_code, 'EMPTY')             AS cpt_code
 FROM nthrive_patient_charges ptn_chg