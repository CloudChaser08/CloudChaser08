INSERT INTO quest_merged_new SELECT
    q.accn_id,
    q.dos_id,
    q.local_order_code,
    q.standard_order_code,
    q.order_name,
    q.loinc_code,
    q.local_result_code,
    q.result_name,
    l.lab_id,
    d.date_of_service,
    d.date_of_collection,
    d.diag_code,
    d.icd_codeset_ind 
FROM quest q
    INNER JOIN quest_dos_unique d USING (accn_id, dos_id)
    INNER JOIN quest_lab l USING (accn_id, dos_id)
WHERE d.date_of_service like :year
ORDER BY d.date_of_service;
