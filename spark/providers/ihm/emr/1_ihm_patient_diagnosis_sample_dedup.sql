SELECT
    diag.hospital_id ,
    diag.encounter_id,
    diag.diagnosis
FROM diag
GROUP BY
    diag.hospital_id ,
    diag.encounter_id,
    diag.diagnosis
