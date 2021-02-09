SELECT
    diag.hospital_id ,
    diag.encounter_id,
    diag.diagnosis
FROM diag
WHERE
    TRIM(UPPER(COALESCE(diag.hospital_id, 'empty'))) <> 'HOSPITALID'
GROUP BY
    diag.hospital_id ,
    diag.encounter_id,
    diag.diagnosis
