SELECT
    cpt.hospital_id ,
    cpt.encounter_id,
    cpt.cpt
FROM cpt
WHERE
    TRIM(UPPER(COALESCE(cpt.hospital_id, 'empty'))) <> 'HOSPITALID'
GROUP BY
    cpt.hospital_id ,
    cpt.encounter_id,
    cpt.cpt
