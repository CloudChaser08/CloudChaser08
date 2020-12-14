SELECT
    cpt.hospital_id ,
    cpt.encounter_id,
    cpt.cpt
FROM cpt
GROUP BY
    cpt.hospital_id ,
    cpt.encounter_id,
    cpt.cpt
