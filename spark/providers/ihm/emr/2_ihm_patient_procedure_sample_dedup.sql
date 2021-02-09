SELECT
    proc.hospital_id ,
    proc.encounter_id,
    proc.procedure_name,
    proc.date_performed

FROM proc
WHERE
    TRIM(UPPER(COALESCE(proc.hospital_id, 'empty'))) <> 'HOSPITALID'
GROUP BY
    proc.hospital_id ,
    proc.encounter_id,
    proc.procedure_name,
    proc.date_performed

