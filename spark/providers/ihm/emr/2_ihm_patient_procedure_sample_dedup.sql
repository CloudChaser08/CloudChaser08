SELECT
    proc.hospital_id ,
    proc.encounter_id,
    proc.procedure_name,
    proc.date_performed

FROM proc
GROUP BY
    proc.hospital_id ,
    proc.encounter_id,
    proc.procedure_name,
    proc.date_performed

