SELECT
    pln.hospital_id ,
    pln.encounter_id,
    pln.patient_id  ,
    pln.start_date  ,
    pln.end_date    ,
    discharge_code  ,
    pln.drug_code   ,
    pln.status      ,
    pln.sex         ,
    MAX(hvjoinkey) AS  hvjoinkey
FROM pln
WHERE
    TRIM(UPPER(COALESCE(pln.hospital_id, 'empty'))) <> 'HOSPITALID'
GROUP BY
    pln.hospital_id ,
    pln.encounter_id,
    pln.patient_id  ,
    pln.start_date  ,
    pln.end_date    ,
    pln.drug_code   ,
    discharge_code  ,
    pln.status      ,
    pln.sex
