SELECT
    hvid,
    UPPER(personid) AS tsi_patient_id
FROM tsi_payload
WHERE hvid IS NOT NULL
GROUP BY 1,2