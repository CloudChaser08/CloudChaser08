SELECT
    claim_id        AS claim_id,
    hvid            AS hvid,
    date_of_service AS date_of_service,
    icd_code        AS icd_code,
    CASE 
        WHEN date_of_service >= '2015-10-01' THEN '02' 
        ELSE '01' 
    END             AS icd_code_qual
FROM records r
    INNER JOIN matching_payload ON claim_id = claimId
