SELECT 
    hvid,
    patientid,
    threedigitzip,
    yearofbirth,
    gender,
    age,
    state
 FROM pat_pay
WHERE UPPER(COALESCE(hvid, '')) <> 'HVID'
GROUP BY 
    hvid,
    patientid,
    threedigitzip,
    yearofbirth,
    gender,
    age,
    state
