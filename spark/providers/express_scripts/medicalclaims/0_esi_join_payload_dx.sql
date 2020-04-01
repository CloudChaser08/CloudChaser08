SELECT /** BROADCAST (matching_payload), BROADCAST (dx_pay), BROADCAST (rx_payloads) */
    COALESCE(dx_pay.privateidone, 'UNAVAILABLE')                                            AS claim_id,
    dx_pay.hvjoinkey AS hvjoinkey,
    CASE 
        WHEN rx_pay.hvid IS NOT NULL 
            THEN rx_pay.hvid 
        ELSE CONCAT('17_', dx_pay.patientid)
    END                                                                                     AS hvid, 
    CURRENT_DATE()                                                                          AS created, 
	/* patient_gender */
  CASE
      WHEN SUBSTR(UPPER(rx_pay.gender), 1, 1) IN ('F', 'M') THEN SUBSTR(UPPER(rx_pay.gender), 1, 1)
      ELSE 'U' 
  END                                                                                   AS patient_gender, 
  rx_pay.year_of_birth AS year_of_birth,
  /* patient_zip3 */
  CASE
    WHEN SUBSTR(rx_pay.zip, 1, 3) IN ('036','102','203','205','369','556','692','821','823','878','879','884','893')
      THEN '000'
    ELSE
      SUBSTR(rx_pay.zip, 1, 3)
  END                                      AS patient_zip3

FROM matching_payload dx_pay
LEFT OUTER JOIN rx_payloads rx_pay ON dx_pay.patientid = rx_pay.patient_id
