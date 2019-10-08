SELECT
    enc_id,
    npi_number,
    physician_role
 FROM
(
    SELECT
        enc_id,
        physician_role_priority,
        claim_id,
        npi_number,
        physician_role,
        ROW_NUMBER() OVER 
                        (
                            PARTITION BY enc_id 
                            ORDER BY 
                                physician_role_priority, 
                                claim_id,
                                npi_number
                        )                                                                   AS row_num
     FROM
    (
        SELECT
            enc.parent                                                                      AS enc_id,
            CASE
                WHEN SUBSTR(UPPER(COALESCE(prv.physician_role, '')), 1, 9) = 'RENDERING'
                    THEN 1
                WHEN SUBSTR(UPPER(COALESCE(prv.physician_role, '')), 1, 9) = 'ATTENDING'
                    THEN 2
                WHEN SUBSTR(UPPER(COALESCE(prv.physician_role, '')), 1, 9) = 'ADMITTING'
                    THEN 3
                WHEN SUBSTR(UPPER(COALESCE(prv.physician_role, '')), 1, 9) = 'OPERATING'
                    THEN 4
                WHEN SUBSTR(UPPER(COALESCE(prv.physician_role, '')), 1, 9) = 'EMERGENCY'
                    THEN 5
                WHEN SUBSTR(UPPER(COALESCE(prv.physician_role, '')), 1, 5) = 'OTHER'
                    THEN 6
                ELSE 99
            END                                                                             AS physician_role_priority,
            prv.claim_id,
            prv.npi_number,
            prv.physician_role
         FROM prv
        INNER JOIN sentry_temp11_claim clm
                ON prv.claim_id = clm.claim_id
        INNER JOIN sentry_temp14_parent_child enc
                ON clm.row_num = enc.child
        WHERE prv.npi_number IS NOT NULL
          AND SUBSTR(UPPER(COALESCE(prv.physician_role, '')), 1, 5) IN ('RENDE', 'ATTEN', 'ADMIT', 'OPERA', 'EMERG', 'OTHER')
    )
)
WHERE row_num = 1
