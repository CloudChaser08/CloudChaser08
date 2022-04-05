SELECT
    src_era_claim_id,
    priority+1                                                                                                                    AS clm_adjmt_seq_num,
    SUBSTR(TRIM(txn.value), 1                          , LOCATE(':', TRIM(txn.value))-1                          )                AS clm_adjmt_grp_cd,
    SUBSTR(TRIM(txn.value), LOCATE(':', TRIM(txn.value))+1, LOCATE('=', TRIM(txn.value))-1 - LOCATE(':', TRIM(txn.value)))        AS clm_adjmt_rsn_cd,
    CAST(SUBSTR(TRIM(txn.value), LOCATE('>', TRIM(txn.value))+1                                                  )  AS FLOAT)     AS clm_adjmt_amt

FROM
(
    src_era_claim_id,
    clm_cas,
    POSEXPLODE(SPLIT(REPLACE(clm_cas, '"', ''), ',')) AS (priority, value)
  FROM txn
  WHERE clm_cas IS NOT NULL
) txn
GROUP BY 1,2,3,4,5
--ORDER BY 1,2,3,4
--limit 1
