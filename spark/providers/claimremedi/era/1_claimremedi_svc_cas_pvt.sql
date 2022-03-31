SELECT
    txn.src_era_svc_id,
    priority+1                                                                                                                     AS svc_ln_adjmt_seq_num,
    SUBSTR(TRIM(txn.value), 1                          , LOCATE(':', TRIM(txn.value))-1                          )                 AS svc_ln_adjmt_grp_cd,
    SUBSTR(TRIM(txn.value), LOCATE(':', TRIM(txn.value))+1, LOCATE('=', TRIM(txn.value))-1 - LOCATE(':', TRIM(txn.value)))         AS svc_ln_adjmt_rsn_cd,
    CAST(SUBSTR(TRIM(txn.value), LOCATE('>', TRIM(txn.value))+1                                                  )  AS FLOAT)      AS svc_ln_adjmt_amt
FROM
(
  SELECT
    src_era_svc_id,
    svc_cas,
    POSEXPLODE(SPLIT(REPLACE(svc_cas, '"', ''), ',')) AS (priority, value)
  FROM txn
  WHERE svc_cas IS NOT NULL
) txn
GROUP BY 1,2,3,4,5
--ORDER BY 1,2,3,4
--limit 1
