   SELECT
    src_claim_id                                 AS src_claim_id,
    CASE
      WHEN LOCATE("=>", proc) <> 0  THEN REPLACE(SUBSTR(proc, LOCATE("=>", proc) + 2), '"','')
    ELSE
      proc
    END                                          AS proc_cd,
    CASE 
        WHEN proc IS NULL THEN NULL  ELSE proc_cd_qual 
    END                                          AS proc_cd_qual,
    principal_indicator                          AS principal_indicator,
    proc_modfr_1,
    proc_modfr_2,
    proc_modfr_3,
    proc_modfr_4  
  FROM
 (
   ---------------------------------------------------- 
   ---- Explosion
   ----------------------------------------------------
   SELECT
     txn.src_claim_id, 
     EXPLODE_OUTER(SPLIT(txn.other_proc_codes, '\,')) AS proc,
     txn.proc_cd_qual,
     NULL AS principal_indicator,
     NULL AS proc_modfr_1,
     NULL AS proc_modfr_2,
     NULL AS proc_modfr_3,
     NULL AS proc_modfr_4    
   FROM claimremedi_837_dedup txn
   ---------------------------------------------------- 
   ---- UNION procedure
   ----------------------------------------------------
   UNION ALL
   SELECT
     txn.src_claim_id, 
     txn.proc_cd,
     txn.proc_cd_qual,
     NULL AS principal_indicator,
     proc_modfr_1,
     proc_modfr_2,
     proc_modfr_3,
     proc_modfr_4   
   FROM claimremedi_837_dedup txn
   ---------------------------------------------------- 
   ---- UNION principal procedure 
   ----------------------------------------------------
   UNION ALL
   SELECT
     txn.src_claim_id, 
     txn.prinpl_proc_cd,
     txn.proc_cd_qual,
     CASE
       WHEN txn.prinpl_proc_cd IS NOT NULL THEN "Y" 
     ELSE NULL
     END AS principal_indicator,
     NULL AS proc_modfr_1,
     NULL AS proc_modfr_2,
     NULL AS proc_modfr_3,
     NULL AS proc_modfr_4    
   FROM claimremedi_837_dedup txn
 )
 WHERE
 LENGTH(
    CONCAT(COALESCE(REPLACE(SUBSTR(proc, LOCATE("=>", proc) + 2), '"',''),''),
           COALESCE(proc, ''),
           COALESCE(principal_indicator,''))  
           ) <> 0
  GROUP BY src_claim_id, proc        , proc_cd_qual, principal_indicator,     
           proc_modfr_1, proc_modfr_2, proc_modfr_3, proc_modfr_4  
  ORDER BY src_claim_id
