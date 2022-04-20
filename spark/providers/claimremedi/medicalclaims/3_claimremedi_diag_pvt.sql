 SELECT
   src_claim_id,
   REPLACE(SUBSTR(diag_and_priority, 1, LOCATE("=>", diag_and_priority) - 1), '"','') AS diag,
   REPLACE(SUBSTR(diag_and_priority,    LOCATE("=>", diag_and_priority) + 2), '"','') AS diag_priority
 FROM
 (
   SELECT
   txn.src_claim_id, 
   txn.dx,
   EXPLODE_OUTER(SPLIT(txn.dx, '\,'))  AS diag_and_priority
   FROM claimremedi_837_dedup txn
 )
 GROUP BY src_claim_id , diag_priority, diag
 ---- removed from prod for performanace
 --ORDER BY src_claim_id , diag_priority
