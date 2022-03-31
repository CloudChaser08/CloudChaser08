SELECT
  src_claim_id,
  proc_cd,
  REPLACE(SUBSTR(proc_cd_pvt, 1, LOCATE("=>", proc_cd_pvt) - 1), '"','') AS proc_other_seq,
  REPLACE(SUBSTR(proc_cd_pvt,    LOCATE("=>", proc_cd_pvt) + 2), '"','') AS proc_other,
  prinpl_proc_cd
FROM
(
  SELECT DISTINCT
  txn.src_claim_id,
  txn.proc_cd,
  txn.other_proc_codes,
  EXPLODE_OUTER(SPLIT(txn.other_proc_codes, '\\,'))  AS proc_cd_pvt,
  txn.prinpl_proc_cd
  FROM claimremedi_837_dedup txn
--  group by 1, 2, 3, 4, 5
)
GROUP BY  1, 2, 3, 4, 5