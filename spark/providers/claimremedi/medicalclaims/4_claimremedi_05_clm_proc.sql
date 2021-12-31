SELECT DISTINCT
    claim_svc_num, proc_code
FROM
    claimremedi_04_clm_proc_prep
WHERE
    proc_code IS NOT NULL
    OR claim_svc_num IN
    (
        SELECT
            claim_svc_num
        FROM
            claimremedi_04_clm_proc_prep
        GROUP BY
            claim_svc_num
        HAVING max(proc_code) IS NULL
    )
