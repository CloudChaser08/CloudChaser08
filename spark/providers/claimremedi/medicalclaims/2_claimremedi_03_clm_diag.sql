SELECT DISTINCT
    claim_svc_num, diag_code
FROM
    claimremedi_02_clm_diag_prep
WHERE
    diag_code IS NOT NULL
    OR claim_svc_num IN
    (
        SELECT
            claim_svc_num
        FROM
            claimremedi_02_clm_diag_prep
        GROUP BY
            claim_svc_num
        HAVING max(diag_code) IS NULL
    )