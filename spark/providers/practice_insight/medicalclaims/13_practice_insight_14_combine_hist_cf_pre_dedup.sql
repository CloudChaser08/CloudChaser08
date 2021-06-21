SELECT 
    claim_id,
    created,
    ROW_NUMBER() OVER (PARTITION BY claim_id ORDER BY created DESC) AS row
FROM
    practice_insight_13_combine_hist_cf
GROUP BY
    claim_id, created
ORDER BY created DESC
