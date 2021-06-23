SELECT 
    comb.*
FROM
    practice_insight_13_combine_hist_cf comb
INNER JOIN
    practice_insight_14_combine_hist_cf_pre_dedup predup
        ON comb.claim_id = predup.claim_id
            AND comb.created  = predup.created
            AND predup.row = 1
