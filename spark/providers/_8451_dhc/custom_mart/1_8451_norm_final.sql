SELECT
    norm.*,
    '8451' AS part_provider,
    /* part_best_date */
    CONCAT
	            (
                    SUBSTR('{VDR_FILE_DT}', 1, 4), '-',
                    SUBSTR('{VDR_FILE_DT}', 6, 2), '-01'
                ) AS part_best_date
FROM
    8451_norm norm
WHERE NOT EXISTS (SELECT 1 FROM _temp_rxtoken_nb hist
                        WHERE norm.claim_id = hist.claim_id
                            )
