SELECT
    norm.*,
    'pdx' AS part_provider,
    /* part_best_date */
	CASE
	    WHEN 0 = LENGTH(COALESCE
	                            (
	                                CAP_DATE
                                        (
                                            CAST(EXTRACT_DATE(CAST(norm.date_service AS STRING), '%Y-%m-%d') AS DATE),
                                            COALESCE(CAST('{AVAILABLE_START_DATE}'  AS DATE), CAST('{EARLIEST_SERVICE_DATE}'  AS DATE)),
                                            CAST(EXTRACT_DATE('{VDR_FILE_DT}', '%Y-%m-%d') AS DATE)
                                        ),
                                    ''
                                ))
	        THEN '0_PREDATES_HVM_HISTORY'
	    ELSE CONCAT
	            (
                    SUBSTR(CAST(norm.date_service AS STRING), 1, 4), '-',
                    SUBSTR(CAST(norm.date_service AS STRING), 6, 2), '-01'
                )
	END                                                                                         AS part_best_date
from
    pdx_dhc_crosswalk_norm norm
WHERE NOT EXISTS (SELECT 1 FROM _temp_rxtoken_nb hist
                        WHERE norm.claim_id = hist.claim_id
                            )
