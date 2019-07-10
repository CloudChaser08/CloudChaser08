SELECT
    MONOTONICALLY_INCREASING_ID()                                                           AS record_id,
    /* hvid */
    COALESCE
        (
            pay.hvid, 
            CONCAT('124_', COALESCE(MD5(txn.hshd_id), 'NO_HOUSEHOLD'))
        )                                                                                   AS hvid,
    CURRENT_DATE()                                                                          AS created,
	'09'                                                                                AS model_version,
    SPLIT(txn.input_file_name, '/')[SIZE(SPLIT(txn.input_file_name, '/')) - 1]              AS data_set,
	'124'                                                                               AS data_feed,
	'337'                                                                               AS data_vendor,
	/* patient_age */
	VALIDATE_AGE
        (
            pay.age,
            CAST(EXTRACT_DATE(txn.period_start_dt, '%Y%m%d') AS DATE),
            pay.yearofbirth
        )                                                                                   AS patient_age,
	/* patient_year_of_birth */
	CAP_YEAR_OF_BIRTH
        (
            pay.age,
            CAST(EXTRACT_DATE(txn.period_start_dt, '%Y%m%d') AS DATE),
            pay.yearofbirth
        )                                                                                   AS patient_year_of_birth,
    MASK_ZIP_CODE(SUBSTR(pay.threedigitzip, 1, 3))                                          AS patient_zip3,
    VALIDATE_STATE_CODE(UPPER(COALESCE(pay.state, '')))                                     AS patient_state,
	CLEAN_UP_GENDER(pay.gender)                                                         AS patient_gender,
	MD5(txn.hshd_id)                                                                    AS source_record_id,
	/* source_record_qual */
	CASE
	    WHEN txn.hshd_id IS NOT NULL
	        THEN 'HSHD_ID'
	    ELSE NULL
	END                                                                                 AS source_record_qual,
    /* patient_group */
    CASE 
        WHEN COALESCE
                (
                    txn.pref_store_state,
                    txn.cont_panel_1yr,
                    txn.cont_panel_2yr,
                    txn.hbc_cont_panel,
                    txn.coup_resp_behavior,
                    txn.bsk_coup_proportion_percent,
                    txn.health_hml,
                    txn.quality_hml,
                    txn.convenience_hml,
                    txn.price_hml,
                    txn.inspiration_hml,
                    txn.retail_loyalty,
                    txn.digital_engagement,
                    txn.store_brands_eng_seg,
                    txn.wic_flag
                ) IS NOT NULL 
                    THEN SUBSTR
                            (
                                CONCAT
                                    (
                                        CASE 
                                            WHEN txn.pref_store_state IS NOT NULL 
                                                THEN CONCAT(', PREF_STORE_STATE: ', txn.pref_store_state)
                                            ELSE '' 
                                        END, 
                                        CASE 
                                            WHEN txn.cont_panel_1yr IS NOT NULL 
                                                THEN CONCAT(', CONT_PANEL_1YR: ', txn.cont_panel_1yr)
                                            ELSE '' 
                                        END, 
                                        CASE 
                                            WHEN txn.cont_panel_2yr IS NOT NULL
                                                THEN CONCAT(', CONT_PANEL_2YR: ', txn.cont_panel_2yr)
                                            ELSE '' 
                                        END, 
                                        CASE
                                            WHEN txn.hbc_cont_panel IS NOT NULL
                                                THEN CONCAT(', HBC_CONT_PANEL: ', txn.hbc_cont_panel)
                                            ELSE '' 
                                        END, 
                                        CASE
                                            WHEN txn.coup_resp_behavior IS NOT NULL
                                                THEN CONCAT(', COUP_RESP_BEHAVIOR: ', txn.coup_resp_behavior)
                                            ELSE '' 
                                        END,
                                        CASE
                                            WHEN txn.bsk_coup_proportion_percent IS NOT NULL
                                                THEN CONCAT(', BSK_COUP_PROPORTION_PERCENT: ', txn.bsk_coup_proportion_percent)
                                            ELSE ''
                                        END,
                                        CASE
                                            WHEN txn.health_hml IS NOT NULL
                                                THEN CONCAT(', HEALTH: ', txn.health_hml)
                                            ELSE '' 
                                        END,
                                        CASE
                                            WHEN txn.quality_hml IS NOT NULL
                                                THEN CONCAT(', QUALITY: ', txn.quality_hml)
                                            ELSE ''
                                        END, 
                                        CASE
                                            WHEN txn.convenience_hml IS NOT NULL
                                                THEN CONCAT(', CONVENIENCE: ', txn.convenience_hml)
                                            ELSE ''
                                        END,
                                        CASE
                                            WHEN txn.price_hml IS NOT NULL
                                                THEN CONCAT(', PRICE: ', txn.price_hml)
                                            ELSE ''
                                        END, 
                                        CASE
                                            WHEN txn.inspiration_hml IS NOT NULL
                                                THEN CONCAT(', INSPIRATION: ', txn.inspiration_hml)
                                            ELSE ''
                                        END,
                                        CASE
                                            WHEN txn.retail_loyalty IS NOT NULL
                                                THEN CONCAT(', RETAIL_LOYALTY: ', txn.retail_loyalty)
                                            ELSE '' 
                                        END,
                                        CASE
                                            WHEN txn.digital_engagement IS NOT NULL
                                                THEN CONCAT(', DIGITAL_ENGAGEMENT: ', txn.digital_engagement)
                                            ELSE '' 
                                        END,
                                        CASE
                                            WHEN txn.store_brands_eng_seg IS NOT NULL
                                                THEN CONCAT(', STORE_BRANDS_ENG_SEG: ', txn.store_brands_eng_seg)
                                            ELSE ''
                                        END,
                                        CASE
                                            WHEN txn.wic_flag IS NOT NULL
                                                THEN CONCAT(', WIC_FLAG: ', txn.wic_flag)
                                            ELSE ''
                                        END
                                    ), 3
                            )
        ELSE NULL 
    END                                                                                         AS patient_group,
	txn.agg_level_desc                                                                      AS event,
	txn.agg_total_visits                                                                    AS event_val,
	/* event_val_uom */
	CASE
	    WHEN txn.agg_total_visits IS NOT NULL
	        THEN 'TOTAL_VISITS'
	    ELSE NULL
	END                                                                                     AS event_val_uom,
	txn.agg_total_units                                                                     AS event_units,
	/* event_units_uom */
	CASE
	    WHEN txn.agg_total_units IS NOT NULL
	        THEN 'UNITS_PURCHASED'
	    ELSE NULL
	END                                                                                     AS event_units_uom,
    /* event_date */
	CAP_DATE
        (
            CAST(EXTRACT_DATE(txn.period_start_dt, '%Y%m%d') AS DATE),
            esdt.gen_ref_1_dt,
            CAST(EXTRACT_DATE('{VDR_FILE_DT}', '%Y-%m-%d') AS DATE)
        )                                                                                       AS event_date,
    /* event_date_qual */
    CASE
        WHEN CAP_DATE
                (
                    CAST(EXTRACT_DATE(txn.period_start_dt, '%Y%m%d') AS DATE),
                    esdt.gen_ref_1_dt,
                    CAST(EXTRACT_DATE('{VDR_FILE_DT}', '%Y-%m-%d') AS DATE)
                ) IS NOT NULL
            THEN 'WEEK_STARTING'
        ELSE NULL
    END                                                                                         AS event_date_qual,
    txn.agg_total_spend                                                                         AS event_revenue,
    txn.agg_level_code                                                                          AS event_category_code,
    /* event_category_code_qual */
    CASE
        WHEN txn.agg_level_code IS NOT NULL
            THEN 'AGG_LEVEL_CODE'
        ELSE NULL
    END                                                                                         AS event_category_code_qual,
    txn.agg_level                                                                               AS event_category_name,
	'8451'                                                                                  AS part_provider,
    /* part_best_date */
	CASE
	    WHEN 0 = LENGTH(TRIM(COALESCE
	                            (
	                                CAP_DATE
                                        (
                                            CAST(EXTRACT_DATE(txn.period_start_dt, '%Y%m%d') AS DATE),
                                            COALESCE(ahdt.gen_ref_1_dt, esdt.gen_ref_1_dt),
                                            CAST(EXTRACT_DATE('{VDR_FILE_DT}', '%Y-%m-%d') AS DATE)
                                        ), 
                                    ''
                                )))
	        THEN '0_PREDATES_HVM_HISTORY'
	    ELSE CONCAT
	            (
                    SUBSTR(txn.period_start_dt, 1, 4), '-',
                    SUBSTR(txn.period_start_dt, 5, 2), '-01'
                )
	END                                                                                     AS part_best_date
 FROM meta txn
 LEFT OUTER JOIN 8451_grocery_marketplace_pay_dedup pay
   ON txn.hshd_id = pay.personid
 LEFT OUTER JOIN
    (
        SELECT gen_ref_1_dt
         FROM ref_gen_ref
        WHERE hvm_vdr_feed_id = 86
          AND gen_ref_domn_nm = 'EARLIEST_VALID_SERVICE_DATE'
        LIMIT 1
    ) esdt
   ON 1 = 1
 LEFT OUTER JOIN 
    (
        SELECT gen_ref_1_dt
         FROM ref_gen_ref
        WHERE hvm_vdr_feed_id = 86
          AND gen_ref_domn_nm = 'HVM_AVAILABLE_HISTORY_START_DATE'
        LIMIT 1
    ) ahdt
   ON 1 = 1
/* Remove column headers. */
WHERE LOWER(COALESCE(txn.hshd_id, '')) <> 'hshd_id'
