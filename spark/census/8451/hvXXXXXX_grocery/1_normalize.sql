SELECT
    /* Sunil added 6 columns on 5/23/19 to make delivery consistent with 84.51 RX (with the exception of claim_id which doesn't apply to Grocery Data) */
    CAST(MONOTONICALLY_INCREASING_ID() AS STRING)                                           AS record_id,
    /* Laurie 5/24/19: Moved hvid to below record_id, to match the source-to-target mapping. */
    UPPER(obfuscate_hvid(pay.hvid, {SALT}))                                                 AS hvid,
    CURRENT_DATE()                                                                          AS created,
    /* Laurie 5/24/19: Deleted the model_version column, since there's no HV standard data model involved in this delivery. */
    SPLIT(txn.input_file_name, '/')[SIZE(SPLIT(txn.input_file_name, '/')) - 1]              AS data_set,
	'110'                                                                                   AS data_feed,
	'318'                                                                                   AS data_vendor,
	/* patient_gender */
	CLEAN_UP_GENDER
    	(
        	CASE
        	    WHEN SUBSTR(UPPER(TRIM(COALESCE(txn.gender_code, 'U'))), 1, 1) IN ('F', 'M')
        	        THEN SUBSTR(UPPER(TRIM(COALESCE(txn.gender_code, 'U'))), 1, 1)
        	    WHEN SUBSTR(UPPER(TRIM(COALESCE(pay.gender, 'U'))), 1, 1) IN ('F', 'M') 
        	        THEN SUBSTR(UPPER(TRIM(COALESCE(pay.gender, 'U'))), 1, 1)
        	    ELSE 'U' 
        	END
	    )                                                                                   AS patient_gender,
	/* patient_year_of_birth */
	CAP_YEAR_OF_BIRTH
        (
            pay.age,
            CAST(EXTRACT_DATE(txn.transaction_date_id, '%Y%m%d') AS DATE),
            pay.yearofbirth
        )                                                                                   AS patient_year_of_birth,
	/* patient_age */
	VALIDATE_AGE
        (
            pay.age,
            CAST(EXTRACT_DATE(txn.transaction_date_id, '%Y%m%d') AS DATE),
            pay.yearofbirth
        )                                                                                   AS patient_age,
    /* patient_state */
    VALIDATE_STATE_CODE
        (
            UPPER(COALESCE(txn.hshd_address_state_prov_code, pay.state, ''))
        )                                                                                   AS patient_state,
    /* patient_zip3 */
    MASK_ZIP_CODE
        (
            SUBSTR(pay.threedigitzip, 1, 3)
        )                                                                                   AS patient_zip3,
    /* Vendor-specific columns */
	txn.preferred_store_division															AS preferred_store_division,
	/* preferred_store_zip */
    MASK_ZIP_CODE
        (
            SUBSTR(txn.preferred_store_zip, 1, 3)
        )                                                                                   AS preferred_store_zip,
    /* household_size */
    CASE
        WHEN txn.household_size IS NULL
            THEN NULL
        WHEN CAST(COALESCE(txn.household_size, '0') AS INTEGER) > 5 
            THEN '5'
        WHEN txn.household_size = '5+'
            THEN '5'
        ELSE txn.household_size 
    END																		                AS household_size,
	txn.presence_of_children																AS presence_of_children,
	txn.continuous_panel_1yr																AS continuous_panel_1yr,
	txn.continuous_panel_2yr																AS continuous_panel_2yr,
	txn.hbc_continuous_panel																AS hbc_continuous_panel,
	txn.wic_flag																			AS wic_flag,
	txn.pricebands																			AS pricebands,
	txn.mylife																				AS mylife,
	txn.retail_loyalty																		AS retail_loyalty,
	txn.digital_engagement																	AS digital_engagement,
	txn.coupon_response_behavior															AS coupon_response_behavior,
	txn.basket_coupon_proportion_percent													AS basket_coupon_proportion_percent,
	txn.store_brands_engagement_segmentation												AS store_brands_engagement_segmentation,
	txn.health																				AS health,
	txn.quality																				AS quality,
	txn.convenience																			AS convenience,
	txn.price																				AS price,
	txn.inspiration																			AS inspiration,
	txn.household_optup_engagment															AS household_optup_engagment,
	MD5(txn.transaction_bask_fid)															AS transaction_bask_fid,
	MD5(txn.transaction_item_fid)															AS transaction_item_fid,
	txn.transaction_date_id																	AS transaction_date_id,
    /* zip */
    MASK_ZIP_CODE
        (
            SUBSTR(txn.zip, 1, 3)
        )																					AS zip,
	txn.division_code																		AS division_code,
	txn.item_qty																			AS item_qty,
	txn.unit_qty																			AS unit_qty,
	/* net_spend_amt */
	CASE 
	    WHEN txn.net_spend_amt IS NULL 
	        THEN NULL 
	    WHEN ABS(CAST(txn.net_spend_amt AS FLOAT)) > CAST(300 AS FLOAT) 
	        THEN CAST(SIGNUM(CAST(txn.net_spend_amt AS FLOAT)) * 300 AS STRING) 
	    ELSE txn.net_spend_amt 
	END																		                AS net_spend_amt,
	/* spend_amt */
	CASE 
	    WHEN txn.spend_amt IS NULL 
	        THEN NULL 
	    WHEN ABS(CAST(txn.spend_amt AS FLOAT)) > CAST(300 AS FLOAT) 
	        THEN CAST(SIGNUM(CAST(txn.spend_amt AS FLOAT)) * 300 AS STRING) 
	    ELSE txn.spend_amt 
	END																		                AS spend_amt,
	txn.gov_snap_flag																		AS gov_snap_flag,
	txn.gov_wic_flag																		AS gov_wic_flag,
	txn.prod_group_code																		AS prod_group_code,
	txn.prod_group_desc																		AS prod_group_desc,
	txn.prod_group_alt_code																	AS prod_group_alt_code,
	txn.prod_code																			AS prod_code,
	txn.prod_desc																			AS prod_desc,
	txn.prod_alt_code																		AS prod_alt_code,
	txn.prod_merch_l10_code																	AS prod_merch_l10_code,
	txn.prod_merch_l20_code																	AS prod_merch_l20_code,
	txn.prod_merch_l21_code																	AS prod_merch_l21_code,
	txn.prod_merch_l22_code																	AS prod_merch_l22_code,
	txn.prod_merch_l30_code																	AS prod_merch_l30_code,
	txn.major_brand_name																	AS major_brand_name,
	txn.prod_merch_l10_desc																	AS prod_merch_l10_desc,
	txn.prod_merch_l20_desc																	AS prod_merch_l20_desc,
	txn.prod_merch_l21_desc																	AS prod_merch_l21_desc,
	txn.prod_merch_l22_desc																	AS prod_merch_l22_desc,
	txn.prod_merch_l30_desc																	AS prod_merch_l30_desc,
	/* weight_uom_qty */
	CASE 
	    WHEN txn.net_spend_amt IS NULL 
	     AND txn.spend_amt IS NULL 
	        THEN txn.weight_uom_qty 
	    WHEN ABS(CAST(COALESCE(txn.net_spend_amt, '0') AS FLOAT)) > CAST(300 AS FLOAT) 
	      OR ABS(CAST(COALESCE(txn.spend_amt, '0') AS FLOAT)) > CAST(300 AS FLOAT) 
	        THEN NULL
	    ELSE txn.weight_uom_qty 
	END                                                                                     AS weight_uom_qty,
	/* Laurie 6/5/19: Added the hashed household code. */
	MD5(txn.hshd_code)                                                                      AS hshd_code
 FROM 8451_grocery_transactions txn
 LEFT OUTER JOIN deduplicated_payload pay
   ON txn.card_code = pay.join_keys
