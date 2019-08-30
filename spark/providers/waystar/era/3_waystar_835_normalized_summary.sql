SELECT
    MONOTONICALLY_INCREASING_ID()                                                           AS row_id,
    CONCAT('145_', clm.supplier_claim_payment_number)                                       AS hv_medcl_clm_pymt_sumry_id,
    CURRENT_DATE()                                                                          AS crt_dt,
	'05'                                                                                    AS mdl_vrsn_num,
    SPLIT(clm.input_file_name, '/')[SIZE(SPLIT(clm.input_file_name, '/')) - 1]              AS data_set_nm,
	'34'                                                                                    AS hvm_vdr_id,
	'145'                                                                                   AS hvm_vdr_feed_id,
    clm.supplier_claim_payment_number                                                       AS vdr_medcl_clm_pymt_sumry_id,
    CASE
        WHEN clm.supplier_claim_payment_number IS NOT NULL
        THEN 'VENDOR'
    ELSE NULL END                                                                           AS vdr_medcl_clm_pymt_sumry_id_qual,
	/* ptnt_birth_yr */
	CAP_YEAR_OF_BIRTH
        (
            payload.age,
            CAST(EXTRACT_DATE(COALESCE(line.service_from_date, clm.statement_from_date), '%m/%d/%Y') AS DATE),
            payload.yearofbirth
        )                                                                                   AS ptnt_birth_yr,
	/* ptnt_age_num */
	VALIDATE_AGE
        (
            payload.age,
            CAST(EXTRACT_DATE(COALESCE(line.service_from_date, clm.statement_from_date), '%m/%d/%Y') AS DATE),
            payload.yearofbirth
        )                                                                                   AS ptnt_age_num,

	/* ptnt_gender_cd */
	CLEAN_UP_GENDER(payload.gender)                                                         AS ptnt_gender_cd,
    /* ptnt_state_cd */
    VALIDATE_STATE_CODE(payload.state)                                                      AS ptnt_state_cd,
   /* ptnt_zip3_cd */
    MASK_ZIP_CODE(SUBSTR(payload.threeDigitZip, 1, 3))                                      AS ptnt_zip3_cd,

    /* clm_stmt_perd_start_dt
    If clm.statement_from_date is less than min_max_dt.min_service_from_date (claim start date is after service line start date) or it is NULL
    than populate statement_from_date with min_max_dt.min_service_from_date
    EXTRACT_DATE(clm.statement_from_date,'%m/%d/%Y') > EXTRACT_DATE(line.service_from_date ,'%m/%d/%Y')
    */
        (
        CASE
            WHEN CAST(EXTRACT_DATE(clm.statement_from_date , '%m/%d/%Y') AS DATE)  > CAST(EXTRACT_DATE(min_max_dt.min_service_from_date ,'%m/%d/%Y') AS DATE)
                OR CAST(EXTRACT_DATE(clm.statement_from_date, '%m/%d/%Y') AS DATE) IS NULL
            THEN
            	CAP_DATE
            	(
                    CAST(EXTRACT_DATE(min_max_dt.min_service_from_date, '%m/%d/%Y') AS DATE),
                    esdt.gen_ref_1_dt,
                    CAST(EXTRACT_DATE('{VDR_FILE_DT}', '%Y-%m-%d') AS DATE)
                )
        ELSE
            	CAP_DATE
            	(
                    CAST(EXTRACT_DATE(clm.statement_from_date, '%m/%d/%Y') AS DATE),
                    esdt.gen_ref_1_dt,
                    CAST(EXTRACT_DATE('{VDR_FILE_DT}', '%Y-%m-%d') AS DATE)
                )
        END
        )                                                                                   AS clm_stmt_perd_start_dt,

    /* clm_stmt_perd_end_dt
    If clm.clm_stmt_perd_end_dt is less than min_max_dt.max_service_to_date (claim end date is before service line end date) or clm_stmt_perd_end_dt is NULL
    than populate clm_stmt_perd_end_dt with min_max_dt.max_service_to_date
    EXTRACT_DATE(clm.statement_to_date,'%m/%d/%Y')   <  EXTRACT_DATE(line.service_to_date ,'%m/%d/%Y'
    */
        (
        CASE
            WHEN (
                    CAST(EXTRACT_DATE(clm.statement_to_date , '%m/%d/%Y') AS DATE)  < CAST(EXTRACT_DATE(min_max_dt.max_service_to_date ,'%m/%d/%Y') AS DATE)
                    OR CAST(EXTRACT_DATE(clm.statement_to_date, '%m/%d/%Y') AS DATE) IS NULL
                 )
            THEN
                CAP_DATE
            	(
                    CAST(EXTRACT_DATE(min_max_dt.max_service_to_date, '%m/%d/%Y') AS DATE),
                    esdt.gen_ref_1_dt,
                    CAST(EXTRACT_DATE('{VDR_FILE_DT}', '%Y-%m-%d') AS DATE)
                )
        ELSE
                CAP_DATE
                (
                    CAST(EXTRACT_DATE(clm.statement_to_date, '%m/%d/%Y') AS DATE),
                    esdt.gen_ref_1_dt,
                    CAST(EXTRACT_DATE('{VDR_FILE_DT}',  '%Y-%m-%d') AS DATE)
                )
        END
        )                                                                                   AS clm_stmt_perd_end_dt,

    /* payr_clm_recpt_dt */
    CAP_DATE
        (
            CAST(EXTRACT_DATE(clm.received_date, '%m/%d/%Y') AS DATE),
            esdt.gen_ref_1_dt,
            CAST(EXTRACT_DATE('{VDR_FILE_DT}',  '%Y-%m-%d') AS DATE)
        )                                                                                   AS payr_clm_recpt_dt,

    /* payr_id  */
    clm.payer_id	                                                                        AS payr_id,
    CASE
        WHEN clm.payer_id IS NOT NULL
        THEN 'VENDOR'
    ELSE NULL END                                                                           AS payr_id_qual,
    clm.payer_name                                                                          AS payr_nm,
    /* bllg_prov_npi */
	CLEAN_UP_NPI_CODE
	    (
        	CASE
        	    WHEN LPAD(line.facility_type_cd, 2, '0') IN ('05', '06', '07', '08', '09', '12', '13', '14', '33', '99')
        	    THEN NULL
        	ELSE clm.billing_provider_npi
        	END
	    )   																				AS bllg_prov_npi,
    /* bllg_prov_tax_id */
	    (
        	CASE
        	    WHEN LPAD(line.facility_type_cd, 2, '0') IN ('05', '06', '07', '08', '09', '12', '13', '14', '33', '99')
        	    THEN NULL
        	ELSE clm.billing_provider_tax_id
        	END
	    )   																				AS bllg_prov_tax_id,
    /* bllg_prov_1_nm */
    clm.billing_provider_name                                                               AS bllg_prov_1_nm,
     /* bllg_prov_state_cd */
    VALIDATE_STATE_CODE
        (
        	CASE
        	    WHEN LPAD(line.facility_type_cd, 2, '0') IN ('05', '06', '07', '08', '09', '12', '13', '14', '33', '99')
        	    THEN NULL
        	ELSE clm.billing_provider_address_state_cd
        	END
	    )                                                                                   AS bllg_prov_state_cd,

      /* bllg_prov_zip_cd */
    MASK_ZIP_CODE
        (
        	CASE
        	    WHEN LPAD(line.facility_type_cd, 2, '0') IN ('05', '06', '07', '08', '09', '12', '13', '14', '33', '99')
        	    THEN NULL
        	ELSE clm.billing_provider_address_zip
        	END
	    )                                                                                   AS bllg_prov_zip_cd,
      /* clm_prov_pymt_dt */
    CAST(EXTRACT_DATE(COALESCE(clm.check_paid_date), '%m/%d/%Y') AS DATE)                   AS clm_prov_pymt_dt,
      /* clm_stat_cd */
    clm.claim_status	                                                                    AS clm_stat_cd,
      /* clm_submtd_chg_amt */
    CAST(clm.total_claim_charge_amount AS FLOAT)	                                        AS clm_submtd_chg_amt,
      /* clm_pymt_amt */
    CAST(clm.total_claim_paid_amount AS FLOAT)	                                            AS clm_pymt_amt,
      /* ptnt_respbty_amt */
    CAST(clm.patient_responsibility_amount AS FLOAT)	                                    AS ptnt_respbty_amt,
      /* medcl_covrg_typ_cd */
    clm.filing_indicator_code			                                                    AS medcl_covrg_typ_cd,
      /* payr_clm_ctl_num */
    clm.payer_claim_control_number 	                                                        AS payr_clm_ctl_num,

      /* pos_cd */
 	CASE
	    WHEN LPAD(line.facility_type_cd, 2, '0') IN ('05', '06', '07', '08', '09', '12', '13', '14', '33', '99')
	    THEN NULL
	ELSE line.facility_type_cd
	END                                                                                     AS pos_cd,
      /* drg_code */
	CASE
	    WHEN clm.drg_code IN  ('283', '284', '285', '789')
	    THEN NULL
	ELSE clm.drg_code
	END                                                                                     AS drg_cd,
      /* rndrg_prov_npi */
    CLEAN_UP_NPI_CODE
        (
        	CASE
        	    WHEN LPAD(line.facility_type_cd, 2, '0') IN ('05', '06', '07', '08', '09', '12', '13', '14', '33', '99')
        	    THEN NULL
        	ELSE clm.rendering_provider_npi
        	END
	    )                                                                                   AS rndrg_prov_npi,
      /* rndrg_prov_tax_id */
        (
        	CASE
        	    WHEN LPAD(line.facility_type_cd, 2, '0') IN ('05', '06', '07', '08', '09', '12', '13', '14', '33', '99')
        	    THEN NULL
        	ELSE clm.rendering_provider_tax_id
        	END
        )                                                                                   AS rndrg_prov_tax_id,
     /* rndrg_prov_1_nm */
    clm.rendering_provider_last_name	                                                    AS rndrg_prov_1_nm,
     /* rndrg_prov_2_nm */
    clm.rendering_provider_first_name	                                                    AS rndrg_prov_2_nm,
     /* rndrg_prov_2_nm */
    clm.crossover_payer_name	                                                            AS cob_pyr_nm,
    /* clm_adjmt_seq_num */
    CASE WHEN
        (densify_2d_array
          (ARRAY(
                ARRAY(clm.adjustment_group_code_1,clm.adjustment_reason_code_1, clm.adjustment_amount_1, clm.adjustment_quantity_1),
                ARRAY(clm.adjustment_group_code_2,clm.adjustment_reason_code_2, clm.adjustment_amount_2, clm.adjustment_quantity_2),
                ARRAY(clm.adjustment_group_code_3,clm.adjustment_reason_code_3, clm.adjustment_amount_3, clm.adjustment_quantity_3),
                ARRAY(clm.adjustment_group_code_4,clm.adjustment_reason_code_4, clm.adjustment_amount_4, clm.adjustment_quantity_4),
                ARRAY(clm.adjustment_group_code_5,clm.adjustment_reason_code_5, clm.adjustment_amount_5, clm.adjustment_quantity_5)
            ))[x.clmadjgrp] = CAST(ARRAY(NULL, NULL, NULL, NULL) AS ARRAY<STRING>)
        )
        THEN NULL
        ELSE x.clmadjgrp + 1
    END                                                                                     AS clm_adjmt_seq_num,
    /* Group Code: clm_adjmt_grp_cd */
   densify_2d_array(ARRAY(
            ARRAY(clm.adjustment_group_code_1,clm.adjustment_reason_code_1, clm.adjustment_amount_1, clm.adjustment_quantity_1),
            ARRAY(clm.adjustment_group_code_2,clm.adjustment_reason_code_2, clm.adjustment_amount_2, clm.adjustment_quantity_2),
            ARRAY(clm.adjustment_group_code_3,clm.adjustment_reason_code_3, clm.adjustment_amount_3, clm.adjustment_quantity_3),
            ARRAY(clm.adjustment_group_code_4,clm.adjustment_reason_code_4, clm.adjustment_amount_4, clm.adjustment_quantity_4),
            ARRAY(clm.adjustment_group_code_5,clm.adjustment_reason_code_5, clm.adjustment_amount_5, clm.adjustment_quantity_5)
    ))[x.clmadjgrp][0]                                                                      AS clm_adjmt_grp_cd,

    /* Reason Code: clm_adjmt_rsn_cd */
   densify_2d_array(ARRAY(
            ARRAY(clm.adjustment_group_code_1,clm.adjustment_reason_code_1, clm.adjustment_amount_1, clm.adjustment_quantity_1),
            ARRAY(clm.adjustment_group_code_2,clm.adjustment_reason_code_2, clm.adjustment_amount_2, clm.adjustment_quantity_2),
            ARRAY(clm.adjustment_group_code_3,clm.adjustment_reason_code_3, clm.adjustment_amount_3, clm.adjustment_quantity_3),
            ARRAY(clm.adjustment_group_code_4,clm.adjustment_reason_code_4, clm.adjustment_amount_4, clm.adjustment_quantity_4),
            ARRAY(clm.adjustment_group_code_5,clm.adjustment_reason_code_5, clm.adjustment_amount_5, clm.adjustment_quantity_5)
    ))[x.clmadjgrp][1]                                                                      AS clm_adjmt_rsn_cd,
    /* Adjustment Amount: clm_adjmt_amt */
   densify_2d_array(ARRAY(
            ARRAY(clm.adjustment_group_code_1,clm.adjustment_reason_code_1, clm.adjustment_amount_1, clm.adjustment_quantity_1),
            ARRAY(clm.adjustment_group_code_2,clm.adjustment_reason_code_2, clm.adjustment_amount_2, clm.adjustment_quantity_2),
            ARRAY(clm.adjustment_group_code_3,clm.adjustment_reason_code_3, clm.adjustment_amount_3, clm.adjustment_quantity_3),
            ARRAY(clm.adjustment_group_code_4,clm.adjustment_reason_code_4, clm.adjustment_amount_4, clm.adjustment_quantity_4),
            ARRAY(clm.adjustment_group_code_5,clm.adjustment_reason_code_5, clm.adjustment_amount_5, clm.adjustment_quantity_5)
    ))[x.clmadjgrp][2]                                                                      AS clm_adjmt_amt,
    /* Adjustment Quantity: clm_adjmt_qty */
   densify_2d_array(ARRAY(
            ARRAY(clm.adjustment_group_code_1,clm.adjustment_reason_code_1, clm.adjustment_amount_1, clm.adjustment_quantity_1),
            ARRAY(clm.adjustment_group_code_2,clm.adjustment_reason_code_2, clm.adjustment_amount_2, clm.adjustment_quantity_2),
            ARRAY(clm.adjustment_group_code_3,clm.adjustment_reason_code_3, clm.adjustment_amount_3, clm.adjustment_quantity_3),
            ARRAY(clm.adjustment_group_code_4,clm.adjustment_reason_code_4, clm.adjustment_amount_4, clm.adjustment_quantity_4),
            ARRAY(clm.adjustment_group_code_5,clm.adjustment_reason_code_5, clm.adjustment_amount_5, clm.adjustment_quantity_5)
    ))[x.clmadjgrp][3]                                                                      AS clm_adjmt_qty,
    /* medcl_clm_lnk_txt */
    payload.pcn	                                                                            AS medcl_clm_lnk_txt,
--------------------------------
	'145'                                                                                   AS part_hvm_vdr_feed_id,
    /* part_best_date */
	CASE
	    WHEN 0 = LENGTH(TRIM(COALESCE
	                            (
                                        (
                                            CASE
                                                WHEN CAST(EXTRACT_DATE(clm.statement_from_date , '%m/%d/%Y') AS DATE)  > CAST(EXTRACT_DATE(min_max_dt.min_service_from_date ,'%m/%d/%Y') AS DATE)
                                                    OR CAST(EXTRACT_DATE(clm.statement_from_date, '%m/%d/%Y') AS DATE) IS NULL
                                                THEN
                                                	CAP_DATE
                                                	(
                                                        CAST(EXTRACT_DATE(min_max_dt.min_service_from_date, '%m/%d/%Y') AS DATE),
                                                        COALESCE(ahdt.gen_ref_1_dt, esdt.gen_ref_1_dt, CAST(EXTRACT_DATE('1901-01-01', '%Y-%m-%d') AS DATE)),
                                                        CAST(EXTRACT_DATE('{VDR_FILE_DT}', '%Y-%m-%d') AS DATE)
                                                    )
                                            ELSE
                                                	CAP_DATE
                                                	(
                                                        CAST(EXTRACT_DATE(clm.statement_from_date, '%m/%d/%Y') AS DATE),
                                                        COALESCE(ahdt.gen_ref_1_dt, esdt.gen_ref_1_dt, CAST(EXTRACT_DATE('1901-01-01', '%Y-%m-%d') AS DATE)),
                                                        CAST(EXTRACT_DATE('{VDR_FILE_DT}', '%Y-%m-%d') AS DATE)
                                                    )
                                            END
                                        ),
                                    ''
                                )))
	        THEN '0_PREDATES_HVM_HISTORY'
	 ELSE
	    CONCAT(
	            SUBSTR(COALESCE
	                            (
                                        (
                                            CASE
                                                WHEN CAST(EXTRACT_DATE(clm.statement_from_date , '%m/%d/%Y') AS DATE)  > CAST(EXTRACT_DATE(min_max_dt.min_service_from_date ,'%m/%d/%Y') AS DATE)
                                                    OR CAST(EXTRACT_DATE(clm.statement_from_date, '%m/%d/%Y') AS DATE) IS NULL
                                                THEN
                                                	CAP_DATE
                                                	(
                                                        CAST(EXTRACT_DATE(min_max_dt.min_service_from_date, '%m/%d/%Y') AS DATE),
                                                        COALESCE(ahdt.gen_ref_1_dt, esdt.gen_ref_1_dt, CAST(EXTRACT_DATE('1901-01-01', '%Y-%m-%d') AS DATE)),
                                                        CAST(EXTRACT_DATE('{VDR_FILE_DT}', '%Y-%m-%d') AS DATE)
                                                    )
                                            ELSE
                                                	CAP_DATE
                                                	(
                                                        CAST(EXTRACT_DATE(clm.statement_from_date, '%m/%d/%Y') AS DATE),
                                                        COALESCE(ahdt.gen_ref_1_dt, esdt.gen_ref_1_dt, CAST(EXTRACT_DATE('1901-01-01', '%Y-%m-%d') AS DATE)),
                                                        CAST(EXTRACT_DATE('{VDR_FILE_DT}', '%Y-%m-%d') AS DATE)
                                                    )
                                            END
                                        ),
                                    ''
                                ),1, 4) , '-',

	         SUBSTR(COALESCE
	                            (
                                        (
                                            CASE
                                                WHEN CAST(EXTRACT_DATE(clm.statement_from_date , '%m/%d/%Y') AS DATE)  > CAST(EXTRACT_DATE(min_max_dt.min_service_from_date ,'%m/%d/%Y') AS DATE)
                                                    OR CAST(EXTRACT_DATE(clm.statement_from_date, '%m/%d/%Y') AS DATE) IS NULL
                                                THEN
                                                	CAP_DATE
                                                	(
                                                        CAST(EXTRACT_DATE(min_max_dt.min_service_from_date, '%m/%d/%Y') AS DATE),
                                                        COALESCE(ahdt.gen_ref_1_dt, esdt.gen_ref_1_dt, CAST(EXTRACT_DATE('1901-01-01', '%Y-%m-%d') AS DATE)),
                                                        CAST(EXTRACT_DATE('{VDR_FILE_DT}', '%Y-%m-%d') AS DATE)
                                                    )
                                            ELSE
                                                	CAP_DATE
                                                	(
                                                        CAST(EXTRACT_DATE(clm.statement_from_date, '%m/%d/%Y') AS DATE),
                                                        COALESCE(ahdt.gen_ref_1_dt, esdt.gen_ref_1_dt, CAST(EXTRACT_DATE('1901-01-01', '%Y-%m-%d') AS DATE)),
                                                        CAST(EXTRACT_DATE('{VDR_FILE_DT}', '%Y-%m-%d') AS DATE)
                                                    )
                                            END
                                        ),
                                    ''
                                ),6, 2)
                )
     END                                                                                 AS part_mth
FROM waystar_835_dedup_claims clm
/* Get only the first row from the service line */
LEFT OUTER JOIN
    (
        SELECT * FROM
            (SELECT *, row_number()  OVER (PARTITION BY claim_payment_number ORDER BY CAST(service_line_number AS FLOAT)  ) as rownum
                FROM waystar_835_dedup_lines
            ) WHERE rownum = 1
    ) line ON clm.supplier_claim_payment_number = line.claim_payment_number
LEFT OUTER JOIN matching_payload payload ON clm.hvjoinkey = payload.hvjoinkey
LEFT OUTER JOIN
/* Get the min and max dates for each claim_payment_number. */
(
    SELECT
        line_1.claim_payment_number,
        MIN(line_1.service_from_date)                                   AS min_service_from_date,
        MAX(COALESCE(line_1.service_to_date, line_1.service_from_date)) AS max_service_to_date
    FROM waystar_835_dedup_lines line_1
    GROUP BY 1
) min_max_dt                                 ON clm.supplier_claim_payment_number = min_max_dt.claim_payment_number
CROSS JOIN (SELECT EXPLODE(ARRAY(0, 1, 2, 3, 4)) AS clmadjgrp) x
LEFT OUTER JOIN
(
    SELECT gen_ref_1_dt
     FROM ref_gen_ref
    WHERE hvm_vdr_feed_id = 145
      AND gen_ref_domn_nm = 'EARLIEST_VALID_SERVICE_DATE'
    LIMIT 1
) esdt
ON 1 = 1
LEFT OUTER JOIN
(
    SELECT gen_ref_1_dt
     FROM ref_gen_ref
    WHERE hvm_vdr_feed_id = 145
      AND gen_ref_domn_nm = 'HVM_AVAILABLE_HISTORY_START_DATE'
    LIMIT 1
) ahdt
ON 1 = 1
WHERE
    (densify_2d_array
      (ARRAY(
            ARRAY(clm.adjustment_group_code_1,clm.adjustment_reason_code_1, clm.adjustment_amount_1, clm.adjustment_quantity_1),
            ARRAY(clm.adjustment_group_code_2,clm.adjustment_reason_code_2, clm.adjustment_amount_2, clm.adjustment_quantity_2),
            ARRAY(clm.adjustment_group_code_3,clm.adjustment_reason_code_3, clm.adjustment_amount_3, clm.adjustment_quantity_3),
            ARRAY(clm.adjustment_group_code_4,clm.adjustment_reason_code_4, clm.adjustment_amount_4, clm.adjustment_quantity_4),
            ARRAY(clm.adjustment_group_code_5,clm.adjustment_reason_code_5, clm.adjustment_amount_5, clm.adjustment_quantity_5)
        ))[x.clmadjgrp] != CAST(ARRAY(NULL, NULL, NULL, NULL) AS ARRAY<STRING>) OR x.clmadjgrp = 0
    )
