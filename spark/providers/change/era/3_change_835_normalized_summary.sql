SELECT
    MONOTONICALLY_INCREASING_ID()                                                           AS row_id,
    CONCAT('186_', clm.claim_payment_number)                                                AS hv_medcl_clm_pymt_sumry_id,   
    CURRENT_DATE()                                                                          AS crt_dt,
	'06'                                                                                    AS mdl_vrsn_num,
    SPLIT(clm.input_file_name, '/')[SIZE(SPLIT(clm.input_file_name, '/')) - 1]              AS data_set_nm,
	'576'                                                                                   AS hvm_vdr_id,
	'186'                                                                                   AS hvm_vdr_feed_id,
    clm.claim_payment_number                                                                AS vdr_medcl_clm_pymt_sumry_id,
    CASE 
        WHEN clm.claim_payment_number IS NOT NULL
        THEN UPPER('claim_payment_number') 
    ELSE NULL END                                                                           AS vdr_medcl_clm_pymt_sumry_id_qual,
    /* clm_stmt_perd_start_dt  
    If clm.statement_from_date is BEFORE (less than)  min_max_dt.min_service_from_date  or it is NULL 
        than populate statement_from_date with min_max_dt.min_service_from_date
    ELSE populate clm.statement_from_date
    */
        (
        CASE 
            WHEN TO_DATE(clm.statement_from_date, 'yyyyMMdd')  >  TO_DATE(min_max_dt.min_service_from_date, 'yyyyMMdd')
              OR TO_DATE(clm.statement_from_date, 'yyyyMMdd') IS NULL
            THEN 
                CASE
                  WHEN TO_DATE(min_max_dt.min_service_from_date, 'yyyyMMdd') < CAST(${EARLIEST_VALID_SERVICE_DATE} AS DATE)
                    OR TO_DATE(min_max_dt.min_service_from_date, 'yyyyMMdd') > CAST('{VDR_FILE_DT}'                 AS DATE) THEN NULL
                  ELSE TO_DATE(min_max_dt.min_service_from_date, 'yyyyMMdd')
                END
        ELSE
                CASE
                  WHEN TO_DATE(clm.statement_from_date, 'yyyyMMdd')  < CAST(${EARLIEST_VALID_SERVICE_DATE} AS DATE)
                    OR TO_DATE(clm.statement_from_date, 'yyyyMMdd')  > CAST('{VDR_FILE_DT}'                 AS DATE) THEN NULL
                  ELSE TO_DATE(clm.statement_from_date, 'yyyyMMdd')  
                END
        END
        )                                                                                   AS clm_stmt_perd_start_dt,
        
    /* clm_stmt_perd_end_dt  
    If claim end date is AFTER  service line end date or clm_stmt_perd_end_dt is NULL
    than populate clm_stmt_perd_end_dt with min_max_dt.max_service_to_date
    */
        (
        CASE 
            WHEN (
                    TO_DATE(clm.statement_to_date, 'yyyyMMdd')   <  TO_DATE(min_max_dt.max_service_to_date, 'yyyyMMdd') 
                 OR TO_DATE(clm.statement_to_date, 'yyyyMMdd')   IS NULL
                 )
            THEN
                CASE
                  WHEN TO_DATE(min_max_dt.max_service_to_date, 'yyyyMMdd') < CAST(${EARLIEST_VALID_SERVICE_DATE} AS DATE)
                    OR TO_DATE(min_max_dt.max_service_to_date, 'yyyyMMdd') > CAST('{VDR_FILE_DT}'                 AS DATE) THEN NULL
                ELSE   TO_DATE(min_max_dt.max_service_to_date, 'yyyyMMdd')
                END
        ELSE
                CASE
                  WHEN TO_DATE(clm.statement_to_date, 'yyyyMMdd') < CAST(${EARLIEST_VALID_SERVICE_DATE} AS DATE)
                    OR TO_DATE(clm.statement_to_date, 'yyyyMMdd') > CAST('{VDR_FILE_DT}'                 AS DATE) THEN NULL
                ELSE   TO_DATE(clm.statement_to_date, 'yyyyMMdd')
                END
        END
        )                                                                         AS clm_stmt_perd_end_dt,
        
    /* payr_clm_recpt_dt - clm.claim_received_by_payer_date */
    CASE
      WHEN TO_DATE(clm.claim_received_by_payer_date, 'yyyyMMdd') < CAST(${EARLIEST_VALID_SERVICE_DATE} AS DATE)
        OR TO_DATE(clm.claim_received_by_payer_date, 'yyyyMMdd') > CAST('{VDR_FILE_DT}'                 AS DATE) THEN NULL
      ELSE TO_DATE(clm.claim_received_by_payer_date, 'yyyyMMdd')
    END                                                                                  AS payr_clm_recpt_dt,
    clm.payer_name                                                                          AS payr_nm,
    /* bllg_prov_npi */
	CLEAN_UP_NPI_CODE
	    (
        	CASE 
        	    WHEN LPAD(line.place_of_service, 2, '0') IN ('05', '06', '07', '08', '09', '12', '13', '14', '33', '99') THEN NULL 
        	    WHEN SUBSTR(clm.type_of_bill, 1, 1)  = '3'                                                               THEN NULL         	    
        	ELSE clm.billing_pr_npi 
        	END
	    )   																				AS bllg_prov_npi,  
    /* bllg_prov_tax_id */	
	CASE 
	    WHEN LPAD(line.place_of_service, 2, '0') IN ('05', '06', '07', '08', '09', '12', '13', '14', '33', '99') THEN NULL 
	    WHEN SUBSTR(clm.type_of_bill, 1, 1)  = '3'                                                               THEN NULL         	    
	ELSE clm.billing_pr_id 
	END  																				    AS bllg_prov_tax_id,
    /* bllg_prov_1_nm */	
	CASE 
	    WHEN LPAD(line.place_of_service, 2, '0') IN ('05', '06', '07', '08', '09', '12', '13', '14', '33', '99') THEN NULL 
	    WHEN SUBSTR(clm.type_of_bill, 1, 1)  = '3'                                                               THEN NULL         	    
	ELSE clm.billing_pr_name 
	END                                                                                     AS bllg_prov_1_nm,
	
    /* bllg_prov_addr_1_txt */	
	CASE 
	    WHEN LPAD(line.place_of_service, 2, '0') IN ('05', '06', '07', '08', '09', '12', '13', '14', '33', '99') THEN NULL 
	    WHEN SUBSTR(clm.type_of_bill, 1, 1)  = '3'                                                               THEN NULL         	    
	ELSE clm.billing_pr_adr_line1 
	END                                                                                     AS bllg_prov_addr_1_txt,

	CASE 
	    WHEN LPAD(line.place_of_service, 2, '0') IN ('05', '06', '07', '08', '09', '12', '13', '14', '33', '99') THEN NULL 
	    WHEN SUBSTR(clm.type_of_bill, 1, 1)  = '3'                                                               THEN NULL         	    
	ELSE clm.billing_pr_adr_line2 
	END                                                                                     AS bllg_prov_addr_2_txt,

	CASE 
	    WHEN LPAD(line.place_of_service, 2, '0') IN ('05', '06', '07', '08', '09', '12', '13', '14', '33', '99') THEN NULL 
	    WHEN SUBSTR(clm.type_of_bill, 1, 1)  = '3'                                                               THEN NULL         	    
	ELSE clm.billing_pr_adr_city 
	END                                                                                     AS bllg_prov_city_nm,
     /* bllg_prov_state_cd */	    
    VALIDATE_STATE_CODE
        (
            CASE 
                WHEN LPAD(line.place_of_service, 2, '0') IN ('05', '06', '07', '08', '09', '12', '13', '14', '33', '99') THEN NULL 
                WHEN SUBSTR(clm.type_of_bill, 1, 1)  = '3'                                                               THEN NULL         	    
            ELSE clm.billing_pr_adr_state 
            END
	    )                                                                                   AS bllg_prov_state_cd,
    
      /* bllg_prov_zip_cd */	    
    MASK_ZIP_CODE
        (
            CASE 
    	        WHEN LPAD(line.place_of_service, 2, '0') IN ('05', '06', '07', '08', '09', '12', '13', '14', '33', '99') THEN NULL 
    	        WHEN SUBSTR(clm.type_of_bill, 1, 1)  = '3'                                                               THEN NULL         	    
            ELSE clm.billing_pr_adr_zip 
            END
	    )                                                                                   AS bllg_prov_zip_cd, 
      /* clm_prov_pymt_dt */		 
    TO_DATE(line.date_paid, 'yyyyMMdd')                                                     AS clm_prov_pymt_dt,
    clm.claim_status	                                                                    AS clm_stat_cd,
    CAST(clm.total_claim_charge_amount AS FLOAT)	                                        AS clm_submtd_chg_amt,
    CAST(clm.total_paid_amt AS FLOAT)	                                                    AS clm_pymt_amt,
    CAST(clm.patient_responsibility_amount AS FLOAT)	                                    AS ptnt_respbty_amt,
    clm.type_of_coverage     			                                                    AS medcl_covrg_typ_cd,
    MD5(clm.payer_claim_control_number) 	                                                AS payr_clm_ctl_num,
      /* pos_cd */	    
    CASE 
        WHEN LPAD(line.place_of_service, 2, '0') IN ('05', '06', '07', '08', '09', '12', '13', '14', '33', '99') THEN NULL 
    ELSE line.place_of_service 
    END                                                                                     AS pos_cd,

    CASE 
        WHEN SUBSTR(clm.type_of_bill, 1, 1)  = '3'  THEN CONCAT('X',SUBSTR(clm.type_of_bill, 2))          	    
    ELSE clm.type_of_bill
    END                                                                                    AS instnl_typ_of_bll_cd,
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
    	        WHEN LPAD(line.place_of_service, 2, '0') IN ('05', '06', '07', '08', '09', '12', '13', '14', '33', '99') THEN NULL 
    	        WHEN SUBSTR(clm.type_of_bill, 1, 1)  = '3'                                                               THEN NULL         	    
        	ELSE clm.rendering_pr_npi 
        	END
	    )                                                                                   AS rndrg_prov_npi,      
        (
        	CASE 
    	        WHEN LPAD(line.place_of_service, 2, '0') IN ('05', '06', '07', '08', '09', '12', '13', '14', '33', '99') THEN NULL 
    	        WHEN SUBSTR(clm.type_of_bill, 1, 1)  = '3'                                                               THEN NULL         	    
        	ELSE 
            	CASE
                    WHEN UPPER(clm.rendering_pr_last_or_org_name) = 'UNKNOWN' OR UPPER(clm.rendering_pr_first_name) = 'UNKNOWN'
                        THEN NULL        
                    WHEN clm.rendering_pr_last_or_org_name IS NOT NULL AND clm.rendering_pr_first_name IS NOT NULL
                        THEN CONCAT(rendering_pr_last_or_org_name, ', ', rendering_pr_first_name)
                    WHEN clm.rendering_pr_last_or_org_name IS NOT NULL AND clm.rendering_pr_first_name IS  NULL
                        THEN clm.rendering_pr_last_or_org_name
                    WHEN clm.rendering_pr_last_or_org_name IS     NULL AND clm.rendering_pr_first_name IS NOT NULL
                        THEN clm.rendering_pr_first_name
                ELSE NULL
                END   
        	END
	    )                                                                                   AS rndrg_prov_1_nm, 

    CAST(clm.patient_amount_paid AS FLOAT)	                                                AS clm_amt,
    CASE 
        WHEN clm.patient_amount_paid IS NOT NULL THEN 'F5' 
    ELSE NULL 
    END                                                                                     AS clm_amt_qual,
    /* clm_adjmt_seq_num 
    
    "clm.
clm.
clm.
clm."
    */
    CASE WHEN
        (densify_2d_array
          (ARRAY(
                ARRAY(clm.group_code_1,clm.reason_code_1, clm.adjustment_amount_1, clm.adjustment_quantity_1),
                ARRAY(clm.group_code_2,clm.reason_code_2, clm.adjustment_amount_2, clm.adjustment_quantity_2),
                ARRAY(clm.group_code_3,clm.reason_code_3, clm.adjustment_amount_3, clm.adjustment_quantity_3),
                ARRAY(clm.group_code_4,clm.reason_code_4, clm.adjustment_amount_4, clm.adjustment_quantity_4),
                ARRAY(clm.group_code_5,clm.reason_code_5, clm.adjustment_amount_5, clm.adjustment_quantity_5)                
            ))[x.clmadjgrp] = CAST(ARRAY(NULL, NULL, NULL, NULL) AS ARRAY<STRING>) 
        )
        THEN NULL
        ELSE x.clmadjgrp + 1 
    END                                                                                     AS clm_adjmt_seq_num, 
    /* Group Code: clm_adjmt_grp_cd */     
   densify_2d_array(ARRAY(
                ARRAY(clm.group_code_1,clm.reason_code_1, clm.adjustment_amount_1, clm.adjustment_quantity_1),
                ARRAY(clm.group_code_2,clm.reason_code_2, clm.adjustment_amount_2, clm.adjustment_quantity_2),
                ARRAY(clm.group_code_3,clm.reason_code_3, clm.adjustment_amount_3, clm.adjustment_quantity_3),
                ARRAY(clm.group_code_4,clm.reason_code_4, clm.adjustment_amount_4, clm.adjustment_quantity_4),
                ARRAY(clm.group_code_5,clm.reason_code_5, clm.adjustment_amount_5, clm.adjustment_quantity_5)                
   ))[x.clmadjgrp][0]                                                                      AS clm_adjmt_grp_cd,
     /* Reason Code: clm_adjmt_rsn_cd */    
   densify_2d_array(ARRAY(
                ARRAY(clm.group_code_1,clm.reason_code_1, clm.adjustment_amount_1, clm.adjustment_quantity_1),
                ARRAY(clm.group_code_2,clm.reason_code_2, clm.adjustment_amount_2, clm.adjustment_quantity_2),
                ARRAY(clm.group_code_3,clm.reason_code_3, clm.adjustment_amount_3, clm.adjustment_quantity_3),
                ARRAY(clm.group_code_4,clm.reason_code_4, clm.adjustment_amount_4, clm.adjustment_quantity_4),
                ARRAY(clm.group_code_5,clm.reason_code_5, clm.adjustment_amount_5, clm.adjustment_quantity_5)                
    ))[x.clmadjgrp][1]                                                                      AS clm_adjmt_rsn_cd,
    /* Adjustment Amount: clm_adjmt_amt */    
   densify_2d_array(ARRAY(
                ARRAY(clm.group_code_1,clm.reason_code_1, clm.adjustment_amount_1, clm.adjustment_quantity_1),
                ARRAY(clm.group_code_2,clm.reason_code_2, clm.adjustment_amount_2, clm.adjustment_quantity_2),
                ARRAY(clm.group_code_3,clm.reason_code_3, clm.adjustment_amount_3, clm.adjustment_quantity_3),
                ARRAY(clm.group_code_4,clm.reason_code_4, clm.adjustment_amount_4, clm.adjustment_quantity_4),
                ARRAY(clm.group_code_5,clm.reason_code_5, clm.adjustment_amount_5, clm.adjustment_quantity_5)                
    ))[x.clmadjgrp][2]                                                                      AS clm_adjmt_amt,
    /* Adjustment Quantity: clm_adjmt_qty */    
   densify_2d_array(ARRAY(
                ARRAY(clm.group_code_1,clm.reason_code_1, clm.adjustment_amount_1, clm.adjustment_quantity_1),
                ARRAY(clm.group_code_2,clm.reason_code_2, clm.adjustment_amount_2, clm.adjustment_quantity_2),
                ARRAY(clm.group_code_3,clm.reason_code_3, clm.adjustment_amount_3, clm.adjustment_quantity_3),
                ARRAY(clm.group_code_4,clm.reason_code_4, clm.adjustment_amount_4, clm.adjustment_quantity_4),
                ARRAY(clm.group_code_5,clm.reason_code_5, clm.adjustment_amount_5, clm.adjustment_quantity_5)                
    ))[x.clmadjgrp][3]                                                                      AS clm_adjmt_qty,
   
	'186'                                                                                   AS part_hvm_vdr_feed_id,
    /* part_best_date 
    
    If 
    
    */
	CASE
	    WHEN 0 = LENGTH(TRIM(COALESCE
	                            (
                                        (
                                            CASE 
                                                WHEN 
                                                        TO_DATE(clm.statement_to_date, 'yyyyMMdd')  <  TO_DATE(min_max_dt.max_service_to_date, 'yyyyMMdd')
                                                     OR TO_DATE(clm.statement_to_date, 'yyyyMMdd')  IS NULL
                                                     
                                                THEN 
                                                    CASE
                                                      WHEN TO_DATE(min_max_dt.max_service_to_date, 'yyyyMMdd')  < CAST(${EARLIEST_VALID_SERVICE_DATE} AS DATE)
                                                        OR TO_DATE(min_max_dt.max_service_to_date, 'yyyyMMdd')  > CAST('{VDR_FILE_DT}'                 AS DATE) THEN NULL
                                                    ELSE   TO_DATE(min_max_dt.max_service_to_date, 'yyyyMMdd') 
                                                    END
                                            ELSE
                                                CASE
                                                  WHEN TO_DATE(clm.statement_to_date, 'yyyyMMdd') < CAST(${EARLIEST_VALID_SERVICE_DATE} AS DATE)
                                                    OR TO_DATE(clm.statement_to_date, 'yyyyMMdd') > CAST('{VDR_FILE_DT}'                 AS DATE) THEN NULL
                                                ELSE   TO_DATE(clm.statement_to_date, 'yyyyMMdd')
                                                END
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
                                                WHEN 
                                                        TO_DATE(clm.statement_to_date, 'yyyyMMdd')   <  TO_DATE(min_max_dt.max_service_to_date, 'yyyyMMdd')
                                                     OR TO_DATE(clm.statement_to_date, 'yyyyMMdd')   IS NULL
                                                     
                                                THEN 
                                                    CASE
                                                      WHEN TO_DATE(min_max_dt.max_service_to_date, 'yyyyMMdd') < CAST(${EARLIEST_VALID_SERVICE_DATE} AS DATE)
                                                        OR TO_DATE(min_max_dt.max_service_to_date, 'yyyyMMdd') > CAST('{VDR_FILE_DT}'                 AS DATE) THEN NULL
                                                    ELSE   TO_DATE(min_max_dt.max_service_to_date, 'yyyyMMdd')
                                                    END
                                            ELSE
                                                CASE
                                                  WHEN TO_DATE(clm.statement_to_date, 'yyyyMMdd') < CAST(${EARLIEST_VALID_SERVICE_DATE} AS DATE)
                                                    OR TO_DATE(clm.statement_to_date, 'yyyyMMdd') > CAST('{VDR_FILE_DT}'                 AS DATE) THEN NULL
                                                ELSE   TO_DATE(clm.statement_to_date, 'yyyyMMdd')
                                                END
                                            END
                                        ), 
                                    ''
                                ),1, 4) , '-',
                                
	         SUBSTR(COALESCE
	                            (
                                        (
                                            CASE 
                                                WHEN 
                                                        TO_DATE(clm.statement_to_date, 'yyyyMMdd')  <  CAST(TO_DATE(min_max_dt.max_service_to_date, 'yyyyMMdd') AS DATE)
                                                     OR TO_DATE(clm.statement_to_date, 'yyyyMMdd')  IS NULL
                                                     
                                                THEN 
                                                    CASE
                                                      WHEN TO_DATE(min_max_dt.max_service_to_date, 'yyyyMMdd') < CAST(${EARLIEST_VALID_SERVICE_DATE} AS DATE)
                                                        OR TO_DATE(min_max_dt.max_service_to_date, 'yyyyMMdd') > CAST('{VDR_FILE_DT}'                 AS DATE) THEN NULL
                                                    ELSE   TO_DATE(min_max_dt.max_service_to_date, 'yyyyMMdd')
                                                    END
                                            ELSE
                                                CASE
                                                  WHEN TO_DATE(clm.statement_to_date, 'yyyyMMdd') < CAST(${EARLIEST_VALID_SERVICE_DATE} AS DATE)
                                                    OR TO_DATE(clm.statement_to_date, 'yyyyMMdd') > CAST('{VDR_FILE_DT}'                 AS DATE) THEN NULL
                                                ELSE   TO_DATE(clm.statement_to_date, 'yyyyMMdd')
                                                END
                                            END
                                        ), 
                                    ''
                                ),6, 2), '-01'
                )
     END                                                                                        AS part_mth
FROM change_835_claims clm
/* Get only the first row from the service line */
LEFT OUTER JOIN
    (
        SELECT * FROM
            (SELECT *, row_number()  OVER (PARTITION BY claim_payment_number ORDER BY CAST(line_number AS FLOAT)  ) as rownum 
                FROM change_835_lines
            ) WHERE rownum = 1
    ) line ON clm.claim_payment_number = line.claim_payment_number
LEFT OUTER JOIN
/* Get the min and max dates for each claim_payment_number. */
(
    SELECT
        line_1.claim_payment_number,
        MIN(line_1.service_from_date)                                   AS min_service_from_date,
        MAX(COALESCE(line_1.service_to_date, line_1.service_from_date)) AS max_service_to_date
    FROM change_835_lines line_1
    GROUP BY 1
) min_max_dt                                 ON clm.claim_payment_number = min_max_dt.claim_payment_number
CROSS JOIN (SELECT EXPLODE(ARRAY(0, 1, 2, 3, 4)) AS clmadjgrp) x 

WHERE
    (densify_2d_array
      (ARRAY(
                ARRAY(clm.group_code_1,clm.reason_code_1, clm.adjustment_amount_1, clm.adjustment_quantity_1),
                ARRAY(clm.group_code_2,clm.reason_code_2, clm.adjustment_amount_2, clm.adjustment_quantity_2),
                ARRAY(clm.group_code_3,clm.reason_code_3, clm.adjustment_amount_3, clm.adjustment_quantity_3),
                ARRAY(clm.group_code_4,clm.reason_code_4, clm.adjustment_amount_4, clm.adjustment_quantity_4),
                ARRAY(clm.group_code_5,clm.reason_code_5, clm.adjustment_amount_5, clm.adjustment_quantity_5)  
        ))[x.clmadjgrp] != CAST(ARRAY(NULL, NULL, NULL, NULL) AS ARRAY<STRING>) OR x.clmadjgrp = 0
    )


