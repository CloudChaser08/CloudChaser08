SELECT
    MONOTONICALLY_INCREASING_ID()                                                           AS row_id,
    CONCAT('186_', line.claim_payment_number,'_', line.line_number)                         AS hv_medcl_clm_pymt_dtl_id,   
    CURRENT_DATE()                                                                          AS crt_dt,
	'06'                                                                                    AS mdl_vrsn_num,
    SPLIT(line.input_file_name, '/')[SIZE(SPLIT(line.input_file_name, '/')) - 1]            AS data_set_nm,
	'576'                                                                                   AS hvm_vdr_id,
	'186'                                                                                   AS hvm_vdr_feed_id,
    clm.claim_payment_number			                                                    AS vdr_medcl_clm_pymt_sumry_id,
    CASE 
        WHEN clm.claim_payment_number IS NOT NULL
        THEN UPPER('claim_payment_number') 
    ELSE NULL END                                                                           AS vdr_medcl_clm_pymt_sumry_id_qual, 
    CONCAT(line.claim_payment_number,'_', line.line_number)                                 AS vdr_medcl_clm_pymt_dtl_id,  
    CASE 
        WHEN line.claim_payment_number IS NOT NULL
       THEN UPPER('claim_payment_number') 
    ELSE NULL END                                                                           AS vdr_medcl_clm_pymt_dtl_id_qual,     
    /* svc_ln_start_dt  */
    CASE 
        WHEN TO_DATE(COALESCE(line.service_from_date, clm.statement_from_date), 'yyyyMMdd')  < CAST(${EARLIEST_VALID_SERVICE_DATE} AS DATE)
          OR TO_DATE(COALESCE(line.service_from_date, clm.statement_from_date), 'yyyyMMdd')  > CAST('{VDR_FILE_DT}'                 AS DATE) THEN NULL
        ELSE TO_DATE(COALESCE(line.service_from_date, clm.statement_from_date), 'yyyyMMdd') 
    END                                                                                      AS svc_ln_start_dt,
    /* svc_ln_end_dt  */
    CASE 
        WHEN TO_DATE(COALESCE(line.service_to_date, clm.statement_to_date), 'yyyyMMdd')  < CAST(${EARLIEST_VALID_SERVICE_DATE} AS DATE)
          OR TO_DATE(COALESCE(line.service_to_date, clm.statement_to_date), 'yyyyMMdd')  > CAST('{VDR_FILE_DT}'                 AS DATE) THEN NULL
        ------------------------------------------------------------------------------------------------------------------------------------------
        --------- If the service line end date is NULL and claim_line end date < service_from_date (service line START date) then do not populate
        -----------------------------------------------------------------------------------------------------------------------------------------
        WHEN line.service_to_date IS NULL 
            AND TO_DATE(clm.statement_to_date, 'yyyyMMdd')  <  TO_DATE(LINE.service_from_date, 'yyyyMMdd')                 THEN NULL
        ------------------------------------------------------------------------------------------------------------------------------------------
        --------- If the service line end date is NULL and statement_to_date < statement_from_date then do not populate
        -----------------------------------------------------------------------------------------------------------------------------------------
        WHEN line.service_to_date IS NULL 
                AND  TO_DATE(clm.statement_to_date, 'yyyyMMdd') <  TO_DATE(clm.statement_from_date, 'yyyyMMdd')           THEN NULL
        
        ELSE TO_DATE(COALESCE(line.service_to_date, clm.statement_to_date), 'yyyyMMdd')
    END                                                                                      AS svc_ln_end_dt,
    
    /* rndrg_prov_npi */
	CLEAN_UP_NPI_CODE
	    (
        	CASE 
        	    WHEN LPAD(line.place_of_service, 2, '0') IN ('05', '06', '07', '08', '09', '12', '13', '14', '33', '99') THEN NULL 
        	    WHEN SUBSTR(clm.type_of_bill, 1, 1)  = '3'                                                               THEN NULL         	    
        	ELSE line.line_rendering_prov_npi 
        	END
	    )   																				AS rndrg_prov_npi,  
  
    /* adjctd_proc_cd */
    SUBSTR
    (
        CLEAN_UP_PROCEDURE_CODE
    	    (
            	CASE 
            	    WHEN UPPER(line.adjudicated_procedure_cd_qual) = 'HC'  THEN line.adjudicated_procedure_cd
            	ELSE NULL
            	END
    	    )
     ,1,7
    )                                                                                       AS adjctd_proc_cd, 	
    /* adjctd_proc_cd_qual */	
    CASE 
        WHEN UPPER(line.adjudicated_procedure_cd_qual) = 'HC'  AND line.adjudicated_procedure_cd IS NOT NULL
            THEN line.adjudicated_procedure_cd_qual
    ELSE NULL
    END																				        AS adjctd_proc_cd_qual,
	    
    SUBSTR(CLEAN_UP_ALPHANUMERIC_CODE(UPPER(line.adjudicated_proc_modifier_1)),1,2)          AS adjctd_proc_cd_1_modfr,
    SUBSTR(CLEAN_UP_ALPHANUMERIC_CODE(UPPER(line.adjudicated_proc_modifier_2)),1,2)          AS adjctd_proc_cd_2_modfr,
    SUBSTR(CLEAN_UP_ALPHANUMERIC_CODE(UPPER(line.adjudicated_proc_modifier_3)),1,2)          AS adjctd_proc_cd_3_modfr,
    SUBSTR(CLEAN_UP_ALPHANUMERIC_CODE(UPPER(line.adjudicated_proc_modifier_4)),1,2)          AS adjctd_proc_cd_4_modfr,
    SUBSTR
    (
        CLEAN_UP_PROCEDURE_CODE
    	    (
            	CASE 
            	    WHEN UPPER(line.submitted_procedure_cd_qual) = 'HC'  THEN line.submitted_procedure_cd
            	ELSE NULL
            	END
    	    )
     ,1,7
    )                                                                                       AS orig_submtd_proc_cd, 
    
    CASE 
        WHEN UPPER(line.submitted_procedure_cd_qual) = 'HC'  AND line.submitted_procedure_cd IS NOT NULL
            THEN line.submitted_procedure_cd_qual
    ELSE NULL
    END                                                                                     AS orig_submtd_proc_cd_qual,
    SUBSTR(CLEAN_UP_ALPHANUMERIC_CODE(UPPER(line.submitted_proc_modifier_1)),1,2)           AS orig_submtd_proc_cd_1_modfr,
    SUBSTR(CLEAN_UP_ALPHANUMERIC_CODE(UPPER(line.submitted_proc_modifier_2)),1,2)           AS orig_submtd_proc_cd_2_modfr,
    SUBSTR(CLEAN_UP_ALPHANUMERIC_CODE(UPPER(line.submitted_proc_modifier_3)),1,2)           AS orig_submtd_proc_cd_3_modfr,
    SUBSTR(CLEAN_UP_ALPHANUMERIC_CODE(UPPER(line.submitted_proc_modifier_4)),1,2)           AS orig_submtd_proc_cd_4_modfr,
    CAST(line.line_charge_amount AS FLOAT)                                                  AS svc_ln_submtd_chg_amt,
    CAST(line.line_paid_amount AS FLOAT)                                                    AS svc_ln_prov_pymt_amt,
    line.revenue_code                                                                       AS rev_cd,
    CLEAN_UP_NDC_CODE
    (
        CASE 
            WHEN UPPER(line.adjudicated_procedure_cd_qual) IN ('N4') THEN UPPER(line.adjudicated_procedure_cd)
        ELSE NULL
        END
    )                                                                                       AS ndc_cd, 	
	CAST(line.paid_units_of_service AS FLOAT)			                                    AS paid_svc_unt_cnt,
	CAST(line.submitted_units_of_service AS FLOAT)			                                AS orig_svc_unt_cnt,
    /* Group Code: svc_ln_adjmt_grp_cd */	
   densify_2d_array(ARRAY(
            ARRAY(line.group_code_1, line.reason_code_1, line.adjustment_amount_1, line.adjustment_quantity_1),
            ARRAY(line.group_code_2, line.reason_code_2, line.adjustment_amount_2, line.adjustment_quantity_2),
            ARRAY(line.group_code_3, line.reason_code_3, line.adjustment_amount_3, line.adjustment_quantity_3),
            ARRAY(line.group_code_4, line.reason_code_4, line.adjustment_amount_4, line.adjustment_quantity_4),
            ARRAY(line.group_code_5, line.reason_code_5, line.adjustment_amount_5, line.adjustment_quantity_5)            
    ))[x.svcadjgrp][0]                                                                    AS svc_ln_adjmt_grp_cd,
    /* Sequenece Number: svc_ln_adjmt_seq_num */        
    CASE WHEN
        (densify_2d_array
            (ARRAY(
            ARRAY(line.group_code_1, line.reason_code_1, line.adjustment_amount_1, line.adjustment_quantity_1),
            ARRAY(line.group_code_2, line.reason_code_2, line.adjustment_amount_2, line.adjustment_quantity_2),
            ARRAY(line.group_code_3, line.reason_code_3, line.adjustment_amount_3, line.adjustment_quantity_3),
            ARRAY(line.group_code_4, line.reason_code_4, line.adjustment_amount_4, line.adjustment_quantity_4),
            ARRAY(line.group_code_5, line.reason_code_5, line.adjustment_amount_5, line.adjustment_quantity_5)            
            ))[x.svcadjgrp] = CAST(ARRAY(NULL, NULL, NULL, NULL) AS ARRAY<STRING>)
        )
        THEN NULL
        ELSE  x.svcadjgrp + 1   
    END                                                                                     AS svc_ln_adjmt_seq_num, 
    /* Reason Code: svc_ln_adjmt_rsn_cd */    
    densify_2d_array(ARRAY(
            ARRAY(line.group_code_1, line.reason_code_1, line.adjustment_amount_1, line.adjustment_quantity_1),
            ARRAY(line.group_code_2, line.reason_code_2, line.adjustment_amount_2, line.adjustment_quantity_2),
            ARRAY(line.group_code_3, line.reason_code_3, line.adjustment_amount_3, line.adjustment_quantity_3),
            ARRAY(line.group_code_4, line.reason_code_4, line.adjustment_amount_4, line.adjustment_quantity_4),
            ARRAY(line.group_code_5, line.reason_code_5, line.adjustment_amount_5, line.adjustment_quantity_5)            
    ))[x.svcadjgrp][1]                                                                    AS svc_ln_adjmt_rsn_cd,
    /* Adjustment Amount: svc_ln_adjmt_amt */    
    densify_2d_array(ARRAY(
            ARRAY(line.group_code_1, line.reason_code_1, line.adjustment_amount_1, line.adjustment_quantity_1),
            ARRAY(line.group_code_2, line.reason_code_2, line.adjustment_amount_2, line.adjustment_quantity_2),
            ARRAY(line.group_code_3, line.reason_code_3, line.adjustment_amount_3, line.adjustment_quantity_3),
            ARRAY(line.group_code_4, line.reason_code_4, line.adjustment_amount_4, line.adjustment_quantity_4),
            ARRAY(line.group_code_5, line.reason_code_5, line.adjustment_amount_5, line.adjustment_quantity_5)            
    ))[x.svcadjgrp][2]                                                                    AS svc_ln_adjmt_amt,
    /* Adjustment Qty: svc_ln_adjmt_qty */    
    densify_2d_array(ARRAY(
            ARRAY(line.group_code_1, line.reason_code_1, line.adjustment_amount_1, line.adjustment_quantity_1),
            ARRAY(line.group_code_2, line.reason_code_2, line.adjustment_amount_2, line.adjustment_quantity_2),
            ARRAY(line.group_code_3, line.reason_code_3, line.adjustment_amount_3, line.adjustment_quantity_3),
            ARRAY(line.group_code_4, line.reason_code_4, line.adjustment_amount_4, line.adjustment_quantity_4),
            ARRAY(line.group_code_5, line.reason_code_5, line.adjustment_amount_5, line.adjustment_quantity_5)            
    ))[x.svcadjgrp][3]                                                                    AS svc_ln_adjmt_qty,
    
    CAST(line.line_allowed_amount AS FLOAT)			                                      AS svc_ln_suplmtl_amt,
    CASE 
        WHEN line.line_allowed_amount IS NOT NULL THEN 'B6'
    ELSE NULL
    END                                                                                   AS svc_ln_suplmtl_amt_qual,
	'186'                                                                                 AS part_hvm_vdr_feed_id,
    /* part_best_date */
	CASE
	    WHEN 0 = LENGTH
	        (COALESCE
	            (
	           CAP_DATE
                (
                  TO_DATE(COALESCE(line.service_from_date, clm.statement_from_date), 'yyyyMMdd')              ,
                  CAST(COALESCE(${AVAILABLE_HISTORY_START_DATE}, ${EARLIEST_VALID_SERVICE_DATE})      AS DATE), 
                  CAST('{VDR_FILE_DT}'                                                                 AS DATE)
                ), 
                ''
                )
            )
	        THEN '0_PREDATES_HVM_HISTORY'
	    ELSE CONCAT
	            (
                    SUBSTR(COALESCE(line.service_from_date, clm.statement_from_date), 1, 4), '-',
                    SUBSTR(COALESCE(line.service_from_date, clm.statement_from_date), 5, 2), '-01'
                )
	END                                                                                     AS part_mth
 FROM change_835_lines line
 LEFT OUTER JOIN change_835_claims  clm     ON clm.claim_payment_number = line.claim_payment_number  

 CROSS JOIN (SELECT EXPLODE(ARRAY(0, 1, 2, 3, 4)) AS svcadjgrp) x
WHERE
    (densify_2d_array
      (ARRAY(
            ARRAY(line.group_code_1, line.reason_code_1, line.adjustment_amount_1, line.adjustment_quantity_1),
            ARRAY(line.group_code_2, line.reason_code_2, line.adjustment_amount_2, line.adjustment_quantity_2),
            ARRAY(line.group_code_3, line.reason_code_3, line.adjustment_amount_3, line.adjustment_quantity_3),
            ARRAY(line.group_code_4, line.reason_code_4, line.adjustment_amount_4, line.adjustment_quantity_4),
            ARRAY(line.group_code_5, line.reason_code_5, line.adjustment_amount_5, line.adjustment_quantity_5)
        ))[x.svcadjgrp] != CAST(ARRAY(NULL, NULL,NULL, NULL) AS ARRAY<STRING>) OR x.svcadjgrp = 0
    )

--LIMIT 100
