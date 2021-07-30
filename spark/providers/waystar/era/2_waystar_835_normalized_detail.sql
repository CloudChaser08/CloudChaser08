SELECT
    MONOTONICALLY_INCREASING_ID()                                                           AS row_id,
    CONCAT('145_', line.claim_payment_number)                                               AS hv_medcl_clm_pymt_dtl_id,   
    CURRENT_DATE()                                                                          AS crt_dt,
	'07'                                                                                    AS mdl_vrsn_num,
    SPLIT(line.input_file_name, '/')[SIZE(SPLIT(line.input_file_name, '/')) - 1]            AS data_set_nm,

	'34'                                                                                    AS hvm_vdr_id,
	'145'                                                                                   AS hvm_vdr_feed_id,
	/* vdr_medcl_clm_pymt_sumry_id: supplier_claim_payment_number from claim table  */  	
    clm.supplier_claim_payment_number			                                            AS vdr_medcl_clm_pymt_sumry_id,
    CASE 
        WHEN clm.supplier_claim_payment_number IS NOT NULL
        THEN UPPER('supplier_claim_payment_number') 
    ELSE NULL END                                                                           AS vdr_medcl_clm_pymt_sumry_id_qual, 
	/* vdr_medcl_clm_pymt_dtl_id: claim_payment_number from Line table  */    
    line.claim_payment_number                                                               AS vdr_medcl_clm_pymt_dtl_id,  
    CASE 
        WHEN line.claim_payment_number IS NOT NULL
       THEN UPPER('claim_payment_number') 
    ELSE NULL END                                                                           AS vdr_medcl_clm_pymt_dtl_id_qual,     
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
	----------------------------------------------------------------------------------------------------------------------
    -- ptnt_gender_cd
    ----------------------------------------------------------------------------------------------------------------------
	CASE
    	WHEN payload.gender IS NULL THEN NULL
    	WHEN SUBSTR(UPPER(payload.gender        ), 1, 1) IN ('F', 'M', 'U') THEN SUBSTR(UPPER(payload.gender        ), 1, 1)
        ELSE 'U' 
    END                                                                                     AS ptnt_gender_cd,
    VALIDATE_STATE_CODE(payload.state)                                                      AS ptnt_state_cd,	
    MASK_ZIP_CODE(SUBSTR(payload.threeDigitZip, 1, 3))                                      AS ptnt_zip3_cd,    

    /* svc_ln_start_dt  */
	CAP_DATE
        (
            CAST(EXTRACT_DATE(COALESCE(line.service_from_date, clm.statement_from_date), '%m/%d/%Y') AS DATE),  
            CAST('{EARLIEST_SERVICE_DATE}' AS DATE),
            CAST(EXTRACT_DATE('{VDR_FILE_DT}',  '%Y-%m-%d') AS DATE)
        )                                                                                   AS svc_ln_start_dt,
    /* svc_ln_end_dt  */
	CAP_DATE
        (
            CAST(EXTRACT_DATE(COALESCE(line.service_to_date, line.service_from_date, clm.statement_to_date), '%m/%d/%Y') AS DATE),
            CAST('{EARLIEST_SERVICE_DATE}' AS DATE),
            CAST(EXTRACT_DATE('{VDR_FILE_DT}',  '%Y-%m-%d') AS DATE)
        )                                                                                   AS svc_ln_end_dt,
    
    /* rndrg_prov_npi */
	CLEAN_UP_NPI_CODE
	    (
        	CASE 
        	    WHEN LPAD(line.facility_type_cd, 2, '0') IN ('05', '06', '07', '08', '09', '12', '13', '14', '33', '99') 
        	    THEN NULL 
        	ELSE clm.rendering_provider_npi 
        	END
	    )   																				AS rndrg_prov_npi,  
    /* rndrg_prov_tax_id */
	    (
        	CASE 
        	    WHEN LPAD(line.facility_type_cd, 2, '0') IN ('05', '06', '07', '08', '09', '12', '13', '14', '33', '99') 
        	    THEN NULL 
        	ELSE clm.rendering_provider_tax_id
        	END
	    )   																				AS rndrg_prov_tax_id, 
    /* adjctd_proc_cd */
	    (
        	CASE 
        	    WHEN UPPER(line.adjudicated_procedure_code_qual) IN ('NU', 'N4') 
        	    THEN NULL 
        	ELSE UPPER(line.adjudicated_procedure_code)
        	END
	    )   																				AS adjctd_proc_cd, 	

    /* adjctd_proc_cd_qual */
	    (
        	CASE 
        	    WHEN UPPER(line.adjudicated_procedure_code_qual) NOT IN ('NU', 'N4') 
        	        AND 0 <> LENGTH(COALESCE(line.adjudicated_procedure_code,''))
        	   THEN line.adjudicated_procedure_code_qual
        	ELSE NULL
        	END
	    )   																				AS adjctd_proc_cd_qual, 	
	CAST(line.service_line_charge AS FLOAT)		                                            AS svc_ln_submtd_chg_amt,
	CAST(line.service_line_paid_amount AS FLOAT)			                                AS svc_ln_prov_pymt_amt,
    /* adjctd_proc_cd = rev_cd */
	    (
        	CASE 
        	    WHEN UPPER(line.adjudicated_procedure_code_qual) IN ('NU') 
        	    THEN UPPER(line.adjudicated_procedure_code)
        	ELSE NULL
        	END
	    )   																				AS rev_cd, 	
	    
    /* adjctd_proc_cd = ndc_cd */
	    (
        	CASE 
        	    WHEN UPPER(line.adjudicated_procedure_code_qual) IN ('N4') 
        	    THEN UPPER(line.adjudicated_procedure_code)
        	ELSE NULL
        	END
	    )   																				AS ndc_cd, 	
    /* paid_svc_unt_cnt */	    
	CAST(line.paid_units AS FLOAT)			                                                AS paid_svc_unt_cnt,
    /* orig_svc_unt_cnt */	    
	CAST(line.billed_units	AS FLOAT)			                                            AS orig_svc_unt_cnt,
    /* Group Code: svc_ln_adjmt_grp_cd */	
   densify_2d_array(ARRAY(
            ARRAY(line.adjustment_group_code_1,line.adjustment_reason_code_1, line.adjustment_amount_1),
            ARRAY(line.adjustment_group_code_2,line.adjustment_reason_code_2, line.adjustment_amount_2),
            ARRAY(line.adjustment_group_code_3,line.adjustment_reason_code_3, line.adjustment_amount_3),
            ARRAY(line.adjustment_group_code_4,line.adjustment_reason_code_4, line.adjustment_amount_4),
            ARRAY(line.adjustment_group_code_5,line.adjustment_reason_code_5, line.adjustment_amount_5)
    ))[x.svcadjgrp][0]                                                                    AS svc_ln_adjmt_grp_cd,
    /* Sequenece Number: svc_ln_adjmt_seq_num */        
    CASE WHEN
        (densify_2d_array
            (ARRAY(
            ARRAY(line.adjustment_group_code_1,line.adjustment_reason_code_1, line.adjustment_amount_1),
            ARRAY(line.adjustment_group_code_2,line.adjustment_reason_code_2, line.adjustment_amount_2),
            ARRAY(line.adjustment_group_code_3,line.adjustment_reason_code_3, line.adjustment_amount_3),
            ARRAY(line.adjustment_group_code_4,line.adjustment_reason_code_4, line.adjustment_amount_4),
            ARRAY(line.adjustment_group_code_5,line.adjustment_reason_code_5, line.adjustment_amount_5)
            ))[x.svcadjgrp] = CAST(ARRAY(NULL, NULL, NULL) AS ARRAY<STRING>)
        )
        THEN NULL
        ELSE  x.svcadjgrp + 1   
    END                                                                                     AS svc_ln_adjmt_seq_num, 
    
    /* Reason Code: svc_ln_adjmt_rsn_cd */    
    densify_2d_array(ARRAY(
            ARRAY(line.adjustment_group_code_1,line.adjustment_reason_code_1, line.adjustment_amount_1),
            ARRAY(line.adjustment_group_code_2,line.adjustment_reason_code_2, line.adjustment_amount_2),
            ARRAY(line.adjustment_group_code_3,line.adjustment_reason_code_3, line.adjustment_amount_3),
            ARRAY(line.adjustment_group_code_4,line.adjustment_reason_code_4, line.adjustment_amount_4),
            ARRAY(line.adjustment_group_code_5,line.adjustment_reason_code_5, line.adjustment_amount_5)
    ))[x.svcadjgrp][1]                                                                    AS svc_ln_adjmt_rsn_cd,
    /* Adjustment Amount: svc_ln_adjmt_amt */    
    densify_2d_array(ARRAY(
            ARRAY(line.adjustment_group_code_1,line.adjustment_reason_code_1, line.adjustment_amount_1),
            ARRAY(line.adjustment_group_code_2,line.adjustment_reason_code_2, line.adjustment_amount_2),
            ARRAY(line.adjustment_group_code_3,line.adjustment_reason_code_3, line.adjustment_amount_3),
            ARRAY(line.adjustment_group_code_4,line.adjustment_reason_code_4, line.adjustment_amount_4),
            ARRAY(line.adjustment_group_code_5,line.adjustment_reason_code_5, line.adjustment_amount_5)
    ))[x.svcadjgrp][2]                                                                    AS svc_ln_adjmt_amt,
----------    
    CAST(line.service_line_allowed AS FLOAT)			                                  AS svc_ln_suplmtl_amt,
--------------------------------
    /* Service line Number */
    CAST(line.service_line_number AS INTEGER)                                             AS svc_ln_num,
	'145'                                                                                 AS part_hvm_vdr_feed_id,
    /* part_best_date */
	CASE
	    WHEN 0 = LENGTH(TRIM(COALESCE
	                            (
	                                CAP_DATE
                                        (
                                            CAST(EXTRACT_DATE(COALESCE(line.service_from_date, clm.statement_from_date), '%m/%d/%Y') AS DATE),
                                            COALESCE(CAST('{AVAILABLE_START_DATE}' AS DATE), CAST('{EARLIEST_SERVICE_DATE}' AS DATE),  CAST(EXTRACT_DATE('1901-01-01', '%Y-%m-%d') AS DATE)),
                                            CAST(EXTRACT_DATE('{VDR_FILE_DT}', '%Y-%m-%d') AS DATE)
                                        ), 
                                    ''
                                )))
	        THEN '0_PREDATES_HVM_HISTORY'
	    ELSE CONCAT
	            (
                    SUBSTR(COALESCE(line.service_from_date, clm.statement_from_date), 7, 4), '-',
                    SUBSTR(COALESCE(line.service_from_date, clm.statement_from_date), 1, 2)
                )
	END                                                                                 AS part_mth
 FROM waystar_835_dedup_lines line
 LEFT OUTER JOIN waystar_835_dedup_claims  clm     ON clm.supplier_claim_payment_number = line.claim_payment_number  
 LEFT OUTER JOIN matching_payload payload ON clm.hvjoinkey = payload.hvjoinkey
 CROSS JOIN (SELECT EXPLODE(ARRAY(0, 1, 2, 3, 4)) AS svcadjgrp) x
WHERE
    (densify_2d_array
      (ARRAY(
            ARRAY(line.adjustment_group_code_1,line.adjustment_reason_code_1, line.adjustment_amount_1),
            ARRAY(line.adjustment_group_code_2,line.adjustment_reason_code_2, line.adjustment_amount_2),
            ARRAY(line.adjustment_group_code_3,line.adjustment_reason_code_3, line.adjustment_amount_3),
            ARRAY(line.adjustment_group_code_4,line.adjustment_reason_code_4, line.adjustment_amount_4),
            ARRAY(line.adjustment_group_code_5,line.adjustment_reason_code_5, line.adjustment_amount_5)
        ))[x.svcadjgrp] != CAST(ARRAY(NULL, NULL,NULL) AS ARRAY<STRING>) OR x.svcadjgrp = 0
    )
   