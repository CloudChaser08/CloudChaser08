SELECT 
    MONOTONICALLY_INCREASING_ID()                                                           AS row_id,
    /* hv_enc_id */
    CASE 
        WHEN COALESCE(epi.provider_id, epi.record_id) IS NOT NULL
            THEN CONCAT
                    (
                        '149_',
                        COALESCE(epi.provider_id, 'UNAVAILABLE'),
                        '_',
                        COALESCE(epi.record_id, 'UNAVAILABLE')
                    )
        ELSE NULL
    END                                                                                     AS hv_enc_id,
    CURRENT_DATE()                                                                          AS crt_dt,
	'04'                                                                                    AS mdl_vrsn_num,
    SPLIT(epi.input_file_name, '/')[SIZE(SPLIT(epi.input_file_name, '/')) - 1]              AS data_set_nm,
	492                                                                                     AS hvm_vdr_id,
	149                                                                                     AS hvm_vdr_feed_id,
	epi.provider_id                                                                         AS vdr_org_id,
	epi.record_id                                                                           AS vdr_enc_id,
    /* hvid */
    COALESCE
        (
            pay.hvid, 
            CONCAT
                (
                    '492_', 
                    COALESCE
                        (
                            epi.unique_patient_id, 
                            ptn.unique_patient_id
                        )
                )
        )                                                                                   AS hvid,
    /* ptnt_birth_yr */
	CAP_YEAR_OF_BIRTH
	    (
            COALESCE(epi.age, pay.age),
            to_date(epi.discharge_dt, 'yyyyMMdd'),
            SUBSTR(COALESCE(ptn.patientdob, pay.yearofbirth), 1, 4)
        )                                                                                   AS ptnt_birth_yr,
    /* ptnt_age_num */
	VALIDATE_AGE
	    (
            COALESCE(epi.age, pay.age),
            to_date(epi.discharge_dt, 'yyyyMMdd'),
            SUBSTR(COALESCE(ptn.patientdob, pay.yearofbirth), 1, 4)
	    )                                                                                   AS ptnt_age_num,
	/* ptnt_gender_cd */
	CASE
	    WHEN SUBSTR(UPPER(epi.gender), 1, 1) IN ('F', 'M')
	        THEN SUBSTR(UPPER(epi.gender), 1, 1)
	    WHEN SUBSTR(UPPER(pay.gender), 1, 1) IN ('F', 'M')
	        THEN SUBSTR(UPPER(pay.gender), 1, 1)
	    ELSE 'U'
	END                                                                                     AS ptnt_gender_cd,
	VALIDATE_STATE_CODE(RIGHT(COALESCE(ptn.patientstate, pay.state), 2))                    AS ptnt_state_cd,
    /* ptnt_zip3_cd */
    MASK_ZIP_CODE
        (
            SUBSTR
                (
                    COALESCE
                        (
                            epi.zip_code,
                            ptn.patientzipcode,
                            pay.threedigitzip,
                            ptn.facilityzip
                        ), 1, 3
                )
        )                                                                                   AS ptnt_zip3_cd,
	/* enc_start_dt */
	CAP_DATE
	    (
            to_date(epi.admit_dt, 'yyyyMMdd'),
            CAST('{EARLIEST_SERVICE_DATE}' AS DATE),
            CAST('{VDR_FILE_DT}' AS DATE)
	    )                                                                                   AS enc_start_dt,
	/* enc_end_dt */
	CAP_DATE
	    (
            to_date(epi.discharge_dt, 'yyyyMMdd'),
            CAST('{EARLIEST_SERVICE_DATE}' AS DATE),
            CAST('{VDR_FILE_DT}' AS DATE)
	    )                                                                                   AS enc_end_dt,
    CLEAN_UP_NPI_CODE(prv.provider_npi)                                                     AS enc_fclty_npi,
    epi.provider_id                                                                         AS enc_fclty_id,
    /* enc_fclty_id_qual */
    CASE
        WHEN epi.provider_id IS NULL
            THEN NULL
        ELSE 'FACILITY_VENDOR_ID'
    END                                                                                     AS enc_fclty_id_qual,
    VALIDATE_STATE_CODE(prv.state)                                                          AS enc_fclty_state_cd,
    ptn.facilityzip                                                                         AS enc_fclty_zip_cd,
    /* enc_fclty_geo_txt */
    CASE
        WHEN COALESCE
                (
                    prv.urban_rural, 
                    prv.region,
                    prv.region_desc, 
                    prv.geo_division, 
                    prv.geo_division_desc,
                    prv_cty.county,
                    prv_cty.county_res_ratio,
                    prv_cbsa.cbsa,
                    prv_cbsa.cbsa_res_ratio
                ) IS NULL
            THEN NULL
        ELSE SUBSTR
                (
                    CONCAT
                        (
                            CASE
                                WHEN prv.urban_rural IS NULL
                                    THEN ''
                                ELSE CONCAT(' | URBAN_RURAL: ', prv.urban_rural)
                            END,
                            CASE
                                WHEN COALESCE(prv.region, prv.region_desc) IS NULL
                                    THEN ''
                                ELSE CONCAT(' | REGION: ', COALESCE(prv.region, ''), ' - ', COALESCE(prv.region_desc, ''))
                            END,
                            CASE
                                WHEN COALESCE(prv.geo_division, prv.geo_division_desc) IS NULL
                                    THEN ''
                                ELSE CONCAT(' | GEO_DIVISION: ', COALESCE(prv.geo_division, ''), ' - ', COALESCE(prv.geo_division_desc, ''))
                            END,
                            CASE
                                WHEN prv_cty.county IS NULL
                                    THEN ''
                                ELSE CONCAT(' | COUNTY: ', prv_cty.county)
                            END,
                            CASE
                                WHEN prv_cty.county_res_ratio IS NULL
                                    THEN ''
                                ELSE CONCAT(' | COUNTY_RESIDENCE_RATIO: ', prv_cty.county_res_ratio)
                            END,
                            CASE
                                WHEN prv_cbsa.cbsa IS NULL
                                    THEN ''
                                ELSE CONCAT(' | CBSA: ', prv_cbsa.cbsa)
                            END,
                            CASE
                                WHEN prv_cbsa.cbsa_res_ratio IS NULL
                                    THEN ''
                                ELSE CONCAT(' | CBSA_RESIDENCE_RATIO: ', prv_cbsa.cbsa_res_ratio)
                            END
                        ), 4
                )
    END                                                                                     AS enc_fclty_geo_txt,
    /* enc_fclty_grp_txt */
    CASE
        WHEN COALESCE
                (
                    prv.bed_grp, 
                    prv.bed_grp_desc, 
                    prv.teaching, 
                    prv.rcc
                ) IS NULL
            THEN NULL
        ELSE SUBSTR
                (
                    CONCAT
                        (
                            CASE
                                WHEN COALESCE(prv.bed_grp, prv.bed_grp_desc) IS NULL
                                    THEN ''
                                ELSE CONCAT(' | BED_GRP: ', COALESCE(prv.bed_grp, ''), ' - ', COALESCE(prv.bed_grp_desc, ''))
                            END,
                            CASE
                                WHEN prv.teaching IS NULL
                                    THEN ''
                                ELSE CONCAT(' | TEACHING: ', prv.teaching)
                            END,
                            CASE
                                WHEN prv.rcc IS NULL
                                    THEN ''
                                ELSE CONCAT(' | RCC: ', prv.rcc)
                            END
                        ), 4
                )
    END                                                                                     AS enc_fclty_grp_txt,
    /*---------------------------------------------------------- Best Calculated Provider ----------------------------------------------------------*/
    /* The next two columns are for the best provider ID available. */
    /* The hierarchy is as follows: Rendering (aka Attending), Admitting, Operating, Serving. */
    /* enc_prov_id */
    CASE
        WHEN epi.attendingphysiciannpi IS NOT NULL
            THEN MD5(epi.attendingphysiciannpi)
        WHEN epi.admittingphysiciannpi IS NOT NULL
            THEN MD5(epi.admittingphysiciannpi)
        WHEN epi.operatingphysiciannpi IS NOT NULL
            THEN MD5(epi.operatingphysiciannpi)
        WHEN epi.servingphysiciannpi IS NOT NULL
            THEN MD5(epi.servingphysiciannpi)
        ELSE NULL
    END                                                                                     AS enc_prov_id,
    /* enc_prov_id_qual */
    CASE
        WHEN epi.attendingphysiciannpi IS NOT NULL
            THEN 'RENDERING_PROVIDER_NPI'
        WHEN epi.admittingphysiciannpi IS NOT NULL
            THEN 'ADMITTING_PROVIDER_NPI'
        WHEN epi.operatingphysiciannpi IS NOT NULL
            THEN 'OPERATING_PROVIDER_NPI'
        WHEN epi.servingphysiciannpi IS NOT NULL
            THEN 'SERVING_PROVIDER_NPI'
        ELSE NULL
    END                                                                                     AS enc_prov_id_qual,
    COALESCE(epi.unique_patient_id, ptn.unique_patient_id)                                  AS vdr_ptnt_id,
    /* enc_grp_txt */
    CASE
        WHEN COALESCE
                (
                    epi.patient_type, 
                    ptn.patienttype, 
                    wts.weight
                ) IS NULL
            THEN NULL
        ELSE SUBSTR
                (
                    CONCAT
                        (
                            CASE
                                WHEN COALESCE(epi.patient_type, 'X') IN ('I', 'O')
                                    THEN 
                                        CASE
                                            WHEN SUBSTR(UPPER(epi.patient_type), 1, 1) = 'I'
                                                THEN ' | ENCOUNTER_VISIT_TYPE: Inpatient'
                                            WHEN SUBSTR(UPPER(epi.patient_type), 1, 1) = 'O'
                                                THEN ' | ENCOUNTER_VISIT_TYPE: Outpatient'
                                            ELSE ''
                                        END
                                WHEN COALESCE(ptn.patienttype, 'X') IN ('I', 'O')
                                    THEN 
                                        CASE
                                            WHEN SUBSTR(UPPER(ptn.patienttype), 1, 1) = 'I'
                                                THEN ' | ENCOUNTER_VISIT_TYPE: Inpatient'
                                            WHEN SUBSTR(UPPER(ptn.patienttype), 1, 1) = 'O'
                                                THEN ' | ENCOUNTER_VISIT_TYPE: Outpatient'
                                            ELSE ''
                                        END
                                ELSE ''
                            END,
                            CASE
                                WHEN wts.weight IS NULL
                                    THEN ''
                                ELSE CONCAT(' | HOSPITAL_WEIGHT: ', wts.weight)
                            END
                        ), 4
                )
	END			                                                                            AS enc_grp_txt,
	epi.los		                                                    	                    AS los_day_cnt,
    /* los_txt */
    CASE
        WHEN COALESCE
                (
                    epi.los_adm_pri_proc, 
                    epi.los_pri_proc_disch
                ) IS NULL
            THEN NULL
        ELSE SUBSTR
                (
                    CONCAT
                        (
                            CASE
                                WHEN epi.los_adm_pri_proc IS NULL
                                    THEN ''
                                ELSE CONCAT(' | LOS_ADMISSION_TO_PRINCIPLE_PROCEDURE: ', epi.los_adm_pri_proc)
                            END,
                            CASE
                                WHEN epi.los_pri_proc_disch IS NULL
                                    THEN ''
                                ELSE CONCAT(' | LOS_PRINCIPLE_PROCEDURE_TO_DISCHARGE: ', epi.los_pri_proc_disch)
                            END
                        ), 4
                )
    END                                                                                     AS los_txt,
    epi.admission_source                                                                    AS admsn_src_std_cd,
    epi.admission_type                                                                      AS admsn_typ_std_cd,
    CASE
        WHEN CAST('{VDR_FILE_DT}' AS DATE) >= '2020-01-01' THEN
        	CASE
        	    WHEN COALESCE(epi.discharge_status, '') IN ('21', '69', '87') THEN '0'
            ELSE epi.discharge_status
        	END   
	ELSE
    	CASE
    	    WHEN COALESCE(epi.discharge_status, '') IN ('20', '21', '40', '41', '42', '69', '87') THEN '0'
            ELSE epi.discharge_status
    	END  	
	END                  AS dischg_stat_std_cd,
	/* bill_typ_std_cd */
	CASE
	    WHEN epi.bill_type IS NULL
	        THEN NULL
	    WHEN SUBSTR(epi.bill_type, 1, 1) = '3'
	        THEN CONCAT('X', SUBSTR(epi.bill_type, 2))
        ELSE epi.bill_type
	END                                                                                     AS bill_typ_std_cd,
	/* drg_cd */
	CASE
	    WHEN COALESCE(ms_drg.ms_drg_code, drg.drg_code) IS NULL
	        THEN NULL
        /* If the code is blacklisted, load NULL. */
	    WHEN ms_drg.ms_drg_code IS NOT NULL
	     AND ref2.gen_ref_cd IS NOT NULL
	        THEN NULL
	    WHEN ms_drg.ms_drg_code IS NOT NULL
	        THEN ms_drg.ms_drg_code
        /* If the code is blacklisted, load NULL. */
	    WHEN drg.drg_code IS NOT NULL
	     AND ref1.gen_ref_cd IS NOT NULL
	        THEN NULL
	    WHEN drg.drg_code IS NOT NULL
	        THEN drg.drg_code
	    ELSE NULL
	END                                                                                     AS drg_cd,
	/* drg_cd_qual */
	CASE
	    WHEN ms_drg.ms_drg_code IS NOT NULL
	        THEN 'MS_DRG'
	    WHEN drg.drg_code IS NOT NULL
	        THEN 'DRG'
	    ELSE NULL
	END                                                                                     AS drg_cd_qual,
    /*---------------------------------------------------------- Summary Amounts ----------------------------------------------------------*/
	CAST(epi.total_charges AS FLOAT)                                                        AS tot_chg_amt,
    CAST(epi.payment_actual AS FLOAT)                                                       AS tot_actl_pymt_amt,
    CAST(epi.payment_expected AS FLOAT)                                                     AS tot_expctd_pymt_amt,
    CAST(epi.discount_amount AS FLOAT)                                                      AS tot_disc_amt,
    CAST(epi.pat_liability_amount AS FLOAT)                                                 AS tot_ptnt_liabty_amt,
    CAST(epi.pat_payment AS FLOAT)                                                          AS tot_ptnt_pymt_amt,
    CAST(epi.account_balance AS FLOAT)                                                      AS tot_bal_amt,
    CAST(epi.eob_writeoff AS FLOAT)                                                         AS tot_eob_wrtoff_amt,
    CAST(epi.contractual_adjustment AS FLOAT)                                               AS tot_cntrctl_adjmt_amt,
    CAST(epi.other_adjustment AS FLOAT)                                                     AS tot_other_adjmt_amt,
    CAST(epi.other_payment AS FLOAT)                                                        AS tot_other_pymt_amt,
    CAST(epi.amount_denied AS FLOAT)                                                        AS tot_denied_amt,
    CAST(epi.amount_copay AS FLOAT)                                                         AS tot_ptnt_cpy_amt,
    CAST(epi.amount_deductible AS FLOAT)                                                    AS tot_ptnt_ddctbl_amt,
    CAST(epi.sec_insurance_payment AS FLOAT)                                                AS tot_secdy_payr_pymt_amt,
    CAST(epi.total_cost AS FLOAT)                                                           AS tot_recalcd_cost_amt,
    /* tot_recalcd_cost_amt_qual */
    CASE
        WHEN CAST(epi.total_cost AS FLOAT) IS NOT NULL
            THEN 'RCC_METHODOLOGY'
        ELSE NULL
    END                                                                                     AS tot_recalcd_cost_amt_qual,
    /*-------------------------------------------------------------------------------------------------------------------------------------*/
    /* payr_grp_txt */
    CASE
        WHEN COALESCE
                (
                    ptn_pyr1.payer_desc, 
                    ptn_pyr1.ins_plan_desc, 
                    ptn_pyr1.payer_cat,
                    ptn_pyr2.payer_desc, 
                    ptn_pyr2.ins_plan_desc, 
                    ptn_pyr2.payer_cat,
                    ptn_pyr3.payer_desc, 
                    ptn_pyr3.ins_plan_desc, 
                    ptn_pyr3.payer_cat,
                    ptn_pyr4.payer_desc, 
                    ptn_pyr4.ins_plan_desc, 
                    ptn_pyr4.payer_cat,
                    ptn_pyr5.payer_desc, 
                    ptn_pyr5.ins_plan_desc, 
                    ptn_pyr5.payer_cat
                ) IS NULL
            THEN NULL
        ELSE SUBSTR
                (
                    CONCAT
                        (
                            CASE
                                WHEN ptn_pyr1.payer_desc IS NULL
                                    THEN ''
                                ELSE CONCAT(' | PAYER_DESCRIPTION_1: ', ptn_pyr1.payer_desc)
                            END,
                            CASE
                                WHEN ptn_pyr1.ins_plan_desc IS NULL
                                    THEN ''
                                ELSE CONCAT(' | INSURANCE_PLAN_DESCRIPTION_1: ', ptn_pyr1.ins_plan_desc)
                            END,
                            CASE
                                WHEN ptn_pyr1.payer_cat IS NULL
                                    THEN ''
                                ELSE CONCAT(' | PAYER_CATEGORY_1: ', ptn_pyr1.payer_cat)
                            END,
                            CASE
                                WHEN ptn_pyr2.payer_desc IS NULL
                                    THEN ''
                                ELSE CONCAT(' | PAYER_DESCRIPTION_2: ', ptn_pyr2.payer_desc)
                            END,
                            CASE
                                WHEN ptn_pyr2.ins_plan_desc IS NULL
                                    THEN ''
                                ELSE CONCAT(' | INSURANCE_PLAN_DESCRIPTION_2: ', ptn_pyr2.ins_plan_desc)
                            END,
                            CASE
                                WHEN ptn_pyr2.payer_cat IS NULL
                                    THEN ''
                                ELSE CONCAT(' | PAYER_CATEGORY_2: ', ptn_pyr2.payer_cat)
                            END,
                            CASE
                                WHEN ptn_pyr3.payer_desc IS NULL
                                    THEN ''
                                ELSE CONCAT(' | PAYER_DESCRIPTION_3: ', ptn_pyr3.payer_desc)
                            END,
                            CASE
                                WHEN ptn_pyr3.ins_plan_desc IS NULL
                                    THEN ''
                                ELSE CONCAT(' | INSURANCE_PLAN_DESCRIPTION_3: ', ptn_pyr3.ins_plan_desc)
                            END,
                            CASE
                                WHEN ptn_pyr3.payer_cat IS NULL
                                    THEN ''
                                ELSE CONCAT(' | PAYER_CATEGORY_3: ', ptn_pyr3.payer_cat)
                            END,
                            CASE
                                WHEN ptn_pyr4.payer_desc IS NULL
                                    THEN ''
                                ELSE CONCAT(' | PAYER_DESCRIPTION_4: ', ptn_pyr4.payer_desc)
                            END,
                            CASE
                                WHEN ptn_pyr4.ins_plan_desc IS NULL
                                    THEN ''
                                ELSE CONCAT(' | INSURANCE_PLAN_DESCRIPTION_4: ', ptn_pyr4.ins_plan_desc)
                            END,
                            CASE
                                WHEN ptn_pyr4.payer_cat IS NULL
                                    THEN ''
                                ELSE CONCAT(' | PAYER_CATEGORY_4: ', ptn_pyr4.payer_cat)
                            END,
                            CASE
                                WHEN ptn_pyr5.payer_desc IS NULL
                                    THEN ''
                                ELSE CONCAT(' | PAYER_DESCRIPTION_5: ', ptn_pyr5.payer_desc)
                            END,
                            CASE
                                WHEN ptn_pyr5.ins_plan_desc IS NULL
                                    THEN ''
                                ELSE CONCAT(' | INSURANCE_PLAN_DESCRIPTION_5: ', ptn_pyr5.ins_plan_desc)
                            END,
                            CASE
                                WHEN ptn_pyr5.payer_cat IS NULL
                                    THEN ''
                                ELSE CONCAT(' | PAYER_CATEGORY_5: ', ptn_pyr5.payer_cat)
                            END
                        ), 4
                )
    END                                                                                     AS payr_grp_txt,
    /*------------------------------ New death indicator only avaialbe in the special views (JKS 2020-08-13) -----------------------------*/
    CASE
        WHEN epi.mortality = '0' THEN 'Y'
        WHEN epi.mortality = '1' THEN 'N'
    ELSE NULL
    END                                                                                     AS ptnt_lvg_flg,
    CASE
        WHEN epi.mortality = '1' 
         AND TO_DATE(epi.discharge_dt, 'yyyyMMdd') IS NOT NULL THEN TO_DATE(CONCAT(SUBSTR(epi.discharge_dt,1,6),'01'),'yyyyMMdd')
    ELSE NULL 
    END                                                                                     AS ptnt_dth_dt,
    CASE 
        WHEN CAST(epi.icu_ccu_days AS INT) > 0 THEN epi.icu_ccu_days
    ELSE NULL
    END                                                                                     AS icu_ccu_days_cnt,
    /*-------------------------------------------------------------------------------------------------------------------------------------*/
    'episodes'                                                                              AS prmy_src_tbl_nm,
	'149'                                                                                   AS part_hvm_vdr_feed_id,
	/* part_mth */
	CASE
	    WHEN 0 = LENGTH(TRIM(COALESCE(CAP_DATE
                                        (
                                            to_date(epi.admit_dt, 'yyyyMMdd'),
                                            COALESCE(CAST('{AVAILABLE_START_DATE}' AS DATE), CAST('{EARLIEST_SERVICE_DATE}' AS DATE)),
                                            CAST('{VDR_FILE_DT}' AS DATE)
                                        ), '')))
	        THEN '0_PREDATES_HVM_HISTORY'
	    ELSE CONCAT
	            (
                    SUBSTR(epi.admit_dt, 1, 4), '-',
                    SUBSTR(epi.admit_dt, 5, 2)
                )
	END                                                                                     AS part_mth
 FROM episodes epi
 LEFT OUTER JOIN providers prv
   ON COALESCE(epi.provider_id, 'EMPTY') = COALESCE(prv.provider_id, 'EMPTY')
 LEFT OUTER JOIN patient ptn
   ON COALESCE(epi.record_id, 'EMPTY') = COALESCE(ptn.record_id, 'EMPTY')
 LEFT OUTER JOIN matching_payload pay
   ON COALESCE(ptn.hvjoinkey, 'EMPTY') = COALESCE(pay.hvjoinkey, 'EMPTY')
 LEFT OUTER JOIN patient_payer ptn_pyr1
   ON COALESCE(epi.record_id, 'EMPTY') = COALESCE(ptn_pyr1.record_id, 'EMPTY')
  AND COALESCE(ptn_pyr1.payer_order, '0') = '1'
 LEFT OUTER JOIN patient_payer ptn_pyr2
   ON COALESCE(epi.record_id, 'EMPTY') = COALESCE(ptn_pyr2.record_id, 'EMPTY')
  AND COALESCE(ptn_pyr2.payer_order, '0') = '2'
 LEFT OUTER JOIN patient_payer ptn_pyr3
   ON COALESCE(epi.record_id, 'EMPTY') = COALESCE(ptn_pyr3.record_id, 'EMPTY')
  AND COALESCE(ptn_pyr3.payer_order, '0') = '3'
 LEFT OUTER JOIN patient_payer ptn_pyr4
   ON COALESCE(epi.record_id, 'EMPTY') = COALESCE(ptn_pyr4.record_id, 'EMPTY')
  AND COALESCE(ptn_pyr1.payer_order, '0') = '4'
 LEFT OUTER JOIN patient_payer ptn_pyr5
   ON COALESCE(epi.record_id, 'EMPTY') = COALESCE(ptn_pyr5.record_id, 'EMPTY')
  AND COALESCE(ptn_pyr1.payer_order, '0') = '5'
 LEFT OUTER JOIN provider_county prv_cty
   ON COALESCE(epi.provider_id, 'EMPTY') = COALESCE(prv_cty.provider_id, 'EMPTY')
 LEFT OUTER JOIN provider_cbsa prv_cbsa
   ON COALESCE(epi.provider_id, 'EMPTY') = COALESCE(prv_cbsa.provider_id, 'EMPTY')
 LEFT OUTER JOIN weights wts
   ON COALESCE(epi.provider_id, 'EMPTY') = COALESCE(wts.provider_id, 'EMPTY')
  AND COALESCE(epi.patient_type, 'EMPTY') = COALESCE(wts.patient_type, 'EMPTY')
  AND SUBSTR(COALESCE(epi.discharge_dt, 'EMPTY'), 1, 6) = COALESCE(wts.discharge_month, 'EMPTY')
  AND wts.weight IS NOT NULL
 LEFT OUTER JOIN master_drg drg
   ON COALESCE(epi.drg_id, 'EMPTY') = COALESCE(drg.drg_id, 'EMPTY')
  AND COALESCE(epi.drg_id, '0') <> '0'
 LEFT OUTER JOIN ref_gen_ref ref1
   ON ref1.gen_ref_domn_nm = 'drg_code_blacklist'
  AND ref1.whtlst_flg = 'N'
  AND COALESCE(drg.drg_code, 'EMPTY') = COALESCE(ref1.gen_ref_cd, 'EMPTY')
 LEFT OUTER JOIN master_ms_drg ms_drg
   ON COALESCE(epi.ms_drg_id, 'EMPTY') = COALESCE(ms_drg.ms_drg_id, 'EMPTY')
  AND COALESCE(epi.ms_drg_id, '0') <> '0'
 LEFT OUTER JOIN ref_gen_ref ref2
   ON ref2.gen_ref_domn_nm = 'ms_drg_code_blacklist'
  AND ref2.whtlst_flg = 'N'
  AND COALESCE(ms_drg.ms_drg_code, 'EMPTY') = COALESCE(ref2.gen_ref_cd, 'EMPTY')
/* Eliminate column headers. */
WHERE UPPER(COALESCE(epi.record_id, '')) <> 'RECORD_ID'
