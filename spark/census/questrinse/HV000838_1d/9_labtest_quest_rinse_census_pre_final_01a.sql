SELECT DISTINCT
    norm_pre01.HV_claim_id	,
    norm_pre01.hvid	        ,
    norm_pre01.created	    ,
    norm_pre01.model_version,
    norm_pre01.data_set	    ,
    norm_pre01.data_feed	,
    norm_pre01.data_vendor	,
    norm_pre01.HV_patient_gender,
    norm_pre01.HV_patient_age	,
    norm_pre01.HV_patient_year_of_birth	,
    norm_pre01.HV_patient_zip3	,
    norm_pre01.HV_patient_state	,
    norm_pre01.date_service	    ,
    norm_pre01.date_specimen	,
    norm_pre01.HV_date_report	,
    norm_pre01.loinc_code	    ,
    ---------------------------------------------------------------------------------------------------------------->
    --(2021-03-24 JKS - scope change Excep only NUMERIC LOINC code)
    ---------------------------------------------------------------------------------------------------------------->
    CASE
        WHEN norm_pre01.hv_loinc_code NOT RLIKE '^[0-9-]+$'   THEN NULL
        ELSE norm_pre01.hv_loinc_code
    END             AS hv_loinc_code   ,
--    norm_pre01.hv_loinc_code	,
    norm_pre01.lab_id	        ,
    norm_pre01.test_id	        ,
    norm_pre01.HV_test_number	,
    norm_pre01.test_battery_local_id,
    norm_pre01.test_battery_std_id	,
    norm_pre01.test_battery_name	,
    norm_pre01.test_ordered_local_id,
    norm_pre01.test_ordered_std_id	,
    norm_pre01.test_ordered_name	,
    norm_pre01.result_id	        ,
    norm_pre01.result	            ,
    norm_pre01.result_name	        ,
    norm_pre01.result_unit_of_measure	,
    norm_pre01.result_desc	        ,
    norm_pre01.HV_ref_range_alpha	,
    norm_pre01.HV_fasting_status	,
--    norm_pre01.HV_ONE_diagnosis_code,
    norm_pre01.HV_procedure_code	,
    norm_pre01.HV_procedure_code_qual	,
    norm_pre01.HV_ordering_npi	    ,
    norm_pre01.payer_id	            ,
    norm_pre01.payer_name	        ,
    norm_pre01.lab_other_id	,
    norm_pre01.HV_lab_other_qual	,
    norm_pre01.ordering_market_type	,
    norm_pre01.ordering_specialty	,
    norm_pre01.ordering_state_license,
    norm_pre01.ordering_upin	    ,
    norm_pre01.ordering_address_1	,
    norm_pre01.ordering_address_2	,
    norm_pre01.ordering_city	    ,
    norm_pre01.HV_ordering_state	,
    norm_pre01.ordering_zip	        ,
    norm_pre01.part_provider	    ,
    norm_pre01.HV_part_best_date	,
    norm_pre01.unique_accession_id	,
    norm_pre01.diag_accn_id	        ,
    norm_pre01.diag_date_of_service	,
    norm_pre01.diag_lab_code	    ,
    norm_pre01.acct_id	            ,
    norm_pre01.acct_number	        ,
    norm_pre01.diag_dos_yyyymm	    ,
    norm_pre01.rslt_accn_id	        ,
    norm_pre01.lab_code	            ,
    norm_pre01.phy_id	            ,
    norm_pre01.rslt_accession_number,
    norm_pre01.accn_dom_id	        ,
    norm_pre01.cmdm_spclty_cd	    ,
    norm_pre01.acct_name	        ,
    norm_pre01.cmdm_licstate	    ,
    norm_pre01.billing_client_number,
    norm_pre01.fasting_hours	    ,
    norm_pre01.qbs_ordering_client_num	,
    norm_pre01.date_order_entry	   ,
    norm_pre01.informed_consent_flag	,
    norm_pre01.legal_entity	       ,
    norm_pre01.specimen_type	   ,
    norm_pre01.ordering_site_code  ,
    norm_pre01.canceled_accn_ind   ,
    norm_pre01.copy_to_clns	       ,
    norm_pre01.non_physician_name  ,
    norm_pre01.non_physician_id	   ,
    norm_pre01.long_description	   ,
    norm_pre01.phy_name	,
    norm_pre01.suffix	,
    norm_pre01.degree	,
    norm_pre01.idw_analyte_code	,
    norm_pre01.qls_service_code	,
    norm_pre01.reportable_results_ind	,
    norm_pre01.ord_seq	,
    norm_pre01.res_seq	,
    norm_pre01.abnormal_ind	,
    norm_pre01.amended_report_ind	,
    norm_pre01.alpha_normal_flag	,
    norm_pre01.instrument_id	,
    norm_pre01.result_release_date	,
    norm_pre01.enterprise_ntc_code	,
    norm_pre01.derived_profile_code	,
    norm_pre01.qtim_as_ordered_code	,
    norm_pre01.qtim_profile_ind	,
    norm_pre01.rslt_dos_yyyymm	,
    norm_pre01.sister_lab	,
    norm_pre01.bill_type_cd	,
    norm_pre01.idw_report_change_status	,
    norm_pre01.ins_seq	,
    norm_pre01.active_ind	,
    norm_pre01.bill_code	,
    norm_pre01.insurance_billing_type	,
    norm_pre01.qbs_payor_cd	,
    norm_pre01.dos_id	,
    norm_pre01.lab_name	,
    norm_pre01.lab_lis_type	,
    norm_pre01.confidential_order_ind	,
    norm_pre01.daard_client_flag	,
    norm_pre01.ptnt_accession_number,
    norm_pre01.ptnt_date_of_service	,
    norm_pre01.ptnt_lab_code	    ,
    norm_pre01.accn_enterprise_id	,
    norm_pre01.age_code	            ,
    norm_pre01.species	            ,
    norm_pre01.pat_country	        ,
    norm_pre01.external_patient_id	,
    norm_pre01.pat_master_id	    ,
    norm_pre01.lab_reference_number	,
    norm_pre01.room_number	        ,
    norm_pre01.bed_number	        ,
    norm_pre01.hospital_location	,
    norm_pre01.ward	                ,
    norm_pre01.admission_date	    ,
    norm_pre01.health_id	        ,
    norm_pre01.pm_eid	            ,
    norm_pre01.idw_pm_email_address	,
    norm_pre01.date_reported	,
    norm_pre01.ref_range_low	,
    norm_pre01.ref_range_high	,
    norm_pre01.ref_range_alpha	,
    norm_pre01.requisition_number	,
    norm_pre01.fasting_ind	,
    norm_pre01.cpt_code	,
    norm_pre01.npi	,
    norm_pre01.phy_last_name	,
    norm_pre01.phy_first_name	,
    norm_pre01.phy_middle_name	,
    norm_pre01.acct_state	,
    norm_pre01.date_final_report	,

    norm_pre01.s_diag_code_codeset_ind	,
    norm_pre01.HV_s_diag_code_codeset_ind,

    norm_pre01.HV_result_value_operator	,
    norm_pre01.HV_result_value_numeric	,
    norm_pre01.HV_result_value_alpha	,
    norm_pre01.HV_result_value	,
    norm_pre01.profile_name_qtim	,
    norm_pre01.order_name_qtim	,
    norm_pre01.specimen_type_desc_qtim	,
    norm_pre01.methodology_qtim	,
    norm_pre01.result_name_qtim	,
    norm_pre01.unit_of_measure_qtim	,
    norm_pre01.loinc_number_qtim	,
    --------hv_abnormal_indicator  (CASE sequence is important)--------
    CASE
        WHEN UPPER(norm_pre01.abnormal_ind)   in ('H', 'L', 'A', 'AA', 'HH', 'LL')             THEN 'Y'--1.
        WHEN UPPER(norm_pre01.abnormal_ind)   = 'N'                                            THEN 'N' --2.

        WHEN (norm_pre01.result IS NULL OR (UPPER(TRIM(norm_pre01.result)) = UPPER('Not Applicable')
          OR UPPER(gold.gen_ref_desc) in ( UPPER('Test Not Performed'), UPPER('Do Not Report'), UPPER('Not Given')) --added
          OR UPPER(TRIM(norm_pre01.ref_range_alpha)) = UPPER('Not Established')))              THEN 'NA'  -- 3.  --good

        WHEN (UPPER(TRIM(norm_pre01.result)) LIKE UPPER('See %') or UPPER(TRIM(norm_pre01.ref_range_alpha)) LIKE UPPER('See %'))
                                                                                        THEN 'Unknown1' --4. good
        WHEN UPPER(norm_pre01.result) like 'NONE%' and Substring(ref_range_alpha,1,1) = '<' THEN 'N' --6

        WHEN norm_pre01.result IS NOT NULL AND ref_range_low IS NULL AND ref_range_high IS NULL AND (norm_pre01.ref_range_alpha IS NULL or upper(ref_range_alpha)='NULL')
                                                                                        THEN 'Unknown2'   --6.
        -- in range
        WHEN replace(norm_pre01.result,' ' ,'') rlike ('^(-|\\+|\\.)?[0-9]+(\\.)?[0-9]*$') -- numeric results ---8.
         AND ref_range_alpha IS NULL and ref_range_low IS NOT NULL AND ref_range_high IS NOT NULL --numeric range
         AND CAST(norm_pre01.result AS FLOAT) >= CAST(norm_pre01.ref_range_low  AS FLOAT) AND CAST(norm_pre01.result AS FLOAT) <= CAST(norm_pre01.ref_range_high  AS FLOAT)  THEN 'N'

        -- out range
        WHEN replace(norm_pre01.result,' ' ,'') rlike ('^(-|\\+|\\.)?[0-9]+(\\.)?[0-9]*$') -- numeric results --- 8.
         AND ref_range_alpha IS NULL and ref_range_low IS NOT NULL AND ref_range_high IS NOT NULL --numeric range
         AND CAST(norm_pre01.result AS FLOAT) < CAST(norm_pre01.ref_range_low  AS FLOAT) OR CAST(norm_pre01.result AS FLOAT) > CAST(norm_pre01.ref_range_high  AS FLOAT)    THEN 'Y'

          -- has one range value
        WHEN replace(norm_pre01.result,' ' ,'') rlike ('^(-|\\+|\\.)?[0-9]+(\\.)?[0-9]*$') AND norm_pre01.ref_range_alpha IS NULL
        AND ((norm_pre01.ref_range_low IS NULL  AND norm_pre01.ref_range_high IS NOT NULL) OR (norm_pre01.ref_range_low IS NOT NULL AND norm_pre01.ref_range_high IS NULL))
                                                                                                                  THEN 'Unknown3'  -- 9
        -- no operator & numeric value
        WHEN replace(norm_pre01.result,' ' ,'') rlike ('^(-|\\+|\\.)?[0-9]+(\\.)?[0-9]*$') AND REPLACE(norm_pre01.ref_range_alpha,' ','') rlike ('^(-|\\+|\\.)?[0-9]+(\\.)?[0-9]*$')
                                                                                                                  THEN 'Unknown3' --10

        --Numeric results
           -- ref_range_alpha has operator: <
        WHEN CAST(norm_pre01.result AS FLOAT) < CAST(substring_index(replace(upper(norm_pre01.ref_range_alpha),' ',''),'<',-1) as float) and substr(norm_pre01.ref_range_alpha,1,1) = '<' then 'N'
        WHEN CAST(norm_pre01.result AS FLOAT) >= CAST(substring_index(replace(upper(norm_pre01.ref_range_alpha),' ',''),'<',-1) as float) and substr(norm_pre01.ref_range_alpha,1,1) = '<' then 'Y'
          -- ref_range_alpha has operator: >
        WHEN CAST(norm_pre01.result AS FLOAT) > CAST(substring_index(replace(upper(norm_pre01.ref_range_alpha),' ',''),'>',-1) as float) and substr(norm_pre01.ref_range_alpha,1,1) = '>' then 'N'
        WHEN CAST(norm_pre01.result AS FLOAT) <= CAST(substring_index(replace(upper(norm_pre01.ref_range_alpha),' ',''),'>',-1) as float) and substr(norm_pre01.ref_range_alpha,1,1) = '>' then 'Y'
          -- ref_range_alpha has operator: =
        WHEN CAST(norm_pre01.result AS FLOAT) = CAST(substring_index(replace(upper(norm_pre01.ref_range_alpha),' ',''),'=',-1) as float) and substr(norm_pre01.ref_range_alpha,1,1) = '='  then 'N'
        WHEN CAST(norm_pre01.result AS FLOAT) <> CAST(substring_index(replace(upper(norm_pre01.ref_range_alpha),' ',''),'=',-1) as float) and substr(norm_pre01.ref_range_alpha,1,1) = '=' then 'Y'

          -- ref_range_alpha has operator: <OR=
        WHEN CAST(norm_pre01.result AS FLOAT) <= CAST(substring_index(replace(upper(norm_pre01.ref_range_alpha),' ',''),'<OR=',-1) as float) and SUBSTR(replace(upper(norm_pre01.ref_range_alpha),' ',''), 1,4) = '<OR='    then 'N'
        WHEN CAST(norm_pre01.result AS FLOAT)  > cast(substring_index(replace(upper(norm_pre01.ref_range_alpha),' ',''),'<OR=',-1) as float) and SUBSTR(replace(upper(norm_pre01.ref_range_alpha),' ',''), 1,4) = '<OR='    then 'Y'
          -- ref_range_alpha has operator: >OR=
        WHEN CAST(norm_pre01.result AS FLOAT) >= cast(substring_index(replace(upper(norm_pre01.ref_range_alpha),' ',''),'>OR=',-1) as float) and upper(norm_pre01.ref_range_alpha) like '%OR%=%'then 'N'
        WHEN CAST(norm_pre01.result AS FLOAT) < cast(substring_index(replace(upper(norm_pre01.ref_range_alpha),' ',''),'>OR=',-1) as float) and upper(norm_pre01.ref_range_alpha) like  '%OR%=%'then 'Y'
          -- ref_range_alpha has operator: <=
        WHEN CAST(norm_pre01.result AS FLOAT) <= CAST(substring_index(replace(upper(norm_pre01.ref_range_alpha),' ',''),'<=',-1) as float) and upper(norm_pre01.ref_range_alpha) like '%=%'then 'N'
        WHEN CAST(norm_pre01.result AS FLOAT) > CAST(substring_index(replace(upper(norm_pre01.ref_range_alpha),' ',''),'<=',-1) as float) and upper(norm_pre01.ref_range_alpha) like  '%=%'then 'Y'
          -- ref_range_alpha has operator: >=
        WHEN CAST(norm_pre01.result AS FLOAT) >= CAST(substring_index(replace(upper(norm_pre01.ref_range_alpha),' ',''),'>=',-1) as float) and upper(norm_pre01.ref_range_alpha) like '%=%'then 'N'
        WHEN CAST(norm_pre01.result AS FLOAT) < CAST(substring_index(replace(upper(norm_pre01.ref_range_alpha),' ',''),'>=',-1) as float) and upper(norm_pre01.ref_range_alpha) like  '%=%'then 'Y'
          -- ref_range_alpha  has operator: <>
        WHEN CAST(norm_pre01.result AS FLOAT) <> CAST(substring_index(replace(upper(norm_pre01.ref_range_alpha),' ',''),'<>',-1) as float) and SUBSTR(REPLACE(UPPER(norm_pre01.ref_range_alpha),' ',''), 1, 2) = '<>' then 'N'
        WHEN CAST(norm_pre01.result AS FLOAT) <  CAST(substring_index(replace(upper(norm_pre01.ref_range_alpha),' ',''),'<>',-1) as float) and SUBSTR(REPLACE(UPPER(norm_pre01.ref_range_alpha),' ',''), 1, 2) = '<>' then 'Y'
        WHEN CAST(norm_pre01.result AS FLOAT) >  CAST(substring_index(replace(upper(norm_pre01.ref_range_alpha),' ',''),'<>',-1) as float) and SUBSTR(REPLACE(UPPER(norm_pre01.ref_range_alpha),' ',''), 1, 2) = '<>' then 'Y'

          -- ref_range_alpha has a range #-# or +#-+#
        WHEN substring(norm_pre01.ref_range_alpha,1,1) !='-'  and replace(norm_pre01.ref_range_alpha,' ' ,'') rlike ( '^[0123456789\\-\\+\\.\\s]+$')
         AND (cast(norm_pre01.result as float) >= cast(substring_index(replace(upper(norm_pre01.ref_range_alpha),' ',''),'-',1) as float)
           and cast(norm_pre01.result as float) <= cast(substring_index(replace(upper(norm_pre01.ref_range_alpha),' ',''),'-',-1) as float))
                                                                                                                                        then 'N'
        WHEN substring(norm_pre01.ref_range_alpha,1,1) !='-' and replace(norm_pre01.ref_range_alpha,' ' ,'') rlike ( '^[0123456789\\-\\+\\.\\s]+$')
         AND (cast(norm_pre01.result as float) <  cast(substring_index(replace(upper(norm_pre01.ref_range_alpha),' ',''),'-',1) as float)
          or cast(norm_pre01.result as float)  > cast(substring_index(replace(upper(norm_pre01.ref_range_alpha),' ',''),'-',-1) as float))
                                                                                                                                        then 'Y'

          -- ref_range_alpha has a range negative -#-+#, -#--#
        WHEN  substring(norm_pre01.ref_range_alpha,1,1) ='-' and replace(norm_pre01.ref_range_alpha,' ' ,'') rlike ( '^[0123456789\\-\\+\\.\\s]+$')
         AND (cast(norm_pre01.result as float) >= cast(substring_index(replace(upper(norm_pre01.ref_range_alpha),' ',''),'-',2)  as float)
          or  cast(norm_pre01.result as float) <= cast(substring_index(replace(upper(norm_pre01.ref_range_alpha),' ',''),'-',-1) as float))
                                                                                                                                        then 'N'
        WHEN substring(norm_pre01.ref_range_alpha,1,1) ='-' and replace(norm_pre01.ref_range_alpha,' ' ,'') rlike ( '^[0123456789\\-\\+\\.\\s]+$')
        AND (cast(norm_pre01.result as float)  < cast(substring_index(replace(upper(norm_pre01.ref_range_alpha),' ',''),'-',2)  as float)
         or  cast(norm_pre01.result as float) >  cast(substring_index(replace(upper(norm_pre01.ref_range_alpha),' ',''),'-',-1) as float))
                                                                                                                                        then 'Y'
    --- Alpha results
        WHEN norm_pre01.result NOT rlike ('^(-|\\+|\\.)?[0-9]+(\\.)?[0-9]*$') AND norm_pre01.ref_range_alpha IS NULL                 THEN 'Unknown2'

        WHEN norm_pre01.result NOT rlike ('^(-|\\+|\\.)?[0-9]+(\\.)?[0-9]*$') AND replace(upper(norm_pre01.result),' ','') = replace(upper(norm_pre01.ref_range_alpha),' ','')
                                                                                                                               THEN 'N'

     ELSE 'Unknown4'
    END   AS hv_abnormal_indicator      ,

    norm_pre01.hv_provider_name_cmdm	,
    norm_pre01.hv_ind_spclty_tp_cmdm	,
    norm_pre01.hv_ind_spclty_cd_cmdm	,
    norm_pre01.hv_ind_spclty_desc_cmdm	,
    norm_pre01.hv_ind_spclty_tp2_cmdm	,
    norm_pre01.hv_ind_spclty_cd2_cmdm	,
    norm_pre01.hv_ind_spclty_desc2_cmdm ,
    norm_pre01.hv_ind_spclty_tp3_cmdm	,
    norm_pre01.hv_ind_spclty_cd3_cmdm	,
    norm_pre01.hv_ind_spclty_desc3_cmdm,
    norm_pre01.hv_act_spclty_tp_cmdm	,
    norm_pre01.hv_act_spclty_cd_cmdm	,
    norm_pre01.hv_act_spclty_desc_cmdm	,
    norm_pre01.client_acct_number

FROM labtest_quest_rinse_census_pre_final_01 norm_pre01
LEFT OUTER JOIN labtest_quest_rinse_result_gold_alpha gold ON UPPER(TRIM(gold.gen_ref_cd)) = UPPER(TRIM(norm_pre01.result))
--limit 1