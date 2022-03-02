SELECT
    norm_pre01.HV_claim_id             ,
    norm_pre01.hvid                    ,
    norm_pre01.created                 ,
    norm_pre01.model_version           ,
    norm_pre01.data_set                ,
    norm_pre01.data_feed               ,
    norm_pre01.data_vendor             ,
    norm_pre01.HV_patient_gender       ,
    norm_pre01.HV_patient_age          ,
    norm_pre01.HV_patient_year_of_birth,
    norm_pre01.HV_patient_zip3         ,
    norm_pre01.HV_patient_state        ,
    norm_pre01.date_service            ,
    norm_pre01.date_specimen           ,
    norm_pre01.HV_date_report          ,
    norm_pre01.loinc_code              ,
    ---------------------------------------------------------------------------------------------------------------->
    --(2021-03-24 JKS - scope change Excep only NUMERIC LOINC code)
    ---------------------------------------------------------------------------------------------------------------->
    CASE
        WHEN norm_pre01.hv_loinc_code NOT RLIKE '^[0-9-]+$'   THEN NULL
        ELSE norm_pre01.hv_loinc_code
    END             AS hv_loinc_code   ,
    norm_pre01.lab_id                  ,
    norm_pre01.test_id                 ,
    norm_pre01.HV_test_number          ,
    norm_pre01.test_battery_local_id   ,
    norm_pre01.test_battery_std_id     ,
    norm_pre01.test_battery_name       ,
    norm_pre01.test_ordered_local_id   ,
    norm_pre01.test_ordered_std_id     ,
    norm_pre01.test_ordered_name       ,
    norm_pre01.result_id               ,
    norm_pre01.result                  ,
    norm_pre01.result_name             ,
    norm_pre01.result_unit_of_measure  ,
    norm_pre01.result_desc             ,
    norm_pre01.HV_ref_range_alpha      ,
    norm_pre01.HV_fasting_status       ,
    norm_pre01.HV_procedure_code       ,
    norm_pre01.HV_procedure_code_qual  ,
    norm_pre01.HV_ordering_npi         ,
    norm_pre01.payer_id                ,
    norm_pre01.payer_name              ,
    norm_pre01.lab_other_id            ,
    norm_pre01.HV_lab_other_qual       ,
    norm_pre01.ordering_market_type    ,
    norm_pre01.ordering_specialty      ,
    norm_pre01.ordering_state_license  ,
    norm_pre01.ordering_upin           ,
    norm_pre01.ordering_address_1      ,
    norm_pre01.ordering_address_2      ,
    norm_pre01.ordering_city           ,
    norm_pre01.HV_ordering_state       ,
    norm_pre01.ordering_zip            ,
    norm_pre01.part_provider           ,
    norm_pre01.HV_part_best_date       ,
    norm_pre01.unique_accession_id     ,
    norm_pre01.diag_accn_id            ,
    norm_pre01.diag_date_of_service    ,
    norm_pre01.diag_lab_code           ,
    norm_pre01.acct_id                 ,
    norm_pre01.acct_number             ,
    norm_pre01.diag_dos_yyyymm         ,
    norm_pre01.rslt_accn_id            ,
    norm_pre01.lab_code                ,
    norm_pre01.phy_id                  ,
    norm_pre01.rslt_accession_number   ,
    norm_pre01.accn_dom_id             ,
    norm_pre01.cmdm_spclty_cd          ,
    norm_pre01.acct_name               ,
    norm_pre01.cmdm_licstate           ,
    norm_pre01.billing_client_number   ,
    norm_pre01.fasting_hours           ,
    norm_pre01.qbs_ordering_client_num ,
    norm_pre01.date_order_entry        ,
    norm_pre01.informed_consent_flag   ,
    norm_pre01.legal_entity            ,
    norm_pre01.specimen_type           ,
    norm_pre01.ordering_site_code      ,
    norm_pre01.canceled_accn_ind       ,
    norm_pre01.copy_to_clns            ,
    norm_pre01.non_physician_name      ,
    norm_pre01.non_physician_id        ,
    norm_pre01.long_description        ,
    norm_pre01.phy_name                ,
    norm_pre01.suffix                  ,
    norm_pre01.degree                  ,
    norm_pre01.idw_analyte_code        ,
    norm_pre01.qls_service_code        ,
    norm_pre01.reportable_results_ind  ,
    norm_pre01.ord_seq                 ,
    norm_pre01.res_seq                 ,
    norm_pre01.abnormal_ind            ,
    norm_pre01.amended_report_ind      ,
    norm_pre01.alpha_normal_flag       ,
    norm_pre01.instrument_id           ,
    norm_pre01.result_release_date     ,
    norm_pre01.enterprise_ntc_code     ,
    norm_pre01.derived_profile_code    ,
    norm_pre01.qtim_as_ordered_code    ,
    norm_pre01.qtim_profile_ind        ,
    norm_pre01.rslt_dos_yyyymm         ,
    norm_pre01.sister_lab              ,
    norm_pre01.bill_type_cd            ,
    norm_pre01.idw_report_change_status,
    norm_pre01.ins_seq                 ,
    norm_pre01.active_ind              ,
    norm_pre01.bill_code               ,
    norm_pre01.insurance_billing_type  ,
    norm_pre01.qbs_payor_cd            ,
    norm_pre01.dos_id                  ,
    norm_pre01.lab_name                ,
    norm_pre01.lab_lis_type            ,
    norm_pre01.confidential_order_ind  ,
    norm_pre01.daard_client_flag       ,
    norm_pre01.ptnt_accession_number   ,
    norm_pre01.ptnt_date_of_service    ,
    norm_pre01.ptnt_lab_code           ,
    norm_pre01.accn_enterprise_id      ,
    norm_pre01.age_code                ,
    norm_pre01.species                 ,
    norm_pre01.pat_country             ,
    norm_pre01.external_patient_id     ,
    norm_pre01.pat_master_id           ,
    norm_pre01.lab_reference_number    ,
    norm_pre01.room_number             ,
    norm_pre01.bed_number              ,
    norm_pre01.hospital_location       ,
    norm_pre01.ward                    ,
    norm_pre01.admission_date          ,
    norm_pre01.health_id               ,
    norm_pre01.pm_eid                  ,
    norm_pre01.idw_pm_email_address    ,
-------------------------------------------------------------------------------------------------
---------- New fields added per request from QUEST 2020-05-21
-------------------------------------------------------------------------------------------------
    norm_pre01.date_reported           ,
    norm_pre01.ref_range_low           ,
    norm_pre01.ref_range_high          ,
    norm_pre01.ref_range_alpha         ,
-------------------------------------------------------------------------------------------------
---------- New fields added per request from QUEST 2020-06-08
-------------------------------------------------------------------------------------------------
    norm_pre01.requisition_number      ,
    norm_pre01.fasting_ind             ,
    norm_pre01.cpt_code                ,
    norm_pre01.npi                     ,
    norm_pre01.phy_last_name           ,
    norm_pre01.phy_first_name          ,
    norm_pre01.phy_middle_name         ,
    norm_pre01.acct_state              ,
    ----- if the numeric value is date, nullify it  (HV_result_value_operator)
    CASE
         ----- if the numeric value is date, nullify it  (HV_result_value_numeric)
        WHEN
        (
          SUBSTR(HV_result_value_numeric,2,1) rlike '[0-9]' AND
          SUBSTR(HV_result_value_numeric,3,1) rlike '[0-9]' AND
          SUBSTR(HV_result_value_numeric,4,1) = '/'         AND
          SUBSTR(HV_result_value_numeric,5,1) rlike '[0-9]' AND
          SUBSTR(HV_result_value_numeric,6,1) rlike '[0-9]'
        )
         OR
        (
          SUBSTR(HV_result_value_numeric,2,1) rlike '[0-9]' AND
          SUBSTR(HV_result_value_numeric,3,1) rlike '[0-9]' AND
          SUBSTR(HV_result_value_numeric,4,1) = '/'         AND
          SUBSTR(HV_result_value_numeric,5,1) rlike '[0-9]' AND
          SUBSTR(HV_result_value_numeric,6,1) rlike '[0-9]' AND
          SUBSTR(HV_result_value_numeric,7,1) = '/'         AND
          SUBSTR(HV_result_value_numeric,8,1) rlike '[0-9]' AND
          SUBSTR(HV_result_value_numeric,9,1) rlike '[0-9]'
         )                                                        THEN NULL
        WHEN SUBSTR(norm_pre01.HV_result_value_numeric,1,2) = '[]'
         AND HV_result_value_alpha IS NULL                         THEN NULL

    ELSE     norm_pre01.HV_result_value_operator
    END AS HV_result_value_operator,
    CASE
        ----- if the numeric value is one word and that is alphabet
        WHEN SPLIT(HV_result_value_numeric,' ')[0]  rlike '[A-Za-z]' AND SPLIT(HV_result_value_numeric,' ')[1]  IS NULL THEN NULL

        ----- if the numeric value is date, nullify it  (HV_result_value_numeric)
        WHEN
        (
          SUBSTR(HV_result_value_numeric,2,1) rlike '[0-9]' AND
          SUBSTR(HV_result_value_numeric,3,1) rlike '[0-9]' AND
          SUBSTR(HV_result_value_numeric,4,1) = '/'         AND
          SUBSTR(HV_result_value_numeric,5,1) rlike '[0-9]' AND
          SUBSTR(HV_result_value_numeric,6,1) rlike '[0-9]'
         )
         OR
         (
          SUBSTR(HV_result_value_numeric,2,1) rlike '[0-9]' AND
          SUBSTR(HV_result_value_numeric,3,1) rlike '[0-9]' AND
          SUBSTR(HV_result_value_numeric,4,1) = '/'         AND
          SUBSTR(HV_result_value_numeric,5,1) rlike '[0-9]' AND
          SUBSTR(HV_result_value_numeric,6,1) rlike '[0-9]' AND
          SUBSTR(HV_result_value_numeric,7,1) = '/'         AND
          SUBSTR(HV_result_value_numeric,8,1) rlike '[0-9]' AND
          SUBSTR(HV_result_value_numeric,9,1) rlike '[0-9]'
         )                                                          THEN NULL

        WHEN SUBSTR(norm_pre01.HV_result_value_numeric,1,2) = '[:'  THEN REPLACE(CONCAT('['  ,SUBSTR(norm_pre01.HV_result_value_numeric,3)), ' ', '')
        WHEN SUBSTR(norm_pre01.HV_result_value_numeric,1,2) = '[/'  THEN REPLACE(CONCAT('['  ,SUBSTR(norm_pre01.HV_result_value_numeric,3)), ' ', '')
        ----- REPLACE('.5/.25','.','0.')
        WHEN SUBSTR(norm_pre01.HV_result_value_numeric,1,2) = '[.'
                AND LOCATE ('/.', norm_pre01.HV_result_value_numeric) <> 0            THEN REPLACE(CONCAT('[0.', SUBSTR(norm_pre01.HV_result_value_numeric,3)) , '/.', '/0.')
        WHEN SUBSTR(norm_pre01.HV_result_value_numeric,1,2) = '[.'                    THEN  CONCAT('[0.', SUBSTR(norm_pre01.HV_result_value_numeric,3))

        -- 2021-05-07
        WHEN SUBSTR(REVERSE(norm_pre01.HV_result_value_numeric),1,3) = ']./'          THEN REPLACE(norm_pre01.HV_result_value_numeric, '/.]', ']')

        WHEN LOCATE ('/.', norm_pre01.HV_result_value_numeric) <> 0                   THEN REPLACE(norm_pre01.HV_result_value_numeric , '/.', '/0.')
        WHEN SUBSTR(norm_pre01.HV_result_value_numeric,1,2) = '[]'  THEN NULL
        WHEN SUBSTR(norm_pre01.HV_result_value_numeric,1,3) = '[-]' THEN NULL
        WHEN SUBSTR(REVERSE(norm_pre01.HV_result_value_numeric),1,1) = '/'  THEN REPLACE(norm_pre01.HV_result_value_numeric, '/', '')

        WHEN SUBSTR(REVERSE(norm_pre01.HV_result_value_numeric),1,2) = ']/' THEN REPLACE(norm_pre01.HV_result_value_numeric, '/]', ']')
        WHEN SUBSTR(REVERSE(norm_pre01.HV_result_value_numeric),1,2) = ']+' THEN REPLACE(norm_pre01.HV_result_value_numeric, '+]', ']')
       -- 2021-05-07 2021-05-12
        WHEN SUBSTR(REVERSE(norm_pre01.HV_result_value_numeric),1,3) = ']0 ' THEN REPLACE(norm_pre01.HV_result_value_numeric, ' 0]', ']')

        WHEN LOCATE('ghs ', norm_pre01.result) <> 0
        AND norm_pre01.HV_result_value_operator IS NOT NULL  THEN  SUBSTR( REPLACE(norm_pre01.result, norm_pre01.HV_result_value_operator, ''), 1, LOCATE('ghs ',REPLACE(norm_pre01.result, norm_pre01.HV_result_value_operator, ''))-1)
        WHEN LOCATE('gnb ', norm_pre01.result) <> 0
        AND norm_pre01.HV_result_value_operator IS NOT NULL  THEN  SUBSTR( REPLACE(norm_pre01.result, norm_pre01.HV_result_value_operator, ''), 1, LOCATE('gnb ',REPLACE(norm_pre01.result, norm_pre01.HV_result_value_operator, ''))-1)


    ELSE norm_pre01.HV_result_value_numeric
    END AS HV_result_value_numeric,
        -----------------------------Nullify alfa
    CASE
        WHEN gold.gen_ref_desc IS NOT NULL                      THEN gold.gen_ref_desc
        ---- nullify if only - is present
        WHEN TRIM(norm_pre01.HV_result_value_alpha) IN ( '-' , '*') THEN NULL
        ------------- NEW CODE (if there is date in the numberic column make the alfa NULL)
        WHEN
        (
          SUBSTR(HV_result_value_numeric,2,1) rlike '[0-9]' AND
          SUBSTR(HV_result_value_numeric,3,1) rlike '[0-9]' AND
          SUBSTR(HV_result_value_numeric,4,1) = '/'         AND
          SUBSTR(HV_result_value_numeric,5,1) rlike '[0-9]' AND
          SUBSTR(HV_result_value_numeric,6,1) rlike '[0-9]'
        )
         OR
        (
          SUBSTR(HV_result_value_numeric,2,1) rlike '[0-9]' AND
          SUBSTR(HV_result_value_numeric,3,1) rlike '[0-9]' AND
          SUBSTR(HV_result_value_numeric,4,1) = '/'         AND
          SUBSTR(HV_result_value_numeric,5,1) rlike '[0-9]' AND
          SUBSTR(HV_result_value_numeric,6,1) rlike '[0-9]' AND
          SUBSTR(HV_result_value_numeric,7,1) = '/'         AND
          SUBSTR(HV_result_value_numeric,8,1) rlike '[0-9]' AND
          SUBSTR(HV_result_value_numeric,9,1) rlike '[0-9]'
         )                                                            THEN NULL
        ------------- Exception for mg/dL
        WHEN LOCATE(' mg/dL ', norm_pre01.HV_result_value_alpha) <> 0
        AND norm_pre01.HV_result_value_operator IS NOT NULL     THEN TRIM(norm_pre01.HV_result_value_alpha)
        ------------- Exception for MG/DL 2021-05-07
        WHEN LOCATE('MGDL',  norm_pre01.HV_result_value_alpha) <> 0 AND norm_pre01.HV_result_value_operator IS NOT NULL     THEN 'MG/DL'
        ------------- Exception for MG/DL 2021-05-12
        WHEN LOCATE('mgdL',  norm_pre01.HV_result_value_alpha) <> 0 AND norm_pre01.HV_result_value_operator IS NOT NULL     THEN 'mg/dL'
        WHEN LOCATE('EUDL',  norm_pre01.HV_result_value_alpha) <> 0 AND norm_pre01.HV_result_value_operator IS NOT NULL     THEN 'EU/DL'
        WHEN LOCATE('MILML', norm_pre01.HV_result_value_alpha) <> 0 AND norm_pre01.HV_result_value_operator IS NOT NULL     THEN 'MIL/ML'

        ------------- Exception for CLASS 2021-05-07
        WHEN LOCATE('CLASS ', norm_pre01.result) <> 0
        AND norm_pre01.HV_result_value_operator IS NOT NULL     THEN SUBSTR(norm_pre01.result, LOCATE('CLASS ', norm_pre01.result))

        WHEN LOCATE('gnb ', norm_pre01.result) <> 0
        AND norm_pre01.HV_result_value_operator IS NOT NULL     THEN SUBSTR(norm_pre01.result, LOCATE('gnb ', norm_pre01.result))

        WHEN LOCATE('ghs ', norm_pre01.result) <> 0
        AND norm_pre01.HV_result_value_operator IS NOT NULL     THEN SUBSTR(norm_pre01.result, LOCATE('ghs ', norm_pre01.result))

        -------- Exception for mcg/ML 2021-03-29
        WHEN LOCATE('mL', norm_pre01.HV_result_value_alpha) <> 0
         AND LOCATE('/mL', norm_pre01.result) <> 0
         AND LOCATE('/mL', norm_pre01.HV_result_value_alpha) = 0
        AND norm_pre01.HV_result_value_operator IS NOT NULL           THEN REPLACE(norm_pre01.HV_result_value_alpha, 'mL', '/mL')
        -------- Exception for U/L
        WHEN LOCATE('UL', norm_pre01.HV_result_value_alpha) <> 0
         AND LOCATE('U/L', norm_pre01.HV_result_value_alpha) = 0
        AND norm_pre01.HV_result_value_operator IS NOT NULL           THEN REPLACE(norm_pre01.HV_result_value_alpha, 'UL', 'U/L')
        -------- Exception for U/L
        WHEN LOCATE('ngliter', norm_pre01.HV_result_value_alpha) <> 0
         AND LOCATE('ng/liter', norm_pre01.HV_result_value_alpha) = 0
        AND norm_pre01.HV_result_value_operator IS NOT NULL           THEN REPLACE(norm_pre01.HV_result_value_alpha, 'ngliter', 'ng/liter')


        WHEN LENGTH(TRIM(COALESCE(norm_pre01.HV_result_value_numeric ,''))) <> 0
         AND LENGTH(TRIM(COALESCE(norm_pre01.HV_result_value_operator,''))) <> 0
         AND norm_pre01.HV_result_value_alpha NOT IN ( 'IN' , 'TO')               ---- Exception disregard IN TO
                                                                      THEN  norm_pre01.HV_result_value_alpha
    ELSE NULL
    END AS HV_result_value_alpha ,
    norm_pre01.date_final_report ,
    norm_pre01.profile_name_qtim,
    norm_pre01.order_name_qtim,
    norm_pre01.specimen_type_desc_qtim,
    norm_pre01.methodology_qtim,
    norm_pre01.result_name_qtim,
    norm_pre01.unit_of_measure_qtim,
    norm_pre01.loinc_number_qtim,

    norm_pre02.s_diag_code_codeset_ind ,
    norm_pre02.HV_s_diag_code_codeset_ind,

    norm_pre01.hv_abnormal_indicator    ,

    norm_pre01.hv_provider_name_cmdm	,
    norm_pre01.hv_ind_spclty_tp_cmdm	,
    norm_pre01.hv_ind_spclty_cd_cmdm	,
    norm_pre01.hv_ind_spclty_desc_cmdm	,
    norm_pre01.hv_ind_spclty_tp2_cmdm	,
    norm_pre01.hv_ind_spclty_cd2_cmdm	,
    norm_pre01.hv_ind_spclty_desc2_cmdm ,
    norm_pre01.hv_ind_spclty_tp3_cmdm	,
    norm_pre01.hv_ind_spclty_cd3_cmdm	,
    norm_pre01.hv_ind_spclty_desc3_cmdm ,
    norm_pre01.hv_act_spclty_tp_cmdm	,
    norm_pre01.hv_act_spclty_cd_cmdm	,
    norm_pre01.hv_act_spclty_desc_cmdm	,
    norm_pre01.client_acct_number


FROM labtest_quest_rinse_census_pre_final_01a norm_pre01
LEFT OUTER JOIN labtest_quest_rinse_census_pre_final_02  norm_pre02  ON norm_pre01.unique_accession_id = norm_pre02.unique_accession_id
LEFT OUTER JOIN labtest_quest_rinse_result_gold_alpha gold          ON UPPER(TRIM(gold.gen_ref_cd)) = UPPER(TRIM(norm_pre01.HV_result_value_alpha))

GROUP BY
    1,     2,   3,   4,   5,   6,   7,   8,   9,  10,  11,  12,  13,  14,  15,  16,  17,  18,  19,
    20,   21,  22,  23,  24,  25,  26,  27,  28,  29,  30,  31,  32,  33,  34,  35,  36,  37,  38,  39,
    40,   41,  42,  43,  44,  45,  46,  47,  48,  49,  50,  51,  52,  53,  54,  55,  56,  57,  58,  59,
    60,   61,  62,  63,  64,  65,  66,  67,  68,  69,  70,  71,  72,  73,  74,  75,  76,  77,  78,  79,
    80,   81,  82,  83,  84,  85,  86,  87,  88,  89,  90,  91,  92,  93,  94,  95,  96,  97,  98,  99,
    100, 101, 102, 103, 104, 105, 106, 107, 108, 109, 110, 111, 112, 113, 114, 115, 116, 117, 118, 119,
    120, 121, 122, 123, 124, 125, 126, 127, 128, 129, 130, 131, 132, 133, 134, 135, 136, 137, 138, 139,
    140, 141, 142, 143, 144, 145, 146, 147, 148, 149, 150, 151, 152, 153, 154, 155,	156, 157, 158, 159,
    160, 161, 162, 163,	164, 165, 166, 167,	168
