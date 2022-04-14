SELECT
    rslt.*,
    -------DIAG
    diaglkp.s_diag_code_codeset_ind,
    diaglkp.HV_s_diag_code_codeset_ind,
    -------PHY
    CASE WHEN phy.phy_id IS NOT NULL THEN phy.phy_last_name   ELSE rslt.phy_last_name   END AS c_phy_last_name,
    CASE WHEN phy.phy_id IS NOT NULL THEN phy.phy_first_name  ELSE rslt.phy_first_name  END AS c_phy_first_name,
    CASE WHEN phy.phy_id IS NOT NULL THEN phy.phy_middle_name ELSE rslt.phy_middle_name END AS c_phy_middle_name,
    -------UDF Value
    CASE WHEN alpha_rvlkp.gen_ref_cd IS NOT NULL THEN alpha_rvlkp.udf_operator ELSE rvlkp.udf_operator END AS HV_result_value_operator,
    CASE WHEN alpha_rvlkp.gen_ref_cd IS NOT NULL THEN alpha_rvlkp.udf_numeric ELSE rvlkp.udf_numeric END AS HV_result_value_numeric,
    CASE WHEN alpha_rvlkp.gen_ref_cd IS NOT NULL THEN alpha_rvlkp.udf_operator ELSE rvlkp.udf_alpha END AS HV_result_value_alpha,
    CASE WHEN alpha_rvlkp.gen_ref_cd IS NOT NULL THEN alpha_rvlkp.udf_passthru ELSE rvlkp.udf_passthru END AS HV_result_value,
    -------QTIM with new join QTM2
   CASE 
      WHEN qtim.compendium_code IS NOT NULL AND UPPER(qtim.profile_ind) ='Y'               THEN qtim.lab_reprt_titles_concat

      WHEN qtim.compendium_code IS NOT NULL AND coalesce(UPPER(qtim.profile_ind),'') <>'Y' THEN NULL
      WHEN UPPER(qtim2.profile_ind) ='Y'                                                   THEN qtim2.lab_reprt_titles_concat
      ELSE NULL 
   END                                                                                               AS profile_name_qtim,

   CASE 
      WHEN qtim.compendium_code IS NOT NULL THEN qtim.lab_reprt_titles_concat  
      ELSE qtim2.lab_reprt_titles_concat  
   END                                                                                               AS order_name_qtim,
   CASE 
      WHEN qtim.compendium_code IS NOT NULL THEN qtim.specimen_type_desc       
      ELSE qtim2.specimen_type_desc      
   END                                                                                               AS specimen_type_desc_qtim,
   CASE 
      WHEN qtim.compendium_code IS NOT NULL THEN COALESCE(qtim.methodology_dos, qtim.methodology_lis) 
      ELSE COALESCE(qtim2.methodology_dos, qtim2.methodology_lis) 
   END                                                                                               AS methodology_qtim,
   CASE 
      WHEN qtim.compendium_code IS NOT NULL THEN qtim.analyte_name             
      ELSE qtim2.analyte_name 
   END                                                                                               AS result_name_qtim,
   CASE 
      WHEN qtim.compendium_code IS NOT NULL THEN qtim.unit_of_measure          
      ELSE qtim2.unit_of_measure 
   END                                                                                               AS unit_of_measure_qtim,
   CASE 
      WHEN qtim.compendium_code IS NOT NULL THEN qtim.loinc_number             
      ELSE qtim2.loinc_number    
   END                                                                                               AS loinc_number_qtim, 
    ------------------ CMDM
    CASE WHEN  cmdm.ind_npi IS NOT NULL THEN cmdm.ind_full_nm               ELSE NULL END AS hv_provider_name_cmdm,
    CASE WHEN  cmdm.ind_npi IS NOT NULL THEN cmdm.ind_spclty                ELSE NULL END AS hv_ind_spclty_tp_cmdm,
    CASE WHEN  cmdm.ind_npi IS NOT NULL THEN cmdm.ind_spclty_cd             ELSE NULL END AS hv_ind_spclty_cd_cmdm,
    CASE WHEN  cmdm.ind_npi IS NOT NULL THEN cmdm.ind_spclty_desc           ELSE NULL END AS hv_ind_spclty_desc_cmdm,
    CASE WHEN  cmdm.ind_npi IS NOT NULL THEN cmdm.ind_spclty_tp_secondary   ELSE NULL END AS hv_ind_spclty_tp2_cmdm,
    CASE WHEN  cmdm.ind_npi IS NOT NULL THEN cmdm.ind_spclty_cd_secondary   ELSE NULL END AS hv_ind_spclty_cd2_cmdm,
    CASE WHEN  cmdm.ind_npi IS NOT NULL THEN cmdm.ind_spclty_desc_secondary ELSE NULL END AS hv_ind_spclty_desc2_cmdm,
    CASE WHEN  cmdm.ind_npi IS NOT NULL THEN cmdm.ind_spclty_tp_tertiary    ELSE NULL END AS hv_ind_spclty_tp3_cmdm,
    CASE WHEN  cmdm.ind_npi IS NOT NULL THEN cmdm.ind_spclty_cd_tertiary    ELSE NULL END AS hv_ind_spclty_cd3_cmdm,
    CASE WHEN  cmdm.ind_npi IS NOT NULL THEN cmdm.ind_spclty_desc_tertiary  ELSE NULL END AS hv_ind_spclty_desc3_cmdm,
    CASE WHEN acct.act_client_no IS NOT NULL THEN acct.act_spclty_tp         ELSE NULL END AS hv_act_spclty_tp_cmdm,
    CASE WHEN acct.act_client_no IS NOT NULL THEN acct.act_spclty_cd         ELSE NULL END AS hv_act_spclty_cd_cmdm,
    CASE WHEN acct.act_client_no IS NOT NULL THEN acct.act_spclty_desc       ELSE NULL END AS hv_act_spclty_desc_cmdm,
    delta.acct_number	                                                                   AS client_acct_number
FROM order_result rslt
LEFT OUTER JOIN labtest_quest_rinse_census_diag_code diaglkp ON rslt.unique_accession_id = diaglkp.unique_accession_id
LEFT OUTER JOIN ref_gold_alpha_lkp alpha_rvlkp ON UPPER(TRIM(rslt.result_value)) = UPPER(TRIM(alpha_rvlkp.gen_ref_cd))
LEFT OUTER JOIN ref_result_value_lkp rvlkp ON TRIM(rslt.result_value)  = TRIM(rvlkp.gen_ref_cd)
--------------------QTIM
LEFT OUTER JOIN labtest_quest_rinse_ref_questrinse_qtim_all  qtim ON  rslt.lab_code              = qtim.compendium_code
                     AND  rslt.idw_Local_order_code  = qtim.unit_code
                     AND  rslt.local_result_code     = qtim.analyte_code
---------------------QTIM join for AMD, AMP and SJC labs
LEFT OUTER JOIN labtest_quest_rinse_ref_questrinse_qtim_all qtim2 ON  rslt.lab_code             IN ('AMD','AMP','SJC') 
                           AND rslt.lab_code              = qtim2.compendium_code 
                           AND rslt.derived_profile_code  = qtim2.unit_code
                           AND rslt.local_result_code     = qtim2.analyte_code
------------------------GOLD ALPHA
--LEFT OUTER JOIN labtest_quest_rinse_result_gold_alpha gold          ON UPPER(TRIM(gold.gen_ref_cd)) = UPPER(TRIM(rslt.result_value))
-- ---------------------CMDM
LEFT OUTER JOIN labtest_quest_rinse_ref_questrinse_cmdm_npi     cmdm  ON COALESCE(cmdm.ind_npi ,'Empty')    = COALESCE(CLEAN_UP_NPI_CODE(rslt.npi) ,'blank')
-- ------------------ Delta main
LEFT OUTER JOIN
    (
        SELECT accession_number, lab_code, dos_id, ord_seq, res_seq, ins_seq, acct_number FROM lab_account_cpt
      GROUP BY accession_number, lab_code, dos_id, ord_seq, res_seq, ins_seq, acct_number
    ) delta
  ON  rslt.accession_number = delta.accession_number
  AND COALESCE(rslt.lab_code,'Empty') = COALESCE(delta.lab_code,'Blank')
  AND COALESCE(rslt.dos_id  ,'Empty') = COALESCE(delta.dos_id  ,'Blank')
  AND COALESCE(rslt.ord_seq ,'Empty') = COALESCE(delta.ord_seq ,'Blank')
  AND COALESCE(rslt.res_seq ,'Empty') = COALESCE(delta.res_seq ,'Blank')
  AND COALESCE(rslt.ins_seq ,'NULL')  = COALESCE(delta.ins_seq ,'NULL')
-- ------------------ CMDM LIMITED
 LEFT OUTER JOIN labtest_quest_rinse_ref_questrinse_cmdm_acct acct
  ON delta.lab_code = acct.act_idw_lab_code
  AND CASE WHEN delta.acct_number rlike ('^\\\D') THEN substr(delta.acct_number,2,length(delta.acct_number)) ELSE delta.acct_number END = acct.act_client_no
  AND UPPER(acct.act_addr_tp) = 'DELIVER RESULTS'
-- ------------------ LEFT OUTER JOIN FOR PHYSCIAN NAME 2022-02-24 (JKS)
 LEFT OUTER JOIN ref_questrinse_physicians phy ON  rslt.phy_id=phy.phy_id

WHERE
 LOWER(COALESCE(rslt.unique_accession_id, '')) <> 'unique_accession_id'
