SELECT
    unique_accession_id     ,
    CASE
        WHEN CONCAT_WS(' | ', COLLECT_SET(s_diag_code_codeset_ind)) = '' THEN NULL
    ELSE CONCAT_WS(' | ', SORT_ARRAY(COLLECT_SET(s_diag_code_codeset_ind)))
    END as s_diag_code_codeset_ind,

    CASE
        WHEN CONCAT_WS(' | ', COLLECT_SET(HV_ONE_diagnosis_code)) = '' THEN NULL
    ELSE CONCAT_WS(' | ', SORT_ARRAY(COLLECT_SET(HV_ONE_diagnosis_code)))
    END as HV_s_diag_code_codeset_ind
    
FROM labtest_quest_rinse_census_pre_final_01

GROUP BY 1
