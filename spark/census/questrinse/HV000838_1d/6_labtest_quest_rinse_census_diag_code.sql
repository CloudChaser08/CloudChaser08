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

FROM
    (
        SELECT DISTINCT
            rslt.unique_accession_id,
            CASE
                WHEN diag.s_diag_code IS NULL THEN NULL
                WHEN CLEAN_UP_DIAGNOSIS_CODE(diag.s_diag_code,
                    CASE
                        WHEN diag.s_icd_codeset_ind = '9'  THEN '01'
                        WHEN diag.s_icd_codeset_ind = '10' THEN '02'
                    ELSE NULL
                    END, CAST(TO_DATE(rslt.date_of_service, 'yyyy-MM-dd') AS DATE)) IS NULL THEN NULL
            ELSE
                CONCAT(diag.s_icd_codeset_ind, '^', UPPER(diag.s_diag_code))
            END AS HV_ONE_diagnosis_code ,
            CASE
                WHEN diag.s_diag_code IS NULL THEN NULL
            ELSE CONCAT(diag.s_icd_codeset_ind, '^', UPPER(diag.s_diag_code))
            END AS s_diag_code_codeset_ind
        ---------- New fields added per Operator
        FROM order_result rslt
        INNER JOIN diagnosis_skinny diag ON rslt.unique_accession_id = diag.unique_accession_id
    ) rd
GROUP BY 1
