SELECT
    clm.claimuid,
    clm.hvid,
    clm.createddate,
    clm.servicedate,
    clm.servicethrudate,
    clm.ordinalposition,
    ARRAY( clm.cpt, clm.hcpcs, clm.icd9_proc, clm.icd10_proc)[proc_explode.n] AS proc_code,
    ARRAY( clm.cpt_mod, clm.hcpcs_mod)[procmod_explode.n] AS proc_mod,
    ARRAY( clm.icd9_diag,clm.icd9_diag1, clm.icd10_diag, clm.icd10_diag1)[diag_explode.n] AS diag_code,
    CASE
        WHEN clm.icd9_diag_admin_flg  = 'Y' AND clm.icd9_diag1  = ARRAY( clm.icd9_diag,clm.icd9_diag1, clm.icd10_diag, clm.icd10_diag1)[diag_explode.n] THEN 'Y'
        WHEN clm.icd10_diag_admin_flg = 'Y' AND clm.icd10_diag1 = ARRAY( clm.icd9_diag,clm.icd9_diag1, clm.icd10_diag, clm.icd10_diag1)[diag_explode.n] THEN 'Y'
        WHEN clm.icd9_diag_admin_flg  = 'N' THEN 'N'
        WHEN clm.icd10_diag_admin_flg = 'N' THEN 'N'
    END      AS admit_diag_code_fld,
    clm.diag_qualifier,
    clm.APDRG,
    clm.MSDRG,
    clm.Rev_Code, pos_code, tob_code,
    value_exist,
    mom_baby_link_table,
    clm.input_file_name

FROM
(
    -----------------------------------------------------------------------------------------------------------------------------
    ------------------------------------ MOTHER  AND BABY
    -----------------------------------------------------------------------------------------------------------------------------
    SELECT
        clm.claimuid,
        pay.hvid,
        clm.createddate,
        clm.servicedate,
        clm.servicethrudate,
        ccd.ordinalposition,
        ------------ cpt cpt mod hcpcs hcpcs mod
        (CASE WHEN ccd.codetype = '3'  THEN  ccd.codevalue  END) AS cpt,
        (CASE WHEN ccd.codetype = '5'  THEN  ccd.codevalue  END) AS hcpcs,
        (CASE WHEN ccd.codetype = '8'  THEN  ccd.codevalue  END) AS icd9_proc,
        (CASE WHEN ccd.codetype = '18' THEN  ccd.codevalue  END) AS icd10_proc,
        ------------ icd9 icd10
        (CASE WHEN ccd.codetype  = '7'  THEN  ccd.codevalue  END) AS icd9_diag,
        (CASE WHEN ccd.codetype = '19'  THEN  ccd.codevalue  END) AS icd9_diag1,
        (CASE WHEN ccd.codetype = '19'  THEN  "Y"            END) AS icd9_diag_admin_flg,

        (CASE WHEN ccd.codetype = '17'  THEN  ccd.codevalue  END) AS icd10_diag,
        (CASE WHEN ccd.codetype = '22'  THEN  ccd.codevalue  END) AS icd10_diag1,
        (CASE WHEN ccd.codetype = '22'  THEN  "Y"            END) AS icd10_diag_admin_flg,
        ------------ diag_qualifier
        CASE
            WHEN  ccd.codetype IN ('7','19')   THEN '01'
            WHEN  ccd.codetype IN ('17','22')  THEN '02'
        END                                                      AS diag_qualifier,
        ------------ APDRG Code
        (CASE WHEN ccd.codetype  = '2'  THEN  ccd.codevalue  END) AS APDRG,
        ------------ MSDRG Code
        (CASE WHEN ccd.codetype  = '9'  THEN  ccd.codevalue  END) AS MSDRG,
        ------------ Revenue Code
        (CASE WHEN ccd.codetype  = '16'  THEN  ccd.codevalue END) AS Rev_Code,
        ------------ cpt_mod
        (CASE WHEN ccd.codetype = '4'   THEN  ccd.codevalue  END) AS cpt_mod,
        ------------ hcpcs_mod
        (CASE WHEN ccd.codetype = '6'   THEN  ccd.codevalue  END) AS hcpcs_mod,
        ------------ Adding TOB and POS
        (CASE WHEN ccd.codetype = '10'  THEN  ccd.codevalue  END) AS pos_code,
        (CASE WHEN ccd.codetype = '13'  THEN  ccd.codevalue  END) AS tob_code,

        --------------------------------------------------------------------------------------------------
        --- diag_or_proc_exist  (diag proc or rev code)
        --------------------------------------------------------------------------------------------------
        CONCAT
            (
                CASE WHEN ccd.codetype = '2'   THEN  ccd.codevalue  ELSE '' END,
                CASE WHEN ccd.codetype = '3'   THEN  ccd.codevalue  ELSE '' END,
                CASE WHEN ccd.codetype = '5'   THEN  ccd.codevalue  ELSE '' END,
                CASE WHEN ccd.codetype = '8'   THEN  ccd.codevalue  ELSE '' END,
                CASE WHEN ccd.codetype = '9'   THEN  ccd.codevalue  ELSE '' END,
                CASE WHEN ccd.codetype = '10'  THEN  ccd.codevalue  ELSE '' END,
                CASE WHEN ccd.codetype = '13'  THEN  ccd.codevalue  ELSE '' END,
                CASE WHEN ccd.codetype = '18'  THEN  ccd.codevalue  ELSE '' END,
                CASE WHEN ccd.codetype  = '7'  THEN  ccd.codevalue  ELSE '' END,
                CASE WHEN ccd.codetype = '19'  THEN  "Y"            ELSE '' END,
                CASE WHEN ccd.codetype = '17'  THEN  ccd.codevalue  ELSE '' END,
                CASE WHEN ccd.codetype = '22'  THEN  "Y"            ELSE '' END

            )
        AS value_exist,
        'LINK_MOTHER_UID' AS mom_baby_link_table,
        clm.input_file_name,
    'end'
FROM inv_norm_0_dx_transaction clm
    --- Filter the other codes from the code table otherwise it will create a row with NULL values
    LEFT OUTER JOIN     ccd       ON clm.claimuid     = ccd.claimuid
  AND ccd.codetype IN ('3', '5', '8', '18', '7', '19', '17', '22','2', '9', '16', '4', '6', '2', '9', '10', '13')
    LEFT OUTER JOIN (SELECT DISTINCT claimid, hvid, gender FROM matching_payload)    pay       ON clm.memberuid    = pay.claimid
    ----------- Cohort has both mom and baby   PRODUCTION
    --INNER JOIN dw_mom._mom_cohort  mom ON pay.hvid = mom.hvid
    ----------- Cohort has both mom and baby     TEMPORARY
    INNER JOIN  _mom_cohort mom ON pay.hvid = mom.hvid


)
clm
CROSS JOIN (SELECT EXPLODE(ARRAY(0, 1, 2, 3)) AS n) proc_explode
CROSS JOIN (SELECT EXPLODE(ARRAY(0, 1, 2, 3)) AS n) diag_explode
CROSS JOIN (SELECT EXPLODE(ARRAY(0, 1))       AS n) procmod_explode
WHERE
    (
        ARRAY( clm.cpt, clm.hcpcs, clm.icd9_proc, clm.icd10_proc)[proc_explode.n] IS NOT NULL
        OR
        (
            COALESCE(clm.cpt, clm.hcpcs, clm.icd9_proc, clm.icd10_proc) IS NULL
            AND proc_explode.n = 0
        )
    )
AND
    (
        ARRAY( clm.icd9_diag, clm.icd9_diag1, clm.icd10_diag, clm.icd10_diag1)[diag_explode.n] IS NOT NULL
        OR
        (
            COALESCE( clm.icd9_diag, clm.icd9_diag1, clm.icd10_diag, clm.icd10_diag1) IS NULL
            AND diag_explode.n = 0
        )
    )
AND
    (
        ARRAY( clm.cpt_mod, clm.hcpcs_mod)[procmod_explode.n] IS NOT NULL
        OR
        (
            COALESCE(  clm.cpt_mod, clm.hcpcs_mod) IS NULL
            AND procmod_explode.n = 0
        )
    )
AND
    (
        ARRAY( clm.cpt_mod, clm.hcpcs_mod)[procmod_explode.n] IS NOT NULL
        OR
        (
            COALESCE(  clm.cpt_mod, clm.hcpcs_mod) IS NULL
            AND procmod_explode.n = 0
        )
    )
------------------ Diag or CPT exist
AND LENGTH(TRIM(clm.value_exist)) > 0

GROUP BY 1,  2,  3,  4,  5,  6,  7, 8, 9, 10,
        11, 12, 13, 14, 15, 16, 17, 18, 19
ORDER BY clm.claimuid , CAST(clm.ordinalposition AS INT)
