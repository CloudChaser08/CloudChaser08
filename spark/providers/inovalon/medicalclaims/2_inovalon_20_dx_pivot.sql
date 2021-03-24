SELECT
    clm.claimuid,
    clm.memberuid,
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
    clm.Rev_Code,
--    clm.tob_code,
--    clm.pos_code,
    clm.input_file_name

FROM
(
    SELECT
        clm.claimuid,
        clm.memberuid,
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
        ------------------------------------------------------- New Code for TOB/POS 2020-09-03 Removed 2021-03-03
        -- ------------ TOB
        --     (CASE WHEN ccd_13.codetype = '13'  THEN  ccd_13.codevalue  END) AS tob_code,
        -- ------------ POS
        --     (CASE WHEN ccd_10.codetype = '10'  THEN  ccd_10.codevalue  END) AS pos_code,
        ------------ cpt_mod
        (CASE WHEN ccd.codetype = '4'   THEN  ccd.codevalue  END) AS cpt_mod,
        ------------ hcpcs_mod
        (CASE WHEN ccd.codetype = '6'   THEN  ccd.codevalue  END) AS hcpcs_mod,
        clm.input_file_name,
    'end'
FROM clm
--- Filter the other codes from the code table otherwise it will create a row with NULL values
LEFT OUTER JOIN ccd       ON clm.claimuid     = ccd.claimuid AND ccd.codetype IN ('3', '5', '8', '18', '7', '19', '17', '22','2', '9', '16', '4', '6')  --  '13', '10'
-- ---- We want TOB and POS in all rows
-- LEFT OUTER JOIN (SELECT claimuid, ordinalposition, codetype , codevalue FROM inovalon_dx_claim_code_00 WHERE  codetype  =  '10')  ccd_10   ON clm.claimuid     =  ccd_10.claimuid
-- LEFT OUTER JOIN (SELECT claimuid, ordinalposition, codetype , codevalue FROM inovalon_dx_claim_code_00 WHERE  codetype  =  '13')  ccd_13   ON clm.claimuid     =  ccd_13.claimuid

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
GROUP BY 1,  2,  3,  4,  5,  6,  7, 8, 9, 10,
        11, 12, 13, 14, 15 --, 16, 17
ORDER BY clm.claimuid , CAST(clm.ordinalposition AS INT)
