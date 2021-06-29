SELECT DISTINCT
    tmp.cpm_desc,
    CASE
        WHEN ref.gen_ref_itm_nm IS NULL
            THEN 'Y'
        ELSE 'N'
    END                                     AS whtlst_flg
 FROM
(
    SELECT DISTINCT
        s_cdm.cpm_desc
     FROM nthrive_patient_charges ptn_chg
     LEFT OUTER JOIN nthrive_chargemaster cdm
       ON COALESCE(ptn_chg.charge_id, 'EMPTY') = COALESCE(cdm.charge_id, 'DUMMY')
     LEFT OUTER JOIN nthrive_standard_chargemaster s_cdm
       ON COALESCE(cdm.cdm_std_id, 'EMPTY') = COALESCE(s_cdm.cdm_std_id, 'DUMMY')
    WHERE s_cdm.cpm_desc IS NOT NULL
) tmp
 LEFT OUTER JOIN dw.ref_gen_ref ref
   ON ref.gen_ref_domn_nm = 'cdm_desc_blacklist'
  AND tmp.cpm_desc RLIKE REGEXP_REPLACE(ref.gen_ref_itm_nm, "\\.", "\\.")