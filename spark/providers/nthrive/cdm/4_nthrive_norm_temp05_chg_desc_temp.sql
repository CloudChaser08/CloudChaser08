SELECT DISTINCT
    tmp.charge_desc,
    CASE
        WHEN ref.gen_ref_itm_nm IS NULL
            THEN 'Y'
        ELSE 'N'
    END                                     AS whtlst_flg
 FROM
(
    SELECT DISTINCT
        cdm.charge_desc
     FROM patient_charges ptn_chg
     LEFT OUTER JOIN chargemaster cdm
       ON COALESCE(ptn_chg.charge_id, 'EMPTY') = COALESCE(cdm.charge_id, 'DUMMY')
    WHERE cdm.charge_desc IS NOT NULL
) tmp
 LEFT OUTER JOIN ref_gen_ref ref
   ON ref.gen_ref_domn_nm = 'cdm_desc_blacklist'
   
  AND tmp.charge_desc RLIKE REGEXP_REPLACE(ref.gen_ref_itm_nm, "\\.", "\\.")