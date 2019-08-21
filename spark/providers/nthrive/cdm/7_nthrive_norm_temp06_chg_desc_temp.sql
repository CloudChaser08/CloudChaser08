SELECT DISTINCT
    charge_desc,
    CASE
        WHEN ref.gen_ref_itm_nm IS NULL
            THEN 'Y'
        ELSE 'N'
    END                 AS whtlst_flg
 FROM nthrive_norm_temp05_chg_desc_temp tmp
 LEFT OUTER JOIN ref_gen_ref ref
   ON ref.gen_ref_domn_nm = 'cdm_desc_blacklist'
  AND tmp.charge_desc RLIKE REGEXP_REPLACE(ref.gen_ref_itm_nm, "\\.", "\\.")
