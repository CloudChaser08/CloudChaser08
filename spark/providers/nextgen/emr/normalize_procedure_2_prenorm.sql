SELECT
    ord.*,
    CASE
        WHEN LENGTH(TRIM(ord.vcxcode)) <> 0
            THEN ord.vcxcode
        ELSE NULL
    END                                             AS real_vcxcode,
    CASE 
        WHEN cpt_codes.code IS NOT NULL
            THEN cpt_codes.code
        WHEN hcpcs_codes.hcpc IS NOT NULL
            THEN hcpcs_codes.hcpc
        WHEN icd_diag_codes.code IS NOT NULL
            THEN clean_up_diagnosis_code(
                icd_diag_codes.code,
                NULL,
                COALESCE(ord.prov_ord_dt, ord.enc_dt)
            )
        WHEN icd_proc_codes.code IS NOT NULL
            THEN icd_proc_codes.code
        WHEN loinc_codes.loinc_num IS NOT NULL
            THEN loinc_codes.loinc_num
        WHEN regexp_extract(ord.clean_actcode, '(^NG[0-9]+$)') != ''
            THEN ord.clean_actcode
        WHEN ref3.gen_ref_cd IS NOT NULL
            THEN ref3.gen_ref_cd
        ELSE NULL END                               AS real_actcode
FROM (SELECT *,
             UPPER(actcode) as clean_actcode,
             clean_up_freetext(actdiagnosiscode, false) as clean_actdiagnosiscode,
             extract_date(
                 substring(encounterdate, 1, 8), '%Y%m%d', CAST({min_date} AS DATE), CAST({max_date} AS DATE)
                 ) as enc_dt,
             extract_date(
                 substring(orderdate, 1, 8), '%Y%m%d', CAST({min_date} AS DATE), CAST({max_date} AS DATE)
                 ) as prov_ord_dt
      FROM order) ord
    -- NG is a very common value in this field and slows down transformation significantly
    -- (It's 20x more common then the next most common value)
    LEFT JOIN cpt_codes ON ord.clean_actcode = cpt_codes.code
        AND ord.clean_actcode IS NOT NULL AND ord.clean_actcode != ''
        AND ord.clean_actcode != 'NG'
    LEFT JOIN hcpcs_codes ON ord.clean_actcode = hcpc
        AND ord.clean_actcode IS NOT NULL AND ord.clean_actcode != ''
        AND ord.clean_actcode != 'NG'
    LEFT JOIN icd_diag_codes ON ord.clean_actcode = icd_diag_codes.code
        AND ord.clean_actcode IS NOT NULL AND ord.clean_actcode != ''
        AND ord.clean_actcode != 'NG'
    LEFT JOIN icd_proc_codes ON ord.clean_actcode = icd_proc_codes.code
        AND ord.clean_actcode IS NOT NULL AND ord.clean_actcode != ''
        AND ord.clean_actcode != 'NG'
    LEFT JOIN loinc_codes ON translate(ord.clean_actcode, '-', '') = loinc_codes.loinc_num
        AND ord.clean_actcode IS NOT NULL AND ord.clean_actcode != ''
        AND ord.clean_actcode != 'NG'
    LEFT JOIN (SELECT DISTINCT gen_ref_cd
            FROM ref_gen_ref
            WHERE gen_ref_domn_nm = 'emr_prov_ord.prov_ord_cd'
                AND whtlst_flg = 'Y'
        ) ref3
        ON ord.clean_actcode = ref3.gen_ref_cd
            AND ord.clean_actcode IS NOT NULL AND ord.clean_actcode != ''
    LEFT JOIN icd_diag_codes diag2 ON clean_up_freetext(ord.actdiagnosiscode, true) = diag2.code
        AND ord.clean_actdiagnosiscode IS NOT NULL AND ord.clean_actdiagnosiscode != ''
