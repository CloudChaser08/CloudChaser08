SELECT DISTINCT
    clm.claimuid                                                                           AS claim_id,
    pay.hvid                                                                               AS hvid,
    CURRENT_DATE()                                                                         AS created,
	'1'                                                                                    AS model_version,
    SPLIT(clm.input_file_name, '/')[SIZE(SPLIT(clm.input_file_name, '/')) - 1]             AS data_set,
	'176'                                                                                  AS data_feed,
	'543'                                                                                  AS data_vendor,
    CASE
        WHEN SUBSTR(UPPER(mbr.gendercode), 1, 1) IN ('F', 'M', 'U') THEN SUBSTR(UPPER(mbr.gendercode), 1, 1)
        WHEN SUBSTR(UPPER(pay.gender)    , 1, 1) IN ('F', 'M', 'U') THEN SUBSTR(UPPER(pay.gender)    , 1, 1)
        ELSE NULL
    END                                                                                    AS patient_gender,
    COALESCE(race.gen_ref_itm_nm ,'OTHER/UNKNOWN/UNAVAILABLE') AS race,
    -----------------------------------------------------------------------------------------------------------------------------
    ------------------------------------ patient_year_of_birth
    -----------------------------------------------------------------------------------------------------------------------------
    CASE
        WHEN mbr.birthyear   IS NOT NULL   THEN mbr.birthyear
        WHEN pay.yearofbirth IS NOT NULL   THEN pay.yearofbirth
    ELSE
        NULL
    END                                                                                    AS patient_year_of_birth,
    CASE
        WHEN UPPER(clm.claimformtypecode)     IN ('I','P')     THEN UPPER(claimformtypecode)
        WHEN UPPER(clm.institutionaltypecode) IN ('I','O','U') THEN 'I'
        WHEN UPPER(clm.professionaltypecode)  IN ('I','O','U') then 'P'
        ELSE NULL
    END                                                                                    AS claim_type,
    -----------------------------------------------------------------------------------------------------------------------------
    ------------------------------------ Run regular privacy rule - date shifts will happen in the view
    -----------------------------------------------------------------------------------------------------------------------------
    CAP_DATE
        (

            CAST(EXTRACT_DATE(clm.createddate                 , '%Y-%m-%d') AS DATE),
            CAST(EXTRACT_DATE('{EARLIEST_SERVICE_DATE}', '%Y-%m-%d') AS DATE),
            CAST(EXTRACT_DATE('{VDR_FILE_DT}'                , '%Y-%m-%d') AS DATE)
        )                                                                                   AS date_received,
   	CAP_DATE
        (
            CAST(EXTRACT_DATE(clm.servicedate               , '%Y-%m-%d') AS DATE),
            CAST(EXTRACT_DATE('{EARLIEST_SERVICE_DATE}', '%Y-%m-%d') AS DATE),
            CAST(EXTRACT_DATE('{VDR_FILE_DT}'                , '%Y-%m-%d') AS DATE)
        )
                                                                                             AS date_service,
    CAP_DATE
        (
            CAST(EXTRACT_DATE(clm.servicethrudate           , '%Y-%m-%d') AS DATE),
            CAST(EXTRACT_DATE('{EARLIEST_SERVICE_DATE}', '%Y-%m-%d') AS DATE),
            CAST(EXTRACT_DATE('{VDR_FILE_DT}'                , '%Y-%m-%d') AS DATE)
        )                                                                                   AS date_service_end,
    CASE
        WHEN COALESCE(clm.ubpatientdischargestatuscode, '') IN (  '69', '87')
            THEN NULL
        ELSE clm.ubpatientdischargestatuscode
    END                                                                                     AS inst_discharge_status_std_id,
    CASE
        WHEN (UPPER(COALESCE(clm.claimformtypecode, '')) = 'I'
           OR UPPER(clm.institutionaltypecode)  IN ('I','O','U') )  THEN clm.tob_code
        ELSE NULL
    END                                                                                     AS inst_type_of_bill_std_id,
    -----------------------------------------------------------------------------------------------------------------------------
    ------------------------------------ inst_drg_std_id
    -----------------------------------------------------------------------------------------------------------------------------
    CASE
         WHEN (UPPER(clm.claimformtypecode)   IN ('I') OR UPPER(clm.institutionaltypecode) IN ('I','O','U') )  THEN clm.msdrg
         ELSE NULL
    END                                                                                    AS inst_drg_std_id,

    CASE
         WHEN (UPPER(clm.claimformtypecode)   IN ('I') OR UPPER(clm.institutionaltypecode) IN ('I','O','U') )  THEN clm.apdrg
         ELSE NULL
    END                                                                                    AS inst_drg_vendor_id,


    CASE
        WHEN  UPPER(COALESCE(clm.claimformtypecode, ''))    = 'P'
         AND  LPAD(clm.pos_code, 2, 0) IN ('05', '06', '07', '08', '09', '12', '13', '14', '33','99')  THEN '99'
        WHEN  UPPER(COALESCE(clm.claimformtypecode, ''))    = 'P'                                      THEN LPAD(clm.pos_code, 2, 0)
        WHEN  UPPER(COALESCE(clm.professionaltypecode, ''))  IN ('I','O','U')
         AND  LPAD(clm.pos_code, 2, 0) IN ('05', '06', '07', '08', '09', '12', '13', '14', '33','99')  THEN '99'
        WHEN  UPPER(COALESCE(clm.professionaltypecode, ''))  IN ('I','O','U')                          THEN LPAD(clm.pos_code, 2, 0)
        ELSE NULL
    END    	                                                                                AS place_of_service_std_id,
    -----------------------------------------------------------------------------------------------------------------------------
    ------------------------------------ DIAG
    -----------------------------------------------------------------------------------------------------------------------------
    CASE
        WHEN clm.diag_qualifier = '01' THEN
                CASE
                    WHEN clm.diag_code RLIKE ('^(E95.*|E96.*|E97.*|E99.*|E928.*|E8[0-4].*)$') THEN NULL
                    WHEN clm.diag_code RLIKE ('^V854[1-5]$')                                  THEN 'V854'
                    ELSE clm.diag_code
                END
        WHEN clm.diag_qualifier = '02' THEN
                CASE
                    WHEN clm.diag_code RLIKE ('^(Y35.*|Y36.*|Y37.*|Y38.*|X92.*|Y09.*|X52.*|V.*)$') THEN NULL
                    WHEN clm.diag_code RLIKE ('^Z684[1-5]$')                                       THEN 'Z684'
                    ELSE clm.diag_code
                END
      WHEN clm.diag_qualifier IS NULL AND CAST(EXTRACT_DATE(clm.servicethrudate, '%Y-%m-%d') AS DATE) <  CAST('2015-10-01' AS DATE) THEN
                CASE
                    WHEN clm.diag_code RLIKE ('^(E95.*|E96.*|E97.*|E99.*|E928.*|E8[0-4].*)$') THEN NULL
                    WHEN clm.diag_code RLIKE ('^V854[1-5]$')                                  THEN 'V854'
                    ELSE clm.diag_code
                END
      WHEN clm.diag_qualifier IS NULL AND CAST(EXTRACT_DATE(clm.servicethrudate, '%Y-%m-%d') AS DATE) >=  CAST('2015-10-01' AS DATE) THEN
                CASE
                    WHEN clm.diag_code RLIKE ('^(Y35.*|Y36.*|Y37.*|Y38.*|X92.*|Y09.*|X52.*|V.*)$') THEN NULL
                    WHEN clm.diag_code RLIKE ('^Z684[1-5]$')                                       THEN 'Z684'
                    ELSE clm.diag_code
                END
      ELSE clm.diag_code
    END                                                                                         AS diagnosis_code,
    CASE
        WHEN clm.diag_code IS NULL THEN NULL
    ELSE
        clm.diag_qualifier
    END                                                                                         AS diagnosis_code_qual,
    -----------------------------------------------------------------------------------------------------------------------------
    ------------------------------------ admit_diagnosis_ind
    -----------------------------------------------------------------------------------------------------------------------------
    CASE
        WHEN (UPPER(clm.claimformtypecode) = 'I'  OR UPPER(clm.institutionaltypecode)  IN ('I','O','U') )
         AND admit_diag_code_fld  = 'Y'  THEN 'Y'
        WHEN (UPPER(clm.claimformtypecode) = 'I'  OR UPPER(clm.institutionaltypecode)  IN ('I','O','U') )
         AND admit_diag_code_fld  = 'N'  THEN 'N'
        WHEN (UPPER(clm.claimformtypecode) = 'P'  OR UPPER(clm.professionaltypecode)   IN ('I','O','U') )
            THEN NULL    --- Null for Professional
        -- Null for Unknown claim type
    ELSE NULL
    END                                                                                     AS admit_diagnosis_ind,
    -----------------------------------------------------------------------------------------------------------------------------
    ------------------------------------ procedure_code and qual
    -----------------------------------------------------------------------------------------------------------------------------
    CLEAN_UP_PROCEDURE_CODE(clm.proc_code)                                                  AS procedure_code,
    CASE
        WHEN clm.proc_code IS NULL THEN NULL
    ELSE
        'HC'
    END                                                                                     AS procedure_code_qual,

    clm.serviceunitquantity	                                                                AS procedure_units_billed,
    clm.proc_mod                                                                            AS procedure_modifier_1,
    clm.rev_code                                                                            AS revenue_code,
--    COALESCE(ipps.unadjustedprice, nonipps.unadjustedprice)	                                AS total_charge,
    MD5(clm.renderingprovidernpi)                                                           AS prov_rendering_vendor_id,
    COALESCE(clm.billingprovideruid, clm.provideruid)                                       AS prov_billing_vendor_id,

    CASE
        WHEN clm.claimstatuscode = 'A' THEN 'ADJUSTMENT TO ORIGINAL CLAIM'
        WHEN clm.claimstatuscode = 'D' THEN 'DENIED CLAIMS'
        WHEN clm.claimstatuscode = 'I' THEN 'INITIAL PAY CLAIM'
        WHEN clm.claimstatuscode = 'P' THEN 'PENDED FOR ADJUDICATION'
        WHEN clm.claimstatuscode = 'R' THEN 'REVERSAL TO ORIGINAL CLAIM'
        WHEN clm.claimstatuscode = 'U' THEN 'UNKNOWN'
    ELSE NULL
    END                                                                                         AS logical_delete_reason,
	'inovalon'                                                                                  AS part_provider,

    -----------------------------------------------------------------------------------------------------------------------------
    ------------------------------------ part_best_date '{AVG_START_DATE}'
    -----------------------------------------------------------------------------------------------------------------------------
     	CASE
    	    WHEN 0 = LENGTH(COALESCE
    	                            (
    	                                CAP_DATE
                                            (
                                                CAST(EXTRACT_DATE(clm.servicedate, '%Y-%m-%d') AS DATE),
                                                CAST(EXTRACT_DATE(COALESCE('{AVAILABLE_START_DATE}','{EARLIEST_SERVICE_DATE}'), '%Y-%m-%d') AS DATE),
                                                CAST(EXTRACT_DATE('{VDR_FILE_DT}', '%Y-%m-%d') AS DATE)
                                            )
                                     , '')
                            )
    	        THEN '0_PREDATES_HVM_HISTORY'
    	    ELSE CONCAT
    	            (
                        SUBSTR(clm.servicedate, 1, 4), '-',
                        SUBSTR(clm.servicedate, 6, 2), '-01'
                     )
     	END         AS part_best_date


FROM inv_norm_30_dx_base clm
LEFT OUTER JOIN matching_payload     pay       ON clm.memberuid    = pay.claimid
LEFT OUTER JOIN    mbr       ON clm.memberuid    = mbr.memberuid
-- ----------- Proxy Amount
-- LEFT OUTER JOIN inovalon_xref     xref    ON clm.claimuid      = xref.claimuid
-- LEFT OUTER JOIN inovalon_ipps     ipps    ON xref.dischargeuid = ipps.dischargeclaimuid
-- LEFT OUTER JOIN inovalon_non_ipps nonipps ON cLm.claimuid      = nonipps.claimuid

LEFT OUTER JOIN ref_gen_ref         race          ON
        CASE
            WHEN CAST(mbr.raceethnicitytypecode AS INT) IS NOT NULL THEN CAST(mbr.raceethnicitytypecode AS INT)
        ELSE mbr.raceethnicitytypecode
        END
        = race.gen_ref_cd AND gen_ref_domn_nm = 'race' AND whtlst_flg ='Y'

