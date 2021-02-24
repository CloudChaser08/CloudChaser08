SELECT
    MONOTONICALLY_INCREASING_ID()                                                           AS row_id,
    --------------------------------------------------------------------------------------------------
    ---  hv_medctn_id
    --------------------------------------------------------------------------------------------------
    CASE
        WHEN COALESCE(med.organizationid, med.factmedicationorderid) IS NOT NULL
            THEN CONCAT
                    (
                        '156_',
                        COALESCE(med.organizationid, 'UNAVAILABLE'),
                        '_',
                        COALESCE(med.factmedicationorderid, 'UNAVAILABLE')
                    )
        ELSE NULL
    END                                                                                     AS hv_medctn_id,
    CURRENT_DATE()                                                                          AS crt_dt,
	'11'                                                                                    AS mdl_vrsn_num,
    SPLIT(med.input_file_name, '/')[SIZE(SPLIT(med.input_file_name, '/')) - 1]              AS data_set_nm,
	511                                                                                     AS hvm_vdr_id,
	156                                                                                     AS hvm_vdr_feed_id,
	UPPER(dorg.OrganizationCode)                                                            AS vdr_org_id,
    --------------------------------------------------------------------------------------------------
    --- vdr_medctn_ord_id and vdr_medctn_ord_id_qual
    --------------------------------------------------------------------------------------------------
	med.factmedicationorderid                                                            AS vdr_medctn_ord_id,
	CASE
	    WHEN med.factmedicationorderid IS NOT NULL THEN 'FACT_MEDICATION_ORDER_ID'
    ELSE NULL
	END                                                                                     AS vdr_medctn_ord_id_qual,
    --------------------------------------------------------------------------------------------------
    --- hvid
    --------------------------------------------------------------------------------------------------
	CASE
	    WHEN 0 <> LENGTH(TRIM(COALESCE(pay.hvid, '')))        THEN pay.hvid
	    WHEN 0 <> LENGTH(TRIM(COALESCE(med.residentid, '')))  THEN CONCAT('156_', med.residentid)
	    ELSE NULL
	END																				        AS hvid,
    --------------------------------------------------------------------------------------------------
    --- ptnt_birth_yr
    --------------------------------------------------------------------------------------------------
    CAST(
        CAP_YEAR_OF_BIRTH
	    (
	        pay.age,
	        COALESCE( CAST(EXTRACT_DATE(med.orderstartdateid, '%Y%m%d') AS DATE), CAST('{VDR_FILE_DT}' AS DATE)) ,
	        pay.yearofbirth
	    )
      AS INT)                                                                               AS ptnt_birth_yr,
    --------------------------------------------------------------------------------------------------
    --- ptnt_gender_cd
    --------------------------------------------------------------------------------------------------
	CASE
	    WHEN SUBSTR(UPPER(pay.gender), 1, 1) IN ('F', 'M', 'U')  THEN SUBSTR(UPPER(pay.gender), 1, 1)
	ELSE NULL
	END																				    	AS ptnt_gender_cd,
	VALIDATE_STATE_CODE(UPPER(pay.state))													AS ptnt_state_cd,
	MASK_ZIP_CODE(SUBSTR(COALESCE(pay.threedigitzip, '000'), 1, 3))						    AS ptnt_zip3_cd,
    --------------------------------------------------------------------------------------------------
    --- medctn_ord_dt
    --------------------------------------------------------------------------------------------------
    CASE
        WHEN CAST(EXTRACT_DATE(med.orderstartdateid, '%Y%m%d') AS DATE)  < CAST('{EARLIEST_SERVICE_DATE}' AS DATE)
          OR CAST(EXTRACT_DATE(med.orderstartdateid, '%Y%m%d') AS DATE)  > CAST('{VDR_FILE_DT}' AS DATE) THEN NULL
    ELSE     CAST(EXTRACT_DATE(med.orderstartdateid, '%Y%m%d') AS DATE)
    END                                                                                   AS medctn_ord_dt,
    --------------------------------------------------------------------------------------------------
    --- medctn_start_dt
    --------------------------------------------------------------------------------------------------
    CASE
        WHEN CAST(EXTRACT_DATE(med.orderstartdateid, '%Y%m%d') AS DATE)  < CAST('{EARLIEST_SERVICE_DATE}' AS DATE)
          OR CAST(EXTRACT_DATE(med.orderstartdateid, '%Y%m%d') AS DATE)  > CAST('{VDR_FILE_DT}' AS DATE) THEN NULL
    ELSE     CAST(EXTRACT_DATE(med.orderstartdateid, '%Y%m%d') AS DATE)
    END                                                                                   AS medctn_start_dt,
    --------------------------------------------------------------------------------------------------
    --- medctn_end_dt
    --------------------------------------------------------------------------------------------------
    CASE
        WHEN CAST(EXTRACT_DATE(med.orderenddateid, '%Y%m%d') AS DATE)  < CAST('{EARLIEST_SERVICE_DATE}' AS DATE)
          OR CAST(EXTRACT_DATE(med.orderenddateid, '%Y%m%d') AS DATE)  > CAST('{VDR_FILE_DT}' AS DATE) THEN NULL
    ELSE     CAST(EXTRACT_DATE(med.orderenddateid, '%Y%m%d') AS DATE)
    END                                                                                   AS medctn_end_dt,

    -----------------------------------------------------------------------------------------------------------
    ---- NDC 2020-03-24
    -----------------------------------------------------------------------------------------------------------
    CLEAN_UP_NDC_CODE(ref_ndc_ddid.ndc_upc_hri)                                             AS medctn_ndc,
    -----------------------------------------------------------------------------------------------------------
    ---- medctn_alt_cd and medctn_alt_cd_qual
    -----------------------------------------------------------------------------------------------------------
	ddru.gpi                                                                                AS medctn_alt_cd,
	CASE
	    WHEN ddru.gpi IS NOT NULL THEN 'GPI'
    ELSE NULL
	END                                                                                     AS medctn_alt_cd_qual,
	ddru.drugname                                                                           AS medctn_brd_nm,
	ddru.lvl4drugbasename                                                                   AS medctn_genc_nm,
	COALESCE(ddru.form, ddru.lvl6drugform)                                               AS medctn_admin_form_nm,
    -----------------------------------------------------------------------------------------------------------
    ---- medctn_strth_txt
    -----------------------------------------------------------------------------------------------------------
	CASE
	    WHEN COALESCE(ddru.strength, ddru.strengthunit) IS NOT NULL
	        THEN TRIM(CONCAT
    	                (
    	                    COALESCE(ddru.strength, ''), ' ',
    	                    COALESCE(ddru.strengthunit, '')
    	                ))
	    ELSE ddru.lvl7drugstrength
	END                                                                                     AS medctn_strth_txt,

	'fact_medication_order'																	AS prmy_src_tbl_nm,
	'156'																			        AS part_hvm_vdr_feed_id,
    -----------------------------------------------------------------------------------------------------------
    ---- part_mth
    -----------------------------------------------------------------------------------------------------------
    CASE
        WHEN med.Orderstartdateid = ''
          OR med.Orderstartdateid IS NULL
          OR CAST(EXTRACT_DATE(med.Orderstartdateid, '%Y%m%d') AS DATE)  < CAST('{AVAILABLE_START_DATE}' AS DATE)
          OR CAST(EXTRACT_DATE(med.Orderstartdateid, '%Y%m%d') AS DATE)  > CAST('{VDR_FILE_DT}' AS DATE)
        THEN '0_PREDATES_HVM_HISTORY'
    ELSE  CONCAT
             (
                 SUBSTR(med.Orderstartdateid, 1, 4), '-',
                 SUBSTR(med.Orderstartdateid, 5, 2)
                )
    END                                                                         AS part_mth
FROM factmedicationorder med
LEFT OUTER JOIN matching_payload pay            ON med.residentid           = pay.personid            AND COALESCE(med.residentid, '0') <> '0'
LEFT OUTER JOIN dimorganization dorg            ON med.organizationid       = dorg.organizationid AND COALESCE(med.organizationid, '0') <> '0'
LEFT OUTER JOIN dimdrug ddru                    ON med.drugid               = ddru.drugid                     AND COALESCE(med.drugid, '0') <> '0'
LEFT OUTER JOIN ref_ndc_ddid                    ON CAST(ddru.ddid AS INT)   = CAST(ref_ndc_ddid.drug_descriptor_id AS INT)  AND COALESCE(ddru.ddid, '0') <> '0'
                                                                      AND ref_ndc_ddid.id_number_format_code IN ('1', '2', '3', '6')
WHERE TRIM(lower(COALESCE(med.orderstartdateid, 'empty'))) <> 'orderstartdateid'
