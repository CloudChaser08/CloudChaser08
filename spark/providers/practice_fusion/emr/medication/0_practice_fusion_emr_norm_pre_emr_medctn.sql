SELECT
--    MONOTONICALLY_INCREASING_ID()                                                           AS row_id,
    -------------------------------------------------------------------------------------------------------------------------
    --  hv_lab_test_id
    -------------------------------------------------------------------------------------------------------------------------
    CONCAT('136_', prx.prescription_id)							AS hv_medctn_id,
    CURRENT_DATE()                                                                          AS crt_dt,
	'10'                                                                                    AS mdl_vrsn_num,
    SPLIT(prx.input_file_name, '/')[SIZE(SPLIT(prx.input_file_name, '/')) - 1]                AS data_set_nm,
	439                                                                                     AS hvm_vdr_id,
	136                                                                                     AS hvm_vdr_feed_id,
    ptn.practice_id                                                                         AS vdr_org_id,
    prx.prescription_id																		AS vdr_medctn_ord_id,
	CASE
	    WHEN prx.prescription_id IS NOT NULL THEN 'PRESCRIPTION_ID'
	    ELSE NULL
	END																		                AS vdr_medctn_ord_id_qual,
    -------------------------------------------------------------------------------------------------------------------------
    --  hvid
    -------------------------------------------------------------------------------------------------------------------------
    CASE
        WHEN pay.hvid       IS NOT NULL THEN pay.hvid
        WHEN prx.patient_id IS NOT NULL THEN CONCAT('136_' , prx.patient_id)
        ELSE NULL
    END																			            AS hvid,

    -------------------------------------------------------------------------------------------------------------------------
    --  ptnt_birth_yr
    -------------------------------------------------------------------------------------------------------------------------
	CAP_YEAR_OF_BIRTH
	    (
	        VALIDATE_AGE(pay.age, CAST(EXTRACT_DATE(COALESCE(prx.dos         , CAST('{VDR_FILE_DT}' AS DATE)), '%Y-%m-%d') AS DATE), COALESCE(ptn.birth_year, pay.yearofbirth)),
	        CAST(EXTRACT_DATE(COALESCE(prx.dos, CAST('{VDR_FILE_DT}' AS DATE)), '%Y-%m-%d') AS DATE),
	        COALESCE(ptn.birth_year, pay.yearofbirth)

	    )																					AS ptnt_birth_yr,

    -------------------------------------------------------------------------------------------------------------------------
    --  ptnt_gender_cd
    -------------------------------------------------------------------------------------------------------------------------
	CASE
	    WHEN SUBSTR(UPPER(ptn.gender), 1, 1) IN ('F', 'M', 'U')  THEN SUBSTR(UPPER(ptn.gender), 1, 1)
	    WHEN SUBSTR(UPPER(pay.gender), 1, 1) IN ('F', 'M', 'U')  THEN SUBSTR(UPPER(pay.gender), 1, 1)
	    ELSE NULL
	END																				    	AS ptnt_gender_cd,
    -------------------------------------------------------------------------------------------------------------------------
    --  ptnt_state_cd
    -------------------------------------------------------------------------------------------------------------------------
	VALIDATE_STATE_CODE
	    (
	        CASE
	            WHEN LOCATE(' OR ', UPPER(ptn.state)) <> 0 THEN NULL
	            WHEN LOCATE(' OR ', UPPER(pay.state)) <> 0 THEN NULL
	            ELSE SUBSTR(UPPER(COALESCE(ptn.state, pay.state, '')), 1, 2)
	        END
	    )																					AS ptnt_state_cd,
   -------------------------------------------------------------------------------------------------------------------------
    --  ptnt_zip_cd
    -------------------------------------------------------------------------------------------------------------------------
	MASK_ZIP_CODE
	    (
	        CASE
	            WHEN LOCATE (' OR ', UPPER(ptn.zip)) <> 0            THEN '000'
	            WHEN LOCATE (' OR ', UPPER(pay.threedigitzip)) <> 0  THEN '000'
	            ELSE SUBSTR(COALESCE(ptn.zip, pay.threedigitzip), 1, 3)
	        END
	    )																					AS ptnt_zip3_cd,
    -------------------------------------------------------------------------------------------------------------------------
    -- hv_enc_id
    -------------------------------------------------------------------------------------------------------------------------
    CONCAT
        (
            '136', '_',
            COALESCE(prx.prescription_id, 'NO_PRESCRIPTION_ID'), '_',
            COALESCE(trs.transcript_id, 'NO_TRANSCRIPT_ID')
        )                                                                                       AS hv_enc_id,
    -------------------------------------------------------------------------------------------------------------------------
    --  enc_dt
    -------------------------------------------------------------------------------------------------------------------------
	CAP_DATE
	    (
            CAST(EXTRACT_DATE(SUBSTR(trs.dos, 1, 10), '%Y-%m-%d') AS DATE),
            CAST('{EARLIEST_SERVICE_DATE}' AS DATE),
            CAST('{VDR_FILE_DT}' AS DATE)
	    )																					AS enc_dt,
    -------------------------------------------------------------------------------------------------------------------------
    --  medctn_ord_dt  CONCAT('136|', COALESCE(appointment.transcript_id, 'NO_TRANSCRIPT_ID'), '|', COALESCE(appointment.appointment_id, 'NO_APPOINTMENT_ID'))
    -------------------------------------------------------------------------------------------------------------------------
	CAP_DATE
	    (
            CAST(EXTRACT_DATE(COALESCE(prx.dos,prx.start_date), '%Y-%m-%d') AS DATE),
            CAST('{EARLIEST_SERVICE_DATE}' AS DATE),
            CAST('{VDR_FILE_DT}' AS DATE)
	    )																					AS medctn_ord_dt,
    -------------------------------------------------------------------------------------------------------------------------
    --  medctn_prov_nucc_taxnmy_cd
    -------------------------------------------------------------------------------------------------------------------------
	CASE
	    WHEN prv.derived_ama_taxonomy IS NOT NULL  AND  UPPER(SUBSTR(prv.derived_ama_taxonomy,1,1)) <> 'X' THEN prv.derived_ama_taxonomy
	    WHEN spc.npi_classification IS NOT NULL    AND  UPPER(SUBSTR(spc.npi_classification,1,1)) <> 'X'   THEN spc.npi_classification
	    ELSE NULL
	END                                                                                     AS medctn_prov_nucc_taxnmy_cd,
    -------------------------------------------------------------------------------------------------------------------------
    --  medctn_prov_alt_speclty_id and medctn_prov_alt_speclty_id_qual
    -------------------------------------------------------------------------------------------------------------------------
	CASE
	    WHEN prv.derived_specialty IS NOT NULL  AND UPPER(SUBSTR(prv.derived_specialty,1,1)) <> 'X' THEN prv.derived_specialty
	    WHEN spc.name IS NOT NULL               AND UPPER(SUBSTR(spc.name,1,1)) <> 'X'              THEN spc.name
	    ELSE NULL
	END	                                                  							        AS medctn_prov_alt_speclty_id,

	CASE
	    WHEN prv.derived_specialty IS NOT NULL AND UPPER(SUBSTR(prv.derived_specialty,1,1)) <> 'X' THEN 'PROVIDER_DERIVED_SPECIALTY'
	    WHEN spc.name IS NOT NULL              AND UPPER(SUBSTR(spc.name,1,1)) <> 'X'              THEN 'SPECIALTY_NAME'
	    ELSE NULL
	END			    																		AS medctn_prov_alt_speclty_id_qual,

    phy.name  AS medctn_prov_fclty_nm,
    -------------------------------------------------------------------------------------------------------------------------
    --  medctn_prov_state_cd  phy.state, prc.state,
    -------------------------------------------------------------------------------------------------------------------------
	VALIDATE_STATE_CODE(
	        CASE
	            WHEN LOCATE(' OR ', UPPER(phy.state)) <> 0 THEN NULL
	            WHEN LOCATE(' OR ', UPPER(prc.state)) <> 0 THEN NULL
	            ELSE SUBSTR(UPPER(COALESCE(phy.state, prc.state, '')), 1, 2)
	        END
    	)																					AS medctn_prov_state_cd,
    -------------------------------------------------------------------------------------------------------------------------
    --  ptnt_zip_cd UBSTR(phy.zip, prc.zip, 1, 3)
    -------------------------------------------------------------------------------------------------------------------------
	MASK_ZIP_CODE
	    (
	        CASE
	            WHEN LOCATE (' OR ', UPPER(phy.zip)) <> 0  THEN '000'
	            WHEN LOCATE (' OR ', UPPER(prc.zip)) <> 0  THEN '000'
	            ELSE SUBSTR(COALESCE(phy.zip, prc.zip), 1, 3)
	        END
	    )																					AS medctn_prov_zip_cd,
    -------------------------------------------------------------------------------------------------------------------------
    --  medctn_start_dt
    -------------------------------------------------------------------------------------------------------------------------
	CAP_DATE
	    (
            CAST(EXTRACT_DATE(prx.start_date, '%Y-%m-%d') AS DATE),
            CAST('{EARLIEST_SERVICE_DATE}' AS DATE),
            CAST('{VDR_FILE_DT}' AS DATE)
	    )																					AS medctn_start_dt,
    -------------------------------------------------------------------------------------------------------------------------
    --  medctn_end_dt
    -------------------------------------------------------------------------------------------------------------------------
	CAP_DATE
	    (
            CAST(EXTRACT_DATE(prx.stop_date, '%Y-%m-%d') AS DATE),
            CAST('{EARLIEST_SERVICE_DATE}' AS DATE),
            CAST('{VDR_FILE_DT}' AS DATE)
	    )																					AS medctn_end_dt,
    -------------------------------------------------------------------------------------------------------------------------
    --  medctn_diag_cd 	    d10.icd10  d09.icd9
    -------------------------------------------------------------------------------------------------------------------------
   CLEAN_UP_DIAGNOSIS_CODE
    (
        CASE
            WHEN ARRAY (icd9.icd9,   icd10.icd10 )[diag_explode.n] IS NULL   THEN NULL
            ELSE ARRAY (icd9.icd9,   icd10.icd10 )[diag_explode.n]
        END,
        CASE
            WHEN diag_explode.n = 0     THEN '01'
            WHEN diag_explode.n = 1     THEN '02'
            ELSE NULL
        END,
        CAST(EXTRACT_DATE(prx.dos, '%Y-%m-%d') AS DATE)
    )                                                                                       AS medctn_diag_cd,

    CASE
        WHEN diag_explode.n = 0     THEN '01'
        WHEN diag_explode.n = 1     THEN '02'
        ELSE NULL
	END 																					AS medctn_diag_cd_qual,

    CLEAN_UP_NDC_CODE(prx.medication_id)                                                    AS medctn_ndc,
    -------------------------------------------------------------------------------------------------------------------------
    --  medctn_alt_cd medctn_alt_cd_qual
    -------------------------------------------------------------------------------------------------------------------------
	med.rxnorm_cui                                                                          AS medctn_alt_cd,
	CASE
	    WHEN med.rxnorm_cui IS NOT NULL THEN 'RXNORM'
	    ELSE NULL
	END                                                                                     AS medctn_alt_cd_qual,
    -------------------------------------------------------------------------------------------------------------------------
    --  medctn_genc_ok_flg
    -------------------------------------------------------------------------------------------------------------------------
	CASE
	    WHEN SUBSTR(UPPER(prx.dispensed_as_written), 1, 1) = 'F' THEN 'Y'
	    WHEN SUBSTR(UPPER(prx.dispensed_as_written), 1, 1) = 'T' THEN 'N'
	    ELSE NULL
	END                                                                                     AS medctn_genc_ok_flg,
	med.trade_name                                                                          AS medctn_brd_nm,
	med.generic_name                                                                        AS medctn_genc_nm,
    -------------------------------------------------------------------------------------------------------------------------
    --  medctn_rx_flg
    -------------------------------------------------------------------------------------------------------------------------
	CASE
	    WHEN SUBSTR(UPPER(med.rx_or_otc), 1, 1) IN ('O', 'P') THEN 'N'
	    WHEN SUBSTR(UPPER(med.rx_or_otc), 1, 1) IN ('R', 'S') THEN 'Y'
	    ELSE NULL
	END                                                                                     AS medctn_rx_flg,
	COALESCE(prx.quantity, prx.c_quantity)                                                  AS medctn_rx_qty,
	COALESCE(prx.daily_admin, prx.c_daily_admin)                                            AS medctn_dly_qty,
	prx.days_supply                                                                         AS medctn_days_supply_qty,
	COALESCE(prx.unit, prx.c_quantity_unit)                                                 AS medctn_admin_form_nm,
	med.strength                                                                            AS medctn_strth_txt,
    -------------------------------------------------------------------------------------------------------------------------
    --  medctn_dose_txt
    -------------------------------------------------------------------------------------------------------------------------
	CASE
	    WHEN prx.c_dose_amount IS NOT NULL AND prx.c_dose_amount_unit IS NOT NULL THEN CONCAT(prx.c_dose_amount, ' ', prx.c_dose_amount_unit)
	    WHEN prx.c_dose_amount IS     NULL AND prx.c_dose_amount_unit IS NOT NULL THEN prx.c_dose_amount_unit
	    WHEN prx.c_dose_amount IS NOT NULL AND prx.c_dose_amount_unit IS     NULL THEN prx.c_dose_amount
	    ELSE NULL
	END                                                                                     AS medctn_dose_txt,
	med.route                                                                               AS medctn_admin_rte_txt,
	prx.num_refills                                                                         AS medctn_remng_rfll_qty,
    -------------------------------------------------------------------------------------------------------------------------
    --  medctn_elect_rx_flg
    -------------------------------------------------------------------------------------------------------------------------
	CASE
	    WHEN SUBSTR(UPPER(prx.erx), 1, 1) =  '0' THEN 'N'
	    WHEN SUBSTR(UPPER(prx.erx), 1, 1) =  '1' THEN 'Y'
	    ELSE NULL
	END                                                                                     AS medctn_elect_rx_flg,
    -------------------------------------------------------------------------------------------------------------------------
    --  data_captr_dt
    -------------------------------------------------------------------------------------------------------------------------
	CAP_DATE
	    (
            CAST(EXTRACT_DATE(prx.last_modified, '%Y-%m-%d') AS DATE),
            CAST('{EARLIEST_SERVICE_DATE}' AS DATE),
            CAST('{VDR_FILE_DT}' AS DATE)
	    )																					AS data_captr_dt,
	'prescription'																	    	AS prmy_src_tbl_nm,
	'136'																			        AS part_hvm_vdr_feed_id,
    -------------------------------------------------------------------------------------------------------------------------
    --  data_captr_dt
    -------------------------------------------------------------------------------------------------------------------------
	CASE
	    WHEN CAP_DATE
        	    (
                    CAST(EXTRACT_DATE(COALESCE(prx.dos, prx.start_date, SUBSTR(trs.dos, 1, 10), prx.stop_date), '%Y-%m-%d') AS DATE),
                    CAST('{AVAILABLE_START_DATE}' AS DATE),
                    CAST('{VDR_FILE_DT}' AS DATE)
        	    ) IS NULL
            THEN '0_PREDATES_HVM_HISTORY'
	    ELSE SUBSTR(COALESCE(prx.dos, prx.start_date, SUBSTR(trs.dos, 1, 10), prx.stop_date), 1, 7)
	END																					    AS part_mth
FROM prescription prx
LEFT OUTER JOIN pharmacy phy     ON COALESCE(prx.pharmacy_id, 'NULL') = COALESCE(phy.pharmacy_id, 'empty')
LEFT OUTER JOIN medication   med ON COALESCE(prx.medication_id, 'NULL') = COALESCE(med.medication_id, 'empty')
LEFT OUTER JOIN transcript_prescription trx  ON COALESCE(trx.prescription_id, 'NULL') = COALESCE(prx.prescription_id, 'empty')
LEFT OUTER JOIN transcript trs   ON COALESCE(trx.transcript_id, 'NULL') = COALESCE(trs.transcript_id, 'empty')

LEFT OUTER JOIN diagnosis_icd9  icd9           ON COALESCE(prx.diagnosis_id, 'NULL')         = COALESCE(icd9.diagnosis_id, 'empty')
LEFT OUTER JOIN diagnosis_icd10 icd10          ON COALESCE(prx.diagnosis_id, 'NULL')         = COALESCE(icd10.diagnosis_id, 'empty')
LEFT OUTER JOIN patient ptn    ON COALESCE(prx.patient_id, 'NULL') = COALESCE(ptn.patient_id, 'empty')
--LEFT OUTER JOIN patient ptn    ON COALESCE(trs.patient_id, 'NULL') = COALESCE(ptn.patient_id, 'empty')
LEFT OUTER JOIN provider prv   ON COALESCE(trs.provider_id, 'NULL') = COALESCE(prv.provider_id, 'empty')
LEFT OUTER JOIN practice prc   ON COALESCE(prv.practice_id, 'NULL') = COALESCE(prc.practice_id, 'empty')
LEFT OUTER JOIN specialty spc  ON COALESCE(prv.primary_specialty_id, 'NULL') = COALESCE(spc.specialty_id, 'empty')
LEFT OUTER JOIN matching_payload pay    ON LOWER(COALESCE(ptn.patient_id, 'NULL'))    =  LOWER(COALESCE(pay.claimid, 'empty'))

CROSS JOIN (SELECT EXPLODE(ARRAY(0, 1)) AS n) diag_explode

WHERE
---------- Diagnosis code explosion
    (
        ARRAY
            (
            icd9.icd9,
            icd10.icd10
            )[diag_explode.n] IS NOT NULL
     OR
        (
            COALESCE( icd9.icd9,  icd10.icd10) IS NULL
            AND diag_explode.n = 0
        )
    )
AND TRIM(UPPER(COALESCE(prx.prescription_id, 'empty'))) <> 'PRESCRIPTION_ID'
--LIMIT 10
