SELECT
    CAST(NULL AS BIGINT)                                                                    AS record_id,
    txn.ediclaim_id                                                                         AS claim_id,
    /* hvid */
    COALESCE
        (
            txn.hvid,
            CONCAT
                (
                    '41_',
                    COALESCE(txn.master_patient_id, 'NO_MASTER_PATIENT_ID')
                )
        )                                                                                   AS hvid,
    CURRENT_DATE()                                                                          AS created,
	'08'                                                                                    AS model_version,
    SPLIT(txn.input_file_name, '/')[SIZE(SPLIT(txn.input_file_name, '/')) - 1]              AS data_set,
	'41'                                                                                    AS data_feed,
	'188'                                                                                   AS data_vendor,
	txn.tenant_id                                                                           AS vendor_org_id,
	/* patient_gender */
	CLEAN_UP_GENDER
    	(
        	CASE
        	    WHEN SUBSTR(UPPER(TRIM(COALESCE(txn.patientgender, 'U'))), 1, 1) IN ('F', 'M')
        	        THEN SUBSTR(UPPER(TRIM(COALESCE(txn.patientgender, 'U'))), 1, 1)
        	    WHEN SUBSTR(UPPER(TRIM(COALESCE(txn.gender, 'U'))), 1, 1) IN ('F', 'M')
        	        THEN SUBSTR(UPPER(TRIM(COALESCE(txn.gender, 'U'))), 1, 1)
        	    ELSE 'U'
        	END
	    )                                                                                   AS patient_gender,
	/* patient_year_of_birth */
	CAP_YEAR_OF_BIRTH
        (
            NULL,
            mmdt.max_dt,
            COALESCE(txn.patientdob, txn.yearofbirth)
        )                                                                                   AS patient_year_of_birth,
    'P'                                                                                     AS claim_type,
    /* date_service */
	CAP_DATE
        (
            mmdt.min_dt,
            esdt.gen_ref_1_dt,
            CAST(EXTRACT_DATE('{VDR_FILE_DT}', '%Y-%m-%d') AS DATE)
        )                                                                                   AS date_service,
    /* date_service_end */
	CAP_DATE
        (
            mmdt.max_dt,
            esdt.gen_ref_1_dt,
            CAST(EXTRACT_DATE('{VDR_FILE_DT}', '%Y-%m-%d') AS DATE)
        )                                                                                   AS date_service_end,
    /* place_of_service_std_id */
    CASE
        WHEN txn.facilitycode IS NULL
            THEN NULL
        WHEN SUBSTR(CONCAT('00', txn.facilitycode), -2) IN ('05', '06', '07', '08', '09', '12', '13', '14', '33')
            THEN '99'
        ELSE SUBSTR(CONCAT('00', txn.facilitycode), -2)
    END                                                                                     AS place_of_service_std_id,
    CAST(NULL AS STRING)                                                                    AS service_line_number,
    CAST(NULL AS STRING)                                                                    AS service_line_id,
    /* diagnosis_code */
    CLEAN_UP_DIAGNOSIS_CODE
        (
            txn.diagnosis_code,
            NULL,
            mmdt.min_dt
        )                                                                                   AS diagnosis_code,
    CAST(NULL AS STRING)                                                                    AS diagnosis_priority,
    CAST(NULL AS STRING)                                                                    AS procedure_code,
    CAST(NULL AS STRING)                                                                    AS procedure_code_qual,
    CAST(NULL AS FLOAT)                                                                     AS procedure_units_billed,
    CAST(NULL AS STRING)                                                                    AS procedure_modifier_1,
    CAST(NULL AS STRING)                                                                    AS procedure_modifier_2,
    CAST(NULL AS STRING)                                                                    AS procedure_modifier_3,
    CAST(NULL AS STRING)                                                                    AS procedure_modifier_4,
    CAST(NULL AS STRING)                                                                    AS ndc_code,
    CAST(NULL AS FLOAT)                                                                     AS line_charge,
    CAST(txn.submittedchargetotal AS FLOAT)                                                 AS total_charge,
    CAST(NULL AS STRING)                                                                    AS prov_rendering_npi,
    /* prov_billing_npi */
    CASE
        WHEN SUBSTR(CONCAT('00', txn.facilitycode), -2) IN ('05', '06', '07', '08', '09', '12', '13', '14', '33')
            THEN NULL
        WHEN txn.billprovideridqualifier = 'XX'
         AND 11 = LENGTH(TRIM(COALESCE(txn.billprovidernpid, '')))
            THEN CLEAN_UP_NPI_CODE
                    (
                        COALESCE(txn.billproviderid, txn.billprovidernpid)
                    )
        WHEN txn.billprovideridqualifier = 'XX'
            THEN CLEAN_UP_NPI_CODE
                    (
                        txn.billproviderid
                    )
        ELSE NULL
    END                                                                                     AS prov_billing_npi,
    CAST(NULL AS STRING)                                                                    AS prov_referring_npi,
    CAST(NULL AS STRING)                                                                    AS prov_facility_npi,
    txn.payerid                                                                             AS payer_vendor_id,
    txn.payername                                                                           AS payer_name,
    CAST(NULL AS STRING)                                                                    AS prov_rendering_vendor_id,
    CAST(NULL AS STRING)                                                                    AS prov_rendering_name_1,
    CAST(NULL AS STRING)                                                                    AS prov_rendering_std_taxonomy,
    /* prov_billing_vendor_id */
    CASE
        WHEN SUBSTR(CONCAT('00', txn.facilitycode), -2) IN ('05', '06', '07', '08', '09', '12', '13', '14', '33')
            THEN NULL
        WHEN txn.billprovideridqualifier <> 'XX'
            THEN txn.billproviderid
        ELSE NULL
    END                                                                                     AS prov_billing_vendor_id,
    /* prov_billing_name_1 */
    CASE
        WHEN SUBSTR(CONCAT('00', txn.facilitycode), -2) IN ('05', '06', '07', '08', '09', '12', '13', '14', '33')
            THEN NULL
        ELSE txn.billprovidername
    END                                                                                     AS prov_billing_name_1,
    txn.billprovidertaxonomycode                                                            AS prov_billing_std_taxonomy,
    CAST(NULL AS STRING)                                                                    AS prov_referring_vendor_id,
    CAST(NULL AS STRING)                                                                    AS prov_referring_name_1,
    CAST(NULL AS STRING)                                                                    AS prov_facility_vendor_id,
    CAST(NULL AS STRING)                                                                    AS prov_facility_name_1,
    CAST(NULL AS STRING)                                                                    AS prov_facility_address_1,
    CAST(NULL AS STRING)                                                                    AS prov_facility_city,
    CAST(NULL AS STRING)                                                                    AS prov_facility_state,
    CAST(NULL AS STRING)                                                                    AS prov_facility_zip,
	'cardinal_pms'                                                                          AS part_provider
 FROM cardinal_pms_837_hvm_norm03_pvt_clm txn
 /* Select the min and max dates for each claim ID. */
 LEFT OUTER JOIN
    (
        SELECT
            claim_id,
            MIN(date_service) AS min_dt,
            MAX(date_service) AS max_dt
         FROM cardinal_pms_837_hvm_norm02_nrm_lns
        WHERE date_service IS NOT NULL
        GROUP BY 1
    ) mmdt
   ON txn.ediclaim_id = mmdt.claim_id
 LEFT OUTER JOIN
    (
        SELECT gen_ref_1_dt
         FROM ref_gen_ref
        WHERE hvm_vdr_feed_id = 41
          AND gen_ref_domn_nm = 'EARLIEST_VALID_SERVICE_DATE'
        LIMIT 1
    ) esdt
   ON 1 = 1
 LEFT OUTER JOIN
    (
        SELECT gen_ref_1_dt
         FROM ref_gen_ref
        WHERE hvm_vdr_feed_id = 41
          AND gen_ref_domn_nm = 'HVM_AVAILABLE_HISTORY_START_DATE'
        LIMIT 1
    ) ahdt
   ON 1 = 1
WHERE NOT EXISTS
  /* Select only where the claim/diagnosis code combination
     was not already loaded in the service lines load */
    (
        SELECT 1
         FROM cardinal_pms_837_hvm_norm01_pvt_lns lns
        WHERE txn.ediclaim_id = lns.ediclaim_id
          AND txn.diagnosis_code = lns.diagnosis_code
    )