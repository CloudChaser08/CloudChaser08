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
            CAST(EXTRACT_DATE(txn.dateservicestart, '%Y%m%d') AS DATE),
            COALESCE(txn.patientdob, txn.yearofbirth)
        )                                                                                   AS patient_year_of_birth,
    'P'                                                                                     AS claim_type,
    /* date_service */
	CAP_DATE
        (
            CAST(EXTRACT_DATE(txn.dateservicestart, '%Y%m%d') AS DATE),
            esdt.gen_ref_1_dt,
            CAST(EXTRACT_DATE('{VDR_FILE_DT}', '%Y-%m-%d') AS DATE)
        )                                                                                   AS date_service,
    /* date_service_end */
	CAP_DATE
        (
            CAST(EXTRACT_DATE(txn.dateservicestart, '%Y%m%d') AS DATE),
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
    txn.linesequencenumber                                                                  AS service_line_number,
    txn.id_3                                                                                AS service_line_id,
    /* diagnosis_code */
    CLEAN_UP_DIAGNOSIS_CODE
        (
            txn.diagnosis_code,
            NULL,
            CAST(EXTRACT_DATE(txn.dateservicestart, '%Y%m%d') AS DATE)
        )                                                                                   AS diagnosis_code,
    txn.diagnosis_pointer                                                                   AS diagnosis_priority,
    CLEAN_UP_PROCEDURE_CODE(txn.procedurecode)                                              AS procedure_code,
    txn.procedurecodequalifier                                                              AS procedure_code_qual,
    CAST(txn.submittedunits AS FLOAT)                                                       AS procedure_units_billed,
    CLEAN_UP_FREETEXT(SUBSTR(UPPER(txn.proceduremodifierone), 1, 2))                        AS procedure_modifier_1,
    CLEAN_UP_FREETEXT(SUBSTR(UPPER(txn.proceduremodifiertwo), 1, 2))                        AS procedure_modifier_2,
    CLEAN_UP_FREETEXT(SUBSTR(UPPER(txn.proceduremodifierthree), 1, 2))                      AS procedure_modifier_3,
    CLEAN_UP_FREETEXT(SUBSTR(UPPER(txn.proceduremodifierfour), 1, 2))                       AS procedure_modifier_4,
    /* ndc_code */
    CLEAN_UP_NDC_CODE
        (
            CASE
                WHEN COALESCE(txn.product_service_id_qualifier, '') = 'N4'
                    THEN txn.product_service_id
                ELSE NULL
            END
        )                                                                                   AS ndc_code,
    CAST(txn.submittedcharge AS FLOAT)                                                      AS line_charge,
    CAST(txn.submittedchargetotal AS FLOAT)                                                 AS total_charge,
    /* prov_rendering_npi */
    CASE
        WHEN SUBSTR(CONCAT('00', txn.facilitycode), -2) IN ('05', '06', '07', '08', '09', '12', '13', '14', '33')
            THEN NULL
        WHEN txn.renderingprovideridqualifier = 'XX'
         AND 11 = LENGTH(TRIM(COALESCE(txn.renderingprovidernpid, '')))
            THEN CLEAN_UP_NPI_CODE
                    (
                        COALESCE(txn.renderingproviderid, txn.renderingprovidernpid)
                    )
        WHEN txn.renderingprovideridqualifier = 'XX'
            THEN CLEAN_UP_NPI_CODE
                    (
                        txn.renderingproviderid
                    )
        ELSE NULL
    END                                                                                     AS prov_rendering_npi,
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
    /* prov_referring_npi */
    CASE
        WHEN SUBSTR(CONCAT('00', txn.facilitycode), -2) IN ('05', '06', '07', '08', '09', '12', '13', '14', '33')
            THEN NULL
        WHEN txn.referringprovideridqualifier = 'XX'
            THEN CLEAN_UP_NPI_CODE
                    (
                        txn.referringproviderid
                    )
        ELSE NULL
    END                                                                                     AS prov_referring_npi,
    /* prov_facility_npi */
    CASE
        WHEN SUBSTR(CONCAT('00', txn.facilitycode), -2) IN ('05', '06', '07', '08', '09', '12', '13', '14', '33')
            THEN NULL
        WHEN txn.servicefacilityidqualifier = 'XX'
            THEN CLEAN_UP_NPI_CODE
                    (
                        txn.servicefacilityid
                    )
        ELSE NULL
    END                                                                                     AS prov_facility_npi,
    txn.payerid                                                                             AS payer_vendor_id,
    txn.payername                                                                           AS payer_name,
    /* prov_rendering_vendor_id */
    CASE
        WHEN SUBSTR(CONCAT('00', txn.facilitycode), -2) IN ('05', '06', '07', '08', '09', '12', '13', '14', '33')
            THEN NULL
        WHEN txn.renderingprovideridqualifier <> 'XX'
            THEN txn.renderingproviderid
        ELSE NULL
    END                                                                                     AS prov_rendering_vendor_id,
    /* prov_rendering_name_1 */
    CASE
        WHEN SUBSTR(CONCAT('00', txn.facilitycode), -2) IN ('05', '06', '07', '08', '09', '12', '13', '14', '33')
            THEN NULL
        ELSE txn.renderingprovidername
    END                                                                                     AS prov_rendering_name_1,
    /* prov_rendering_std_taxonomy */
    /* NOTE: Many of the values for the renderingprovidertaxonomycode column in the latest source data (6/28/19)
       are names rather than taxonomy codes. The code below is used to eliminate those names, and load only valid
       taxonomy codes. If the format of the NUCC taxonomy code changes, we will need to change this code. */
    CASE
        WHEN LENGTH(TRIM(COALESCE(txn.renderingprovidertaxonomycode, ''))) = 10
         AND SUBSTR(TRIM(COALESCE(txn.renderingprovidertaxonomycode, '')), 1, 1) IN (
                                                                                        '0', '1', '2', '3', '4',
                                                                                        '5', '6', '7', '8', '9'
                                                                                    )
         AND SUBSTR(REVERSE(TRIM(COALESCE(txn.renderingprovidertaxonomycode, ''))), 1, 1) = 'X'
            THEN txn.renderingprovidertaxonomycode
        ELSE NULL
    END                                                                                     AS prov_rendering_std_taxonomy,
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
    /* prov_referring_vendor_id */
    CASE
        WHEN SUBSTR(CONCAT('00', txn.facilitycode), -2) IN ('05', '06', '07', '08', '09', '12', '13', '14', '33')
            THEN NULL
        WHEN txn.referringprovideridqualifier <> 'XX'
            THEN txn.referringproviderid
        ELSE NULL
    END                                                                                     AS prov_referring_vendor_id,
    /* prov_referring_name_1 */
    CASE
        WHEN SUBSTR(CONCAT('00', txn.facilitycode), -2) IN ('05', '06', '07', '08', '09', '12', '13', '14', '33')
            THEN NULL
        ELSE txn.referringprovidername
    END                                                                                     AS prov_referring_name_1,
    /* prov_facility_vendor_id */
    CASE
        WHEN SUBSTR(CONCAT('00', txn.facilitycode), -2) IN ('05', '06', '07', '08', '09', '12', '13', '14', '33')
            THEN NULL
        WHEN txn.servicefacilityidqualifier <> 'XX'
            THEN txn.servicefacilityid
        ELSE NULL
    END                                                                                     AS prov_facility_vendor_id,
    /* prov_facility_name_1 */
    CASE
        WHEN SUBSTR(CONCAT('00', txn.facilitycode), -2) IN ('05', '06', '07', '08', '09', '12', '13', '14', '33')
            THEN NULL
        ELSE txn.servicefacilityname
    END                                                                                     AS prov_facility_name_1,
    /* prov_facility_address_1 */
    CASE
        WHEN SUBSTR(CONCAT('00', txn.facilitycode), -2) IN ('05', '06', '07', '08', '09', '12', '13', '14', '33')
            THEN NULL
        ELSE txn.servicefacilityaddress
    END                                                                                     AS prov_facility_address_1,
    /* prov_facility_city */
    CASE
        WHEN SUBSTR(CONCAT('00', txn.facilitycode), -2) IN ('05', '06', '07', '08', '09', '12', '13', '14', '33')
            THEN NULL
        ELSE txn.servicefacilitycity
    END                                                                                     AS prov_facility_city,
    /* prov_facility_state */
    CASE
        WHEN SUBSTR(CONCAT('00', txn.facilitycode), -2) IN ('05', '06', '07', '08', '09', '12', '13', '14', '33')
            THEN NULL
        ELSE txn.servicefacilitystate
    END                                                                                     AS prov_facility_state,
    /* prov_facility_zip */
    CASE
        WHEN SUBSTR(CONCAT('00', txn.facilitycode), -2) IN ('05', '06', '07', '08', '09', '12', '13', '14', '33')
            THEN NULL
        ELSE txn.servicefacilityzip
    END                                                                                     AS prov_facility_zip,
	'cardinal_pms'                                                                          AS part_provider,
    /* part_best_date */
	CASE
	    WHEN 0 = LENGTH(TRIM(COALESCE
	                            (
	                                CAP_DATE
                                        (
                                            CAST(EXTRACT_DATE(txn.dateservicestart, '%Y%m%d') AS DATE),
                                            COALESCE(ahdt.gen_ref_1_dt, esdt.gen_ref_1_dt),
                                            CAST(EXTRACT_DATE('{VDR_FILE_DT}', '%Y-%m-%d') AS DATE)
                                        ),
                                    ''
                                )))
	        THEN '0_PREDATES_HVM_HISTORY'
	    ELSE CONCAT
	            (
                    SUBSTR(txn.dateservicestart, 1, 4), '-',
                    SUBSTR(txn.dateservicestart, 5, 2), '-01'
                )
	END                                                                                 AS part_best_date
 FROM cardinal_pms_837_hvm_norm01_pvt_lns txn
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
