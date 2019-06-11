SELECT
/* claim_id */
CONCAT
(
    COALESCE(txn.navicure_client_id, ''),
    COALESCE(txn.unique_claim_id, ''),
    COALESCE(txn.claim_revision_no, '')
)																            		AS claim_id,
pay.hvid																				AS hvid,
CURRENT_DATE()                                                                          AS created,
'08'																					AS model_version,
SPLIT(txn.input_file_name, '/')[SIZE(SPLIT(txn.input_file_name, '/')) - 1]              AS data_set,
'24'																					AS data_feed,
'34'																					AS data_vendor,
/* patient_gender */
CLEAN_UP_GENDER
(
    CASE
                WHEN UPPER(SUBSTR(txn.patient_gender, 1, 1)) IN ('F', 'M')
                    THEN UPPER(SUBSTR(txn.patient_gender, 1, 1))
                WHEN UPPER(SUBSTR(pay.gender, 1, 1)) IN ('F', 'M')
                    THEN UPPER(SUBSTR(pay.gender, 1, 1))
                ELSE 'U'
        END
    )																					AS patient_gender,
    /* patient_age */
    CAP_AGE
    (
        VALIDATE_AGE
        (
            pay.age,
            CAST(EXTRACT_DATE(mmd.min_service_from_date, '%Y%m%d') AS DATE),
            SUBSTR(COALESCE(txn.patient_date_of_birth, pay.yearofbirth), 1, 4)
        )
    )																					AS patient_age,
    /* patient_year_of_birth */
    CAP_YEAR_OF_BIRTH
    (
        pay.age,
        CAST(EXTRACT_DATE(mmd.min_service_from_date, '%Y%m%d') AS DATE),
        SUBSTR(COALESCE(txn.patient_date_of_birth, pay.yearofbirth), 1, 4)
    )																					AS patient_year_of_birth,
    /* patient_zip3 */
    MASK_ZIP_CODE
    (
        SUBSTR(COALESCE(txn.patient_home_address_zip, pay.threedigitzip), 1, 3)
    )	                                                                        		AS patient_zip3,
    VALIDATE_STATE_CODE
    (
        UPPER(COALESCE(txn.patient_home_address_state, pay.state, ''))
    )		                                                                    		AS patient_state,
    'P'												                						AS claim_type,
    CAST(EXTRACT_DATE(txn.claim_submit_date, '%Y%m%d') AS DATE)							    AS date_received,
    /* date_service */
    CAP_DATE
    (
        CAST(EXTRACT_DATE(mmd.min_service_from_date, '%Y%m%d') AS DATE),
        esdt.gen_ref_1_dt,
        CAST(EXTRACT_DATE({VDR_FILE_DT}, '%Y%m%d') AS DATE)
    )                                                                                   AS date_service,
    /* date_service_end */
    CAP_DATE
    (
        CAST(EXTRACT_DATE(mmd.max_service_to_date, '%Y%m%d') AS DATE),
        esdt.gen_ref_1_dt,
        CAST(EXTRACT_DATE({VDR_FILE_DT}, '%Y%m%d') AS DATE)
    )                                                                                   AS date_service_end,
    /* place_of_service_std_id */
    CASE
        WHEN txn.place_of_service_code IS NULL
            THEN NULL
        WHEN SUBSTR(CONCAT('00', txn.place_of_service_code), -2) IN ('05', '06', '07', '08', '09', '12', '13', '14', '33')
            THEN '99'
        ELSE SUBSTR(CONCAT('00', txn.place_of_service_code), -2)
END 																					AS place_of_service_std_id,
CAST(NULL AS STRING)											            			AS service_line_number,
/* diagnosis_code */
CASE
        WHEN ARRAY
            (
                COALESCE(txn.icd10_code_1, txn.icd9_code_1),
                COALESCE(txn.icd10_code_2, txn.icd9_code_2),
                COALESCE(txn.icd10_code_3, txn.icd9_code_3),
                COALESCE(txn.icd10_code_4, txn.icd9_code_4),
                COALESCE(txn.icd10_code_5, txn.icd9_code_5),
                COALESCE(txn.icd10_code_6, txn.icd9_code_6),
                COALESCE(txn.icd10_code_7, txn.icd9_code_7),
                COALESCE(txn.icd10_code_8, txn.icd9_code_8),
                COALESCE(txn.icd10_code_9, txn.icd9_code_9),
                COALESCE(txn.icd10_code_10, txn.icd9_code_10),
                COALESCE(txn.icd10_code_11, txn.icd9_code_11),
                COALESCE(txn.icd10_code_12, txn.icd9_code_12)
            )[diag_explode.n] IS NULL
            THEN NULL
        ELSE CLEAN_UP_DIAGNOSIS_CODE
            (
                ARRAY
                (
                    COALESCE(txn.icd10_code_1, txn.icd9_code_1),
                    COALESCE(txn.icd10_code_2, txn.icd9_code_2),
                    COALESCE(txn.icd10_code_3, txn.icd9_code_3),
                    COALESCE(txn.icd10_code_4, txn.icd9_code_4),
                    COALESCE(txn.icd10_code_5, txn.icd9_code_5),
                    COALESCE(txn.icd10_code_6, txn.icd9_code_6),
                    COALESCE(txn.icd10_code_7, txn.icd9_code_7),
                    COALESCE(txn.icd10_code_8, txn.icd9_code_8),
                    COALESCE(txn.icd10_code_9, txn.icd9_code_9),
                    COALESCE(txn.icd10_code_10, txn.icd9_code_10),
                    COALESCE(txn.icd10_code_11, txn.icd9_code_11),
                    COALESCE(txn.icd10_code_12, txn.icd9_code_12)
                )[diag_explode.n],
                CASE
                        WHEN ARRAY
                            (
                                txn.icd10_code_1,
                                txn.icd10_code_2,
                                txn.icd10_code_3,
                                txn.icd10_code_4,
                                txn.icd10_code_5,
                                txn.icd10_code_6,
                                txn.icd10_code_7,
                                txn.icd10_code_8,
                                txn.icd10_code_9,
                                txn.icd10_code_10,
                                txn.icd10_code_11,
                                txn.icd10_code_12
                            )[diag_explode.n] IS NOT NULL
                            THEN '02'
                        WHEN ARRAY
                            (
                                txn.icd9_code_1,
                                txn.icd9_code_2,
                                txn.icd9_code_3,
                                txn.icd9_code_4,
                                txn.icd9_code_5,
                                txn.icd9_code_6,
                                txn.icd9_code_7,
                                txn.icd9_code_8,
                                txn.icd9_code_9,
                                txn.icd9_code_10,
                                txn.icd9_code_11,
                                txn.icd9_code_12
                            )[diag_explode.n] IS NOT NULL
                            THEN '01'
                        ELSE NULL
                END,
                CAST(EXTRACT_DATE(mmd.min_service_from_date, '%Y%m%d') AS DATE)
            )
END                                                                                     AS diagnosis_code,
/* diagnosis_code_qual */
CASE
        WHEN ARRAY
            (
                COALESCE(txn.icd10_code_1, txn.icd9_code_1),
                COALESCE(txn.icd10_code_2, txn.icd9_code_2),
                COALESCE(txn.icd10_code_3, txn.icd9_code_3),
                COALESCE(txn.icd10_code_4, txn.icd9_code_4),
                COALESCE(txn.icd10_code_5, txn.icd9_code_5),
                COALESCE(txn.icd10_code_6, txn.icd9_code_6),
                COALESCE(txn.icd10_code_7, txn.icd9_code_7),
                COALESCE(txn.icd10_code_8, txn.icd9_code_8),
                COALESCE(txn.icd10_code_9, txn.icd9_code_9),
                COALESCE(txn.icd10_code_10, txn.icd9_code_10),
                COALESCE(txn.icd10_code_11, txn.icd9_code_11),
                COALESCE(txn.icd10_code_12, txn.icd9_code_12)
            )[diag_explode.n] IS NULL
            THEN NULL
        WHEN ARRAY
            (
                txn.icd10_code_1,
                txn.icd10_code_2,
                txn.icd10_code_3,
                txn.icd10_code_4,
                txn.icd10_code_5,
                txn.icd10_code_6,
                txn.icd10_code_7,
                txn.icd10_code_8,
                txn.icd10_code_9,
                txn.icd10_code_10,
                txn.icd10_code_11,
                txn.icd10_code_12
            )[diag_explode.n] IS NOT NULL
            THEN '02'
        WHEN ARRAY
            (
                txn.icd9_code_1,
                txn.icd9_code_2,
                txn.icd9_code_3,
                txn.icd9_code_4,
                txn.icd9_code_5,
                txn.icd9_code_6,
                txn.icd9_code_7,
                txn.icd9_code_8,
                txn.icd9_code_9,
                txn.icd9_code_10,
                txn.icd9_code_11,
                txn.icd9_code_12
            )[diag_explode.n] IS NOT NULL
            THEN '01'
        ELSE NULL
END 																					AS diagnosis_code_qual,
CAST(NULL AS STRING)                                                                    AS diagnosis_priority,
CAST(NULL AS STRING)                                    						    	AS procedure_code,
CAST(NULL AS STRING)																	AS procedure_code_qual,
CAST(NULL AS STRING)                                    								AS procedure_units_billed,
CAST(NULL AS STRING)                            										AS procedure_modifier_1,
CAST(NULL AS STRING)                        											AS procedure_modifier_2,
CAST(NULL AS STRING)                            										AS procedure_modifier_3,
CAST(NULL AS STRING)                            										AS procedure_modifier_4,
CAST(NULL AS STRING)                        											AS ndc_code,
/* medical_coverage_type */
CASE
        WHEN 0 <> LENGTH(TRIM(COALESCE(txn.dest_claim_filing_indicator, '')))
            THEN TRIM(SUBSTR(txn.dest_claim_filing_indicator, 1, 2))
        ELSE NULL
END									                									AS medical_coverage_type,
CAST(NULL AS FLOAT)	                                    							    AS line_charge,
CAST(txn.total_claim_charge_amount AS FLOAT)											AS total_charge,
/* prov_rendering_npi */
CLEAN_UP_NPI_CODE
(
    CASE
                WHEN SUBSTR(CONCAT('00', txn.place_of_service_code), -2) IN ('05', '06', '07', '08', '09', '12', '13', '14', '33', '99')
                    THEN NULL
                ELSE txn.rendering_provider_npi
        END
    )   																				AS prov_rendering_npi,
    /* prov_billing_npi */
    CLEAN_UP_NPI_CODE
    (
        CASE
                WHEN SUBSTR(CONCAT('00', txn.place_of_service_code), -2) IN ('05', '06', '07', '08', '09', '12', '13', '14', '33', '99')
                    THEN NULL
                ELSE txn.billing_npi
        END
    )																					AS prov_billing_npi,
    /* prov_referring_npi */
    CLEAN_UP_NPI_CODE
    (
        CASE
                WHEN SUBSTR(CONCAT('00', txn.place_of_service_code), -2) IN ('05', '06', '07', '08', '09', '12', '13', '14', '33', '99')
                    THEN NULL
                ELSE txn.referring_provider_1_npi
        END
    )																					AS prov_referring_npi,
    /* prov_facility_npi */
    CLEAN_UP_NPI_CODE
    (
        CASE
                WHEN SUBSTR(CONCAT('00', txn.place_of_service_code), -2) IN ('05', '06', '07', '08', '09', '12', '13', '14', '33', '99')
                    THEN NULL
                ELSE txn.facility_npi
        END
    )																					AS prov_facility_npi,
    /* payer_name */
    CASE
        WHEN txn.dest_client_payer_name IS NOT NULL
            AND txn.dest_system_payer_name IS NOT NULL
            THEN CONCAT(txn.dest_client_payer_name, ' | ', txn.dest_system_payer_name)
        WHEN txn.dest_client_payer_name IS NOT NULL
            THEN txn.dest_client_payer_name
        WHEN txn.dest_system_payer_name IS NOT NULL
            THEN txn.dest_system_payer_name
        ELSE NULL
END						                                                                AS payer_name,
/* payer_plan_id */
CASE
        WHEN txn.dest_client_payer_id IS NOT NULL
            AND txn.dest_system_payer_entity IS NOT NULL
            THEN CONCAT(txn.dest_client_payer_id, ' | ', txn.dest_system_payer_entity)
        WHEN txn.dest_client_payer_id IS NOT NULL
            THEN txn.dest_client_payer_id
        WHEN txn.dest_system_payer_entity IS NOT NULL
            THEN txn.dest_system_payer_entity
        ELSE NULL
END                                                                                     AS payer_plan_id,
/* prov_rendering_state_license */
CASE
        WHEN SUBSTR(CONCAT('00', txn.place_of_service_code), -2) IN ('05', '06', '07', '08', '09', '12', '13', '14', '33', '99')
            THEN NULL
        ELSE txn.rendering_state_license_no
END																			    		AS prov_rendering_state_license,
/* prov_rendering_upin */
CASE
        WHEN SUBSTR(CONCAT('00', txn.place_of_service_code), -2) IN ('05', '06', '07', '08', '09', '12', '13', '14', '33', '99')
            THEN NULL
        ELSE txn.rendering_upin
END																    					AS prov_rendering_upin,
/* prov_rendering_commercial_id */
CASE
        WHEN SUBSTR(CONCAT('00', txn.place_of_service_code), -2) IN ('05', '06', '07', '08', '09', '12', '13', '14', '33', '99')
            THEN NULL
        ELSE txn.rendering_commercial
END																				    	AS prov_rendering_commercial_id,
/* prov_rendering_name_1 */
CASE
        WHEN SUBSTR(CONCAT('00', txn.place_of_service_code), -2) IN ('05', '06', '07', '08', '09', '12', '13', '14', '33', '99')
            THEN NULL
        WHEN COALESCE(txn.rendering_last_name, txn.rendering_first_name, txn.rendering_middle_name) IS NULL
            THEN NULL
        ELSE SUBSTR(CONCAT
            (
                CASE
                                WHEN txn.rendering_last_name IS NOT NULL
                                    THEN CONCAT(', ', txn.rendering_last_name)
                                ELSE ''
                        END,
                        CASE
                                WHEN txn.rendering_first_name IS NOT NULL
                                    THEN CONCAT(', ', txn.rendering_first_name)
                                ELSE ''
                        END,
                        CASE
                                WHEN txn.rendering_middle_name IS NOT NULL
                                    THEN CONCAT(', ', txn.rendering_middle_name)
                                ELSE ''
                        END
                ), 3)
END						    															AS prov_rendering_name_1,
txn.rendering_taxonomy_code																AS prov_rendering_std_taxonomy,
/* prov_billing_tax_id */
CASE
        WHEN SUBSTR(CONCAT('00', txn.place_of_service_code), -2) IN ('05', '06', '07', '08', '09', '12', '13', '14', '33', '99')
            THEN NULL
        WHEN txn.billing_tax_id_qualifier = 'EI'
            THEN txn.billing_tax_id
        ELSE NULL
END						    															AS prov_billing_tax_id,
/* prov_billing_ssn */
CASE
        WHEN SUBSTR(CONCAT('00', txn.place_of_service_code), -2) IN ('05', '06', '07', '08', '09', '12', '13', '14', '33', '99')
            THEN NULL
        WHEN txn.billing_tax_id_qualifier = 'SY'
            THEN txn.billing_tax_id
        ELSE NULL
END						    															AS prov_billing_ssn,
/* prov_billing_state_license */
CASE
        WHEN SUBSTR(CONCAT('00', txn.place_of_service_code), -2) IN ('05', '06', '07', '08', '09', '12', '13', '14', '33', '99')
            THEN NULL
        ELSE txn.billing_state_license_no
END						    															AS prov_billing_state_license,
/* prov_billing_commercial_id */
CASE
        WHEN SUBSTR(CONCAT('00', txn.place_of_service_code), -2) IN ('05', '06', '07', '08', '09', '12', '13', '14', '33', '99')
            THEN NULL
        ELSE txn.billing_commercial
END						    															AS prov_billing_commercial_id,
/* prov_billing_name_1 */
CASE
        WHEN SUBSTR(CONCAT('00', txn.place_of_service_code), -2) IN ('05', '06', '07', '08', '09', '12', '13', '14', '33', '99')
            THEN NULL
        WHEN txn.billing_organization_name IS NOT NULL
            AND COALESCE(txn.billing_last_name, txn.billing_first_name, txn.billing_middle_name) IS NOT NULL
            THEN CONCAT
            (
                txn.billing_organization_name, ': ',
                SUBSTR(CONCAT
                    (
                        CASE
                                        WHEN txn.billing_last_name IS NOT NULL
                                            THEN CONCAT(', ', txn.billing_last_name)
                                        ELSE ''
                                END,
                                CASE
                                        WHEN txn.billing_first_name IS NOT NULL
                                            THEN CONCAT(', ', txn.billing_first_name)
                                        ELSE ''
                                END,
                                CASE
                                        WHEN txn.billing_middle_name IS NOT NULL
                                            THEN CONCAT(', ', txn.billing_middle_name)
                                        ELSE ''
                                END
                        ), 3)
                )
        WHEN COALESCE(txn.billing_last_name, txn.billing_first_name, txn.billing_middle_name) IS NOT NULL
            THEN SUBSTR(CONCAT
                (
                    CASE
                                    WHEN txn.billing_last_name IS NOT NULL
                                        THEN CONCAT(', ', txn.billing_last_name)
                                    ELSE ''
                            END,
                            CASE
                                    WHEN txn.billing_first_name IS NOT NULL
                                        THEN CONCAT(', ', txn.billing_first_name)
                                    ELSE ''
                            END,
                            CASE
                                    WHEN txn.billing_middle_name IS NOT NULL
                                        THEN CONCAT(', ', txn.billing_middle_name)
                                    ELSE ''
                            END
                    ), 3)
            ELSE txn.billing_organization_name
END						    															AS prov_billing_name_1,
/* prov_billing_address_1 */
CASE
        WHEN SUBSTR(CONCAT('00', txn.place_of_service_code), -2) IN ('05', '06', '07', '08', '09', '12', '13', '14', '33', '99')
            THEN NULL
        ELSE txn.billing_street_address_line_1
END						    															AS prov_billing_address_1,
/* prov_billing_address_2 */
CASE
        WHEN SUBSTR(CONCAT('00', txn.place_of_service_code), -2) IN ('05', '06', '07', '08', '09', '12', '13', '14', '33', '99')
            THEN NULL
        ELSE txn.billing_street_address_line_2
END						    															AS prov_billing_address_2,
/* prov_billing_city */
CASE
        WHEN SUBSTR(CONCAT('00', txn.place_of_service_code), -2) IN ('05', '06', '07', '08', '09', '12', '13', '14', '33', '99')
            THEN NULL
        ELSE txn.billing_street_address_city
END						    															AS prov_billing_city,
/* prov_billing_state */
VALIDATE_STATE_CODE
(
    CASE
                WHEN SUBSTR(CONCAT('00', txn.place_of_service_code), -2) IN ('05', '06', '07', '08', '09', '12', '13', '14', '33', '99')
                    THEN NULL
                ELSE UPPER(COALESCE(txn.billing_street_address_state, ''))
        END
    )   																				AS prov_billing_state,
    /* prov_billing_zip */
    CASE
        WHEN SUBSTR(CONCAT('00', txn.place_of_service_code), -2) IN ('05', '06', '07', '08', '09', '12', '13', '14', '33', '99')
            THEN NULL
        ELSE txn.billing_street_address_zip
END						    															AS prov_billing_zip,
txn.billing_taxonomy_code																AS prov_billing_std_taxonomy,
/* prov_referring_state_license */
CASE
        WHEN SUBSTR(CONCAT('00', txn.place_of_service_code), -2) IN ('05', '06', '07', '08', '09', '12', '13', '14', '33', '99')
            THEN NULL
        ELSE txn.referring_1_state_license_no
END						    															AS prov_referring_state_license,
/* prov_referring_upin */
CASE
        WHEN SUBSTR(CONCAT('00', txn.place_of_service_code), -2) IN ('05', '06', '07', '08', '09', '12', '13', '14', '33', '99')
            THEN NULL
        ELSE txn.referring_1_upin
END						    															AS prov_referring_upin,
/* prov_referring_commercial_id */
CASE
        WHEN SUBSTR(CONCAT('00', txn.place_of_service_code), -2) IN ('05', '06', '07', '08', '09', '12', '13', '14', '33', '99')
            THEN NULL
        ELSE txn.referring_1_commercial_id
END						    															AS prov_referring_commercial_id,
/* prov_referring_name_1 */
CASE
        WHEN SUBSTR(CONCAT('00', txn.place_of_service_code), -2) IN ('05', '06', '07', '08', '09', '12', '13', '14', '33', '99')
            THEN NULL
        WHEN COALESCE(txn.referring_1_last_name, txn.referring_1_first_name, txn.referring_1_middle_name) IS NULL
            THEN NULL
        ELSE SUBSTR(CONCAT
            (
                CASE
                                WHEN txn.referring_1_last_name IS NOT NULL
                                    THEN CONCAT(', ', txn.referring_1_last_name)
                                ELSE ''
                        END,
                        CASE
                                WHEN txn.referring_1_first_name IS NOT NULL
                                    THEN CONCAT(', ', txn.referring_1_first_name)
                                ELSE ''
                        END,
                        CASE
                                WHEN txn.referring_1_middle_name IS NOT NULL
                                    THEN CONCAT(', ', txn.referring_1_middle_name)
                                ELSE ''
                        END
                ), 3)
END						    															AS prov_referring_name_1,
/* prov_facility_name_1 */
CASE
        WHEN SUBSTR(CONCAT('00', txn.place_of_service_code), -2) IN ('05', '06', '07', '08', '09', '12', '13', '14', '33', '99')
            THEN NULL
        ELSE txn.facility_organization_name
END						    															AS prov_facility_name_1,
/* prov_facility_address_1 */
CASE
        WHEN SUBSTR(CONCAT('00', txn.place_of_service_code), -2) IN ('05', '06', '07', '08', '09', '12', '13', '14', '33', '99')
            THEN NULL
        ELSE txn.facility_street_address_line_1
END						    															AS prov_facility_address_1,
/* prov_facility_address_2 */
CASE
        WHEN SUBSTR(CONCAT('00', txn.place_of_service_code), -2) IN ('05', '06', '07', '08', '09', '12', '13', '14', '33', '99')
            THEN NULL
        ELSE txn.facility_street_address_line_2
END						    															AS prov_facility_address_2,
/* prov_facility_city */
CASE
        WHEN SUBSTR(CONCAT('00', txn.place_of_service_code), -2) IN ('05', '06', '07', '08', '09', '12', '13', '14', '33', '99')
            THEN NULL
        ELSE txn.facility_street_address_city
END						    															AS prov_facility_city,
/* prov_facility_state */
VALIDATE_STATE_CODE
(
    CASE
                WHEN SUBSTR(CONCAT('00', txn.place_of_service_code), -2) IN ('05', '06', '07', '08', '09', '12', '13', '14', '33', '99')
                    THEN NULL
                ELSE UPPER(COALESCE(txn.facility_street_address_state, ''))
        END
    )   																				AS prov_facility_state,
    /* prov_facility_zip */
    CASE
        WHEN SUBSTR(CONCAT('00', txn.place_of_service_code), -2) IN ('05', '06', '07', '08', '09', '12', '13', '14', '33', '99')
            THEN NULL
        ELSE txn.facility_street_address_zip
END						    															AS prov_facility_zip,
txn.box9_client_payer_id                                                                AS cob_payer_vendor_id_1,
txn.box9_claim_filing_indicator                                                         AS cob_payer_claim_filing_ind_code_1,
txn.third_client_payer_id                                                               AS cob_payer_vendor_id_2,
txn.third_claim_filing_indicator                                                        AS cob_payer_claim_filing_ind_code_2,
'navicure'																				AS part_provider,
/* part_best_date */
CASE
        WHEN CAP_DATE
            (
                CAST(EXTRACT_DATE(mmd.min_service_from_date, '%Y%m%d') AS DATE),
                ahdt.gen_ref_1_dt,
                CAST(EXTRACT_DATE({VDR_FILE_DT}, '%Y%m%d') AS DATE)
            ) IS NULL
            THEN '0_PREDATES_HVM_HISTORY'
        ELSE CONCAT
            (
                SUBSTR(mmd.min_service_from_date, 1, 4), '-',
                SUBSTR(mmd.min_service_from_date, 5, 2), '-01'
            )
END 																					AS part_best_date
FROM navicure_dedup_txn txn
/* Take only the first service line number for each claim. */
INNER JOIN
(
    SELECT
    navicure_client_id,
    unique_claim_id,
    claim_revision_no,
    MIN(CAST(COALESCE(service_line, '0') AS INTEGER)) AS first_service_line
    FROM navicure_dedup_txn
    GROUP BY 1, 2, 3
) fsl
ON txn.navicure_client_id = fsl.navicure_client_id
AND txn.unique_claim_id = fsl.unique_claim_id
AND txn.claim_revision_no = fsl.claim_revision_no
AND txn.service_line = fsl.first_service_line
LEFT OUTER JOIN navicure_dedup_pay pay
ON txn.hvjoinkey = pay.hvjoinkey
/* Find the earliest and latest service dates for each claim. */
LEFT OUTER JOIN
(
    SELECT
    navicure_client_id,
    unique_claim_id,
    claim_revision_no,
    MIN(COALESCE(service_from_date, '29991231')) AS min_service_from_date,
    MAX(COALESCE(service_to_date, '19000101')) AS max_service_to_date
    FROM navicure_dedup_txn
    GROUP BY 1, 2, 3
) mmd
ON txn.navicure_client_id = mmd.navicure_client_id
AND txn.unique_claim_id = mmd.unique_claim_id
AND txn.claim_revision_no = mmd.claim_revision_no
CROSS JOIN
(
    SELECT gen_ref_1_dt
    FROM ref_gen_ref
    WHERE hvm_vdr_feed_id = 24
    AND gen_ref_domn_nm = 'EARLIEST_VALID_SERVICE_DATE'
) esdt
CROSS JOIN
(
    SELECT gen_ref_1_dt
    FROM ref_gen_ref
    WHERE hvm_vdr_feed_id = 24
    AND gen_ref_domn_nm = 'HVM_AVAILABLE_HISTORY_START_DATE'
) ahdt
CROSS JOIN (SELECT EXPLODE(ARRAY(0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11)) AS n) diag_explode
WHERE NOT EXISTS
(
    SELECT 1
    FROM navicure_dedup_txn sln
    WHERE COALESCE(txn.navicure_client_id, '') = COALESCE(sln.navicure_client_id, '')
    AND COALESCE(txn.unique_claim_id, '') = COALESCE(sln.unique_claim_id, '')
    AND COALESCE(txn.claim_revision_no, '') = COALESCE(sln.claim_revision_no, '')
    AND ARRAY('1', '2', '3', '4', '5', '6', '7', '8', '9', '10', '11', '12')[diag_explode.n] IN
    (
        COALESCE(sln.diagnosis_code_1, ''),
        COALESCE(sln.diagnosis_code_2, ''),
        COALESCE(sln.diagnosis_code_3, ''),
        COALESCE(sln.diagnosis_code_4, '')
    )
)
---------- Diagnosis code explosion
AND ARRAY
(
    COALESCE(txn.icd10_code_1, txn.icd9_code_1),
    COALESCE(txn.icd10_code_2, txn.icd9_code_2),
    COALESCE(txn.icd10_code_3, txn.icd9_code_3),
    COALESCE(txn.icd10_code_4, txn.icd9_code_4),
    COALESCE(txn.icd10_code_5, txn.icd9_code_5),
    COALESCE(txn.icd10_code_6, txn.icd9_code_6),
    COALESCE(txn.icd10_code_7, txn.icd9_code_7),
    COALESCE(txn.icd10_code_8, txn.icd9_code_8),
    COALESCE(txn.icd10_code_9, txn.icd9_code_9),
    COALESCE(txn.icd10_code_10, txn.icd9_code_10),
    COALESCE(txn.icd10_code_11, txn.icd9_code_11),
    COALESCE(txn.icd10_code_12, txn.icd9_code_12)
)[diag_explode.n] IS NOT NULL
