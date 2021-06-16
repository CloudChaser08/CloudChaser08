SELECT
    clm.claim_number                                                        AS claimid,
    clm.record_type                                                         AS recordtype,
    CAST(EXTRACT_DATE(clm.received_date, '%Y%m%d') AS DATE)                 AS receiveddate,
    claim_type_code                                                         AS claimtype,
    CAST(NULL AS STRING)                                                    AS dvtoken1,
    CAST(NULL AS STRING)                                                    AS dvtoken2,
    CAST(NULL AS STRING)                                                    AS dvtoken2zip3,
    CASE
        WHEN pay.hvid IS NOT NULL THEN pay.hvid
        ELSE NULL
    END                                                                      AS hvid,
    CAST(NULL AS STRING)                                                     AS hvidreview,
    VALIDATE_STATE_CODE
        (
            UPPER
                (
                    COALESCE(clm.member_adr_state, pay.state, '')
                )
        )                                                                   AS patientstate,
    MASK_ZIP_CODE
        (
            SUBSTR
                (COALESCE(clm.member_adr_zip, pay.threedigitzip),1, 3)
        )                                                                    AS patientzip3,
    ----------------------------------------------------------------------------------------------------------------------
    -- patientrelationshipcode
    ----------------------------------------------------------------------------------------------------------------------
    clm.patient_relation                                                     AS patientrelationshipcode,
    ----------------------------------------------------------------------------------------------------------------------
    -- patientgender
    ----------------------------------------------------------------------------------------------------------------------
	CASE
    	WHEN clm.patient_gender IS NULL AND pay.gender IS NULL THEN NULL
    	WHEN SUBSTR(UPPER(clm.patient_gender), 1, 1) IN ('F', 'M', 'U') THEN SUBSTR(UPPER(clm.patient_gender), 1, 1)
    	WHEN SUBSTR(UPPER(pay.gender        ), 1, 1) IN ('F', 'M', 'U') THEN SUBSTR(UPPER(pay.gender        ), 1, 1)
        ELSE 'U' 
    END                                                                  AS patientgender,
    ----------------------------------------------------------------------------------------------------------------------
    -- patientyob
    ----------------------------------------------------------------------------------------------------------------------
	CAP_YEAR_OF_BIRTH  -- Cap year of birth 1927 if age is 85 and over
	    (
            COALESCE(clm.patient_age, pay.age),
            CAST(EXTRACT_DATE(COALESCE(sln.service_from, clm.statement_from), '%Y%m%d') AS DATE),
            COALESCE(clm.patient_yob, pay.yearofbirth)
	    )                                                                AS patientyob,
    ----------------------------------------------------------------------------------------------------------------------
    -- BILLING PROVIDER NPI
    ----------------------------------------------------------------------------------------------------------------------    
    CASE
        WHEN COALESCE(clm.claim_type_code, 'X') NOT IN ('I', 'P')
                THEN NULL
        WHEN clm.claim_type_code = 'P'
            AND LPAD(sln.place_service, 2, '0') IN ('05', '06', '07', '08', '09', '12', '13', '14', '33', '99')
                THEN NULL
        WHEN clm.claim_type_code = 'I' AND SUBSTR(COALESCE(clm.type_bill, 'X'), 1, 1) IN ('3', 'X')
                THEN NULL
    ELSE CLEAN_UP_NPI_CODE(clm.billing_pr_npi)
    END                                                                 AS billingprovidernpi,
    ----------------------------------------------------------------------------------------------------------------------
    -- billingprovidername1
    ----------------------------------------------------------------------------------------------------------------------    
	CASE
	    WHEN COALESCE(clm.claim_type_code, 'X') NOT IN ('I', 'P')
	         THEN NULL
        WHEN clm.claim_type_code = 'P'
         AND LPAD(sln.place_service, 2, '0') IN ('05', '06', '07', '08', '09', '12', '13', '14', '33', '99')
             THEN NULL
        WHEN clm.claim_type_code = 'I' AND SUBSTR(COALESCE(clm.type_bill, 'X'), 1, 1) IN ('3', 'X')
             THEN NULL
        ELSE clmnms.billing_name1
	END		                                                                   AS billingprovidername1,
    ----------------------------------------------------------------------------------------------------------------------
    -- billingprovidername2
    ----------------------------------------------------------------------------------------------------------------------    
   	CASE
	    WHEN COALESCE(clm.claim_type_code, 'X') NOT IN ('I', 'P')
	         THEN NULL
        WHEN clm.claim_type_code = 'P'
         AND LPAD(sln.place_service, 2, '0') IN ('05', '06', '07', '08', '09', '12', '13', '14', '33', '99')
             THEN NULL
        WHEN clm.claim_type_code = 'I' AND SUBSTR(COALESCE(clm.type_bill, 'X'), 1, 1) IN ('3', 'X')
             THEN NULL
        ELSE clmnms.billing_name2
	END	                                                                 AS billingprovidername2,
    ----------------------------------------------------------------------------------------------------------------------
    -- billingprovideraddress1
    ----------------------------------------------------------------------------------------------------------------------    
   	CASE
	    WHEN COALESCE(clm.claim_type_code, 'X') NOT IN ('I', 'P')
	         THEN NULL
        WHEN clm.claim_type_code = 'P'
         AND LPAD(sln.place_service, 2, '0') IN ('05', '06', '07', '08', '09', '12', '13', '14', '33', '99')
             THEN NULL
        WHEN clm.claim_type_code = 'I'
         AND SUBSTR(COALESCE(clm.type_bill, 'X'), 1, 1) IN ('3', 'X')
             THEN NULL
        ELSE clm.billing_adr_line1
	END						                                                                      AS billingprovideraddress1,
    ----------------------------------------------------------------------------------------------------------------------
    -- billingprovideraddress2
    ----------------------------------------------------------------------------------------------------------------------    
   	CASE
	    WHEN COALESCE(clm.claim_type_code, 'X') NOT IN ('I', 'P')
	         THEN NULL
        WHEN clm.claim_type_code = 'P'
         AND LPAD(sln.place_service, 2, '0') IN ('05', '06', '07', '08', '09', '12', '13', '14', '33', '99')
             THEN NULL
        WHEN clm.claim_type_code = 'I'
         AND SUBSTR(COALESCE(clm.type_bill, 'X'), 1, 1) IN ('3', 'X')
             THEN NULL
        ELSE clm.billing_adr_line2
	END						                                                                      AS billingprovideraddress2,
    ----------------------------------------------------------------------------------------------------------------------
    -- city
    ----------------------------------------------------------------------------------------------------------------------    
	CASE
	    WHEN COALESCE(clm.claim_type_code, 'X') NOT IN ('I', 'P')
	         THEN NULL
        WHEN clm.claim_type_code = 'P'
         AND LPAD(sln.place_service, 2, '0') IN ('05', '06', '07', '08', '09', '12', '13', '14', '33', '99')
             THEN NULL
        WHEN clm.claim_type_code = 'I'
         AND SUBSTR(COALESCE(clm.type_bill, 'X'), 1, 1) IN ('3', 'X')
             THEN NULL
        ELSE clm.billing_adr_city
	END						                                                                       AS billingprovidercity,
    ----------------------------------------------------------------------------------------------------------------------
    -- state
    ----------------------------------------------------------------------------------------------------------------------    
    CASE
	    WHEN COALESCE(clm.claim_type_code, 'X') NOT IN ('I', 'P')
	         THEN NULL
        WHEN clm.claim_type_code = 'P'
         AND LPAD(sln.place_service, 2, '0') IN ('05', '06', '07', '08', '09', '12', '13', '14', '33', '99')
             THEN NULL
        WHEN clm.claim_type_code = 'I'
         AND SUBSTR(COALESCE(clm.type_bill, 'X'), 1, 1) IN ('3', 'X')
             THEN NULL
        ELSE VALIDATE_STATE_CODE(clm.billing_adr_state)
	END                                                                                             AS billingproviderstate,
    ----------------------------------------------------------------------------------------------------------------------
    -- zip
    ----------------------------------------------------------------------------------------------------------------------    
	CASE
	    WHEN COALESCE(clm.claim_type_code, 'X') NOT IN ('I', 'P')
	         THEN NULL
        WHEN clm.claim_type_code = 'P'
         AND LPAD(sln.place_service, 2, '0') IN ('05', '06', '07', '08', '09', '12', '13', '14', '33', '99')
             THEN NULL
        WHEN clm.claim_type_code = 'I'
         AND SUBSTR(COALESCE(clm.type_bill, 'X'), 1, 1) IN ('3', 'X')
             THEN NULL
        ELSE MASK_ZIP_CODE(clm.billing_adr_zip)
	END	                                                                                            AS billingproviderzip,
    ----------------------------------------------------------------------------------------------------------------------
    -- REFERRING PROVIDER NPI
    ----------------------------------------------------------------------------------------------------------------------    
    CASE
        WHEN COALESCE(clm.claim_type_code, 'X') NOT IN ('I', 'P')
	         THEN NULL
        WHEN clm.claim_type_code = 'P'
            AND LPAD(sln.place_service, 2, '0') IN ('05', '06', '07', '08', '09', '12', '13', '14', '33', '99')
             THEN NULL
        WHEN clm.claim_type_code = 'I'
            AND SUBSTR(COALESCE(clm.type_bill, 'X'), 1, 1) IN ('3', 'X')
             THEN NULL
        WHEN clm.referring_pr_npi IS NULL THEN CLEAN_UP_NPI_CODE(clm.referring_name1) -- If referring_name1 is not 10 digital numeric value, CLEAN_UP_NPI_CODE will return null.
        ELSE CLEAN_UP_NPI_CODE(clm.referring_pr_npi)
    END                                                                                           AS referringprovidernpi,
    ----------------------------------------------------------------------------------------------------------------------
    -- billingprovidertaxonomy
    ----------------------------------------------------------------------------------------------------------------------    
  	clm.billing_taxonomy                                                                          AS billingprovidertaxonomy,
    ----------------------------------------------------------------------------------------------------------------------
    -- referringprovidername
    ----------------------------------------------------------------------------------------------------------------------    
    CASE
	    WHEN COALESCE(clm.claim_type_code, 'X') NOT IN ('I', 'P')
	         THEN NULL
        WHEN clm.claim_type_code = 'P'
         AND LPAD(sln.place_service, 2, '0') IN ('05', '06', '07', '08', '09', '12', '13', '14', '33', '99')
             THEN NULL
        WHEN clm.claim_type_code = 'I'
         AND SUBSTR(COALESCE(clm.type_bill, 'X'), 1, 1) IN ('3', 'X')
             THEN NULL
        ELSE TRIM(clmnms.referring_name2)
	END	                                                                   AS referringprovidername,
    ----------------------------------------------------------------------------------------------------------------------
    -- renderingprovidernpi
    ----------------------------------------------------------------------------------------------------------------------    
    CASE
	    WHEN COALESCE(clm.claim_type_code, 'X') NOT IN ('I', 'P')
	         THEN NULL
        WHEN clm.claim_type_code = 'P'
         AND LPAD(sln.place_service, 2, '0') IN ('05', '06', '07', '08', '09', '12', '13', '14', '33', '99')
             THEN NULL
        WHEN clm.claim_type_code = 'I'
         AND SUBSTR(COALESCE(clm.type_bill, 'X'), 1, 1) IN ('3', 'X')
             THEN NULL
        ELSE CLEAN_UP_NPI_CODE(clm.rendering_upin)
	END	                                                                AS renderingprovidernpi,
    ----------------------------------------------------------------------------------------------------------------------
    -- renderingprovidername1
    ----------------------------------------------------------------------------------------------------------------------    
    CASE
	    WHEN COALESCE(clm.claim_type_code, 'X') NOT IN ('I', 'P')
	         THEN NULL
        WHEN clm.claim_type_code = 'P'
         AND LPAD(sln.place_service, 2, '0') IN ('05', '06', '07', '08', '09', '12', '13', '14', '33', '99')
             THEN NULL
        WHEN clm.claim_type_code = 'I'
         AND SUBSTR(COALESCE(clm.type_bill, 'X'), 1, 1) IN ('3', 'X')
             THEN NULL
        ELSE TRIM(clmnms.attending_name1)
	END	                                                                    AS renderingprovidername1,
    ----------------------------------------------------------------------------------------------------------------------
    -- renderingprovidername2
    ----------------------------------------------------------------------------------------------------------------------   
    CASE
	    WHEN COALESCE(clm.claim_type_code, 'X') NOT IN ('I', 'P')
	         THEN NULL
        WHEN clm.claim_type_code = 'P'
         AND LPAD(sln.place_service, 2, '0') IN ('05', '06', '07', '08', '09', '12', '13', '14', '33', '99')
             THEN NULL
        WHEN clm.claim_type_code = 'I'
         AND SUBSTR(COALESCE(clm.type_bill, 'X'), 1, 1) IN ('3', 'X')
             THEN NULL
        ELSE TRIM(clmnms.attending_name2)
	END                                                                    AS renderingprovidername2,
    ----------------------------------------------------------------------------------------------------------------------
    -- renderingproviderspecialtycode
    ----------------------------------------------------------------------------------------------------------------------  
    clm.rendering_taxonomy                                                 AS renderingproviderspecialtycode,
    ----------------------------------------------------------------------------------------------------------------------
    -- facilityname
    ----------------------------------------------------------------------------------------------------------------------  
	CASE
	    WHEN COALESCE(clm.claim_type_code, 'X') NOT IN ('I', 'P')
	         THEN NULL
        WHEN clm.claim_type_code = 'P'
         AND LPAD(sln.place_service, 2, '0') IN ('05', '06', '07', '08', '09', '12', '13', '14', '33', '99')
             THEN NULL
        WHEN clm.claim_type_code = 'I'
         AND SUBSTR(COALESCE(clm.type_bill, 'X'), 1, 1) IN ('3', 'X')
             THEN NULL
        ELSE TRIM(COALESCE(clmnms.facility_name1, clmnms.facility_name2))
	END	                                                                   AS facilityname,
    ----------------------------------------------------------------------------------------------------------------------
    -- facility_lab_npi
    ----------------------------------------------------------------------------------------------------------------------  
    CASE
        WHEN COALESCE(clm.claim_type_code, 'X') NOT IN ('I', 'P')
	         THEN NULL
        WHEN clm.claim_type_code = 'P'
            AND LPAD(sln.place_service, 2, '0') IN ('05', '06', '07', '08', '09', '12', '13', '14', '33', '99')
             THEN NULL
        WHEN clm.claim_type_code = 'I'
            AND SUBSTR(COALESCE(clm.type_bill, 'X'), 1, 1) IN ('3', 'X')
             THEN NULL
        ELSE CLEAN_UP_NPI_CODE(clm.facility_npi)
    END                                                                    AS facilitynpi,
    ----------------------------------------------------------------------------------------------------------------------
    -- facilityaddress1
    ----------------------------------------------------------------------------------------------------------------------  
	CASE
	    WHEN COALESCE(clm.claim_type_code, 'X') NOT IN ('I', 'P')
	         THEN NULL
        WHEN clm.claim_type_code = 'P'
         AND LPAD(sln.place_service, 2, '0') IN ('05', '06', '07', '08', '09', '12', '13', '14', '33', '99')
             THEN NULL
        WHEN clm.claim_type_code = 'I'
         AND SUBSTR(COALESCE(clm.type_bill, 'X'), 1, 1) IN ('3', 'X')
             THEN NULL
        ELSE clm.facility_adr_line1
	END                                                                  AS facilityaddress1,
    ----------------------------------------------------------------------------------------------------------------------
    -- facilityaddress2
    ----------------------------------------------------------------------------------------------------------------------  
	CASE
	    WHEN COALESCE(clm.claim_type_code, 'X') NOT IN ('I', 'P')
	         THEN NULL
        WHEN clm.claim_type_code = 'P'
         AND LPAD(sln.place_service, 2, '0') IN ('05', '06', '07', '08', '09', '12', '13', '14', '33', '99')
             THEN NULL
        WHEN clm.claim_type_code = 'I'
         AND SUBSTR(COALESCE(clm.type_bill, 'X'), 1, 1) IN ('3', 'X')
             THEN NULL
        ELSE clm.facility_adr_line2
	END                                                                  AS facilityaddress2,
    ----------------------------------------------------------------------------------------------------------------------
    -- facilitycity
    ----------------------------------------------------------------------------------------------------------------------  
	CASE
	    WHEN COALESCE(clm.claim_type_code, 'X') NOT IN ('I', 'P')
	         THEN NULL
        WHEN clm.claim_type_code = 'P'
         AND LPAD(sln.place_service, 2, '0') IN ('05', '06', '07', '08', '09', '12', '13', '14', '33', '99')
             THEN NULL
        WHEN clm.claim_type_code = 'I'
         AND SUBSTR(COALESCE(clm.type_bill, 'X'), 1, 1) IN ('3', 'X')
             THEN NULL
        ELSE clm.facility_adr_city
	END	                                                                  AS facilitycity,
    ----------------------------------------------------------------------------------------------------------------------
    -- facilitystate
    ----------------------------------------------------------------------------------------------------------------------  
    CASE
        WHEN COALESCE(clm.claim_type_code, 'X') NOT IN ('I', 'P')
        	 THEN NULL
        WHEN clm.claim_type_code = 'P'
            AND LPAD(sln.place_service, 2, '0') IN ('05', '06', '07', '08', '09', '12', '13', '14', '33', '99')
            THEN NULL
        WHEN clm.claim_type_code = 'I'
            AND SUBSTR(COALESCE(clm.type_bill, 'X'), 1, 1) IN ('3', 'X')
            THEN NULL
        ELSE VALIDATE_STATE_CODE(clm.facility_adr_state)
    END                                                                 AS facilitystate,
    ----------------------------------------------------------------------------------------------------------------------
    -- facilityzip
    ----------------------------------------------------------------------------------------------------------------------  
  	CASE
	    WHEN COALESCE(clm.claim_type_code, 'X') NOT IN ('I', 'P')
	         THEN NULL
        WHEN clm.claim_type_code = 'P'
         AND LPAD(sln.place_service, 2, '0') IN ('05', '06', '07', '08', '09', '12', '13', '14', '33', '99')
             THEN NULL
        WHEN clm.claim_type_code = 'I'
         AND SUBSTR(COALESCE(clm.type_bill, 'X'), 1, 1) IN ('3', 'X')
             THEN NULL
        ELSE MASK_ZIP_CODE(clm.facility_adr_zip)
	END	                                                                   AS facilityzip,
    ----------------------------------------------------------------------------------------------------------------------
    -- statementstartdate
    ----------------------------------------------------------------------------------------------------------------------  
    COALESCE
    (
        CAST(EXTRACT_DATE(clm.statement_from,        '%Y%m%d' ) AS DATE),
        CAST(EXTRACT_DATE(sln_date.min_service_from,      '%Y%m%d' ) AS DATE)
    )                                                                      AS statementstartdate,
    ----------------------------------------------------------------------------------------------------------------------
    -- statementenddate
    ----------------------------------------------------------------------------------------------------------------------  
    COALESCE
    (
        CAST(EXTRACT_DATE(clm.statement_to,        '%Y%m%d' ) AS DATE),
        CAST(EXTRACT_DATE(sln_date.max_service_to,      '%Y%m%d' ) AS DATE)
    )                                                                      AS statementenddate,
    ----------------------------------------------------------------------------------------------------------------------
    -- claimcharges
    ----------------------------------------------------------------------------------------------------------------------  
    CAST(clm.total_charge AS FLOAT)                          AS claimcharges,
    ----------------------------------------------------------------------------------------------------------------------
    -- billtypecode
    ----------------------------------------------------------------------------------------------------------------------
    CASE 
	    WHEN clm.type_bill IS NULL
	         THEN NULL
	    WHEN COALESCE(clm.claim_type_code, 'X') <> 'I'
	         THEN NULL
	    WHEN SUBSTR(clm.type_bill, 1, 1) = '3'
	         THEN CONCAT('X', SUBSTR(clm.type_bill, 2)) 
	    ELSE clm.type_bill
	END                                                  AS billtypecode,
    ----------------------------------------------------------------------------------------------------------------------
    -- claimfilingindicatorcode
    ----------------------------------------------------------------------------------------------------------------------
    clm.type_coverage                                                              AS claimfilingindicatorcode,
    ----------------------------------------------------------------------------------------------------------------------
    -- accidentrelatedindicator
    ----------------------------------------------------------------------------------------------------------------------
   CASE 
        WHEN SUBSTR(UPPER(COALESCE(clm.accident_related,'')), 1, 1) IN ('Y', 'N')  THEN SUBSTR(UPPER(clm.accident_related), 1, 1)
        ELSE 'N' 
    END                                                                         AS accidentrelatedindicator,
    ----------------------------------------------------------------------------------------------------------------------
    -- admissiondate
    ----------------------------------------------------------------------------------------------------------------------
    CAST(EXTRACT_DATE(clm.admission_date, '%Y%m%d' )  AS DATE)                  AS admissiondate,
    clm.admit_type_code                                                         AS admissiontypecode,
    clm.admit_src_code                                                          AS admissionsourcecode,
    clm.patient_status_cd                                                       AS patientstatuscode,
    ----------------------------------------------------------------------------------------------------------------------
    -- drg
    ----------------------------------------------------------------------------------------------------------------------
    CASE
	    WHEN clm.drg_code IN ('283', '284', '285', '789')
	         THEN NULL
	    ELSE  CLEAN_UP_NUMERIC_CODE(clm.drg_code)
	END                                                                         AS drg,    
    ----------------------------------------------------------------------------------------------------------------------
    -- icdprocedurecode1
    ----------------------------------------------------------------------------------------------------------------------  
    CLEAN_UP_ALPHANUMERIC_CODE
        (
            UPPER(clm.principal_procedure)
        )   
                                                                                AS icdprocedurecode1,
    ----------------------------------------------------------------------------------------------------------------------
    -- admittingdiagnosiscode
    ----------------------------------------------------------------------------------------------------------------------  
    CLEAN_UP_DIAGNOSIS_CODE
        (
            clm.admit_diagnosis, 
            CASE
                WHEN clm.coding_type = '9' THEN '01'
                WHEN clm.coding_type = 'X' THEN '02'
                ELSE NULL
            END,
            COALESCE(clm.statement_from,sln.service_from)
        )                                                                       AS admittingdiagnosiscode,
    ----------------------------------------------------------------------------------------------------------------------
    -- principaldiagnosiscode
    ----------------------------------------------------------------------------------------------------------------------  
    CLEAN_UP_DIAGNOSIS_CODE
        (
            clm.primary_diagnosis,
            CASE
                WHEN clm.coding_type = '9' THEN '01'
                WHEN clm.coding_type = 'X' THEN '02'
                ELSE NULL
            END,
            COALESCE(clm.statement_from, sln.service_from)
        )                                                                       AS principaldiagnosiscode,
    ----------------------------------------------------------------------------------------------------------------------
    -- secondarydiagnosiscode1 - 24
    ----------------------------------------------------------------------------------------------------------------------  
    CLEAN_UP_DIAGNOSIS_CODE
        (
            clm.diagnosis_code_2,
            CASE
                WHEN clm.coding_type = '9' THEN '01'
                WHEN clm.coding_type = 'X' THEN '02'
                ELSE NULL
            END,
            COALESCE(clm.statement_from, sln.service_from)
        )                                                                       AS secondarydiagnosiscode1,
    CLEAN_UP_DIAGNOSIS_CODE
        (
            clm.diagnosis_code_3,
            CASE
                WHEN clm.coding_type = '9' THEN '01'
                WHEN clm.coding_type = 'X' THEN '02'
                ELSE NULL
            END,
            COALESCE(clm.statement_from, sln.service_from)
        )                                                                       AS secondarydiagnosiscode2,
    CLEAN_UP_DIAGNOSIS_CODE
        (
            clm.diagnosis_code_4,
            CASE
                WHEN clm.coding_type = '9' THEN '01'
                WHEN clm.coding_type = 'X' THEN '02'
                ELSE NULL
            END,
            COALESCE(clm.statement_from, sln.service_from)
        )                                                                       AS secondarydiagnosiscode3,
    CLEAN_UP_DIAGNOSIS_CODE
        (
            clm.diagnosis_code_5,
            CASE
                WHEN clm.coding_type = '9' THEN '01'
                WHEN clm.coding_type = 'X' THEN '02'
                ELSE NULL
            END,
            COALESCE(clm.statement_from, sln.service_from)
        )                                                                       AS secondarydiagnosiscode4,
    CLEAN_UP_DIAGNOSIS_CODE
        (
            clm.diagnosis_code_6,
            CASE
                WHEN clm.coding_type = '9' THEN '01'
                WHEN clm.coding_type = 'X' THEN '02'
                ELSE NULL
            END,
            COALESCE(clm.statement_from, sln.service_from)
        )                                                                       AS secondarydiagnosiscode5,
    CLEAN_UP_DIAGNOSIS_CODE
        (
            clm.diagnosis_code_7,
            CASE
                WHEN clm.coding_type = '9' THEN '01'
                WHEN clm.coding_type = 'X' THEN '02'
                ELSE NULL
            END,
            COALESCE(clm.statement_from, sln.service_from)
        )                                                                       AS secondarydiagnosiscode6,
    CLEAN_UP_DIAGNOSIS_CODE
        (
            clm.diagnosis_code_8,
            CASE
                WHEN clm.coding_type = '9' THEN '01'
                WHEN clm.coding_type = 'X' THEN '02'
                ELSE NULL
            END,
            COALESCE(clm.statement_from, sln.service_from)
        )                                                                       AS secondarydiagnosiscode7,
    CAST(NULL AS STRING)                                                        AS secondarydiagnosiscode8,
    CAST(NULL AS STRING)                                                        AS secondarydiagnosiscode9,
    CAST(NULL AS STRING)                                                        AS secondarydiagnosiscode10,
    CAST(NULL AS STRING)                                                        AS secondarydiagnosiscode11,
    CAST(NULL AS STRING)                                                        AS secondarydiagnosiscode12,
    CAST(NULL AS STRING)                                                        AS secondarydiagnosiscode13,
    CAST(NULL AS STRING)                                                        AS secondarydiagnosiscode14,
    CAST(NULL AS STRING)                                                        AS secondarydiagnosiscode15,
    CAST(NULL AS STRING)                                                        AS secondarydiagnosiscode16,
    CAST(NULL AS STRING)                                                        AS secondarydiagnosiscode17,
    CAST(NULL AS STRING)                                                        AS secondarydiagnosiscode18,
    CAST(NULL AS STRING)                                                        AS secondarydiagnosiscode19,
    CAST(NULL AS STRING)                                                        AS secondarydiagnosiscode20,
    CAST(NULL AS STRING)                                                        AS secondarydiagnosiscode21,
    CAST(NULL AS STRING)                                                        AS secondarydiagnosiscode22,
    CAST(NULL AS STRING)                                                        AS secondarydiagnosiscode23,
    CAST(NULL AS STRING)                                                        AS secondarydiagnosiscode24,
    ----------------------------------------------------------------------------------------------------------------------
    -- icdprocedurecode2 - 25
    ----------------------------------------------------------------------------------------------------------------------  
    CLEAN_UP_ALPHANUMERIC_CODE
        (
            UPPER(clm.other_proc_code_2)
        )                                                                       AS icdprocedurecode2,
    CLEAN_UP_ALPHANUMERIC_CODE
        (
            UPPER(clm.other_proc_code_3)
        )                                                                       AS icdprocedurecode3,
    CLEAN_UP_ALPHANUMERIC_CODE
        (
            UPPER(clm.other_proc_code_4)
        )                                                                       AS icdprocedurecode4,
     CLEAN_UP_ALPHANUMERIC_CODE
        (
            UPPER(clm.other_proc_code_5)
        )                                                                       AS icdprocedurecode5,
    CLEAN_UP_ALPHANUMERIC_CODE
        (
            UPPER(clm.other_proc_code_6)
        )                                                                       AS icdprocedurecode6,
    CLEAN_UP_ALPHANUMERIC_CODE
        (
            UPPER(clm.other_proc_code_7)
        )                                                                       AS icdprocedurecode7,
    CLEAN_UP_ALPHANUMERIC_CODE
        (
            UPPER(clm.other_proc_code_8)
        )                                                                       AS icdprocedurecode8,
    CLEAN_UP_ALPHANUMERIC_CODE
        (
            UPPER(clm.other_proc_code_9)
        )                                                                      AS icdprocedurecode9,
    CLEAN_UP_ALPHANUMERIC_CODE
        (
            UPPER(clm.other_proc_code_10)
        )                                                                     AS icdprocedurecode10,
    CAST(NULL AS STRING)                                                        AS icdprocedurecode11,
    CAST(NULL AS STRING)                                                        AS icdprocedurecode12,
    CAST(NULL AS STRING)                                                        AS icdprocedurecode13,
    CAST(NULL AS STRING)                                                        AS icdprocedurecode14,
    CAST(NULL AS STRING)                                                        AS icdprocedurecode15,
    CAST(NULL AS STRING)                                                        AS icdprocedurecode16,
    CAST(NULL AS STRING)                                                        AS icdprocedurecode17,
    CAST(NULL AS STRING)                                                        AS icdprocedurecode18,
    CAST(NULL AS STRING)                                                        AS icdprocedurecode19,
    CAST(NULL AS STRING)                                                        AS icdprocedurecode20,
    CAST(NULL AS STRING)                                                        AS icdprocedurecode21,
    CAST(NULL AS STRING)                                                        AS icdprocedurecode22,
    CAST(NULL AS STRING)                                                        AS icdprocedurecode23,
    CAST(NULL AS STRING)                                                        AS icdprocedurecode24,
    CAST(NULL AS STRING)                                                        AS icdprocedurecode25,    
    ----------------------------------------------------------------------------------------------------------------------
    -- payer
    ----------------------------------------------------------------------------------------------------------------------  
    clm.payer_id                                                                AS payerid,
    clm.payer_name                                                              AS payername, 
    ----------------------------------------------------------------------------------------------------------------------
    -- codingtype
    ----------------------------------------------------------------------------------------------------------------------  
    CASE
        WHEN clm.coding_type = '9' THEN 'ICD9'
        WHEN clm.coding_type = 'X' THEN 'ICD10' 
        ELSE clm.coding_type
    END                                                                         AS codingtype,
    ----------------------------------------------------------------------------------------------------------------------
    -- sourcefilename
    ----------------------------------------------------------------------------------------------------------------------  
     clm.data_set                                                               AS sourcefilename,
    ----------------------------------------------------------------------------------------------------------------------
    --  data_vendor
    ----------------------------------------------------------------------------------------------------------------------  
    CASE
        WHEN clm.claim_type_code = 'I' THEN 'NelsonInst' 
        WHEN clm.claim_type_code = 'P' THEN 'NelsonProf'
    END                                                                   AS data_vendor,
    ----------------------------------------------------------------------------------------------------------------------
    --  dhcreceiveddate
    ----------------------------------------------------------------------------------------------------------------------  
    CAST(NULL AS STRING)                                                 AS dhcreceiveddate,
    ----------------------------------------------------------------------------------------------------------------------
    -- rownumber
    ----------------------------------------------------------------------------------------------------------------------  
    CAST(NULL AS STRING)                                                    AS rownumber,
    ----------------------------------------------------------------------------------------------------------------------
    -- file
    ----------------------------------------------------------------------------------------------------------------------  
    CAST(NULL AS INT)                                                  AS fileyear,
    CAST(NULL AS INT)                                                  AS filemonth,
    CAST(NULL AS INT)                                                  AS fileday,
    ----------------------------------------------------------------------------------------------------------------------
    -- HV columns
    ---------------------------------------------------------------------------------------------------------------------- 
    '24'                                                                  AS data_feed,
    'navicure'                                                            AS part_provider,
    CASE
        WHEN     
            COALESCE
            (
                CAST(EXTRACT_DATE(clm.statement_from,        '%Y%m%d' ) AS DATE),
                CAST(EXTRACT_DATE(sln_date.min_service_from, '%Y%m%d' ) AS DATE)
               
            ) IS NULL THEN '0_PREDATES_HVM_HISTORY'
        ELSE 
            COALESCE
            (
                CONCAT
                    (
                      SUBSTR(clm.statement_from, 1, 4), '-',
                      SUBSTR(clm.statement_from, 5, 2), '-01'
                    ),
                CONCAT
                    (
                      SUBSTR(sln_date.min_service_from, 1, 4), '-',
                      SUBSTR(sln_date.min_service_from, 5, 2), '-01'
                    )
            )
    END                                                                    AS part_best_date

FROM waystar_dedup_claims clm
 /* Link to the first service line for place of service. */
 LEFT OUTER JOIN waystar_dedup_lines sln
   ON clm.claim_number = sln.claim_number AND sln.line_number = '1'
 LEFT OUTER JOIN matching_payload pay
   ON clm.hvjoinkey = pay.hvjoinkey
  /* Deduplicate the source name columns without trimming and nullifying. */
 /* The source columns sometimes contain trailing blanks (1) and leading */
 /* blanks (2) that are part of the full provider name. */
 LEFT OUTER JOIN
    (
        SELECT DISTINCT
            claim_number,
            attending_name1,
            attending_name2,
            billing_name1,
            billing_name2,
            referring_name1,
            referring_name2,
            facility_name1,
            facility_name2
         FROM claim
    ) clmnms
   ON clm.claim_number = clmnms.claim_number
 LEFT OUTER JOIN 
(
    SELECT sln_1.claim_number, MIN(sln_1.service_from) AS min_service_from, MAX( sln_1.service_to) AS max_service_to
    FROM waystar_dedup_lines sln_1
    GROUP BY sln_1.claim_number
) sln_date  ON clm.claim_number = sln_date.claim_number
WHERE COALESCE(claim_type_code, 'X') in ('I', 'P')
/* Eliminate claims loaded from Navicure source data. */
  AND NOT EXISTS
    (
        SELECT 1
         FROM waystar_medicalclaims_augment_comb aug
        WHERE clm.claim_number = aug.instanceid
    )
ORDER BY clm.claim_number
