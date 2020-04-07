SELECT  /*+ BROADCAST (prv_rend), BROADCAST (prv_rend_psp), BROADCAST(prv_bill), BROADCAST(prv_bill_psp), BROADCAST(prv), BROADCAST(psp)
 */
    --- COALESECE was added in case there are NULL populated in service date or in provider id
    -- DISTINCT
    clm.hv_enc_id,
    clm.hvid AS hvid,
    CURRENT_DATE()                                                                          AS created,
	'10'                                                                                    AS model_version,
     CONCAT('{CHUNK}_', SPLIT(clm.input_file_name, '/')[SIZE(SPLIT(clm.input_file_name, '/')) - 1])         AS data_set,
	'176'                                                                                   AS data_feed,
	'543'                                                                                   AS data_vendor,
	/* patient_gender */
    CLEAN_UP_GENDER
        (
            CASE
                WHEN SUBSTR(UPPER(clm.gender)    , 1, 1) IN ('F', 'M') THEN SUBSTR(UPPER(clm.gender)    , 1, 1)
                ELSE 'U'
            END
        )                                                                                   AS patient_gender,
    CAP_AGE
        (
        VALIDATE_AGE
            (
                clm.age,
                CAST(clm.servicedate AS DATE),
                clm.yearofbirth
            )
        )                                                                                   AS patient_age,
    /* patient_year_of_birth */
    CAP_YEAR_OF_BIRTH
        (
            clm.age,
            CAST(clm.servicedate AS DATE),
            clm.yearofbirth
        )                                                                                   AS patient_year_of_birth,
      MASK_ZIP_CODE
        (
            clm.threedigitzip
        )                                                                                   AS patient_zip3,
    VALIDATE_STATE_CODE(UPPER(COALESCE(clm.state, '')))                      AS patient_state,
    CASE
        WHEN UPPER(clm.claimformtypecode)     IN ('I','P')     THEN UPPER(claimformtypecode)
        WHEN UPPER(clm.institutionaltypecode) IN ('I','O','U') THEN 'I'
        WHEN UPPER(clm.professionaltypecode)  IN ('I','O','U') then 'P'
        ELSE NULL
    END                                                                                     AS claim_type,
	CAP_DATE
        (
            CAST(clm.createddate                AS DATE),
            CAST('{EARLIEST_SERVICE_DATE}' AS DATE),
            CAST('{VDR_FILE_DT}' AS DATE)
        )                                                                                   AS date_received,    
	CAP_DATE
        (
            CAST(clm.servicedate                AS DATE),
            CAST('{EARLIEST_SERVICE_DATE}'  AS DATE),
            CAST('{VDR_FILE_DT}'  AS DATE)
        )                                                                                   AS date_service,    
	CAP_DATE
        (
            CAST(clm.servicethrudate            AS DATE),
            CAST('{EARLIEST_SERVICE_DATE}' AS DATE),
            CAST('{VDR_FILE_DT}' AS DATE)
        )                                                                                   AS date_service_end, 
  /* inst_discharge_status_std_id */
    CASE
        WHEN COALESCE(clm.ubpatientdischargestatuscode, '') IN ('20', '21', '40', '41', '42', '69', '87') 
            THEN NULL
        ELSE clm.ubpatientdischargestatuscode
    END                                                                                     AS inst_discharge_status_std_id,
    /* inst_type_of_bill_std_id */
    CASE 
        WHEN (UPPER(COALESCE(clm.claimformtypecode, '')) = 'I'  OR UPPER(clm.institutionaltypecode)  IN ('I','O','U') ) AND SUBSTR(clm.tob_code , 1, 1) = '3' 
            THEN CONCAT('X', SUBSTR(clm.tob_code, 2))
        WHEN (UPPER(COALESCE(clm.claimformtypecode, '')) = 'I'  OR UPPER(clm.institutionaltypecode)  IN ('I','O','U') )                                  
            THEN clm.tob_code 
        ELSE NULL
    END                                                                                     AS inst_type_of_bill_std_id,
    /* inst_drg_std_id */
     CASE 
         WHEN (UPPER(clm.claimformtypecode)    IN ('I')  OR UPPER(clm.institutionaltypecode)  IN ('I','O','U') )  
                AND clm.msdrg IN ('283', '284', '285', '789') THEN  NULL
         WHEN (UPPER(clm.claimformtypecode)    IN ('I')  OR UPPER(clm.institutionaltypecode)  IN ('I','O','U') )   THEN clm.msdrg  
         ELSE NULL
    END                                                                                     AS inst_drg_std_id,
    /* inst_drg_vendor_id */
    CASE 
         WHEN (UPPER(clm.claimformtypecode)    IN ('I')  OR UPPER(clm.institutionaltypecode)  IN ('I','O','U') )   THEN clm.apdrg  
         ELSE NULL
    END                                                                                     AS inst_drg_vendor_id,  
    /* inst_drg_vendor_desc */
    CASE 
         WHEN (UPPER(clm.claimformtypecode)    IN ('I')  OR UPPER(clm.institutionaltypecode)  IN ('I','O','U') ) AND clm.apdrg IS NOT NULL  THEN 'APDRG'  
         ELSE NULL
    END                                                                                     AS inst_drg_vendor_desc,  
    
    
    
     /* place_of_service_std_id (Place of Service is already derived in the 2.1 inovalon_01_dx_norm_base */
    CASE 
        WHEN  UPPER(COALESCE(clm.claimformtypecode, ''))    = 'P'             AND   LPAD(clm.pos_code, 2, 0) IN ('05', '06', '07', '08', '09', '12', '13', '14', '33','99')  THEN '99'
        WHEN  UPPER(COALESCE(clm.claimformtypecode, ''))    = 'P'                                                                                                            THEN LPAD(clm.pos_code, 2, 0) 
        WHEN  UPPER(COALESCE(clm.professionaltypecode, ''))  IN ('I','O','U') AND   LPAD(clm.pos_code, 2, 0) IN ('05', '06', '07', '08', '09', '12', '13', '14', '33','99')  THEN '99'
        WHEN  UPPER(COALESCE(clm.professionaltypecode, ''))  IN ('I','O','U')                                                                                                THEN LPAD(clm.pos_code, 2, 0) 
        ELSE NULL 
    END    	                                                                                AS place_of_service_std_id, 
    clm.claimuid	                                                                        AS service_line_id,
    CLEAN_UP_DIAGNOSIS_CODE
    (
        clm.diag_code,
        clm.diag_qualifier,
        CAST(clm.servicethrudate AS DATE)
    )
                                                                                            AS diagnosis_code, 
    CASE
        WHEN clm.diag_code IS NULL THEN NULL
    ELSE
        clm.diag_qualifier
    END                                                                                     AS diagnosis_code_qual,
    CASE 
        WHEN (UPPER(clm.claimformtypecode) = 'I'  OR UPPER(clm.institutionaltypecode)  IN ('I','O','U') )  AND admit_diag_code_fld  = 'Y'  THEN 'Y' 
        WHEN (UPPER(clm.claimformtypecode) = 'I'  OR UPPER(clm.institutionaltypecode)  IN ('I','O','U') )  AND admit_diag_code_fld  = 'N'  THEN 'N' 
        WHEN (UPPER(clm.claimformtypecode) = 'P'  OR UPPER(clm.professionaltypecode)   IN ('I','O','U') )                                  THEN NULL    --- Null for Professional  
        -- Null for Unknown claim type
    ELSE NULL
    END                                                                                     AS admit_diagnosis_ind,  
    CLEAN_UP_PROCEDURE_CODE(clm.proc_code)                                                  AS procedure_code,
    CASE
        WHEN clm.proc_code IS NULL THEN NULL
    ELSE
        'HC'
    END                                                                                     AS procedure_code_qual,
    clm.serviceunitquantity	                                                                AS procedure_units_billed,
    clm.proc_mod                                                                            AS procedure_modifier_1,
    clm.rev_code                                                                            AS revenue_code,
    clm.billedamount                                                                        AS line_charge,
    COALESCE(clm.allowedamount, clm.costamount)	                                            AS line_allowed,
    -----------------------------------------------------------------------------------------------------------------------------
    ------------------------------------ prov_rendering_npi ( Per new mapping default value of provideruid removed) 2020-03-20
    ----------------------------------------------------------------------------------------------------------------------------- 
    CASE 
        WHEN ( UPPER(COALESCE(clm.claimformtypecode, 'X')) = 'P' OR  UPPER(COALESCE(clm.professionaltypecode,'X'))  IN ('I','O','U') )
            AND LPAD(clm.pos_code, 2, 0) IN ('05', '06', '07', '08', '09', '12', '13', '14', '33','99') THEN NULL
        WHEN (UPPER(COALESCE(clm.claimformtypecode, 'X')) = 'I'  OR  UPPER(COALESCE(clm.institutionaltypecode,'X')) IN ('I','O','U') ) 
            AND SUBSTR(clm.tob_code , 1, 1) = '3'                                                            THEN NULL 
    ELSE COALESCE(clm.renderingprovidernpi, prv_rend.npi1)  
    END                                                                                     AS prov_rendering_npi,
    -----------------------------------------------------------------------------------------------------------------------------
    ------------------------------------ prov_billing_npi ( Per new mapping default value of provideruid is added)  2020-03-20
    -----------------------------------------------------------------------------------------------------------------------------
    CASE
        WHEN ( UPPER(COALESCE(clm.claimformtypecode, 'X')) = 'P' OR  UPPER(COALESCE(clm.professionaltypecode,'X'))  IN ('I','O','U') )
            AND LPAD(clm.pos_code, 2, 0) IN ('05', '06', '07', '08', '09', '12', '13', '14', '33','99') THEN NULL
        WHEN (UPPER(COALESCE(clm.claimformtypecode, 'X')) = 'I'  OR  UPPER(COALESCE(clm.institutionaltypecode,'X')) IN ('I','O','U') )
            AND SUBSTR(clm.tob_code , 1, 1) = '3'                                                            THEN NULL
    ELSE COALESCE(clm.billingprovidernpi, prv_bill.npi1, prv.npi1, psp.npinumber)
    END                                                                                     AS prov_billing_npi,
    --- provideruid removed 2020-03-27
    clm.renderingprovideruid                                                                AS prov_rendering_vendor_id,

    -----------------------------------------------------------------------------------------
    ------------------------------------ Name and address (Rendering Provider)
    ----------------------------------------------------------------------------------------- 
    CASE 
        WHEN ( UPPER(COALESCE(clm.claimformtypecode, 'X')) = 'P' OR  UPPER(COALESCE(clm.professionaltypecode,'X'))  IN ('I','O','U') )
            AND LPAD(clm.pos_code, 2, 0) IN ('05', '06', '07', '08', '09', '12', '13', '14', '33','99') THEN NULL
        WHEN (UPPER(COALESCE(clm.claimformtypecode, 'X')) = 'I'  OR  UPPER(COALESCE(clm.institutionaltypecode,'X')) IN ('I','O','U') ) 
            AND SUBSTR(clm.tob_code , 1, 1) = '3'                                                            THEN NULL 
    ELSE
        CASE
            WHEN prv_rend.companyname IS NOT NULL THEN prv_rend.companyname
            WHEN COALESCE(prv_rend.lastname, prv_rend.firstname, prv_rend.middlename) IS NOT NULL
            THEN        
            	SUBSTR
                (
                    CONCAT
                        (
                            CASE WHEN prv_rend.lastname   IS NULL THEN '' ELSE CONCAT(', ', prv_rend.lastname)   END, 
                            CASE WHEN prv_rend.firstname  IS NULL THEN '' ELSE CONCAT(', ', prv_rend.firstname)  END, 
                            CASE WHEN prv_rend.middlename IS NULL THEN '' ELSE CONCAT(', ', prv_rend.middlename) END
                        ), 3
                )
            WHEN prv_rend_psp.name IS NOT NULL THEN prv_rend_psp.name
            ELSE NULL
        END                                                                                     
    END                                                                                     AS prov_rendering_name_1,
            
    CASE 
        WHEN ( UPPER(COALESCE(clm.claimformtypecode, 'X')) = 'P' OR  UPPER(COALESCE(clm.professionaltypecode,'X'))  IN ('I','O','U') )
            AND LPAD(clm.pos_code, 2, 0) IN ('05', '06', '07', '08', '09', '12', '13', '14', '33','99') THEN NULL
        WHEN (UPPER(COALESCE(clm.claimformtypecode, 'X')) = 'I'  OR  UPPER(COALESCE(clm.institutionaltypecode,'X')) IN ('I','O','U') ) 
            AND SUBSTR(clm.tob_code , 1, 1) = '3'                                                            THEN NULL 
    ELSE
        CASE
            WHEN COALESCE(prv_rend.lastname, prv_rend.firstname, prv_rend.middlename, prv_rend.companyname) IS NOT NULL
                THEN prv_rend.primarypracticeaddress
            WHEN prv_rend_psp.name IS NOT NULL 
                THEN prv_rend_psp.address1
        END                                                                                     
    END                                                                                     AS prov_rendering_address_1,
    CASE 
        WHEN ( UPPER(COALESCE(clm.claimformtypecode, 'X')) = 'P' OR  UPPER(COALESCE(clm.professionaltypecode,'X'))  IN ('I','O','U') )
            AND LPAD(clm.pos_code, 2, 0) IN ('05', '06', '07', '08', '09', '12', '13', '14', '33','99') THEN NULL
        WHEN (UPPER(COALESCE(clm.claimformtypecode, 'X')) = 'I'  OR  UPPER(COALESCE(clm.institutionaltypecode,'X')) IN ('I','O','U') ) 
            AND SUBSTR(clm.tob_code , 1, 1) = '3'                                                            THEN NULL 
    ELSE
        CASE
            WHEN COALESCE(prv_rend.lastname, prv_rend.firstname, prv_rend.middlename, prv_rend.companyname) IS NOT NULL
                THEN prv_rend.secondarypracticeaddress
            WHEN prv_rend_psp.name IS NOT NULL 
                THEN prv_rend_psp.address2
        END                                                                                     
    END                                                                                     AS prov_rendering_address_2,
    CASE 
        WHEN ( UPPER(COALESCE(clm.claimformtypecode, 'X')) = 'P' OR  UPPER(COALESCE(clm.professionaltypecode,'X'))  IN ('I','O','U') )
            AND LPAD(clm.pos_code, 2, 0) IN ('05', '06', '07', '08', '09', '12', '13', '14', '33','99') THEN NULL
        WHEN (UPPER(COALESCE(clm.claimformtypecode, 'X')) = 'I'  OR  UPPER(COALESCE(clm.institutionaltypecode,'X')) IN ('I','O','U') ) 
            AND SUBSTR(clm.tob_code , 1, 1) = '3'                                                            THEN NULL 
    ELSE
        CASE
            WHEN COALESCE(prv_rend.lastname, prv_rend.firstname, prv_rend.middlename, prv_rend.companyname) IS NOT NULL
                THEN prv_rend.practicecity
            WHEN prv_rend_psp.name IS NOT NULL 
                THEN prv_rend_psp.city
        END
    END                                                                                     AS prov_rendering_city,
    CASE 
        WHEN ( UPPER(COALESCE(clm.claimformtypecode, 'X')) = 'P' OR  UPPER(COALESCE(clm.professionaltypecode,'X'))  IN ('I','O','U') )
            AND LPAD(clm.pos_code, 2, 0) IN ('05', '06', '07', '08', '09', '12', '13', '14', '33','99') THEN NULL
        WHEN (UPPER(COALESCE(clm.claimformtypecode, 'X')) = 'I'  OR  UPPER(COALESCE(clm.institutionaltypecode,'X')) IN ('I','O','U') ) 
            AND SUBSTR(clm.tob_code , 1, 1) = '3'                                                            THEN NULL 
    ELSE
        CASE
            WHEN COALESCE(prv_rend.lastname, prv_rend.firstname, prv_rend.middlename, prv_rend.companyname) IS NOT NULL
                THEN VALIDATE_STATE_CODE(prv_rend.practicestate)
            WHEN prv_rend_psp.name IS NOT NULL 
                THEN VALIDATE_STATE_CODE(prv_rend_psp.state)
        END                                                                                     
    END                                                                                     AS prov_rendering_state,
    CASE 
        WHEN ( UPPER(COALESCE(clm.claimformtypecode, 'X')) = 'P' OR  UPPER(COALESCE(clm.professionaltypecode,'X'))  IN ('I','O','U') )
            AND LPAD(clm.pos_code, 2, 0) IN ('05', '06', '07', '08', '09', '12', '13', '14', '33','99') THEN NULL
        WHEN (UPPER(COALESCE(clm.claimformtypecode, 'X')) = 'I'  OR  UPPER(COALESCE(clm.institutionaltypecode,'X')) IN ('I','O','U') ) 
            AND SUBSTR(clm.tob_code , 1, 1) = '3'                                                            THEN NULL 
    ELSE
        CASE
            WHEN COALESCE(prv_rend.lastname, prv_rend.firstname, prv_rend.middlename, prv_rend.companyname) IS NOT NULL
                THEN
                    CASE
                        WHEN prv_rend.practicezip IS NOT NULL AND prv_rend.practicezip4 IS NOT NULL
                            THEN CONCAT(COALESCE(prv_rend.practicezip,''),  ' - ', COALESCE(prv_rend.practicezip4,''))
                        WHEN prv_rend.practicezip IS NOT NULL AND prv_rend.practicezip4 IS  NULL
                            THEN prv_rend.practicezip
                        WHEN prv_rend.practicezip IS     NULL AND prv_rend.practicezip4 IS NOT NULL
                            THEN prv_rend.practicezip4
                    END
            WHEN prv_rend_psp.name IS NOT NULL 
                THEN prv_rend_psp.zip
        END                                                                                     
    END                                                                                     AS prov_rendering_zip,

-----------------------------------------------------------------------------------------
------------------------------------ Name and address (Billing Provider)
-----------------------------------------------------------------------------------------
   COALESCE(clm.billingprovideruid, clm.provideruid)                                       AS prov_billing_vendor_id,
   CASE
        WHEN ( UPPER(COALESCE(clm.claimformtypecode, 'X')) = 'P' OR  UPPER(COALESCE(clm.professionaltypecode,'X'))  IN ('I','O','U') )
            AND LPAD(clm.pos_code, 2, 0) IN ('05', '06', '07', '08', '09', '12', '13', '14', '33','99') THEN NULL
        WHEN (UPPER(COALESCE(clm.claimformtypecode, 'X')) = 'I'  OR  UPPER(COALESCE(clm.institutionaltypecode,'X')) IN ('I','O','U') ) 
            AND SUBSTR(clm.tob_code , 1, 1) = '3'                                                            THEN NULL 
    ELSE
        CASE
            WHEN prv_bill.companyname IS NOT NULL THEN prv_bill.companyname
            WHEN COALESCE(prv_bill.lastname, prv_bill.firstname, prv_bill.middlename) IS NOT NULL
            THEN        
            	SUBSTR
                (
                    CONCAT
                        (
                            CASE WHEN prv_bill.lastname   IS NULL THEN '' ELSE CONCAT(', ', prv_bill.lastname)   END, 
                            CASE WHEN prv_bill.firstname  IS NULL THEN '' ELSE CONCAT(', ', prv_bill.firstname)  END, 
                            CASE WHEN prv_bill.middlename IS NULL THEN '' ELSE CONCAT(', ', prv_bill.middlename) END
                        ), 3
                )
            WHEN prv.companyname IS NOT NULL THEN prv.companyname
            WHEN COALESCE(prv.lastname, prv.firstname, prv.middlename) IS NOT NULL
            THEN        
            	SUBSTR
                (
                    CONCAT
                        (
                            CASE WHEN prv.lastname   IS NULL THEN '' ELSE CONCAT(', ', prv.lastname)   END, 
                            CASE WHEN prv.firstname  IS NULL THEN '' ELSE CONCAT(', ', prv.firstname)  END, 
                            CASE WHEN prv.middlename IS NULL THEN '' ELSE CONCAT(', ', prv.middlename) END
                        ), 3
                )
            WHEN prv_bill_psp.name IS NOT NULL THEN prv_bill_psp.name
            ELSE NULL
        END                                                                                    
    END                                                                                     AS prov_billing_name_1,
   CASE 
        WHEN ( UPPER(COALESCE(clm.claimformtypecode, 'X')) = 'P' OR  UPPER(COALESCE(clm.professionaltypecode,'X'))  IN ('I','O','U') )
            AND LPAD(clm.pos_code, 2, 0) IN ('05', '06', '07', '08', '09', '12', '13', '14', '33','99') THEN NULL
        WHEN (UPPER(COALESCE(clm.claimformtypecode, 'X')) = 'I'  OR  UPPER(COALESCE(clm.institutionaltypecode,'X')) IN ('I','O','U') ) 
            AND SUBSTR(clm.tob_code , 1, 1) = '3'                                                            THEN NULL 
    ELSE
        CASE
            WHEN COALESCE(prv_bill.lastname, prv_bill.firstname, prv_bill.middlename, prv_bill.companyname) IS NOT NULL THEN prv_bill.primarybillingaddress
            WHEN COALESCE(prv.lastname, prv.firstname, prv.middlename, prv.companyname) IS NOT NULL                     THEN prv.primarybillingaddress            
            WHEN prv_bill_psp.name IS NOT NULL                                                                          THEN prv_bill_psp.address1
            ELSE NULL
        END                                                                                     
    END                                                                                     AS prov_billing_address_1,
 
    CASE 
        WHEN ( UPPER(COALESCE(clm.claimformtypecode, 'X')) = 'P' OR  UPPER(COALESCE(clm.professionaltypecode,'X'))  IN ('I','O','U') )
            AND LPAD(clm.pos_code, 2, 0) IN ('05', '06', '07', '08', '09', '12', '13', '14', '33','99') THEN NULL
        WHEN (UPPER(COALESCE(clm.claimformtypecode, 'X')) = 'I'  OR  UPPER(COALESCE(clm.institutionaltypecode,'X')) IN ('I','O','U') ) 
            AND SUBSTR(clm.tob_code , 1, 1) = '3'                                                            THEN NULL 
    ELSE
        CASE
            WHEN COALESCE(prv_bill.lastname, prv_bill.firstname, prv_bill.middlename, prv_bill.companyname) IS NOT NULL THEN prv_bill.secondarybillingaddress
            WHEN COALESCE(prv.lastname, prv.firstname, prv.middlename, prv.companyname) IS NOT NULL                     THEN prv.secondarybillingaddress            
            WHEN prv_bill_psp.name IS NOT NULL                                                                          THEN prv_bill_psp.address2
            ELSE NULL
        END                                                                                     
    END                                                                                     AS prov_billing_address_2,
   CASE 
        WHEN ( UPPER(COALESCE(clm.claimformtypecode, 'X')) = 'P' OR  UPPER(COALESCE(clm.professionaltypecode,'X'))  IN ('I','O','U') )
            AND LPAD(clm.pos_code, 2, 0) IN ('05', '06', '07', '08', '09', '12', '13', '14', '33','99') THEN NULL
        WHEN (UPPER(COALESCE(clm.claimformtypecode, 'X')) = 'I'  OR  UPPER(COALESCE(clm.institutionaltypecode,'X')) IN ('I','O','U') ) 
            AND SUBSTR(clm.tob_code , 1, 1) = '3'                                                            THEN NULL 
    ELSE
        CASE
            WHEN COALESCE(prv_bill.lastname, prv_bill.firstname, prv_bill.middlename, prv_bill.companyname) IS NOT NULL THEN prv_bill.billingcity
            WHEN COALESCE(prv.lastname, prv.firstname, prv.middlename, prv.companyname) IS NOT NULL                     THEN prv.billingcity            
            WHEN prv_bill_psp.name IS NOT NULL                                                                          THEN prv_bill_psp.city
            ELSE NULL
        END
    END                                                                                     AS prov_billing_city,    
    CASE 
        WHEN ( UPPER(COALESCE(clm.claimformtypecode, 'X')) = 'P' OR  UPPER(COALESCE(clm.professionaltypecode,'X'))  IN ('I','O','U') )
            AND LPAD(clm.pos_code, 2, 0) IN ('05', '06', '07', '08', '09', '12', '13', '14', '33','99') THEN NULL
        WHEN (UPPER(COALESCE(clm.claimformtypecode, 'X')) = 'I'  OR  UPPER(COALESCE(clm.institutionaltypecode,'X')) IN ('I','O','U') ) 
            AND SUBSTR(clm.tob_code , 1, 1) = '3'                                                            THEN NULL 
    ELSE
        CASE
            WHEN COALESCE(prv_bill.lastname, prv_bill.firstname, prv_bill.middlename, prv_bill.companyname) IS NOT NULL THEN VALIDATE_STATE_CODE(prv_bill.billingstate)
            WHEN COALESCE(prv.lastname, prv.firstname, prv.middlename, prv.companyname) IS NOT NULL                     THEN VALIDATE_STATE_CODE(prv.billingstate)
            WHEN prv_bill_psp.name IS NOT NULL                                                                          THEN VALIDATE_STATE_CODE(prv_bill_psp.state)
            ELSE NULL
        END                                                                                     
    END                                                                                     AS prov_billing_state,
   CASE 
        WHEN ( UPPER(COALESCE(clm.claimformtypecode, 'X')) = 'P' OR  UPPER(COALESCE(clm.professionaltypecode,'X'))  IN ('I','O','U') )
            AND LPAD(clm.pos_code, 2, 0) IN ('05', '06', '07', '08', '09', '12', '13', '14', '33','99') THEN NULL
        WHEN (UPPER(COALESCE(clm.claimformtypecode, 'X')) = 'I'  OR  UPPER(COALESCE(clm.institutionaltypecode,'X')) IN ('I','O','U') ) 
            AND SUBSTR(clm.tob_code , 1, 1) = '3'                                                            THEN NULL 
    ELSE
        CASE
            WHEN COALESCE(prv_bill.lastname, prv_bill.firstname, prv_bill.middlename, prv_bill.companyname) IS NOT NULL
                THEN
                    CASE
                        WHEN prv_bill.billingzip IS NOT NULL AND prv_bill.billingzip4 IS NOT NULL
                            THEN CONCAT(COALESCE(prv_bill.billingzip,''),  ' - ', COALESCE(prv_bill.billingzip4,''))
                        WHEN prv_bill.billingzip IS NOT NULL AND prv_bill.billingzip4 IS  NULL
                            THEN prv_bill.practicezip
                        WHEN prv_bill.billingzip IS     NULL AND prv_bill.billingzip4 IS NOT NULL
                            THEN prv_bill.billingzip4
                    END
            --------
             WHEN COALESCE(prv.lastname, prv.firstname, prv.middlename, prv.companyname) IS NOT NULL
                THEN
                    CASE
                        WHEN prv.billingzip IS NOT NULL AND prv.billingzip4 IS NOT NULL
                            THEN CONCAT(COALESCE(prv.billingzip,''),  ' - ', COALESCE(prv.billingzip4,''))
                        WHEN prv.billingzip IS NOT NULL AND prv.billingzip4 IS  NULL
                            THEN prv.practicezip
                        WHEN prv.billingzip IS     NULL AND prv.billingzip4 IS NOT NULL
                            THEN prv_bill.billingzip4
                    END           
            -------
            WHEN prv_bill_psp.name IS NOT NULL 
                THEN prv_bill_psp.zip
            ELSE NULL
        END                                                                                     
    END                                                                                     AS prov_billing_zip,
 
    CASE 
        WHEN clm.claimstatuscode = 'A' THEN 'ADJUSTMENT TO ORIGINAL CLAIM'
        WHEN clm.claimstatuscode = 'D' THEN 'DENIED CLAIMS'
        WHEN clm.claimstatuscode = 'I' THEN 'INITIAL PAY CLAIM'
        WHEN clm.claimstatuscode = 'P' THEN 'PENDED FOR ADJUDICATION'
        WHEN clm.claimstatuscode = 'R' THEN 'REVERSAL TO ORIGINAL CLAIM'
        WHEN clm.claimstatuscode = 'U' THEN 'UNKNOWN'
    ELSE NULL 
    END                                                                                    AS logical_delete_reason,
	'inovalon'                                                                             AS part_provider,
    /* part_best_date */
	CASE
	    WHEN 0 = LENGTH(COALESCE
	                            (
	                                CAP_DATE
                                        (
                                            CAST(clm.servicedate AS DATE),
                                            CAST(COALESCE('{AVAILABLE_START_DATE}', '{EARLIEST_SERVICE_DATE}') AS DATE),
                                            CAST('{VDR_FILE_DT}' AS DATE)
                                        ), 
                                    ''
                                ))
	        THEN '0_PREDATES_HVM_HISTORY'
	    ELSE CONCAT
	            (
                    SUBSTR(clm.servicedate, 1, 4), '-',
                    SUBSTR(clm.servicedate, 6, 2), '-01'
                )
	END          
	                                                                           AS part_best_date     
FROM inovalon_22_dx_norm_base clm
----------- Rendering
LEFT OUTER JOIN prv prv_rend ON COALESCE(clm.renderingprovideruid, CONCAT("NONE", clm.claimuid)) = prv_rend.provideruid
LEFT OUTER JOIN psp prv_rend_psp ON COALESCE(clm.renderingprovideruid, CONCAT("NONE", clm.claimuid)) = prv_rend_psp.provideruid
----------- Billing
LEFT OUTER JOIN prv prv_bill ON COALESCE(clm.billingprovideruid, CONCAT("NONE", clm.claimuid)) = prv_bill.provideruid
LEFT OUTER JOIN psp prv_bill_psp ON COALESCE(clm.billingprovideruid, CONCAT("NONE", clm.claimuid)) = prv_bill_psp.provideruid

----------- Provider 
LEFT OUTER JOIN prv ON COALESCE(clm.provideruid, CONCAT("NONE", clm.claimuid)) = prv.provideruid
LEFT OUTER JOIN psp ON clm.provideruid   = psp.provideruid




