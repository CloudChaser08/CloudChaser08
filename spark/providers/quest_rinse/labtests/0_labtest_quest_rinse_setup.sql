SELECT /*+ BROADCAST(ref_geo_state), BROADCAST(ptnt) */

    obfuscate_hvid(CASE
        WHEN pay.hvid           IS NOT NULL THEN pay.hvid 
        WHEN pay.patientid      IS NOT NULL THEN CONCAT('7_', pay.patientid)
        WHEN ptnt.pat_master_id IS NOT NULL THEN CONCAT('7_', ptnt.pat_master_id)
        ELSE NULL
    END, 'questrinse')                                                                      AS hvid,
	/* patient_gender */
	CLEAN_UP_GENDER
    	(
        	CASE
        	    WHEN SUBSTR(UPPER(ptnt.gender), 1, 1) IN ('F', 'M') THEN SUBSTR(UPPER(ptnt.gender), 1, 1)
        	    WHEN SUBSTR(UPPER(pay.gender ), 1, 1) IN ('F', 'M') THEN SUBSTR(UPPER(pay.gender ), 1, 1)
        	    ELSE 'U' 
        	END
	    )                                                                                   AS patient_gender,
    pay.yearofbirth,
    pay.age,

	MASK_ZIP_CODE(SUBSTR(pay.threedigitzip, 1, 3))                                          AS patient_zip3,
    VALIDATE_STATE_CODE(UPPER(COALESCE(ptnt.pat_state, pay.state)))                         AS patient_state,
    pay.state,

    ptnt.accession_number             AS  ptnt_accession_number   ,
    ptnt.date_of_service              AS  ptnt_date_of_service    ,
    ptnt.lab_code                     AS  ptnt_lab_code           ,
    ptnt.accn_enterprise_id           AS  accn_enterprise_id      ,
    ptnt.age_code                     AS  age_code                ,
    ptnt.species                      AS  species                 ,
    ptnt.pat_country                  AS  pat_country             ,
    ptnt.external_patient_id          AS  external_patient_id     ,
    ptnt.pat_master_id                AS  pat_master_id           ,
    ptnt.lab_reference_number         AS  lab_reference_number    ,
    ptnt.room_number                  AS  room_number             ,
    ptnt.bed_number                   AS  bed_number              ,
    ptnt.hospital_location            AS  hospital_location       ,
    ptnt.ward                         AS  ward                    ,
    ptnt.admission_date               AS  admission_date          ,
    ptnt.health_id                    AS  health_id               ,
    ptnt.pm_eid                       AS  pm_eid                  ,
    ptnt.idw_pm_email_address         AS  idw_pm_email_address,
    ptnt.unique_accession_id


FROM transactions ptnt
LEFT OUTER JOIN matching_payload pay ON ptnt.hvjoinkey = pay.hvJoinKey

WHERE EXISTS
/* Select only valid U.S. states and territories. */
    (
        SELECT 1
         FROM ref_geo_state
        WHERE UPPER(COALESCE(ptnt.pat_state, pay.state, '')) = ref_geo_state.geo_state_pstl_cd
    )

AND UPPER(COALESCE(SUBSTR(ptnt.pat_country,1,2),'US')) = 'US'

