SELECT 
    --------------------------------------------------------------------------------------------------
    --  hv_clin_obsn_id
    --------------------------------------------------------------------------------------------------
    CONCAT('241_', rhe_ltft.encounter_id)														AS hv_clin_obsn_id,
    CURRENT_DATE()                                                                          AS crt_dt,
	'11'                                                                                    AS mdl_vrsn_num,
    SPLIT(rhe_ltft.input_file_name, '/')[SIZE(SPLIT(rhe_ltft.input_file_name, '/')) - 1]            AS data_set_nm,
	1801                                                                                    AS hvm_vdr_id,
	241                                                                                     AS hvm_vdr_feed_id,
	rhe_ltft.practice_id                                                                        AS vdr_org_id,
	rhe_ltft.encounter_id												        				AS vdr_clin_obsn_id,
	CASE
	    WHEN rhe_ltft.encounter_id IS NOT NULL THEN 'ENCOUNTER_ID'
	    ELSE NULL
	END																		                AS vdr_clin_obsn_id_qual,

    --------------------------------------------------------------------------------------------------
    --  hvid
    --------------------------------------------------------------------------------------------------
    CASE
        WHEN pay.hvid           IS NOT NULL THEN pay.hvid
        WHEN pay.personid       IS NOT NULL THEN CONCAT('241_' , pay.personid)
        ELSE NULL
    END																			            AS hvid,
    COALESCE(pln.date_of_birth, pay.yearofbirth)                                            AS ptnt_birth_yr,
    CASE
        WHEN pln.sex IN ('F', 'M', 'U') THEN pln.sex
        ELSE 'U'
    END                                                                                     AS ptnt_gender_cd,
    VALIDATE_STATE_CODE(UPPER(COALESCE(pln.state, pay.state)))                              AS ptnt_state_cd,
    --------------------------------------------------------------------------------------------------
    --  ptnt_zip3_cd
    --------------------------------------------------------------------------------------------------
 	MASK_ZIP_CODE(SUBSTR(COALESCE(pln.zip, pay.threedigitzip),1,3))                         AS ptnt_zip3_cd,
    CONCAT('241_', rhe_ltft.encounter_id)                                                       AS hv_enc_id,
	CAP_DATE
	    (
            CAST(EXTRACT_DATE(SUBSTR(enc.encounter_date_time, 1, 10), '%Y-%m-%d') AS DATE),
            CAST('{EARLIEST_SERVICE_DATE}' AS DATE),
            CAST('{VDR_FILE_DT}' AS DATE)
	    )																					AS enc_dt,
	CAP_DATE
	    (
            CAST(EXTRACT_DATE(SUBSTR(enc.encounter_date_time, 1, 10), '%Y-%m-%d') AS DATE),
            CAST('{EARLIEST_SERVICE_DATE}' AS DATE),
            CAST('{VDR_FILE_DT}' AS DATE)
	    )																					AS clin_obsn_dt,
    CLEAN_UP_NPI_CODE(prov.provider_npi)                                                    AS clin_obsn_prov_npi,

    CASE
        WHEN prov.provider_npi IS NULL THEN NULL
        ELSE 'BILLING_PROVIDER_NPI'
    END                                                                                     AS clin_obsn_prov_qual,
    enc.encounter_rendering_provider_id                                                     AS clin_obsn_prov_vdr_id,

    CASE
        WHEN enc.encounter_rendering_provider_id IS NULL THEN NULL
        ELSE 'RENDERING_PROVIDER_ID'
    END                                                                                     AS clin_obsn_prov_vdr_id_qual,

    CASE
        WHEN prov.provider_local_specialty_code NOT IN ('Provider local specialty code')
            THEN prov.provider_local_specialty_code
    END                                                                                     AS clin_obsn_prov_mdcr_speclty_cd,

    CASE
        WHEN prov.provider_type                     NOT IN ('Provider Type', 'null')
            AND prov.provider_local_specialty_name  NOT IN ('Provider local specialty name', 'null')
            THEN COALESCE(prov.provider_type, prov.provider_local_specialty_name)
        WHEN prov.provider_type                     NOT IN ('Provider Type', 'null')
            AND prov.provider_local_specialty_name      IN ('Provider local specialty name', 'null')
            THEN prov.provider_type
        WHEN prov.provider_type                         IN ('Provider Type', 'null')
            AND prov.provider_local_specialty_name  NOT IN ('Provider local specialty name', 'null')
            THEN prov.provider_local_specialty_name
        ELSE NULL
    END                                                                                     AS clin_obsn_prov_alt_speclty_id,
    CASE
        WHEN prov.provider_type                     NOT IN ('Provider Type', 'null')
            OR prov.provider_local_specialty_name   NOT IN ('Provider local specialty name', 'null')
            THEN 'SPECIALTY_NAME'
    END                                                                                     AS clin_obsn_prov_alt_speclty_id_qual,
    MASK_ZIP_CODE(prov.zip_code)                                                            AS clin_obsn_prov_zip_cd,
    CAST(NULL AS STRING)                                                                    AS clin_obsn_onset_dt,
    CAST(NULL AS STRING)                                                                    AS clin_obsn_resltn_dt,

-- Explode clin_obsn_typ_cd
    CASE
        WHEN clin_obsn_typ_cd_explode.n =	0	THEN "left_foot_ip1_exam"
        WHEN clin_obsn_typ_cd_explode.n =	1	THEN "left_foot_ip2_exam"
        WHEN clin_obsn_typ_cd_explode.n =	2	THEN "left_foot_ip3_exam"
        WHEN clin_obsn_typ_cd_explode.n =	3	THEN "left_foot_ip4_exam"
        WHEN clin_obsn_typ_cd_explode.n =	4	THEN "left_foot_ip5_exam"
        WHEN clin_obsn_typ_cd_explode.n =	5	THEN "left_foot_dip2_exam"
        WHEN clin_obsn_typ_cd_explode.n =	6	THEN "left_foot_dip3_exam"
        WHEN clin_obsn_typ_cd_explode.n =	7	THEN "left_foot_dip4_exam"
        WHEN clin_obsn_typ_cd_explode.n =	8	THEN "left_foot_dip5_exam"
        WHEN clin_obsn_typ_cd_explode.n =	9	THEN "left_foot_mp1_exam"
        WHEN clin_obsn_typ_cd_explode.n =	10	THEN "left_foot_mp2_exam"
        WHEN clin_obsn_typ_cd_explode.n =	11	THEN "left_foot_mp3_exam"
        WHEN clin_obsn_typ_cd_explode.n =	12	THEN "left_foot_mp4_exam"
        WHEN clin_obsn_typ_cd_explode.n =	13	THEN "left_foot_mp5_exam"
    END                                                                                     AS clin_obsn_typ_cd,

    CAST(NULL AS STRING)                                                                    AS clin_obsn_alt_cd,
    CAST(NULL AS STRING)                                                                    AS clin_obsn_alt_cd_qual,
    CAST(NULL AS STRING)                                                                    AS clin_obsn_msrmt,
    CAST(NULL AS STRING)                                                                    AS clin_obsn_uom,
-- There are no "findings" columns like there are in joint_exam3 that correlate with whether the pain and swelling columns are null
ARRAY
                (

                    CONCAT
                        (
                            'SWELLING: ',
                                CASE
                                    WHEN UPPER(rhe_ltft.swell_ip1) IN ('0') THEN 'N'
                                    WHEN UPPER(rhe_ltft.swell_ip1) IN ('1') THEN 'Y'
                                    WHEN UPPER(rhe_ltft.swell_ip1) IN ('SWELL_IP1', NULL) THEN NULL
                                END
                            , ' | ',
                            'PAIN: ',
                                CASE
                                    WHEN UPPER(rhe_ltft.pain_ip1) IN ('0') THEN 'N'
                                    WHEN UPPER(rhe_ltft.pain_ip1) IN ('1') THEN 'Y'
                                    WHEN UPPER(rhe_ltft.pain_ip1) IN ('PAIN_IP1', NULL) THEN NULL
                                END
                        ) ,
                      CONCAT
                        (
                            'SWELLING: ',
                                CASE
                                    WHEN UPPER(rhe_ltft.swell_ip2) IN ('0') THEN 'N'
                                    WHEN UPPER(rhe_ltft.swell_ip2) IN ('1') THEN 'Y'
                                    WHEN UPPER(rhe_ltft.swell_ip2) IN ('SWELL_IP2', NULL) THEN NULL
                                END
                            , ' | ',
                            'PAIN: ',
                                CASE
                                    WHEN UPPER(rhe_ltft.pain_ip2) IN ('0') THEN 'N'
                                    WHEN UPPER(rhe_ltft.pain_ip2) IN ('1') THEN 'Y'
                                    WHEN UPPER(rhe_ltft.pain_ip2) IN ('PAIN_IP2', NULL) THEN NULL
                                END
                        ) ,
                      CONCAT
                        (
                            'SWELLING: ',
                                CASE
                                    WHEN UPPER(rhe_ltft.swell_ip3) IN ('0') THEN 'N'
                                    WHEN UPPER(rhe_ltft.swell_ip3) IN ('1') THEN 'Y'
                                    WHEN UPPER(rhe_ltft.swell_ip3) IN ('SWELL_IP3', NULL) THEN NULL
                                END
                            , ' | ',
                            'PAIN: ',
                                CASE
                                    WHEN UPPER(rhe_ltft.pain_ip3) IN ('0') THEN 'N'
                                    WHEN UPPER(rhe_ltft.pain_ip3) IN ('1') THEN 'Y'
                                    WHEN UPPER(rhe_ltft.pain_ip3) IN ('PAIN_IP3', NULL) THEN NULL
                                END
                        ) ,
                      CONCAT
                        (
                            'SWELLING: ',
                                CASE
                                    WHEN UPPER(rhe_ltft.swell_ip4) IN ('0') THEN 'N'
                                    WHEN UPPER(rhe_ltft.swell_ip4) IN ('1') THEN 'Y'
                                    WHEN UPPER(rhe_ltft.swell_ip4) IN ('SWELL_IP4', NULL) THEN NULL
                                END
                            , ' | ',
                            'PAIN: ',
                                CASE
                                    WHEN UPPER(rhe_ltft.pain_ip4) IN ('0') THEN 'N'
                                    WHEN UPPER(rhe_ltft.pain_ip4) IN ('1') THEN 'Y'
                                    WHEN UPPER(rhe_ltft.pain_ip4) IN ('PAIN_IP4', NULL) THEN NULL
                                END
                        ) ,
                      CONCAT
                        (
                            'SWELLING: ',
                                CASE
                                    WHEN UPPER(rhe_ltft.swell_ip5) IN ('0') THEN 'N'
                                    WHEN UPPER(rhe_ltft.swell_ip5) IN ('1') THEN 'Y'
                                    WHEN UPPER(rhe_ltft.swell_ip5) IN ('SWELL_IP5', NULL) THEN NULL
                                END
                            , ' | ',
                            'PAIN: ',
                                CASE
                                    WHEN UPPER(rhe_ltft.pain_ip5) IN ('0') THEN 'N'
                                    WHEN UPPER(rhe_ltft.pain_ip5) IN ('1') THEN 'Y'
                                    WHEN UPPER(rhe_ltft.pain_ip5) IN ('PAIN_IP5', NULL) THEN NULL
                                END
                        ) ,
                      CONCAT
                        (
                            'SWELLING: ',
                                CASE
                                    WHEN UPPER(rhe_ltft.swell_dip2) IN ('0') THEN 'N'
                                    WHEN UPPER(rhe_ltft.swell_dip2) IN ('1') THEN 'Y'
                                    WHEN UPPER(rhe_ltft.swell_dip2) IN ('SWELL_DIP2', NULL) THEN NULL
                                END
                            , ' | ',
                            'PAIN: ',
                                CASE
                                    WHEN UPPER(rhe_ltft.pain_dip2) IN ('0') THEN 'N'
                                    WHEN UPPER(rhe_ltft.pain_dip2) IN ('1') THEN 'Y'
                                    WHEN UPPER(rhe_ltft.pain_dip2) IN ('PAIN_DIP2', NULL) THEN NULL
                                END
                        ) ,
                      CONCAT
                        (
                            'SWELLING: ',
                                CASE
                                    WHEN UPPER(rhe_ltft.swell_dip3) IN ('0') THEN 'N'
                                    WHEN UPPER(rhe_ltft.swell_dip3) IN ('1') THEN 'Y'
                                    WHEN UPPER(rhe_ltft.swell_dip3) IN ('SWELL_DIP3', NULL) THEN NULL
                                END
                            , ' | ',
                            'PAIN: ',
                                CASE
                                    WHEN UPPER(rhe_ltft.pain_dip3) IN ('0') THEN 'N'
                                    WHEN UPPER(rhe_ltft.pain_dip3) IN ('1') THEN 'Y'
                                    WHEN UPPER(rhe_ltft.pain_dip3) IN ('PAIN_DIP3', NULL) THEN NULL
                                END
                        ) ,
                      CONCAT
                        (
                            'SWELLING: ',
                                CASE
                                    WHEN UPPER(rhe_ltft.swell_dip4) IN ('0') THEN 'N'
                                    WHEN UPPER(rhe_ltft.swell_dip4) IN ('1') THEN 'Y'
                                    WHEN UPPER(rhe_ltft.swell_dip4) IN ('SWELL_DIP4', NULL) THEN NULL
                                END
                            , ' | ',
                            'PAIN: ',
                                CASE
                                    WHEN UPPER(rhe_ltft.pain_dip4) IN ('0') THEN 'N'
                                    WHEN UPPER(rhe_ltft.pain_dip4) IN ('1') THEN 'Y'
                                    WHEN UPPER(rhe_ltft.pain_dip4) IN ('PAIN_DIP4', NULL) THEN NULL
                                END
                        ) ,
                      CONCAT
                        (
                            'SWELLING: ',
                                CASE
                                    WHEN UPPER(rhe_ltft.swell_dip5) IN ('0') THEN 'N'
                                    WHEN UPPER(rhe_ltft.swell_dip5) IN ('1') THEN 'Y'
                                    WHEN UPPER(rhe_ltft.swell_dip5) IN ('SWELL_DIP5', NULL) THEN NULL
                                END
                            , ' | ',
                            'PAIN: ',
                                CASE
                                    WHEN UPPER(rhe_ltft.pain_dip5) IN ('0') THEN 'N'
                                    WHEN UPPER(rhe_ltft.pain_dip5) IN ('1') THEN 'Y'
                                    WHEN UPPER(rhe_ltft.pain_dip5) IN ('PAIN_DIP5', NULL) THEN NULL
                                END
                        ) ,
                    CONCAT
                        (
                            'SWELLING: ',
                                CASE
                                    WHEN UPPER(rhe_ltft.swell_mp1) IN ('0') THEN 'N'
                                    WHEN UPPER(rhe_ltft.swell_mp1) IN ('1') THEN 'Y'
                                    WHEN UPPER(rhe_ltft.swell_mp1) IN ('SWELL_MP1', NULL) THEN NULL
                                END
                            , ' | ',
                            'PAIN: ',
                                CASE
                                    WHEN UPPER(rhe_ltft.pain_mp1) IN ('0') THEN 'N'
                                    WHEN UPPER(rhe_ltft.pain_mp1) IN ('1') THEN 'Y'
                                    WHEN UPPER(rhe_ltft.pain_mp1) IN ('PAIN_MP1', NULL) THEN NULL
                                END
                        ) ,
                      CONCAT
                        (
                            'SWELLING: ',
                                CASE
                                    WHEN UPPER(rhe_ltft.swell_mp2) IN ('0') THEN 'N'
                                    WHEN UPPER(rhe_ltft.swell_mp2) IN ('1') THEN 'Y'
                                    WHEN UPPER(rhe_ltft.swell_mp2) IN ('SWELL_MP2', NULL) THEN NULL
                                END
                            , ' | ',
                            'PAIN: ',
                                CASE
                                    WHEN UPPER(rhe_ltft.pain_mp2) IN ('0') THEN 'N'
                                    WHEN UPPER(rhe_ltft.pain_mp2) IN ('1') THEN 'Y'
                                    WHEN UPPER(rhe_ltft.pain_mp2) IN ('PAIN_MP2', NULL) THEN NULL
                                END
                        ) ,
                      CONCAT
                        (
                            'SWELLING: ',
                                CASE
                                    WHEN UPPER(rhe_ltft.swell_mp3) IN ('0') THEN 'N'
                                    WHEN UPPER(rhe_ltft.swell_mp3) IN ('1') THEN 'Y'
                                    WHEN UPPER(rhe_ltft.swell_mp3) IN ('SWELL_MP3', NULL) THEN NULL
                                END
                            , ' | ',
                            'PAIN: ',
                                CASE
                                    WHEN UPPER(rhe_ltft.pain_mp3) IN ('0') THEN 'N'
                                    WHEN UPPER(rhe_ltft.pain_mp3) IN ('1') THEN 'Y'
                                    WHEN UPPER(rhe_ltft.pain_mp3) IN ('PAIN_MP3', NULL) THEN NULL
                                END
                        ) ,
                      CONCAT
                        (
                            'SWELLING: ',
                                CASE
                                    WHEN UPPER(rhe_ltft.swell_mp4) IN ('0') THEN 'N'
                                    WHEN UPPER(rhe_ltft.swell_mp4) IN ('1') THEN 'Y'
                                    WHEN UPPER(rhe_ltft.swell_mp4) IN ('SWELL_MP4', NULL) THEN NULL
                                END
                            , ' | ',
                            'PAIN: ',
                                CASE
                                    WHEN UPPER(rhe_ltft.pain_mp4) IN ('0') THEN 'N'
                                    WHEN UPPER(rhe_ltft.pain_mp4) IN ('1') THEN 'Y'
                                    WHEN UPPER(rhe_ltft.pain_mp4) IN ('PAIN_MP4', NULL) THEN NULL
                                END
                        ) ,
                      CONCAT
                        (
                            'SWELLING: ',
                                CASE
                                    WHEN UPPER(rhe_ltft.swell_mp5) IN ('0') THEN 'N'
                                    WHEN UPPER(rhe_ltft.swell_mp5) IN ('1') THEN 'Y'
                                    WHEN UPPER(rhe_ltft.swell_mp5) IN ('SWELL_MP5', NULL) THEN NULL
                                END
                            , ' | ',
                            'PAIN: ',
                                CASE
                                    WHEN UPPER(rhe_ltft.pain_mp5) IN ('0') THEN 'N'
                                    WHEN UPPER(rhe_ltft.pain_mp5) IN ('1') THEN 'Y'
                                    WHEN UPPER(rhe_ltft.pain_mp5) IN ('PAIN_MP5', NULL) THEN NULL
                                END
                        )
                )[clin_obsn_typ_cd_explode.n]                                                   AS clin_obsn_grp_txt,

    rhe_ltft.enterprise_id                                                                      AS data_src_cd,

    --------------------------------------------------------------------------------------------------
    --  data_captr_dt
    --------------------------------------------------------------------------------------------------
	CAP_DATE
	    (
            CAST(EXTRACT_DATE(SUBSTR(rhe_ltft.record_create_date_time, 1, 10), '%Y-%m-%d') AS DATE),
            CAST('{EARLIEST_SERVICE_DATE}' AS DATE),
            CAST('{VDR_FILE_DT}' AS DATE)
	    )																					AS data_captr_dt,
    'rhe_left_foot'                                                                         AS prmy_src_tbl_nm,
    '241'                                                                                   AS part_hvm_vdr_feed_id,
    --------------------------------------------------------------------------------------------------
    --  part_mth
    --------------------------------------------------------------------------------------------------
	CASE
	    WHEN CAP_DATE
        	    (
                    CAST(EXTRACT_DATE(SUBSTR(enc.encounter_date_time, 1, 10), '%Y-%m-%d') AS DATE),
                    CAST('{AVAILABLE_START_DATE}' AS DATE),
                    CAST('{VDR_FILE_DT}' AS DATE)
        	    )
                    IS NULL THEN '0_PREDATES_HVM_HISTORY'
	    ELSE SUBSTR(enc.encounter_date_time, 1, 7)
	END																					    AS part_mth

FROM rhe_ltft
LEFT OUTER JOIN encounters enc
                ON enc.encounter_id = rhe_ltft.encounter_id
LEFT OUTER JOIN provider prov
                ON prov.provider_id = enc.encounter_rendering_provider_id
LEFT OUTER JOIN plainout pln
                ON pln.person_id = rhe_ltft.patient_id
LEFT OUTER JOIN matching_payload pay
                ON pay.hvjoinkey = pln.hvjoinkey

-- Add Cross Join Array here
CROSS JOIN (SELECT EXPLODE(ARRAY(0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12,
                                13)) AS n) clin_obsn_typ_cd_explode
WHERE
----------- Patient response code explosion
    (
        ARRAY
            (
                rhe_ltft.pain_ip1,
                rhe_ltft.pain_ip2,
                rhe_ltft.pain_ip3,
                rhe_ltft.pain_ip4,
                rhe_ltft.pain_ip5,
                rhe_ltft.pain_dip2,
                rhe_ltft.pain_dip3,
                rhe_ltft.pain_dip4,
                rhe_ltft.pain_dip5,
                rhe_ltft.pain_mp1,
                rhe_ltft.pain_mp2,
                rhe_ltft.pain_mp3,
                rhe_ltft.pain_mp4,
                rhe_ltft.pain_mp5
            )[clin_obsn_typ_cd_explode.n] IS NOT NULL
    )
-- Remove header records
    AND TRIM(lower(COALESCE(rhe_ltft.patient_id, 'empty'))) <> 'patientid'