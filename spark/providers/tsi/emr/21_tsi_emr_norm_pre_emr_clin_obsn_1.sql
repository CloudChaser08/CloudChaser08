SELECT
    --------------------------------------------------------------------------------------------------
    --  hv_clin_obsn_id
    --------------------------------------------------------------------------------------------------
    CONCAT('241_', jnt3.encounter_id)														AS hv_clin_obsn_id,
    CURRENT_DATE()                                                                          AS crt_dt,
	'11'                                                                                    AS mdl_vrsn_num,
    SPLIT(jnt3.input_file_name, '/')[SIZE(SPLIT(jnt3.input_file_name, '/')) - 1]            AS data_set_nm,
	1801                                                                                    AS hvm_vdr_id,
	241                                                                                     AS hvm_vdr_feed_id,
	jnt3.practice_id                                                                        AS vdr_org_id,
	jnt3.encounter_id												        				AS vdr_clin_obsn_id,
	CASE
	    WHEN jnt3.encounter_id IS NOT NULL THEN 'ENCOUNTER_ID'
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
    CONCAT('241_', jnt3.encounter_id)                                                       AS hv_enc_id,
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
        WHEN clin_obsn_typ_cd_explode.n = 0     THEN 'left_acromioclavicular_joint_exam'
        WHEN clin_obsn_typ_cd_explode.n = 1     THEN 'right_acromioclavicular_joint_exam'
        WHEN clin_obsn_typ_cd_explode.n = 2     THEN 'left_ankle_joint_exam'
        WHEN clin_obsn_typ_cd_explode.n = 3     THEN 'right_ankle_joint_exam'
        WHEN clin_obsn_typ_cd_explode.n = 4     THEN 'left_sternoclavicular_joint_exam'
        WHEN clin_obsn_typ_cd_explode.n = 5     THEN 'right_sternoclavicular_joint_exam'
        WHEN clin_obsn_typ_cd_explode.n = 6     THEN 'left_t_m_j_joint_exam'
        WHEN clin_obsn_typ_cd_explode.n = 7     THEN 'right_t_m_j_joint_exam'
        WHEN clin_obsn_typ_cd_explode.n = 8     THEN 'left_wrist_joint_exam'
        WHEN clin_obsn_typ_cd_explode.n = 9     THEN 'right_wrist_joint_exam'
        WHEN clin_obsn_typ_cd_explode.n = 10    THEN 'left_elbow_joint_exam'
        WHEN clin_obsn_typ_cd_explode.n = 11    THEN 'right_elbow_joint_exam'
        WHEN clin_obsn_typ_cd_explode.n = 12    THEN 'left_foot_joints_exam'
        WHEN clin_obsn_typ_cd_explode.n = 13    THEN 'right_foot_joints_exam'
        WHEN clin_obsn_typ_cd_explode.n = 14    THEN 'left_hand_joints_exam'
        WHEN clin_obsn_typ_cd_explode.n = 15    THEN 'right_hand_joints_exam'
        WHEN clin_obsn_typ_cd_explode.n = 16    THEN 'left_hip_joint_exam'
        WHEN clin_obsn_typ_cd_explode.n = 17    THEN 'right_hip_joint_exam'
        WHEN clin_obsn_typ_cd_explode.n = 18    THEN 'left_knee_joint_exam'
        WHEN clin_obsn_typ_cd_explode.n = 19    THEN 'right_knee_joint_exam'
        WHEN clin_obsn_typ_cd_explode.n = 20    THEN 'left_shoulder_joint_exam'
        WHEN clin_obsn_typ_cd_explode.n = 21    THEN 'right_shoulder_joint_exam'
    END                                                                                     AS clin_obsn_typ_cd,

    CAST(NULL AS STRING)                                                                    AS clin_obsn_alt_cd,
    CAST(NULL AS STRING)                                                                    AS clin_obsn_alt_cd_qual,

    CAST(NULL AS STRING)                                                                    AS clin_obsn_msrmt,
    CAST(NULL AS STRING)                                                                    AS clin_obsn_uom,

    CASE
        WHEN ARRAY  (
                    jnt3.left_acromioclavicular_joint_exam_findings,
                    jnt3.right_acromioclavicular_joint_exam_findings,
                    jnt3.left_ankle_joint_exam_findings,
                    jnt3.right_ankle_joint_exam_findings,
                    jnt3.left_sternoclavicular_joint_exam_findings,
                    jnt3.right_sternoclavicular_joint_exam_findings,
                    jnt3.left_t_m_j_joint_exam_findings,
                    jnt3.right_t_m_j_joint_exam_findings,
                    jnt3.left_wrist_joint_exam_findings,
                    jnt3.right_wrist_joint_exam_findings,
                    jnt3.left_elbow_joint_exam_findings,
                    jnt3.right_elbow_joint_exam_findings,
                    'left_foot_joints_exam_findings',
                    'right_foot_joints_exam_findings',
                    'left_hand_joints_exam_findings',
                    'right_hand_joints_exam',
                    jnt3.left_hip_joint_exam_findings,
                    jnt3.right_hip_joint_exam_findings,
                    jnt3.left_knee_joint_exam_findings,
                    jnt3.right_knee_joint_exam_findings,
                    jnt3.left_shoulder_joint_exam_findings,
                    jnt3.right_shoulder_joint_exam_findings
                    )[clin_obsn_typ_cd_explode.n] IS NULL       THEN NULL
        ELSE ARRAY
                (

                    CONCAT
                        (
                            'NORMAL: ',
                                CASE
                                    WHEN UPPER(jnt.left_acromioclavicular_joint_normal) IN ('0', 'N') THEN 'N'
                                    WHEN UPPER(jnt.left_acromioclavicular_joint_normal) IN ('1', 'Y') THEN 'Y'
                                    WHEN UPPER(jnt.left_acromioclavicular_joint_normal) IN ('LEFT ACROMIOCLAVICULAR JOINT NORMAL', NULL) THEN NULL
                                END
                            , ' | ',
                            'SWELLING: ',
                                CASE
                                    WHEN UPPER(jnt3.left_acromoclavicular_joint_swelling) IN ('0') THEN 'N'
                                    WHEN UPPER(jnt3.left_acromoclavicular_joint_swelling) IN ('1') THEN 'Y'
                                    WHEN UPPER(jnt3.left_acromoclavicular_joint_swelling) IN ('LEFT ACROMIOCLAVICULAR JOINT SWELLING', NULL) THEN NULL
                                END
                            , ' | ',
                            'PAIN: ',
                                CASE
                                    WHEN UPPER(jnt3.left_acromoclavicular_joint_pain) IN ('0') THEN 'N'
                                    WHEN UPPER(jnt3.left_acromoclavicular_joint_pain) IN ('1') THEN 'Y'
                                    WHEN UPPER(jnt3.left_acromoclavicular_joint_pain) IN ('LEFT ACROMIOCLAVICULAR JOINT PAIN', NULL) THEN NULL
                                END
                        ) ,

                    CONCAT
                        (
                            'NORMAL: ',
                                CASE
                                    WHEN UPPER(jnt.right_acromioclavicular_joint_normal) IN ('0', 'N') THEN 'N'
                                    WHEN UPPER(jnt.right_acromioclavicular_joint_normal) IN ('1', 'Y') THEN 'Y'
                                    WHEN UPPER(jnt.right_acromioclavicular_joint_normal) IN ('RIGHT ACROMIOCLAVICULAR JOINT NORMAL', NULL) THEN NULL
                                END
                            , ' | ',
                            'SWELLING: ',
                                CASE
                                    WHEN UPPER(jnt3.right_acromoclavicular_joint_swelling) IN ('0') THEN 'N'
                                    WHEN UPPER(jnt3.right_acromoclavicular_joint_swelling) IN ('1') THEN 'Y'
                                    WHEN UPPER(jnt3.right_acromoclavicular_joint_swelling) IN ('RIGHT ACROMIOCLAVICULAR JOINT SWELLING', NULL) THEN NULL
                                END
                            , ' | ',
                            'PAIN: ',
                                CASE
                                    WHEN UPPER(jnt3.right_acromoclavicular_joint_pain) IN ('0') THEN 'N'
                                    WHEN UPPER(jnt3.right_acromoclavicular_joint_pain) IN ('1') THEN 'Y'
                                    WHEN UPPER(jnt3.right_acromoclavicular_joint_pain) IN ('RIGHT ACROMIOCLAVICULAR JOINT PAIN', NULL) THEN NULL
                                END
                        ) ,

                    CONCAT
                        (
                            'NORMAL: ',
                                CASE
                                    WHEN UPPER(jnt.left_ankle_joint_normal) IN ('0', 'N') THEN 'N'
                                    WHEN UPPER(jnt.left_ankle_joint_normal) IN ('1', 'Y') THEN 'Y'
                                    WHEN UPPER(jnt.left_ankle_joint_normal) IN ('LEFT ANKLE JOINT NORMAL', NULL) THEN NULL
                                END
                            , ' | ',
                            'SWELLING: ',
                                CASE
                                    WHEN UPPER(jnt3.left_acromoclavicular_joint_swelling) IN ('0') THEN 'N'
                                    WHEN UPPER(jnt3.left_acromoclavicular_joint_swelling) IN ('1') THEN 'Y'
                                    WHEN UPPER(jnt3.left_acromoclavicular_joint_swelling) IN ('LEFT ANKLE SWELLING', NULL) THEN NULL
                                END
                            , ' | ',
                            'PAIN: ',
                                CASE
                                    WHEN UPPER(jnt3.left_acromoclavicular_joint_pain) IN ('0') THEN 'N'
                                    WHEN UPPER(jnt3.left_acromoclavicular_joint_pain) IN ('1') THEN 'Y'
                                    WHEN UPPER(jnt3.left_acromoclavicular_joint_pain) IN ('LEFT ANKLE PAIN', NULL) THEN NULL
                                END
                        ) ,

                    CONCAT
                        (
                            'NORMAL: ',
                                CASE
                                    WHEN UPPER(jnt.right_ankle_joint_normal) IN ('0', 'N') THEN 'N'
                                    WHEN UPPER(jnt.right_ankle_joint_normal) IN ('1', 'Y') THEN 'Y'
                                    WHEN UPPER(jnt.right_ankle_joint_normal) IN ('RIGHT ANKLE JOINT NORMAL', NULL) THEN NULL
                                END
                            , ' | ',
                            'SWELLING: ',
                                CASE
                                    WHEN UPPER(jnt3.right_ankle_swelling) IN ('0') THEN 'N'
                                    WHEN UPPER(jnt3.right_ankle_swelling) IN ('1') THEN 'Y'
                                    WHEN UPPER(jnt3.right_ankle_swelling) IN ('RIGHT ANKLE SWELLING', NULL) THEN NULL
                                END
                            , ' | ',
                            'PAIN: ',
                                CASE
                                    WHEN UPPER(jnt3.right_ankle_pain) IN ('0') THEN 'N'
                                    WHEN UPPER(jnt3.right_ankle_pain) IN ('1') THEN 'Y'
                                    WHEN UPPER(jnt3.right_ankle_pain) IN ('RIGHT ANKLE PAIN', NULL) THEN NULL
                                END
                        ) ,

                    CONCAT
                        (
                            'NORMAL: ',
                                CASE
                                    WHEN UPPER(jnt.left_sternoclavicular_joint_normal) IN ('0', 'N') THEN 'N'
                                    WHEN UPPER(jnt.left_sternoclavicular_joint_normal) IN ('1', 'Y') THEN 'Y'
                                WHEN UPPER(jnt.left_sternoclavicular_joint_normal) IN ('LEFT STERNOCLAVICULAR JOINT NORMAL', NULL) THEN NULL
                                END
                            , ' | ',
                            'SWELLING: ',
                                CASE
                                    WHEN UPPER(jnt3.left_sternoclavicular_joint_swelling) IN ('0') THEN 'N'
                                    WHEN UPPER(jnt3.left_sternoclavicular_joint_swelling) IN ('1') THEN 'Y'
                                    WHEN UPPER(jnt3.left_sternoclavicular_joint_swelling) IN ('LEFT STERNOCLAVICULAR JOINT SWELLING', NULL) THEN NULL
                                END
                            , ' | ',
                            'PAIN: ',
                                CASE
                                    WHEN UPPER(jnt3.left_sternoclavicular_joint_pain) IN ('0') THEN 'N'
                                    WHEN UPPER(jnt3.left_sternoclavicular_joint_pain) IN ('1') THEN 'Y'
                                    WHEN UPPER(jnt3.left_sternoclavicular_joint_pain) IN ('LEFT STERNOCLAVICULAR JOINT PAIN', NULL) THEN NULL
                                END
                        ) ,

                    CONCAT
                        (
                            'NORMAL: ',
                                CASE
                                    WHEN UPPER(jnt.right_sternoclavicular_joint_normal) IN ('0', 'N') THEN 'N'
                                    WHEN UPPER(jnt.right_sternoclavicular_joint_normal) IN ('1', 'Y') THEN 'Y'
                                    WHEN UPPER(jnt.right_sternoclavicular_joint_normal) IN ('RIGHT STERNOCLAVICULAR JOINT NORMAL', NULL) THEN NULL
                                END
                            , ' | ',
                            'SWELLING: ',
                                CASE
                                    WHEN UPPER(jnt3.right_sternoclavicular_joint_swelling) IN ('0') THEN 'N'
                                    WHEN UPPER(jnt3.right_sternoclavicular_joint_swelling) IN ('1') THEN 'Y'
                                    WHEN UPPER(jnt3.right_sternoclavicular_joint_swelling) IN ('RIGHT STERNOCLAVICULAR JOINT SWELLING', NULL) THEN NULL
                                END
                            , ' | ',
                            'PAIN: ',
                                CASE
                                    WHEN UPPER(jnt3.right_sternoclavicular_joint_pain) IN ('0') THEN 'N'
                                    WHEN UPPER(jnt3.right_sternoclavicular_joint_pain) IN ('1') THEN 'Y'
                                    WHEN UPPER(jnt3.right_sternoclavicular_joint_pain) IN ('RIGHT STERNOCLAVICULAR JOINT PAIN', NULL) THEN NULL
                                END
                        ) ,

                    CONCAT
                        (
                            'NORMAL: ',
                                CASE
                                    WHEN UPPER(jnt.left_tmj_joint_normal) IN ('0', 'N') THEN 'N'
                                    WHEN UPPER(jnt.left_tmj_joint_normal) IN ('1', 'Y') THEN 'Y'
                                    WHEN UPPER(jnt.left_tmj_joint_normal) IN ('LEFT TMJ JOINT NORMAL', NULL) THEN NULL
                                END
                            , ' | ',
                            'SWELLING: ',
                                CASE
                                    WHEN UPPER(jnt3.left_temporomandibular_joint_swelling) IN ('0') THEN 'N'
                                    WHEN UPPER(jnt3.left_temporomandibular_joint_swelling) IN ('1') THEN 'Y'
                                    WHEN UPPER(jnt3.left_temporomandibular_joint_swelling) IN ('LEFT TMJ JOINT SWELLING', NULL) THEN NULL
                                END
                            , ' | ',
                            'PAIN: ',
                                CASE
                                    WHEN UPPER(jnt3.left_temporomandibular_joint_pain) IN ('0') THEN 'N'
                                    WHEN UPPER(jnt3.right_sternoclavicular_joint_pain) IN ('1') THEN 'Y'
                                    WHEN UPPER(jnt3.right_sternoclavicular_joint_pain) IN ('LEFT TMJ JOINT PAIN', NULL) THEN NULL
                                END
                        ) ,

                    CONCAT
                        (
                            'NORMAL: ',
                                CASE
                                    WHEN UPPER(jnt.right_tmj_joint_normal) IN ('0', 'N') THEN 'N'
                                    WHEN UPPER(jnt.right_tmj_joint_normal) IN ('1', 'Y') THEN 'Y'
                                    WHEN UPPER(jnt.right_tmj_joint_normal) IN ('LEFT TMJ JOINT NORMAL', NULL) THEN NULL
                                END
                            , ' | ',
                            'SWELLING: ',
                                CASE
                                    WHEN UPPER(jnt3.right_temporomandibular_joint_swelling) IN ('0') THEN 'N'
                                    WHEN UPPER(jnt3.right_temporomandibular_joint_swelling) IN ('1') THEN 'Y'
                                    WHEN UPPER(jnt3.right_temporomandibular_joint_swelling) IN ('LEFT TMJ JOINT SWELLING', NULL) THEN NULL
                                END
                            , ' | ',
                            'PAIN: ',
                                CASE
                                    WHEN UPPER(jnt3.right_temporomandibular_joint_pain) IN ('0') THEN 'N'
                                    WHEN UPPER(jnt3.right_temporomandibular_joint_pain) IN ('1') THEN 'Y'
                                    WHEN UPPER(jnt3.right_temporomandibular_joint_pain) IN ('RIGHT TMJ JOINT PAIN', NULL) THEN NULL
                                END
                        ) ,

                    CONCAT
                        (
                            'NORMAL: ',
                                CASE
                                    WHEN UPPER(jnt.left_wrist_joint_normal) IN ('0', 'N') THEN 'N'
                                    WHEN UPPER(jnt.left_wrist_joint_normal) IN ('1', 'Y') THEN 'Y'
                                    WHEN UPPER(jnt.left_wrist_joint_normal) IN ('LEFT WRIST JOINT NORMAL', NULL) THEN NULL
                                END
                            , ' | ',
                            'SWELLING: ',
                                CASE
                                    WHEN UPPER(jnt3.left_wrist_swelling) IN ('0') THEN 'N'
                                    WHEN UPPER(jnt3.left_wrist_swelling) IN ('1') THEN 'Y'
                                    WHEN UPPER(jnt3.left_wrist_swelling) IN ('LEFT WRIST SWELLING', NULL) THEN NULL
                                END
                            , ' | ',
                            'PAIN: ',
                                CASE
                                    WHEN UPPER(jnt3.left_wrist_pain) IN ('0') THEN 'N'
                                    WHEN UPPER(jnt3.left_wrist_pain) IN ('1') THEN 'Y'
                                    WHEN UPPER(jnt3.left_wrist_pain) IN ('LEFT WRIST PAIN', NULL) THEN NULL
                                END
                        ) ,

                    CONCAT
                        (
                            'NORMAL: ',
                                CASE
                                    WHEN UPPER(jnt.right_wrist_joint_normal) IN ('0', 'N') THEN 'N'
                                    WHEN UPPER(jnt.right_wrist_joint_normal) IN ('1', 'Y') THEN 'Y'
                                    WHEN UPPER(jnt.right_wrist_joint_normal) IN ('LEFT WRIST JOINT NORMAL', NULL) THEN NULL
                                END
                            , ' | ',
                            'SWELLING: ',
                                CASE
                                    WHEN UPPER(jnt3.right_wrist_swelling) IN ('0') THEN 'N'
                                    WHEN UPPER(jnt3.right_wrist_swelling) IN ('1') THEN 'Y'
                                    WHEN UPPER(jnt3.right_wrist_swelling) IN ('RIGHT WRIST SWELLING', NULL) THEN NULL
                                END
                            , ' | ',
                            'PAIN: ',
                                CASE
                                    WHEN UPPER(jnt3.right_wrist_pain) IN ('0') THEN 'N'
                                    WHEN UPPER(jnt3.right_wrist_pain) IN ('1') THEN 'Y'
                                    WHEN UPPER(jnt3.right_wrist_pain) IN ('RIGHT WRIST PAIN', NULL) THEN NULL
                                END
                        ) ,

                    CONCAT
                        (
                            'NORMAL: ',
                                CASE
                                    WHEN UPPER(jnt.left_elbow_joint_normal) IN ('0', 'N') THEN 'N'
                                    WHEN UPPER(jnt.left_elbow_joint_normal) IN ('1', 'Y') THEN 'Y'
                                    WHEN UPPER(jnt.left_elbow_joint_normal) IN ('LEFT ELBOW JOINT NORMAL', NULL) THEN NULL
                                END
                            , ' | ',
                            'SWELLING: ',
                                CASE
                                    WHEN UPPER(jnt3.left_elbow_swelling) IN ('0') THEN 'N'
                                    WHEN UPPER(jnt3.left_elbow_swelling) IN ('1') THEN 'Y'
                                    WHEN UPPER(jnt3.left_elbow_swelling) IN ('LEFT ELBOW SWELLING', NULL) THEN NULL
                                END
                            , ' | ',
                            'PAIN: ',
                                CASE
                                    WHEN UPPER(jnt3.left_elbow_pain) IN ('0') THEN 'N'
                                    WHEN UPPER(jnt3.left_elbow_pain) IN ('1') THEN 'Y'
                                    WHEN UPPER(jnt3.left_elbow_pain) IN ('LEFT ELBOW PAIN', NULL) THEN NULL
                                END
                        ) ,


                    CONCAT
                        (
                            'NORMAL: ',
                                CASE
                                    WHEN UPPER(jnt.right_elbow_joint_normal) IN ('0', 'N') THEN 'N'
                                    WHEN UPPER(jnt.right_elbow_joint_normal) IN ('1', 'Y') THEN 'Y'
                                    WHEN UPPER(jnt.right_elbow_joint_normal) IN ('RIGHT ELBOW JOINT NORMAL', NULL) THEN NULL
                                END
                            , ' | ',
                            'SWELLING: ',
                                CASE
                                    WHEN UPPER(jnt3.right_elbow_swelling) IN ('0') THEN 'N'
                                    WHEN UPPER(jnt3.right_elbow_swelling) IN ('1') THEN 'Y'
                                    WHEN UPPER(jnt3.right_elbow_swelling) IN ('RIGHT ELBOW SWELLING', NULL) THEN NULL
                                END
                            , ' | ',
                            'PAIN: ',
                                CASE
                                    WHEN UPPER(jnt3.right_elbow_pain) IN ('0') THEN 'N'
                                    WHEN UPPER(jnt3.right_elbow_pain) IN ('1') THEN 'Y'
                                    WHEN UPPER(jnt3.right_elbow_pain) IN ('RIGHT ELBOW PAIN', NULL) THEN NULL
                                END
                        ) ,

-- No 'left foot joints' data columns provided in jnt3 source table. 'Left foot joints' data only provided in jnt source
                    CONCAT
                        (
                            'NORMAL: ',
                                CASE
                                    WHEN UPPER(jnt.left_foot_joints_normal) IN ('0', 'N') THEN 'N'
                                    WHEN UPPER(jnt.left_foot_joints_normal) IN ('1', 'Y') THEN 'Y'
                                    WHEN UPPER(jnt.left_foot_joints_normal) IN ('LEFT FOOT JOINTS NORMAL', NULL) THEN NULL
                                END
                            , ' | ',
                            'SWELLING: ', ' | ', 'PAIN: '
--                               CASE
--                                    WHEN UPPER(jnt3.left_foot_swelling) IN ('0') THEN 'N'
--                                    WHEN UPPER(jnt3.left_foot_swelling) IN ('1') THEN 'Y'
--                                    WHEN UPPER(jnt3.left_foot_swelling) IN ('LEFT FOOT SWELLING', NULL) THEN NULL
--                                END
--                            , ' | ',
--                            'PAIN: ',
--                                CASE
--                                    WHEN UPPER(jnt3.left_foot_pain) IN ('0') THEN 'N'
--                                    WHEN UPPER(jnt3.left_foot_pain) IN ('1') THEN 'Y'
--                                    WHEN UPPER(jnt3.left_foot_pain) IN ('LEFT FOOT PAIN', NULL) THEN NULL
--                                END
                        ) ,

-- No 'right foot joints' data columns provided in jnt3 source table. 'Right foot joints' data only provided in jnt source
                    CONCAT
                        (
                            'NORMAL: ',
                                CASE
                                    WHEN UPPER(jnt.right_foot_joints_normal) IN ('0', 'N') THEN 'N'
                                    WHEN UPPER(jnt.right_foot_joints_normal) IN ('1', 'Y') THEN 'Y'
                                    WHEN UPPER(jnt.right_foot_joints_normal) IN ('RIGHT FOOT JOINTS NORMAL', NULL) THEN NULL
                                END
                            , ' | ',
                            'SWELLING: ', ' | ', 'PAIN: '
--                               CASE
--                                    WHEN UPPER(jnt3.right_foot_swelling) IN ('0') THEN 'N'
--                                    WHEN UPPER(jnt3.right_foot_swelling) IN ('1') THEN 'Y'
--                                    WHEN UPPER(jnt3.right_foot_swelling) IN ('RIGHT FOOT SWELLING', NULL) THEN NULL
--                                END
--                            , ' | ',
--                            'PAIN: ',
--                                CASE
--                                    WHEN UPPER(jnt3.right_foot_pain) IN ('0') THEN 'N'
--                                    WHEN UPPER(jnt3.right_foot_pain) IN ('1') THEN 'Y'
--                                    WHEN UPPER(jnt3.right_foot_pain) IN ('RIGHT FOOT PAIN', NULL) THEN NULL
--                                END
                        ) ,         -- added below

-- No 'left hand' data columns provided in jnt3 source table. 'Left hand' data only provided in jnt source
                    CONCAT
                        (
                            'NORMAL: ',
                                CASE
                                    WHEN UPPER(jnt.left_hand_joints_normal) IN ('0', 'N') THEN 'N'
                                    WHEN UPPER(jnt.left_hand_joints_normal) IN ('1', 'Y') THEN 'Y'
                                    WHEN UPPER(jnt.left_hand_joints_normal) IN ('LEFT HAND JOINTS NORMAL', NULL) THEN NULL
                                END
                            , ' | ',
                            'SWELLING: ', ' | ', 'PAIN: '
--                               CASE
--                                    WHEN UPPER(jnt3.left_hand_swelling) IN ('0') THEN 'N'
--                                    WHEN UPPER(jnt3.left_hand_swelling) IN ('1') THEN 'Y'
--                                    WHEN UPPER(jnt3.left_hand_swelling) IN ('LEFT HAND SWELLING', NULL) THEN NULL
--                                END
--                            , ' | ',
--                            'PAIN: ',
--                                CASE
--                                    WHEN UPPER(jnt3.left_hand_pain) IN ('0') THEN 'N'
--                                    WHEN UPPER(jnt3.left_hand_pain) IN ('1') THEN 'Y'
--                                    WHEN UPPER(jnt3.left_hand_pain) IN ('LEFT HAND PAIN', NULL) THEN NULL
--                                END
                        ) ,

-- No 'right hand' data columns provided in jnt3 source table. 'Right hand' data only provided in jnt source
                    CONCAT
                        (
                            'NORMAL: ',
                                CASE
                                    WHEN UPPER(jnt.right_hand_joints_normal) IN ('0', 'N') THEN 'N'
                                    WHEN UPPER(jnt.right_hand_joints_normal) IN ('1', 'Y') THEN 'Y'
                                    WHEN UPPER(jnt.right_hand_joints_normal) IN ('RIGHT HAND JOINTS NORMAL', NULL) THEN NULL
                                END
                            , ' | ',
                            'SWELLING: ', ' | ', 'PAIN: '
--                               CASE
--                                    WHEN UPPER(jnt3.right_hand_swelling) IN ('0') THEN 'N'
--                                    WHEN UPPER(jnt3.right_hand_swelling) IN ('1') THEN 'Y'
--                                    WHEN UPPER(jnt3.right_hand_swelling) IN ('RIGHT HAND SWELLING', NULL) THEN NULL
--                                END
--                            , ' | ',
--                            'PAIN: ',
--                                CASE
--                                    WHEN UPPER(jnt3.right_hand_pain) IN ('0') THEN 'N'
--                                    WHEN UPPER(jnt3.right_hand_pain) IN ('1') THEN 'Y'
--                                    WHEN UPPER(jnt3.right_hand_pain) IN ('RIGHT HAND PAIN', NULL) THEN NULL
--                                END
                        ) ,

                    CONCAT
                        (
                            'NORMAL: ',
                                CASE
                                    WHEN UPPER(jnt.left_hip_joint_normal) IN ('0', 'N') THEN 'N'
                                    WHEN UPPER(jnt.left_hip_joint_normal) IN ('1', 'Y') THEN 'Y'
                                    WHEN UPPER(jnt.left_hip_joint_normal) IN ('LEFT HIP JOINT NORMAL', NULL) THEN NULL
                                END
                            , ' | ',
                            'SWELLING: ',
                                CASE
                                    WHEN UPPER(jnt3.left_hip_swelling) IN ('0') THEN 'N'
                                    WHEN UPPER(jnt3.left_hip_swelling) IN ('1') THEN 'Y'
                                    WHEN UPPER(jnt3.left_hip_swelling) IN ('LEFT HIP SWELLING', NULL) THEN NULL
                                END
                            , ' | ',
                            'PAIN: ',
                                CASE
                                    WHEN UPPER(jnt3.left_hip_pain) IN ('0') THEN 'N'
                                    WHEN UPPER(jnt3.left_hip_pain) IN ('1') THEN 'Y'
                                    WHEN UPPER(jnt3.left_hip_pain) IN ('LEFT HIP PAIN', NULL) THEN NULL
                                END
                        ) ,

                    CONCAT
                        (
                            'NORMAL: ',
                                CASE
                                    WHEN UPPER(jnt.right_hip_joint_normal) IN ('0', 'N') THEN 'N'
                                    WHEN UPPER(jnt.right_hip_joint_normal) IN ('1', 'Y') THEN 'Y'
                                    WHEN UPPER(jnt.right_hip_joint_normal) IN ('RIGHT HIP JOINT NORMAL', NULL) THEN NULL
                                END
                            , ' | ',
                            'SWELLING: ',
                                CASE
                                    WHEN UPPER(jnt3.right_hip_swelling) IN ('0') THEN 'N'
                                    WHEN UPPER(jnt3.right_hip_swelling) IN ('1') THEN 'Y'
                                    WHEN UPPER(jnt3.right_hip_swelling) IN ('RIGHT HIP SWELLING', NULL) THEN NULL
                                END
                            , ' | ',
                            'PAIN: ',
                                CASE
                                    WHEN UPPER(jnt3.right_hip_pain) IN ('0') THEN 'N'
                                    WHEN UPPER(jnt3.right_hip_pain) IN ('1') THEN 'Y'
                                    WHEN UPPER(jnt3.right_hip_pain) IN ('RIGHT HIP PAIN', NULL) THEN NULL
                                END
                        ) ,


                    CONCAT
                        (
                            'NORMAL: ',
                                CASE
                                    WHEN UPPER(jnt.left_hip_joint_normal) IN ('0', 'N') THEN 'N'
                                    WHEN UPPER(jnt.left_hip_joint_normal) IN ('1', 'Y') THEN 'Y'
                                    WHEN UPPER(jnt.left_hip_joint_normal) IN ('LEFT HIP JOINT NORMAL', NULL) THEN NULL
                                END
                            , ' | ',
                            'SWELLING: ',
                                CASE
                                    WHEN UPPER(jnt3.left_hip_swelling) IN ('0') THEN 'N'
                                    WHEN UPPER(jnt3.left_hip_swelling) IN ('1') THEN 'Y'
                                    WHEN UPPER(jnt3.left_hip_swelling) IN ('LEFT HIP SWELLING', NULL) THEN NULL
                                END
                            , ' | ',
                            'PAIN: ',
                                CASE
                                    WHEN UPPER(jnt3.left_hip_pain) IN ('0') THEN 'N'
                                    WHEN UPPER(jnt3.left_hip_pain) IN ('1') THEN 'Y'
                                    WHEN UPPER(jnt3.left_hip_pain) IN ('LEFT HIP PAIN', NULL) THEN NULL
                                END
                        ) ,




                    CONCAT
                        (
                            'NORMAL: ',
                                CASE
                                    WHEN UPPER(jnt.right_hip_joint_normal) IN ('0', 'N') THEN 'N'
                                    WHEN UPPER(jnt.right_hip_joint_normal) IN ('1', 'Y') THEN 'Y'
                                    WHEN UPPER(jnt.right_hip_joint_normal) IN ('RIGHT HIP JOINT NORMAL', NULL) THEN NULL
                                END
                            , ' | ',
                            'SWELLING: ',
                                CASE
                                    WHEN UPPER(jnt3.right_hip_swelling) IN ('0') THEN 'N'
                                    WHEN UPPER(jnt3.right_hip_swelling) IN ('1') THEN 'Y'
                                    WHEN UPPER(jnt3.right_hip_swelling) IN ('RIGHT HIP SWELLING', NULL) THEN NULL
                                END
                            , ' | ',
                            'PAIN: ',
                                CASE
                                    WHEN UPPER(jnt3.right_hip_pain) IN ('0') THEN 'N'
                                    WHEN UPPER(jnt3.right_hip_pain) IN ('1') THEN 'Y'
                                    WHEN UPPER(jnt3.right_hip_pain) IN ('RIGHT HIP PAIN', NULL) THEN NULL
                                END
                        ) ,

                    CONCAT
                        (
                            'NORMAL: ',
                                CASE
                                    WHEN UPPER(jnt.left_knee_joint_normal) IN ('0', 'N') THEN 'N'
                                    WHEN UPPER(jnt.left_knee_joint_normal) IN ('1', 'Y') THEN 'Y'
                                    WHEN UPPER(jnt.left_knee_joint_normal) IN ('LEFT KNEE JOINT NORMAL', NULL) THEN NULL
                                END
                            , ' | ',
                            'SWELLING: ',
                                CASE
                                    WHEN UPPER(jnt3.left_knee_joint_swelling) IN ('0') THEN 'N'
                                    WHEN UPPER(jnt3.left_knee_joint_swelling) IN ('1') THEN 'Y'
                                    WHEN UPPER(jnt3.left_knee_joint_swelling) IN ('LEFT KNEE JOINT SWELLING', NULL) THEN NULL
                                END
                            , ' | ',
                            'PAIN: ',
                                CASE
                                    WHEN UPPER(jnt3.left_knee_pain) IN ('0') THEN 'N'
                                    WHEN UPPER(jnt3.left_knee_pain) IN ('1') THEN 'Y'
                                    WHEN UPPER(jnt3.left_knee_pain) IN ('LEFT KNEE PAIN', NULL) THEN NULL
                                END
                        ) ,

                    CONCAT
                        (
                            'NORMAL: ',
                                CASE
                                    WHEN UPPER(jnt.right_knee_joint_normal) IN ('0', 'N') THEN 'N'
                                    WHEN UPPER(jnt.right_knee_joint_normal) IN ('1', 'Y') THEN 'Y'
                                    WHEN UPPER(jnt.right_knee_joint_normal) IN ('LEFT KNEE JOINT NORMAL', NULL) THEN NULL
                                END
                            , ' | ',
                            'SWELLING: ',
                                CASE
                                    WHEN UPPER(jnt3.right_knee_swelling) IN ('0') THEN 'N'
                                    WHEN UPPER(jnt3.right_knee_swelling) IN ('1') THEN 'Y'
                                    WHEN UPPER(jnt3.right_knee_swelling) IN ('RIGHT KNEE SWELLING', NULL) THEN NULL
                                END
                            , ' | ',
                            'PAIN: ',
                                CASE
                                    WHEN UPPER(jnt3.right_knee_pain) IN ('0') THEN 'N'
                                    WHEN UPPER(jnt3.right_knee_pain) IN ('1') THEN 'Y'
                                    WHEN UPPER(jnt3.right_knee_pain) IN ('RIGHT KNEE PAIN', NULL) THEN NULL
                                END
                        ) ,

-- Begin Left Shoulder
                    CONCAT
                        (
                            'NORMAL: ',
                                CASE
                                    WHEN UPPER(jnt.left_shoulder_joint_normal) IN ('0', 'N') THEN 'N'
                                    WHEN UPPER(jnt.left_shoulder_joint_normal) IN ('1', 'Y') THEN 'Y'
                                    WHEN UPPER(jnt.left_shoulder_joint_normal) IN ('LEFT SHOULDER JOINT NORMAL', NULL) THEN NULL
                                END
                            , ' | ',
                            'SWELLING: ',
                                CASE
                                    WHEN UPPER(jnt3.left_glenohumeral_joint_swelling) IN ('0') THEN 'N'
                                    WHEN UPPER(jnt3.left_glenohumeral_joint_swelling) IN ('1') THEN 'Y'
                                    WHEN UPPER(jnt3.left_glenohumeral_joint_swelling) IN ('LEFT SHOULDER JOINT SWELLING', NULL) THEN NULL
                                END
                            , ' | ',
                            'PAIN: ',
                                CASE
                                    WHEN UPPER(jnt3.left_glenohumeral_joint_pain) IN ('0') THEN 'N'
                                    WHEN UPPER(jnt3.left_glenohumeral_joint_pain) IN ('1') THEN 'Y'
                                    WHEN UPPER(jnt3.left_glenohumeral_joint_pain) IN ('LEFT SHOULDER JOINT PAIN', NULL) THEN NULL
                                END
                        ) ,

-- Begin Right Shoulder
                    CONCAT
                        (
                            'NORMAL: ',
                                CASE
                                    WHEN UPPER(jnt.right_shoulder_joint_normal) IN ('0', 'N') THEN 'N'
                                    WHEN UPPER(jnt.right_shoulder_joint_normal) IN ('1', 'Y') THEN 'Y'
                                    WHEN UPPER(jnt.right_shoulder_joint_normal) IN ('RIGHT SHOULDER JOINT NORMAL', NULL) THEN NULL
                                END
                            , ' | ',
                            'SWELLING: ',
                                CASE
                                    WHEN UPPER(jnt3.right_glenohumeral_joint_swelling) IN ('0') THEN 'N'
                                    WHEN UPPER(jnt3.right_glenohumeral_joint_swelling) IN ('1') THEN 'Y'
                                    WHEN UPPER(jnt3.right_glenohumeral_joint_swelling) IN ('RIGHT SHOULDER JOINT SWELLING', NULL) THEN NULL
                                END
                            , ' | ',
                            'PAIN: ',
                                CASE
                                    WHEN UPPER(jnt3.right_glenohumeral_joint_pain) IN ('0') THEN 'N'
                                    WHEN UPPER(jnt3.right_glenohumeral_joint_pain) IN ('1') THEN 'Y'
                                    WHEN UPPER(jnt3.right_glenohumeral_joint_pain) IN ('RIGHT SHOULDER JOINT PAIN', NULL) THEN NULL
                                END
                        )


                )[clin_obsn_typ_cd_explode.n]
    END                                                                                     AS clin_obsn_grp_txt,

    jnt3.enterprise_id                                                                      AS data_src_cd,

    --------------------------------------------------------------------------------------------------
    --  data_captr_dt
    --------------------------------------------------------------------------------------------------
	CAP_DATE
	    (
            CAST(EXTRACT_DATE(SUBSTR(jnt3.record_create_date_time, 1, 10), '%Y-%m-%d') AS DATE),
            CAST('{EARLIEST_SERVICE_DATE}' AS DATE),
            CAST('{VDR_FILE_DT}' AS DATE)
	    )																					AS data_captr_dt,
    'joint_exam3'                                                                           AS prmy_src_tbl_nm,
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

FROM joint_exam3 jnt3
LEFT OUTER JOIN encounters enc
                ON enc.encounter_id = jnt3.encounter_id
LEFT OUTER JOIN joint_exam jnt
                ON jnt.encounter_id = jnt3.encounter_id
LEFT OUTER JOIN provider prov
                ON prov.provider_id = enc.encounter_rendering_provider_id
LEFT OUTER JOIN plainout pln
                ON pln.person_id = jnt3.patient_id
LEFT OUTER JOIN matching_payload pay
                ON pay.hvjoinkey = pln.hvjoinkey

-- Add Cross Join Array here
CROSS JOIN (SELECT EXPLODE(ARRAY(0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12,
                                13, 14, 15, 16, 17, 18, 19, 20, 21)) AS n) clin_obsn_typ_cd_explode
WHERE
----------- Patient response code explosion
    (
        ARRAY
            (
                jnt3.left_acromioclavicular_joint_exam_findings,
                jnt3.right_acromioclavicular_joint_exam_findings,
                jnt3.left_ankle_joint_exam_findings,
                jnt3.right_ankle_joint_exam_findings,
                jnt3.left_sternoclavicular_joint_exam_findings,
                jnt3.right_sternoclavicular_joint_exam_findings,
                jnt3.left_t_m_j_joint_exam_findings,
                jnt3.right_t_m_j_joint_exam_findings,
                jnt3.left_wrist_joint_exam_findings,
                jnt3.right_wrist_joint_exam_findings,
                jnt3.left_elbow_joint_exam_findings,
                jnt3.right_elbow_joint_exam_findings,
                'left_foot_joints_exam_findings',
                'right_foot_joints_exam_findings',
                'left_hand_joints_exam_findings',
                'right_hand_joints_exam',
                jnt3.left_hip_joint_exam_findings,
                jnt3.right_hip_joint_exam_findings,
                jnt3.left_knee_joint_exam_findings,
                jnt3.right_knee_joint_exam_findings,
                jnt3.left_shoulder_joint_exam_findings,
                jnt3.right_shoulder_joint_exam_findings
            )[clin_obsn_typ_cd_explode.n] IS NOT NULL
    )
-- Remove header records
    AND TRIM(lower(COALESCE(jnt3.patient_id, 'empty'))) <> 'patientid'
-- LIMIT 10
