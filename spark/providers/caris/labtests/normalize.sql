INSERT INTO lab_common_model
SELECT DISTINCT * FROM (
    SELECT
        NULL,                                -- record_id
        CASE
        WHEN COALESCE(TRIM(t.customer__patient_id), '') = ''
        AND COALESCE(TRIM(t.ods_id), '') = ''
        THEN NULL
        ELSE CONCAT(
            COALESCE(TRIM(t.customer__patient_id), ''),
            '_',
            COALESCE(TRIM(t.ods_id), '')
            )
        END,                                 -- claim_id
        mp.hvid,                             -- hvid
        NULL,                                -- created
        '1',                                 -- model_version
        NULL,                                -- data_set
        NULL,                                -- data_feed
        NULL,                                -- data_vendor
        NULL,                                -- source_version
        mp.gender,                           -- patient_gender
        NULL,                                -- patient_age
        cap_year_of_birth(
            NULL,
            CAST({date_received} AS DATE),
            mp.yearOfBirth
            ),                               -- patient_year_of_birth
        mp.threeDigitZip,                    -- patient_zip3
        zip3.state,                          -- patient_state
        COALESCE(
        extract_date(
            t.accession_date,
            '%m/%d/%Y',
            CAST({min_date} AS DATE),
            CAST({max_date} AS DATE)
            ),
        extract_date(
            t.sign_out_date,
            '%d-%b-%Y',
            CAST({min_date} AS DATE),
            CAST({max_date} AS DATE)
            ),
        extract_date(
            t.new_accession_date,
            '%Y-%m-%d %H:%M:%S.0',
            CAST({min_date} AS DATE),
            CAST({max_date} AS DATE)
            )
        ),                                   -- date_service
        NULL,                                -- date_specimen
        NULL,                                -- date_report
        NULL,                                -- time_report
        NULL,                                -- loinc_code
        NULL,                                -- lab_id
        NULL,                                -- test_id
        NULL,                                -- test_number
        NULL,                                -- test_battery_local_id
        NULL,                                -- test_battery_std_id
        NULL,                                -- test_battery_name
        NULL,                                -- test_ordered_local_id
        NULL,                                -- test_ordered_std_id
        CASE exploder.n
        WHEN 0   THEN CASE WHEN fish_cmet = 'X' THEN 'FISH_cMET' ELSE NULL END
        WHEN 1   THEN CASE WHEN fish_cmyc = 'X' THEN 'FISH_cMYC' ELSE NULL END
        WHEN 2   THEN CASE WHEN fish_egfr = 'X' THEN 'FISH_EGFR' ELSE NULL END
        WHEN 3   THEN CASE WHEN fish_her2__neu = 'X' THEN 'FISH_Her2/Neu' ELSE NULL END
        WHEN 4   THEN CASE WHEN fish_pik3ca = 'X' THEN 'FISH_PIK3CA' ELSE NULL END
        WHEN 5   THEN CASE WHEN fish_top2a = 'X' THEN 'FISH_TOP2A' ELSE NULL END
        WHEN 6   THEN CASE WHEN fish_mutational_1p19q = 'X' THEN 'FISH-Mutational_1p19q' ELSE NULL END
        WHEN 7   THEN CASE WHEN fish_mutational_alk = 'X' THEN 'FISH-Mutational_ALK' ELSE NULL END
        WHEN 8   THEN CASE WHEN fish_mutational_ros1 = 'X' THEN 'FISH-Mutational_ROS1' ELSE NULL END
        WHEN 9   THEN CASE WHEN cish_cmet = 'X' THEN 'CISH_cMET' ELSE NULL END
        WHEN 10  THEN CASE WHEN cish_egfr = 'X' THEN 'CISH_EGFR' ELSE NULL END
        WHEN 11  THEN CASE WHEN cish_her2__neu = 'X' THEN 'CISH_Her2/Neu' ELSE NULL END
        WHEN 12  THEN CASE WHEN cish_mdm2 = 'X' THEN 'CISH_MDM2' ELSE NULL END
        WHEN 13  THEN CASE WHEN cish_top2a = 'X' THEN 'CISH_TOP2A' ELSE NULL END
        WHEN 14  THEN CASE WHEN ihc_ae1__ae3 = 'X' THEN 'IHC_AE1/AE3' ELSE NULL END
        WHEN 15  THEN CASE WHEN ihc_afp = 'X' THEN 'IHC_AFP' ELSE NULL END
        WHEN 16  THEN CASE WHEN ihc_alk = 'X' THEN 'IHC_ALK' ELSE NULL END
        WHEN 17  THEN CASE WHEN ihc_ar = 'X' THEN 'IHC_AR' ELSE NULL END
        WHEN 18  THEN CASE WHEN ihc_bcl2 = 'X' THEN 'IHC_BCL2' ELSE NULL END
        WHEN 19  THEN CASE WHEN ihc_bcrp = 'X' THEN 'IHC_BCRP' ELSE NULL END
        WHEN 20  THEN CASE WHEN ihc_bombesin = 'X' THEN 'IHC_Bombesin' ELSE NULL END
        WHEN 21  THEN CASE WHEN ihc_brcp = 'X' THEN 'IHC_BRCP' ELSE NULL END
        WHEN 22  THEN CASE WHEN ihc_ca_19_9 = 'X' THEN 'IHC_CA 19.9' ELSE NULL END
        WHEN 23  THEN CASE WHEN ihc_calcitonin = 'X' THEN 'IHC_Calcitonin' ELSE NULL END
        WHEN 24  THEN CASE WHEN ihc_calret = 'X' THEN 'IHC_Calret' ELSE NULL END
        WHEN 25  THEN CASE WHEN ihc_calretinin = 'X' THEN 'IHC_Calretinin' ELSE NULL END
        WHEN 26  THEN CASE WHEN ihc_cam5_2 = 'X' THEN 'IHC_CAM5.2' ELSE NULL END
        WHEN 27  THEN CASE WHEN ihc_cav_1 = 'X' THEN 'IHC_CAV-1' ELSE NULL END
        WHEN 28  THEN CASE WHEN ihc_cd10 = 'X' THEN 'IHC_CD10' ELSE NULL END
        WHEN 29  THEN CASE WHEN ihc_cd20 = 'X' THEN 'IHC_CD20' ELSE NULL END
        WHEN 30  THEN CASE WHEN ihc_cd20__l26 = 'X' THEN 'IHC_CD20 (L26' ELSE NULL END
        WHEN 31  THEN CASE WHEN ihc_cd25 = 'X' THEN 'IHC_CD25' ELSE NULL END
        WHEN 32  THEN CASE WHEN ihc_cd30 = 'X' THEN 'IHC_CD30' ELSE NULL END
        WHEN 33  THEN CASE WHEN ihc_cd31 = 'X' THEN 'IHC_CD31' ELSE NULL END
        WHEN 34  THEN CASE WHEN ihc_cd34 = 'X' THEN 'IHC_CD34' ELSE NULL END
        WHEN 35  THEN CASE WHEN ihc_cd52 = 'X' THEN 'IHC_CD52' ELSE NULL END
        WHEN 36  THEN CASE WHEN ihc_cd56 = 'X' THEN 'IHC_CD56' ELSE NULL END
        WHEN 37  THEN CASE WHEN ihc_cd68 = 'X' THEN 'IHC_CD68' ELSE NULL END
        WHEN 38  THEN CASE WHEN ihc_cd99 = 'X' THEN 'IHC_CD99' ELSE NULL END
        WHEN 39  THEN CASE WHEN ihc_cdx2 = 'X' THEN 'IHC_CDX2' ELSE NULL END
        WHEN 40  THEN CASE WHEN ihc_cea = 'X' THEN 'IHC_CEA' ELSE NULL END
        WHEN 41  THEN CASE WHEN ihc_chromogranin = 'X' THEN 'IHC_Chromogranin' ELSE NULL END
        WHEN 42  THEN CASE WHEN ihc_ck14 = 'X' THEN 'IHC_CK14' ELSE NULL END
        WHEN 43  THEN CASE WHEN ihc_ck17 = 'X' THEN 'IHC_CK17' ELSE NULL END
        WHEN 44  THEN CASE WHEN ihc_ck19 = 'X' THEN 'IHC_CK19' ELSE NULL END
        WHEN 45  THEN CASE WHEN ihc_ck20 = 'X' THEN 'IHC_CK20' ELSE NULL END
        WHEN 46  THEN CASE WHEN ihc_ck5 = 'X' THEN 'IHC_CK5' ELSE NULL END
        WHEN 47  THEN CASE WHEN ihc_ck5__6 = 'X' THEN 'IHC_CK5/6' ELSE NULL END
        WHEN 48  THEN CASE WHEN ihc_ck7 = 'X' THEN 'IHC_CK7' ELSE NULL END
        WHEN 49  THEN CASE WHEN ihc_ck8 = 'X' THEN 'IHC_CK8' ELSE NULL END
        WHEN 50  THEN CASE WHEN ihc_c_kit = 'X' THEN 'IHC_c-kit' ELSE NULL END
        WHEN 51  THEN CASE WHEN ihc_cmet = 'X' THEN 'IHC_cMET' ELSE NULL END
        WHEN 52  THEN CASE WHEN ihc_cox2 = 'X' THEN 'IHC_COX2' ELSE NULL END
        WHEN 53  THEN CASE WHEN ihc_cyclin_d1 = 'X' THEN 'IHC_Cyclin-D1' ELSE NULL END
        WHEN 54  THEN CASE WHEN ihc_dog_1 = 'X' THEN 'IHC_DOG-1' ELSE NULL END
        WHEN 55  THEN CASE WHEN ihc_ebv = 'X' THEN 'IHC_EBV' ELSE NULL END
        WHEN 56  THEN CASE WHEN ihc_ecad = 'X' THEN 'IHC_ECAD' ELSE NULL END
        WHEN 57  THEN CASE WHEN ihc_egfr = 'X' THEN 'IHC_EGFR' ELSE NULL END
        WHEN 58  THEN CASE WHEN ihc_ema = 'X' THEN 'IHC_EMA' ELSE NULL END
        WHEN 59  THEN CASE WHEN ihc_er = 'X' THEN 'IHC_ER' ELSE NULL END
        WHEN 60  THEN CASE WHEN ihc_ercc1 = 'X' THEN 'IHC_ERCC1' ELSE NULL END
        WHEN 61  THEN CASE WHEN ihc_gcdfp = 'X' THEN 'IHC_GCDFP' ELSE NULL END
        WHEN 62  THEN CASE WHEN ihc_gcdfp__brst_ii = 'X' THEN 'IHC_GCDFP (BRST II' ELSE NULL END
        WHEN 63  THEN CASE WHEN ihc_gcdfp_15 = 'X' THEN 'IHC_GCDFP-15' ELSE NULL END
        WHEN 64  THEN CASE WHEN ihc_h3k36me3 = 'X' THEN 'IHC_H3K36me3' ELSE NULL END
        WHEN 65  THEN CASE WHEN ihc_hcg = 'X' THEN 'IHC_HCG' ELSE NULL END
        WHEN 66  THEN CASE WHEN ihc_her2__neu = 'X' THEN 'IHC_Her2/Neu' ELSE NULL END
        WHEN 67  THEN CASE WHEN ihc_hmb45 = 'X' THEN 'IHC_HMB45' ELSE NULL END
        WHEN 68  THEN CASE WHEN ihc_hsp90 = 'X' THEN 'IHC_HSP90' ELSE NULL END
        WHEN 69  THEN CASE WHEN ihc_igf1r = 'X' THEN 'IHC_IGF1R' ELSE NULL END
        WHEN 70  THEN CASE WHEN ihc_inhibin = 'X' THEN 'IHC_INHIBIN' ELSE NULL END
        WHEN 71  THEN CASE WHEN ihc_ki1 = 'X' THEN 'IHC_Ki1' ELSE NULL END
        WHEN 72  THEN CASE WHEN ihc_ki67 = 'X' THEN 'IHC_Ki67' ELSE NULL END
        WHEN 73  THEN CASE WHEN ihc_l26 = 'X' THEN 'IHC_L26' ELSE NULL END
        WHEN 74  THEN CASE WHEN ihc_lca = 'X' THEN 'IHC_LCA' ELSE NULL END
        WHEN 75  THEN CASE WHEN ihc_mammaglobin = 'X' THEN 'IHC_Mammaglobin' ELSE NULL END
        WHEN 76  THEN CASE WHEN ihc_mart_1 = 'X' THEN 'IHC_Mart-1' ELSE NULL END
        WHEN 77  THEN CASE WHEN ihc_melana = 'X' THEN 'IHC_MelanA' ELSE NULL END
        WHEN 78  THEN CASE WHEN ihc_melanoma_specific_antigen = 'X' THEN 'IHC_Melanoma Specific Antigen' ELSE NULL END
        WHEN 79  THEN CASE WHEN ihc_mgmt = 'X' THEN 'IHC_MGMT' ELSE NULL END
        WHEN 80  THEN CASE WHEN ihc_mitf = 'X' THEN 'IHC_MiTF' ELSE NULL END
        WHEN 81  THEN CASE WHEN ihc_mlh1 = 'X' THEN 'IHC_MLH1' ELSE NULL END
        WHEN 82  THEN CASE WHEN ihc_mrp1 = 'X' THEN 'IHC_MRP1' ELSE NULL END
        WHEN 83  THEN CASE WHEN ihc_msh2 = 'X' THEN 'IHC_MSH2' ELSE NULL END
        WHEN 84  THEN CASE WHEN ihc_msh6 = 'X' THEN 'IHC_MSH6' ELSE NULL END
        WHEN 85  THEN CASE WHEN ihc_nse = 'X' THEN 'IHC_NSE' ELSE NULL END
        WHEN 86  THEN CASE WHEN ihc_oc125 = 'X' THEN 'IHC_OC125' ELSE NULL END
        WHEN 87  THEN CASE WHEN ihc_p53 = 'X' THEN 'IHC_P53' ELSE NULL END
        WHEN 88  THEN CASE WHEN ihc_p63 = 'X' THEN 'IHC_p63' ELSE NULL END
        WHEN 89  THEN CASE WHEN ihc_p95 = 'X' THEN 'IHC_p95' ELSE NULL END
        WHEN 90  THEN CASE WHEN ihc_pap = 'X' THEN 'IHC_PAP' ELSE NULL END
        WHEN 91  THEN CASE WHEN ihc_pbrm1 = 'X' THEN 'IHC_PBRM1' ELSE NULL END
        WHEN 92  THEN CASE WHEN ihc_pd_1 = 'X' THEN 'IHC_PD-1' ELSE NULL END
        WHEN 93  THEN CASE WHEN ihc_pdgfr = 'X' THEN 'IHC_PDGFR' ELSE NULL END
        WHEN 94  THEN CASE WHEN ihc_pd_l1 = 'X' THEN 'IHC_PD-L1' ELSE NULL END
        WHEN 95  THEN CASE WHEN ihc_pgp = 'X' THEN 'IHC_PGP' ELSE NULL END
        WHEN 96  THEN CASE WHEN ihc_pms2 = 'X' THEN 'IHC_PMS2' ELSE NULL END
        WHEN 97  THEN CASE WHEN ihc_pr = 'X' THEN 'IHC_PR' ELSE NULL END
        WHEN 98  THEN CASE WHEN ihc_psa = 'X' THEN 'IHC_PSA' ELSE NULL END
        WHEN 99  THEN CASE WHEN ihc_psap = 'X' THEN 'IHC_PSAP' ELSE NULL END
        WHEN 100 THEN CASE WHEN ihc_pten = 'X' THEN 'IHC_PTEN' ELSE NULL END
        WHEN 101 THEN CASE WHEN ihc_rcc = 'X' THEN 'IHC_RCC' ELSE NULL END
        WHEN 102 THEN CASE WHEN ihc_rcca = 'X' THEN 'IHC_RCCA' ELSE NULL END
        WHEN 103 THEN CASE WHEN ihc_rrm1 = 'X' THEN 'IHC_RRM1' ELSE NULL END
        WHEN 104 THEN CASE WHEN ihc_rrm2 = 'X' THEN 'IHC_RRM2' ELSE NULL END
        WHEN 105 THEN CASE WHEN ihc_s100 = 'X' THEN 'IHC_S100' ELSE NULL END
        WHEN 106 THEN CASE WHEN ihc_sma = 'X' THEN 'IHC_SMA' ELSE NULL END
        WHEN 107 THEN CASE WHEN ihc_sparc = 'X' THEN 'IHC_SPARC' ELSE NULL END
        WHEN 108 THEN CASE WHEN ihc_sparc_monoclonal = 'X' THEN 'IHC_SPARC Monoclonal' ELSE NULL END
        WHEN 109 THEN CASE WHEN ihc_sparc_polyclonal = 'X' THEN 'IHC_SPARC Polyclonal' ELSE NULL END
        WHEN 110 THEN CASE WHEN ihc_survivin = 'X' THEN 'IHC_Survivin' ELSE NULL END
        WHEN 111 THEN CASE WHEN ihc_synaptophysin = 'X' THEN 'IHC_Synaptophysin' ELSE NULL END
        WHEN 112 THEN CASE WHEN ihc_thyroglobulin = 'X' THEN 'IHC_Thyroglobulin' ELSE NULL END
        WHEN 113 THEN CASE WHEN ihc_tle3 = 'X' THEN 'IHC_TLE3' ELSE NULL END
        WHEN 114 THEN CASE WHEN ihc_top2a = 'X' THEN 'IHC_TOP2A' ELSE NULL END
        WHEN 115 THEN CASE WHEN ihc_topo1 = 'X' THEN 'IHC_TOPO1' ELSE NULL END
        WHEN 116 THEN CASE WHEN ihc_ts = 'X' THEN 'IHC_TS' ELSE NULL END
        WHEN 117 THEN CASE WHEN ihc_ttf = 'X' THEN 'IHC_TTF' ELSE NULL END
        WHEN 118 THEN CASE WHEN ihc_ttf1 = 'X' THEN 'IHC_TTF1' ELSE NULL END
        WHEN 119 THEN CASE WHEN ihc_tubb3 = 'X' THEN 'IHC_TUBB3' ELSE NULL END
        WHEN 120 THEN CASE WHEN ihc_uchl_1 = 'X' THEN 'IHC_UCHL-1' ELSE NULL END
        WHEN 121 THEN CASE WHEN ihc_vegf = 'X' THEN 'IHC_VEGF' ELSE NULL END
        WHEN 122 THEN CASE WHEN ihc_vimentin = 'X' THEN 'IHC_Vimentin' ELSE NULL END
        WHEN 123 THEN CASE WHEN ihc_wt1 = 'X' THEN 'IHC_WT1' ELSE NULL END
        WHEN 124 THEN CASE WHEN ihc_h_score_egfr = 'X' THEN 'IHC-H-Score_EGFR' ELSE NULL END
        WHEN 125 THEN CASE WHEN ihc_ia_ecad = 'X' THEN 'IHC-IA_ECAD' ELSE NULL END
        WHEN 126 THEN CASE WHEN ihc_ia_er = 'X' THEN 'IHC-IA_ER' ELSE NULL END
        WHEN 127 THEN CASE WHEN ihc_ia_her2__neu = 'X' THEN 'IHC-IA_Her2/Neu' ELSE NULL END
        WHEN 128 THEN CASE WHEN ihc_ia_ki67 = 'X' THEN 'IHC-IA_Ki67' ELSE NULL END
        WHEN 129 THEN CASE WHEN ihc_ia_p53 = 'X' THEN 'IHC-IA_P53' ELSE NULL END
        WHEN 130 THEN CASE WHEN ihc_ia_pr = 'X' THEN 'IHC-IA_PR' ELSE NULL END
        WHEN 131 THEN CASE WHEN TRIM(ngs_offering) != '' THEN CONCAT('NGS_OFFERING_', ngs_offering) ELSE NULL END
        ELSE NULL END as test_ordered_name,  -- test_ordered_name
        NULL,                                -- result_id
        NULL,                                -- result
        NULL,                                -- result_name
        NULL,                                -- result_unit_of_measure
        NULL,                                -- result_desc
        NULL,                                -- result_comments
        NULL,                                -- ref_range_low
        NULL,                                -- ref_range_high
        NULL,                                -- ref_range_alpha
        NULL,                                -- abnormal_flag
        NULL,                                -- fasting_status
        NULL,                                -- diagnosis_code
        NULL,                                -- diagnosis_code_qual
        NULL,                                -- diagnosis_code_priorty
        NULL,                                -- procedure_code
        NULL,                                -- procedure_code_qual
        NULL,                                -- lab_npi
        NULL,                                -- ordering_npi
        NULL,                                -- payer_id
        NULL,                                -- payer_id_qual
        NULL,                                -- payer_name
        NULL,                                -- payer_parent_name
        NULL,                                -- payer_org_name
        NULL,                                -- payer_plan_id
        NULL,                                -- payer_plan_name
        NULL,                                -- payer_type
        NULL,                                -- lab_other_id
        NULL,                                -- lab_other_qual
        NULL,                                -- ordering_other_id
        NULL,                                -- ordering_other_qual
        NULL,                                -- ordering_market_type
        NULL                                 -- ordering_specialty
    FROM raw_transactional t
        CROSS JOIN exploder
        LEFT JOIN matching_payload mp ON t.hv_key = mp.hvJoinKey
        LEFT JOIN zip3_to_state zip3 ON mp.threeDigitZip = zip3.zip3
        ) main
WHERE main.test_ordered_name IS NOT NULL

