INSERT INTO lab_common_model (
        claim_id,
        hvid,
        patient_gender,
        patient_age,
        patient_year_of_birth,
        patient_zip3,
        patient_state,
        test_ordered_name
        ) 
SELECT * FROM (
    SELECT t.customer__patient_id,
        COALESCE(pcm.parentId, mp.hvid),
        mp.gender,
        case
        when (extract('year' from CURRENT_DATE) - mp.yearOfBirth) > 90 
        then '90'
        else (extract('year' from CURRENT_DATE) - mp.yearOfBirth)
        end,
        mp.yearOfBirth,
        mp.threeDigitZip,
        zip3.state,
        CASE t.i
        WHEN 0 THEN CASE WHEN fish_cmet = 'X' THEN 'FISH_cMET' ELSE null END
        WHEN 1 THEN CASE WHEN fish_cmyc = 'X' THEN 'FISH_cMYC' ELSE null END
        WHEN 2 THEN CASE WHEN fish_egfr = 'X' THEN 'FISH_EGFR' ELSE null END
        WHEN 3 THEN CASE WHEN fish_her2__neu = 'X' THEN 'FISH_Her2/Neu' ELSE null END
        WHEN 4 THEN CASE WHEN fish_pik3ca = 'X' THEN 'FISH_PIK3CA' ELSE null END
        WHEN 5 THEN CASE WHEN fish_top2a = 'X' THEN 'FISH_TOP2A' ELSE null END
        WHEN 6 THEN CASE WHEN fish_mutational_1p19q = 'X' THEN 'FISH-Mutational_1p19q' ELSE null END
        WHEN 7 THEN CASE WHEN fish_mutational_alk = 'X' THEN 'FISH-Mutational_ALK' ELSE null END
        WHEN 8 THEN CASE WHEN fish_mutational_ros1 = 'X' THEN 'FISH-Mutational_ROS1' ELSE null END
        WHEN 9 THEN CASE WHEN cish_cmet = 'X' THEN 'CISH_cMET' ELSE null END
        WHEN 10 THEN CASE WHEN cish_egfr = 'X' THEN 'CISH_EGFR' ELSE null END
        WHEN 11 THEN CASE WHEN cish_her2__neu = 'X' THEN 'CISH_Her2/Neu' ELSE null END
        WHEN 12 THEN CASE WHEN cish_mdm2 = 'X' THEN 'CISH_MDM2' ELSE null END
        WHEN 13 THEN CASE WHEN cish_top2a = 'X' THEN 'CISH_TOP2A' ELSE null END
        WHEN 14 THEN CASE WHEN ihc_ae1__ae3 = 'X' THEN 'IHC_AE1/AE3' ELSE null END
        WHEN 15 THEN CASE WHEN ihc_afp = 'X' THEN 'IHC_AFP' ELSE null END
        WHEN 16 THEN CASE WHEN ihc_alk = 'X' THEN 'IHC_ALK' ELSE null END
        WHEN 17 THEN CASE WHEN ihc_ar = 'X' THEN 'IHC_AR' ELSE null END
        WHEN 18 THEN CASE WHEN ihc_bcl2 = 'X' THEN 'IHC_BCL2' ELSE null END
        WHEN 19 THEN CASE WHEN ihc_bcrp = 'X' THEN 'IHC_BCRP' ELSE null END
        WHEN 20 THEN CASE WHEN ihc_bombesin = 'X' THEN 'IHC_Bombesin' ELSE null END
        WHEN 21 THEN CASE WHEN ihc_brcp = 'X' THEN 'IHC_BRCP' ELSE null END
        WHEN 22 THEN CASE WHEN ihc_ca_19_9 = 'X' THEN 'IHC_CA 19.9' ELSE null END
        WHEN 23 THEN CASE WHEN ihc_calcitonin = 'X' THEN 'IHC_Calcitonin' ELSE null END
        WHEN 24 THEN CASE WHEN ihc_calret = 'X' THEN 'IHC_Calret' ELSE null END
        WHEN 25 THEN CASE WHEN ihc_calretinin = 'X' THEN 'IHC_Calretinin' ELSE null END
        WHEN 26 THEN CASE WHEN ihc_cam5_2 = 'X' THEN 'IHC_CAM5.2' ELSE null END
        WHEN 27 THEN CASE WHEN ihc_cav_1 = 'X' THEN 'IHC_CAV-1' ELSE null END
        WHEN 28 THEN CASE WHEN ihc_cd10 = 'X' THEN 'IHC_CD10' ELSE null END
        WHEN 29 THEN CASE WHEN ihc_cd20 = 'X' THEN 'IHC_CD20' ELSE null END
        WHEN 30 THEN CASE WHEN ihc_cd20__l26 = 'X' THEN 'IHC_CD20 (L26' ELSE null END
        WHEN 31 THEN CASE WHEN ihc_cd25 = 'X' THEN 'IHC_CD25' ELSE null END
        WHEN 32 THEN CASE WHEN ihc_cd30 = 'X' THEN 'IHC_CD30' ELSE null END
        WHEN 33 THEN CASE WHEN ihc_cd31 = 'X' THEN 'IHC_CD31' ELSE null END
        WHEN 34 THEN CASE WHEN ihc_cd34 = 'X' THEN 'IHC_CD34' ELSE null END
        WHEN 35 THEN CASE WHEN ihc_cd52 = 'X' THEN 'IHC_CD52' ELSE null END
        WHEN 36 THEN CASE WHEN ihc_cd56 = 'X' THEN 'IHC_CD56' ELSE null END
        WHEN 37 THEN CASE WHEN ihc_cd68 = 'X' THEN 'IHC_CD68' ELSE null END
        WHEN 38 THEN CASE WHEN ihc_cd99 = 'X' THEN 'IHC_CD99' ELSE null END
        WHEN 39 THEN CASE WHEN ihc_cdx2 = 'X' THEN 'IHC_CDX2' ELSE null END
        WHEN 40 THEN CASE WHEN ihc_cea = 'X' THEN 'IHC_CEA' ELSE null END
        WHEN 41 THEN CASE WHEN ihc_chromogranin = 'X' THEN 'IHC_Chromogranin' ELSE null END
        WHEN 42 THEN CASE WHEN ihc_ck14 = 'X' THEN 'IHC_CK14' ELSE null END
        WHEN 43 THEN CASE WHEN ihc_ck17 = 'X' THEN 'IHC_CK17' ELSE null END
        WHEN 44 THEN CASE WHEN ihc_ck19 = 'X' THEN 'IHC_CK19' ELSE null END
        WHEN 45 THEN CASE WHEN ihc_ck20 = 'X' THEN 'IHC_CK20' ELSE null END
        WHEN 46 THEN CASE WHEN ihc_ck5 = 'X' THEN 'IHC_CK5' ELSE null END
        WHEN 47 THEN CASE WHEN ihc_ck5__6 = 'X' THEN 'IHC_CK5/6' ELSE null END
        WHEN 48 THEN CASE WHEN ihc_ck7 = 'X' THEN 'IHC_CK7' ELSE null END
        WHEN 49 THEN CASE WHEN ihc_ck8 = 'X' THEN 'IHC_CK8' ELSE null END
        WHEN 50 THEN CASE WHEN ihc_c_kit = 'X' THEN 'IHC_c-kit' ELSE null END
        WHEN 51 THEN CASE WHEN ihc_cmet = 'X' THEN 'IHC_cMET' ELSE null END
        WHEN 52 THEN CASE WHEN ihc_cox2 = 'X' THEN 'IHC_COX2' ELSE null END
        WHEN 53 THEN CASE WHEN ihc_cyclin_d1 = 'X' THEN 'IHC_Cyclin-D1' ELSE null END
        WHEN 54 THEN CASE WHEN ihc_dog_1 = 'X' THEN 'IHC_DOG-1' ELSE null END
        WHEN 55 THEN CASE WHEN ihc_ebv = 'X' THEN 'IHC_EBV' ELSE null END
        WHEN 56 THEN CASE WHEN ihc_ecad = 'X' THEN 'IHC_ECAD' ELSE null END
        WHEN 57 THEN CASE WHEN ihc_egfr = 'X' THEN 'IHC_EGFR' ELSE null END
        WHEN 58 THEN CASE WHEN ihc_ema = 'X' THEN 'IHC_EMA' ELSE null END
        WHEN 59 THEN CASE WHEN ihc_er = 'X' THEN 'IHC_ER' ELSE null END
        WHEN 60 THEN CASE WHEN ihc_ercc1 = 'X' THEN 'IHC_ERCC1' ELSE null END
        WHEN 61 THEN CASE WHEN ihc_gcdfp = 'X' THEN 'IHC_GCDFP' ELSE null END
        WHEN 62 THEN CASE WHEN ihc_gcdfp__brst_ii = 'X' THEN 'IHC_GCDFP (BRST II' ELSE null END
        WHEN 63 THEN CASE WHEN ihc_gcdfp_15 = 'X' THEN 'IHC_GCDFP-15' ELSE null END
        WHEN 64 THEN CASE WHEN ihc_h3k36me3 = 'X' THEN 'IHC_H3K36me3' ELSE null END
        WHEN 65 THEN CASE WHEN ihc_hcg = 'X' THEN 'IHC_HCG' ELSE null END
        WHEN 66 THEN CASE WHEN ihc_her2__neu = 'X' THEN 'IHC_Her2/Neu' ELSE null END
        WHEN 67 THEN CASE WHEN ihc_hmb45 = 'X' THEN 'IHC_HMB45' ELSE null END
        WHEN 68 THEN CASE WHEN ihc_hsp90 = 'X' THEN 'IHC_HSP90' ELSE null END
        WHEN 69 THEN CASE WHEN ihc_igf1r = 'X' THEN 'IHC_IGF1R' ELSE null END
        WHEN 70 THEN CASE WHEN ihc_inhibin = 'X' THEN 'IHC_INHIBIN' ELSE null END
        WHEN 71 THEN CASE WHEN ihc_ki1 = 'X' THEN 'IHC_Ki1' ELSE null END
        WHEN 72 THEN CASE WHEN ihc_ki67 = 'X' THEN 'IHC_Ki67' ELSE null END
        WHEN 73 THEN CASE WHEN ihc_l26 = 'X' THEN 'IHC_L26' ELSE null END
        WHEN 74 THEN CASE WHEN ihc_lca = 'X' THEN 'IHC_LCA' ELSE null END
        WHEN 75 THEN CASE WHEN ihc_mammaglobin = 'X' THEN 'IHC_Mammaglobin' ELSE null END
        WHEN 76 THEN CASE WHEN ihc_mart_1 = 'X' THEN 'IHC_Mart-1' ELSE null END
        WHEN 77 THEN CASE WHEN ihc_melana = 'X' THEN 'IHC_MelanA' ELSE null END
        WHEN 78 THEN CASE WHEN ihc_melanoma_specific_antigen = 'X' THEN 'IHC_Melanoma Specific Antigen' ELSE null END
        WHEN 79 THEN CASE WHEN ihc_mgmt = 'X' THEN 'IHC_MGMT' ELSE null END
        WHEN 80 THEN CASE WHEN ihc_mitf = 'X' THEN 'IHC_MiTF' ELSE null END
        WHEN 81 THEN CASE WHEN ihc_mlh1 = 'X' THEN 'IHC_MLH1' ELSE null END
        WHEN 82 THEN CASE WHEN ihc_mrp1 = 'X' THEN 'IHC_MRP1' ELSE null END
        WHEN 83 THEN CASE WHEN ihc_msh2 = 'X' THEN 'IHC_MSH2' ELSE null END
        WHEN 84 THEN CASE WHEN ihc_msh6 = 'X' THEN 'IHC_MSH6' ELSE null END
        WHEN 85 THEN CASE WHEN ihc_nse = 'X' THEN 'IHC_NSE' ELSE null END
        WHEN 86 THEN CASE WHEN ihc_oc125 = 'X' THEN 'IHC_OC125' ELSE null END
        WHEN 87 THEN CASE WHEN ihc_p53 = 'X' THEN 'IHC_P53' ELSE null END
        WHEN 88 THEN CASE WHEN ihc_p63 = 'X' THEN 'IHC_p63' ELSE null END
        WHEN 89 THEN CASE WHEN ihc_p95 = 'X' THEN 'IHC_p95' ELSE null END
        WHEN 90 THEN CASE WHEN ihc_pap = 'X' THEN 'IHC_PAP' ELSE null END
        WHEN 91 THEN CASE WHEN ihc_pbrm1 = 'X' THEN 'IHC_PBRM1' ELSE null END
        WHEN 92 THEN CASE WHEN ihc_pd_1 = 'X' THEN 'IHC_PD-1' ELSE null END
        WHEN 93 THEN CASE WHEN ihc_pdgfr = 'X' THEN 'IHC_PDGFR' ELSE null END
        WHEN 94 THEN CASE WHEN ihc_pd_l1 = 'X' THEN 'IHC_PD-L1' ELSE null END
        WHEN 95 THEN CASE WHEN ihc_pgp = 'X' THEN 'IHC_PGP' ELSE null END
        WHEN 96 THEN CASE WHEN ihc_pms2 = 'X' THEN 'IHC_PMS2' ELSE null END
        WHEN 97 THEN CASE WHEN ihc_pr = 'X' THEN 'IHC_PR' ELSE null END
        WHEN 98 THEN CASE WHEN ihc_psa = 'X' THEN 'IHC_PSA' ELSE null END
        WHEN 99 THEN CASE WHEN ihc_psap = 'X' THEN 'IHC_PSAP' ELSE null END
        WHEN 100 THEN CASE WHEN ihc_pten = 'X' THEN 'IHC_PTEN' ELSE null END
        WHEN 101 THEN CASE WHEN ihc_rcc = 'X' THEN 'IHC_RCC' ELSE null END
        WHEN 102 THEN CASE WHEN ihc_rcca = 'X' THEN 'IHC_RCCA' ELSE null END
        WHEN 103 THEN CASE WHEN ihc_rrm1 = 'X' THEN 'IHC_RRM1' ELSE null END
        WHEN 104 THEN CASE WHEN ihc_rrm2 = 'X' THEN 'IHC_RRM2' ELSE null END
        WHEN 105 THEN CASE WHEN ihc_s100 = 'X' THEN 'IHC_S100' ELSE null END
        WHEN 106 THEN CASE WHEN ihc_sma = 'X' THEN 'IHC_SMA' ELSE null END
        WHEN 107 THEN CASE WHEN ihc_sparc = 'X' THEN 'IHC_SPARC' ELSE null END
        WHEN 108 THEN CASE WHEN ihc_sparc_monoclonal = 'X' THEN 'IHC_SPARC Monoclonal' ELSE null END
        WHEN 109 THEN CASE WHEN ihc_sparc_polyclonal = 'X' THEN 'IHC_SPARC Polyclonal' ELSE null END
        WHEN 110 THEN CASE WHEN ihc_survivin = 'X' THEN 'IHC_Survivin' ELSE null END
        WHEN 111 THEN CASE WHEN ihc_synaptophysin = 'X' THEN 'IHC_Synaptophysin' ELSE null END
        WHEN 112 THEN CASE WHEN ihc_thyroglobulin = 'X' THEN 'IHC_Thyroglobulin' ELSE null END
        WHEN 113 THEN CASE WHEN ihc_tle3 = 'X' THEN 'IHC_TLE3' ELSE null END
        WHEN 114 THEN CASE WHEN ihc_top2a = 'X' THEN 'IHC_TOP2A' ELSE null END
        WHEN 115 THEN CASE WHEN ihc_topo1 = 'X' THEN 'IHC_TOPO1' ELSE null END
        WHEN 116 THEN CASE WHEN ihc_ts = 'X' THEN 'IHC_TS' ELSE null END
        WHEN 117 THEN CASE WHEN ihc_ttf = 'X' THEN 'IHC_TTF' ELSE null END
        WHEN 118 THEN CASE WHEN ihc_ttf1 = 'X' THEN 'IHC_TTF1' ELSE null END
        WHEN 119 THEN CASE WHEN ihc_tubb3 = 'X' THEN 'IHC_TUBB3' ELSE null END
        WHEN 120 THEN CASE WHEN ihc_uchl_1 = 'X' THEN 'IHC_UCHL-1' ELSE null END
        WHEN 121 THEN CASE WHEN ihc_vegf = 'X' THEN 'IHC_VEGF' ELSE null END
        WHEN 122 THEN CASE WHEN ihc_vimentin = 'X' THEN 'IHC_Vimentin' ELSE null END
        WHEN 123 THEN CASE WHEN ihc_wt1 = 'X' THEN 'IHC_WT1' ELSE null END
        WHEN 124 THEN CASE WHEN ihc_h_score_egfr = 'X' THEN 'IHC-H-Score_EGFR' ELSE null END
        WHEN 125 THEN CASE WHEN ihc_ia_ecad = 'X' THEN 'IHC-IA_ECAD' ELSE null END
        WHEN 126 THEN CASE WHEN ihc_ia_er = 'X' THEN 'IHC-IA_ER' ELSE null END
        WHEN 127 THEN CASE WHEN ihc_ia_her2__neu = 'X' THEN 'IHC-IA_Her2/Neu' ELSE null END
        WHEN 128 THEN CASE WHEN ihc_ia_ki67 = 'X' THEN 'IHC-IA_Ki67' ELSE null END
        WHEN 129 THEN CASE WHEN ihc_ia_p53 = 'X' THEN 'IHC-IA_P53' ELSE null END
back here        WHEN 130 THEN CASE WHEN ihc_ia_pr = 'X' THEN 'IHC-IA_PR' ELSE null END
        ELSE null END as lab_name
    FROM (  
        SELECT * 
        FROM raw_transactional t
            CROSS JOIN (
            SELECT row_number() over (ORDER BY true) i 
            FROM raw_transactional
                )
            ) t
        LEFT JOIN matching_payload mp ON t.hv_key = mp.personId
        LEFT JOIN parent_child_map pcm ON mp.hvid = pcm.hvid
        LEFT JOIN zip3_to_state zip3 ON mp.threeDigitZip = zip3.zip3
        ) main
WHERE main.lab_name is not null

