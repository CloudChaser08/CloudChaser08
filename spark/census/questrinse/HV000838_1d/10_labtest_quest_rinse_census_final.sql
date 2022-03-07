SELECT
    MONOTONICALLY_INCREASING_ID()           AS record_id    ,
    *

FROM
(SELECT
    HV_claim_id                                             ,
    hvid                                                    ,
    created                                                 ,
    model_version                                           ,
    data_set                                                ,
    data_feed                                               ,
    data_vendor                                             ,
    HV_patient_gender                                       ,
    HV_patient_age                                          ,
    HV_patient_year_of_birth                                ,
    HV_patient_zip3                                         ,
    HV_patient_state                                        ,
    date_service                                            ,
    date_specimen                                           ,
    HV_date_report                                          ,
    loinc_code                                              ,
    hv_loinc_code                                           ,
    lab_id                                                  ,
    test_id                                                 ,
    HV_test_number                                          ,
    test_battery_local_id                                   ,
    test_battery_std_id                                     ,
    test_battery_name                                       ,
    test_ordered_local_id                                   ,
    test_ordered_std_id                                     ,
    test_ordered_name                                       ,
    result_id                                               ,
    result                                                  ,
    result_name                                             ,
    result_unit_of_measure                                  ,
    result_desc                                             ,
    HV_ref_range_alpha                                      ,
    HV_fasting_status                                       ,
    HV_s_diag_code_codeset_ind                              ,
    HV_procedure_code                                       ,
    HV_procedure_code_qual                                  ,
    HV_ordering_npi                                         ,
    payer_id                                                ,
    payer_name                                              ,
    lab_other_id                                            ,
    HV_lab_other_qual                                       ,
    ordering_market_type                                    ,
    ordering_specialty                                      ,
    ordering_state_license                                  ,
    ordering_upin                                           ,
    ordering_address_1                                      ,
    ordering_address_2                                      ,
    ordering_city                                           ,
    HV_ordering_state                                       ,
    ordering_zip                                            ,
    part_provider                                           ,
    HV_part_best_date                                       ,
    unique_accession_id                                     ,
    diag_accn_id                                            ,
    diag_date_of_service                                    ,
    diag_lab_code                                           ,
    acct_id                                                 ,
    acct_number                                             ,
    diag_dos_yyyymm                                         ,
    rslt_accn_id                                            ,
    lab_code                                                ,
    phy_id                                                  ,
    rslt_accession_number                                   ,
    accn_dom_id                                             ,
    cmdm_spclty_cd                                          ,
    acct_name                                               ,
    cmdm_licstate                                           ,
    billing_client_number                                   ,
    fasting_hours                                           ,
    qbs_ordering_client_num                                 ,
    date_order_entry                                        ,
    informed_consent_flag                                   ,
    legal_entity                                            ,
    specimen_type                                           ,
    ordering_site_code                                      ,
    canceled_accn_ind                                       ,
    copy_to_clns                                            ,
    non_physician_name                                      ,
    non_physician_id                                        ,
    long_description                                        ,
    phy_name                                                , --- JKS 2020-06-08
    suffix                                                  ,
    degree                                                  ,
    idw_analyte_code                                        ,
    qls_service_code                                        ,
    reportable_results_ind                                  ,
    ord_seq                                                 ,
    res_seq                                                 ,
    abnormal_ind                                            ,
    amended_report_ind                                      ,
    alpha_normal_flag                                       ,
    instrument_id                                           ,
    result_release_date                                     ,
    enterprise_ntc_code                                     ,
    derived_profile_code                                    ,
    qtim_as_ordered_code                                    ,
    qtim_profile_ind                                        ,
    rslt_dos_yyyymm                                         ,
    sister_lab                                              ,
    bill_type_cd                                            ,
    idw_report_change_status                                ,
    ins_seq                                                 ,
    active_ind                                              ,
    bill_code                                               ,
    insurance_billing_type                                  ,
    qbs_payor_cd                                            ,
    dos_id                                                  ,
    lab_name                                                ,
    lab_lis_type                                            ,
    confidential_order_ind                                  ,
    daard_client_flag                                       ,
    ptnt_accession_number                                   ,
    ptnt_date_of_service                                    ,
    ptnt_lab_code                                           ,
    CAST(accn_enterprise_id AS BIGINT) AS accn_enterprise_id,
    age_code                                                ,
    species                                                 ,
    pat_country                                             ,
    external_patient_id                                     ,
    CAST(pat_master_id AS BIGINT)      AS pat_master_id     ,
    lab_reference_number                                    ,
    room_number                                             ,
    bed_number                                              ,
    hospital_location                                       ,
    ward                                                    ,
    admission_date                                          ,
    health_id                                               ,
    CAST(pm_eid AS BIGINT)             AS pm_eid            ,
    CAST(idw_pm_email_address AS STRING) AS idw_pm_email_address ,
    CAST(date_reported   AS STRING)    AS date_reported     ,
    ----------------- 2020-05-28
    CAST(ref_range_low   AS DOUBLE)    AS ref_range_low     ,
    CAST(ref_range_high  AS DOUBLE)    AS ref_range_high    ,
    ------------------ 2020-05-28
    CAST(ref_range_alpha AS STRING)    AS ref_range_alpha   ,
-------------------------------------------------------------------------------------------------
---------- New fields added per request from QUEST 2020-06-08
-------------------------------------------------------------------------------------------------
    requisition_number                                      ,
    fasting_ind                                             ,
    cpt_code                                                ,
    npi                                                     ,
    phy_last_name                                           ,
    phy_first_name                                          ,
    phy_middle_name                                         ,
    acct_state                                              ,
-------------------------------------------------------------------------------------------------
---------- New fields added per request from QUEST 2020-06-17
-------------------------------------------------------------------------------------------------
    date_final_report                                       ,
    s_diag_code_codeset_ind                                 ,
-------------------------------------------------------------------------------------------------
---------- New fields added per request from QUEST 2020-10-27
-------------------------------------------------------------------------------------------------
    TRIM(HV_result_value_operator) AS HV_result_value_operator   ,
    TRIM(HV_result_value_numeric)  AS HV_result_value_numeric ,
    --------------- ONLY FOR AUTOATION
    --*--REPLACE(REPLACE(TRIM(HV_result_value_numeric), '[',''),']','') AS HV_result_value_numeric ,
    TRIM(REPLACE(HV_result_value_alpha, '"','')) AS HV_result_value_alpha,
    TRIM(HV_result_value) AS HV_result_value                   ,
-------------------------------------------------------------------------------------------------
---------- QTIM
-------------------------------------------------------------------------------------------------
    profile_name_qtim                                       ,
    order_name_qtim                                         ,
    specimen_type_desc_qtim                                 ,
    methodology_qtim                                        ,
    result_name_qtim                                        ,
    unit_of_measure_qtim                                    ,
    loinc_number_qtim                                       ,
    hv_abnormal_indicator                                   ,
    hv_provider_name_cmdm	,
    hv_ind_spclty_tp_cmdm	                                ,
    hv_ind_spclty_cd_cmdm	                                ,
    hv_ind_spclty_desc_cmdm	                                ,
    hv_ind_spclty_tp2_cmdm	                                ,
    hv_ind_spclty_cd2_cmdm	                                ,
    hv_ind_spclty_desc2_cmdm                                ,
    hv_ind_spclty_tp3_cmdm	                                ,
    hv_ind_spclty_cd3_cmdm	                                ,
    hv_ind_spclty_desc3_cmdm                                ,
    hv_act_spclty_tp_cmdm	                                ,
    hv_act_spclty_cd_cmdm	                                ,
    hv_act_spclty_desc_cmdm	                                ,
    client_acct_number
FROM labtest_quest_rinse_census_pre_final_01a
--FROM labtest_quest_rinse_census_pre_final_05
GROUP BY
1	,	2	,	3	,	4	,	5	,	6	,	7	,	8	,	9	,	10	,
11	,	12	,	13	,	14	,	15	,	16	,	17	,	18	,	19	,	20	,
21	,	22	,	23	,	24	,	25	,	26	,	27	,	28	,	29	,	30	,
31	,	32	,	33	,	34	,	35	,	36	,	37	,	38	,	39	,	40	,
41	,	42	,	43	,	44	,	45	,	46	,	47	,	48	,	49	,	50	,
51	,	52	,	53	,	54	,	55	,	56	,	57	,	58	,	59	,	60	,
61	,	62	,	63	,	64	,	65	,	66	,	67	,	68	,	69	,	70	,
71	,	72	,	73	,	74	,	75	,	76	,	77	,	78	,	79	,	80	,
81	,	82	,	83	,	84	,	85	,	86	,	87	,	88	,	89	,	90	,
91	,	92	,	93	,	94	,	95	,	96	,	97	,	98	,	99	,	100	,
101	,	102	,	103	,	104	,	105	,	106	,	107	,	108	,	109	,	110	,
111	,	112	,	113	,	114	,	115	,	116	,	117	,	118	,	119	,	120	,
121	,	122	,	123	,	124	,	125	,	126	,	127	,	128	,	129	,	130	,
131	,	132	,	133	,	134	,	135	,	136	,	137	,	138	,	139	,	140	,
141	,	142	,	143	,	144	,	145	,	146	,	147	,	148	,	149	,	150	,
151	,	152	,	153	,	154	,	155	,	156	,	157	,	158	,	159	,	160	,
161	,	162	,	163	,	164	,	165	,	166	,	167	,	168 ,   169
)
