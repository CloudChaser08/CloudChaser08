dates = {
    'medicalclaims': [
        'date_service', 'inst_date_admitted', 'date_service_end', 'inst_date_discharged', 'date_received'
    ],
    'pharmacyclaims': ['date_service', 'date_authorized', 'date_written'],
    'emr_clin_obsn': ['enc_dt', 'clin_obsn_dt', 'clin_obsn_onset_dt', 'clin_obsn_resltn_dt'],
    'emr_diag': ['enc_dt', 'diag_dt', 'diag_onset_dt', 'diag_resltn_dt'],
    'emr_enc': ['enc_start_dt', 'enc_end_dt'],
    'emr_lab_ord': ['enc_dt', 'lab_ord_dt', 'lab_ord_test_schedd_dt', 'lab_ord_smpl_collctn_dt'],
    'emr_lab_result': [
        'enc_dt', 'lab_test_smpl_collctn_dt', 'lab_test_execd_dt', 'lab_result_dt', 'lab_test_schedd_dt'
    ],
    'emr_medctn': ['enc_dt', 'medctn_admin_dt', 'medctn_start_dt', 'medctn_end_dt', 'medctn_ord_dt'],
    'emr_proc': ['enc_dt', 'proc_dt'],
    'emr_prov_ord': [
        'enc_dt', 'prov_ord_dt', 'prov_ord_start_dt', 'prov_ord_end_dt', 'prov_ord_complt_dt',
        'prov_ord_result_rcvd_dt', 'prov_ord_edctn_dt'
    ],
    'emr_vit_sign': ['enc_dt', 'vit_sign_dt'],
    'emr': ['coalesced_emr_date'],
    'events': ['event_date', 'source_record_date'],
    'labtests': ['date_service', 'date_specimen', 'date_report']
}
