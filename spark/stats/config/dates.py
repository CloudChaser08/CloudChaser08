dates = {
    'medicalclaims': [
        'date_service', 'inst_date_admitted', 'date_service_end', 'inst_date_discharged', 'date_received'
    ],
    'pharmacyclaims': ['date_service', 'date_authorized', 'date_written'],
    'emr_clin_obsn': ['hv_clin_obsn_dt'],
    'emr_diag': ['hv_diag_dt'],
    'emr_enc': ['hv_enc_dt'],
    'emr_medctn': ['hv_medctn_dt'],
    'emr_proc': ['hv_proc_dt'],
    'emr_prov_ord': ['hv_prov_ord_dt'],
    'emr_lab_test': ['hv_lab_test_dt'],
    'emr': ['coalesced_emr_date'],
    'events': ['event_date', 'source_record_date'],
    'labtests': ['date_service', 'date_specimen', 'date_report']
}
