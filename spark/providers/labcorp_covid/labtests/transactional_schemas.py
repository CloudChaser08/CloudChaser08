from spark.helpers.source_table import SourceTable

TABLE_CONF = {
    'txn': SourceTable(
        'csv',
        separator='|',
        confirm_schema=True,
        columns=[
            'patient_first_name'
            , 'patient_last_name'
            , 'patient_dob'
            , 'patient_sex'
            , 'patient_zip5'
            , 'patient_st_addr1'
            , 'patient_st_addr2'
            , 'patient_city'
            , 'patient_st'
            , 'member_id'
            , 'group_id'
            , 'patient_id'
            , 'test_name'
            , 'test_number'
            , 'loinc_code'
            , 'normal_dec_low'
            , 'normal_dec_high'
            , 'result_dec'
            , 'result_abn_code'
            , 'result_abbrev'
            , 'pat_dos'
            , 'perf_lab_code'
            , 'rslt_comments'
            , 'test_ordered_code'
            , 'test_ordered_name'
            , 'npi'
            , 'specialty_code'
            , 'report_zip'
            , 'accession_id'
            , 'hvjoinkey'
        ]
    )
}
