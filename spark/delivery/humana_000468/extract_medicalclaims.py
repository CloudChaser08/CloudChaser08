def extract(runner, hvids, timestamp, start_dt, end_dt):
    t1 = runner.sqlContext.table('medicalclaims')
    t2 = runner.sqlContext.table('dw.ref_vdr_feed')
    t2 = t2[t2.hvm_tile_nm.isin(*SUPPLIERS)]
    
    t1.join(t2, t1['data_feed'] == t2['hvm_vdr_feed_id'], 'left_semi') \
        .join(hvids, t1['hvid'] == hvids['hvid'], 'left_semi')
        .where(t1['date_service'] < end_dt and t1['date_service'] > start_dt)
        .select(*EXTRACT_COLUMNS)

EXTRACT_COLUMNS = [
    'record_id',
    'claim_id',
    'hvid',
    'data_vendor',
    'patient_age',
    'claim_type',
    'date_received',
    'date_service',
    'date_service_end',
    'inst_type_of_bill_std_id',
    'place_of_service_std_id',
    'service_line_number',
    'diagnosis_code',
    'diagnosis_code_qual',
    'diagnosis_priority',
    'procedure_code',
    'procedure_code_qual',
    'procedure_units',
    'revenue_code',
    'line_charge',
    'total_charge',
    'hvm_tile_nm'
]

SUPPLIERS = [
    'Allscripts',
    'Practice Insight',
    'Private Source 14',
    'Private Source 34'
]
