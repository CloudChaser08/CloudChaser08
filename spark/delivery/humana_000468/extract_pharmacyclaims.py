def extract(runner, hvids, timestamp, start_dt, end_dt):
    t1 = runner.sqlContext.table('pharmacyclaims')
    t2 = runner.sqlContext.table('dw.ref_vdr_feed')
    t2 = t2[t2.hvm_tile_nm.isin()]
    
    return t1.join(t2, t1['data_feed'] == t2['hvm_vdr_feed_id'], 'left_semi') \
        .join(hvids, t1['hvid'] == hvids['hvid'], 'left_semi')
        .where(t1['date_service'] < end_dt and t1['date_service'] > start_dt)
        .select(*EXTRACT_COLUMNS)

EXTRACT_COLUMNS = [
    'record_id',
    'claim_id',
    'hvid',
    'date_service',
    'ndc_code',
    'dispensed_quantity',
    'days_supply',
    'prov_dispensing_npi',
    'created',
    'hvm_tile_nm'
]

SUPPLIERS = [
    'Private Source 17',
    'Private Source 22',
    'PDX, Inc.'
]
