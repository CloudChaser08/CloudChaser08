import pyspark.sql.functions as F

def extract_from_table(runner, hvids, timestamp, start_dt, end_dt, claims_table, filter_by_part_processdate=False):
    claims       = runner.sqlContext.table(claims_table).where("part_provider != 'xifin'")
    ref_vdr_feed = runner.sqlContext.table('dw.ref_vdr_feed')
    ref_vdr_feed = ref_vdr_feed[ref_vdr_feed.hvm_vdr_feed_id.isin(*SUPPLIER_FEED_IDS)]
    
    # Extract conditions
    ext = claims.join(ref_vdr_feed, claims['data_feed'] == ref_vdr_feed['hvm_vdr_feed_id'], 'inner') \
        .join(hvids, claims['hvid'] == hvids['hvid'], 'left') \
        .where(hvids['hvid'].isNotNull())

    if filter_by_part_processdate:
        ext = ext\
            .where(claims['part_best_date'] >= start_dt.isoformat())

    ext = ext \
        .where((claims['date_service'] <= end_dt.isoformat()) & (claims['date_service'] >= start_dt.isoformat())) \
        .select(*[claims[c] for c in claims.columns] + [ref_vdr_feed[c] for c in ref_vdr_feed.columns] + [hvids['humana_group_id']])

    # Hashing
    ext = ext.withColumn('hvid', F.md5(F.concat(F.col('hvid'), F.lit('hvid'), F.lit('hv000468'), F.lit(repr(timestamp)), F.col('humana_group_id')))) \
            .withColumn('prov_rendering_npi', F.md5(F.concat(F.col('prov_rendering_npi'), F.lit('npi'), F.lit('hv000468'), F.lit(repr(timestamp)), F.col('humana_group_id')))) \
            .withColumn('prov_billing_npi', F.md5(F.concat(F.col('prov_billing_npi'), F.lit('npi'), F.lit('hv000468'), F.lit(repr(timestamp)), F.col('humana_group_id')))) \
            .withColumn('prov_referring_npi', F.md5(F.concat(F.col('prov_referring_npi'), F.lit('npi'), F.lit('hv000468'), F.lit(repr(timestamp)), F.col('humana_group_id')))) \
            .withColumn('prov_facility_npi', F.md5(F.concat(F.col('prov_facility_npi'), F.lit('npi'), F.lit('hv000468'), F.lit(repr(timestamp)), F.col('humana_group_id'))))

    # Rename columns
    ext = ext.withColumn('data_feed', F.col('hvm_vdr_feed_id')) \
            .withColumn('data_vendor', F.col('hvm_tile_nm'))

    # NULL columns
    for c in NULL_COLUMNS:
        ext = ext.withColumn(c, F.lit(None).cast('string'))

    # Reorder
    return ext.select(*EXTRACT_COLUMNS)

def extract(runner, hvids, timestamp, start_dt, end_dt):
    return extract_from_table(runner, hvids, timestamp, start_dt, end_dt, 'dw.hvm_medicalclaims_v08', True).union(
        extract_from_table(runner, hvids, timestamp, start_dt, end_dt, 'synthetic_medicalclaims', False)
    )

EXTRACT_COLUMNS = [
    'record_id',
    'claim_id',
    'hvid',                     # Hashed
    'created',
    'model_version',
    'data_set',
    'data_feed',                # Feed ID#
    'data_vendor',              # Marketplace tile name
    'source_version',
    'patient_gender',           # NULL per Austin
    'patient_age',              # NULL, was age-group
    'patient_year_of_birth',    # NULL per Austin
    'patient_zip3',             # NULL per Austin
    'patient_state',            # NULL per Austin
    'claim_type',
    'date_received',
    'date_service',
    'date_service_end',
    'inst_date_admitted',
    'inst_date_discharged',
    'inst_admit_type_std_id',
    'inst_admit_type_vendor_id',
    'inst_admit_type_vendor_desc',
    'inst_admit_source_std_id',
    'inst_admit_source_vendor_id',
    'inst_admit_source_vendor_desc',
    'inst_discharge_status_std_id',
    'inst_discharge_status_vendor_id',
    'inst_discharge_status_vendor_desc',
    'inst_type_of_bill_std_id',
    'inst_type_of_bill_vendor_id',
    'inst_type_of_bill_vendor_desc',
    'inst_drg_std_id',
    'inst_drg_vendor_id',
    'inst_drg_vendor_desc',
    'place_of_service_std_id',
    'place_of_service_vendor_id',
    'place_of_service_vendor_desc',
    'service_line_number',
    'diagnosis_code',
    'diagnosis_code_qual',
    'diagnosis_priority',
    'admit_diagnosis_ind',
    'procedure_code',
    'procedure_code_qual',
    'principal_proc_ind',
    'procedure_units_billed',   # Renamed from "procedure_units" in model v3
    'procedure_modifier_1',
    'procedure_modifier_2',
    'procedure_modifier_3',
    'procedure_modifier_4',
    'revenue_code',
    'ndc_code',
    'medical_coverage_type',
    'line_charge',
    'line_allowed',
    'total_charge',
    'total_allowed',
    'prov_rendering_npi',       # Hashed per Austin
    'prov_billing_npi',         # Hashed per Austin
    'prov_referring_npi',       # Hashed per Austin
    'prov_facility_npi',        # Hashed per Austin
    'payer_vendor_id',
    'payer_name',               # NULL per Austin
    'payer_parent_name',        # NULL per Austin
    'payer_org_name',           # NULL per Austin
    'payer_plan_id',
    'payer_plan_name',          # NULL per Austin
    'payer_type',
    'prov_rendering_vendor_id',        # NULL per Austin
    'prov_rendering_tax_id',           # NULL per Austin
    'prov_rendering_dea_id',           # NULL per Austin
    'prov_rendering_ssn',              # NULL per Austin
    'prov_rendering_state_license',    # NULL per Austin
    'prov_rendering_upin',             # NULL per Austin
    'prov_rendering_commercial_id',    # NULL per Austin
    'prov_rendering_name_1',           # NULL per Austin
    'prov_rendering_name_2',           # NULL per Austin
    'prov_rendering_address_1',        # NULL per Austin
    'prov_rendering_address_2',        # NULL per Austin
    'prov_rendering_city',             # NULL per Austin
    'prov_rendering_state',            # NULL per Austin
    'prov_rendering_zip',              # NULL per Austin
    'prov_rendering_std_taxonomy',     # NULL per Austin
    'prov_rendering_vendor_specialty', # NULL per Austin
    'prov_billing_vendor_id',          # NULL per Austin
    'prov_billing_tax_id',             # NULL per Austin
    'prov_billing_dea_id',             # NULL per Austin
    'prov_billing_ssn',                # NULL per Austin
    'prov_billing_state_license',      # NULL per Austin
    'prov_billing_upin',               # NULL per Austin
    'prov_billing_commercial_id',      # NULL per Austin
    'prov_billing_name_1',             # NULL per Austin
    'prov_billing_name_2',             # NULL per Austin
    'prov_billing_address_1',          # NULL per Austin
    'prov_billing_address_2',          # NULL per Austin
    'prov_billing_city',               # NULL per Austin
    'prov_billing_state',              # NULL per Austin
    'prov_billing_zip',                # NULL per Austin
    'prov_billing_std_taxonomy',       # NULL per Austin
    'prov_billing_vendor_specialty',   # NULL per Austin
    'prov_referring_vendor_id',        # NULL per Austin
    'prov_referring_tax_id',           # NULL per Austin
    'prov_referring_dea_id',           # NULL per Austin
    'prov_referring_ssn',              # NULL per Austin
    'prov_referring_state_license',    # NULL per Austin
    'prov_referring_upin',             # NULL per Austin
    'prov_referring_commercial_id',    # NULL per Austin
    'prov_referring_name_1',           # NULL per Austin
    'prov_referring_name_2',           # NULL per Austin
    'prov_referring_address_1',        # NULL per Austin
    'prov_referring_address_2',        # NULL per Austin
    'prov_referring_city',             # NULL per Austin
    'prov_referring_state',            # NULL per Austin
    'prov_referring_zip',              # NULL per Austin
    'prov_referring_std_taxonomy',     # NULL per Austin
    'prov_referring_vendor_specialty', # NULL per Austin
    'prov_facility_vendor_id',         # NULL per Austin
    'prov_facility_tax_id',            # NULL per Austin
    'prov_facility_dea_id',            # NULL per Austin
    'prov_facility_ssn',               # NULL per Austin
    'prov_facility_state_license',     # NULL per Austin
    'prov_facility_upin',              # NULL per Austin
    'prov_facility_commercial_id',     # NULL per Austin
    'prov_facility_name_1',            # NULL per Austin
    'prov_facility_name_2',            # NULL per Austin
    'prov_facility_address_1',         # NULL per Austin
    'prov_facility_address_2',         # NULL per Austin
    'prov_facility_city',              # NULL per Austin
    'prov_facility_state',             # NULL per Austin
    'prov_facility_zip',               # NULL per Austin
    'prov_facility_std_taxonomy',      # NULL per Austin
    'prov_facility_vendor_specialty',  # NULL per Austin
    'cob_payer_vendor_id_1',
    'cob_payer_seq_code_1',
    'cob_payer_hpid_1',
    'cob_payer_claim_filing_ind_code_1',
    'cob_ins_type_code_1',
    'cob_payer_vendor_id_2',
    'cob_payer_seq_code_2',
    'cob_payer_hpid_2',
    'cob_payer_claim_filing_ind_code_2',
    'cob_ins_type_code_2',
    'humana_group_id'
]

# Feed tile names as of 09/05
SUPPLIER_FEED_IDS = [
    '26', # Veradigm Health
    '25', # Veradigm Health
    '22', # Private Source 12
    '15', # Private Source 14
    '24', # Private Source 34
    '35' # Private Source 42
]

NULL_COLUMNS = [
    'patient_gender',
    'patient_age',
    'patient_year_of_birth',
    'patient_zip3',
    'patient_state',
    'payer_name',
    'payer_parent_name',
    'payer_org_name',
    'payer_plan_name',
    'prov_rendering_vendor_id',
    'prov_rendering_tax_id',
    'prov_rendering_dea_id',
    'prov_rendering_ssn',
    'prov_rendering_state_license',
    'prov_rendering_upin',
    'prov_rendering_commercial_id',
    'prov_rendering_name_1',
    'prov_rendering_name_2',
    'prov_rendering_address_1',
    'prov_rendering_address_2',
    'prov_rendering_city',
    'prov_rendering_state',
    'prov_rendering_zip',
    'prov_rendering_std_taxonomy',
    'prov_rendering_vendor_specialty',
    'prov_billing_vendor_id',
    'prov_billing_tax_id',
    'prov_billing_dea_id',
    'prov_billing_ssn',
    'prov_billing_state_license',
    'prov_billing_upin',
    'prov_billing_commercial_id',
    'prov_billing_name_1',
    'prov_billing_name_2',
    'prov_billing_address_1',
    'prov_billing_address_2',
    'prov_billing_city',
    'prov_billing_state',
    'prov_billing_zip',
    'prov_billing_std_taxonomy',
    'prov_billing_vendor_specialty',
    'prov_referring_vendor_id',
    'prov_referring_tax_id',
    'prov_referring_dea_id',
    'prov_referring_ssn',
    'prov_referring_state_license',
    'prov_referring_upin',
    'prov_referring_commercial_id',
    'prov_referring_name_1',
    'prov_referring_name_2',
    'prov_referring_address_1',
    'prov_referring_address_2',
    'prov_referring_city',
    'prov_referring_state',
    'prov_referring_zip',
    'prov_referring_std_taxonomy',
    'prov_referring_vendor_specialty',
    'prov_facility_vendor_id',
    'prov_facility_tax_id',
    'prov_facility_dea_id',
    'prov_facility_ssn',
    'prov_facility_state_license',
    'prov_facility_upin',
    'prov_facility_commercial_id',
    'prov_facility_name_1',
    'prov_facility_name_2',
    'prov_facility_address_1',
    'prov_facility_address_2',
    'prov_facility_city',
    'prov_facility_state',
    'prov_facility_zip',
    'prov_facility_std_taxonomy',
    'prov_facility_vendor_specialty'
]
