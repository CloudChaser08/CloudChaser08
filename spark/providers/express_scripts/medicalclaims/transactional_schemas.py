from spark.helpers.source_table import SourceTable

TABLE_CONF = {
    'transaction': SourceTable(
        'fixedwidth',
        trimmify_nullify=True,
        columns=[
            ('unique_patient_id', 15),
            ('medical_claim_id', 23),
            ('client_med_availability_col', 72),
            ('record_type', 3),
            ('claim_record_seq_num', 4),
            ('medical_code', 15),
            ('medical_qualifier_code', 2),
            ('place_of_service_code', 3),
            ('cpt_modifier_code_01', 2),
            ('cpt_modifier_code_02', 2),
            ('cpt_modifier_code_03', 2),
            ('cpt_modifier_code_04', 2),
            ('patient_status_code', 2),
            ('provider_first_name', 25),
            ('provider_middle_name', 25),
            ('provider_last_name', 35),
            ('provider_name_suffix_text', 10),
            ('professional_title_text', 15),
            ('provider_npi_number', 10),
            ('provider_street_addr_line_1', 40),
            ('provider_street_addr_line_2', 40),
            ('provider_city_name', 30),
            ('provider_state_or_province_code', 2),
            ('provider_postal_code', 15),
            ('provider_fax_number', 35),
            ('provider_phone_number', 15),
            ('first_serviced_date', 8),
            ('last_serviced_date', 8),
            ('transaction_code', 1),
            ('hvjoinkey', 36)
        ]
    )
}
