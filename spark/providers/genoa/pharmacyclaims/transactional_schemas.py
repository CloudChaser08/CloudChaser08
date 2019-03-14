from spark.helpers.source_table import SourceTable

TABLE_CONF = {
    'genoa_rx_raw' : SourceTable(
        'csv',
        separator='|',
        columns=[
            'patient_last_name',
            'patient_first_name',
            'patient_date_of_birth',
            'patient_gender',
            'patient_address_zip',
            'member_id',
            'sub_client_id',
            'patient_address_line_1',
            'patient_address_city',
            'patient_address_state',
            'patient_id',
            'bin_number',
            'processor_control_number',
            'transaction_count_',
            'service_provider_id',
            'service_provider_id_qualifier',
            'date_of_service',
            'patient_location',
            'patient_id_qualifier',
            'prescriber_id',
            'prescriber_last_name',
            'prescriber_id_qualifier',
            'patient_relationship_code',
            'prescription_service_reference_number',
            'fill_number',
            'days_supply',
            'compound_code',
            'product_service_id',
            'number_of_refills_authorized',
            'product_service_id_qualifier',
            'quantity_dispensed',
            'prescription_service_reference_number_qualifier',
            'unit_of_measure',
            'ingredient_cost_paid',
            'dispensing_fee_paid',
            'total_amount_paid',
            'amount_of_copay_coinsurance',
            'pharmacy_location__postal_code_',
            'payer_id',
            'payer_id_qualifier',
            'plan_identification',
            'plan_name',
            'transaction_code', # new column 2017-11-01 
            'response_code', # new column 2017-11-01 
            'sales_key', # new column 2017-11-01 
            'hv_join_key'
        ]
    ),
}
