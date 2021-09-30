"""
8451 hvXXXXXX rx records schemas
"""
from spark.helpers.source_table import SourceTable

TABLE_CONF = {
    '8451_transactions': SourceTable(
        'csv',
        separator='|',
        columns=[
            'med_identity_id',
            'first_name',
            'last_name',
            'year_of_birth',
            'patient_age',
            'birth_date',
            'gender_code',
            'address_line1_name',
            'address_line2_name',
            'address_city_name',
            'address_state_prov_code',
            'address_postal_code',
            'benefit_group_code',
            'med_phm_fill_code',
            'card_code',
            'primary_processor_control_number_code',
            'secondary_processor_control_number_code',
            'primary_bank_industry_number_code',
            'secondary_bank_industry_number_code',
            'med_phm_fill_fid',
            'rx_number',
            'refill_authorization_amount',
            'date_ordered',
            'date_cancelled',
            'transaction_date',
            'date_sold',
            'product_prescribed',
            'product_dispensed',
            'dispense_as_written_description',
            'generic_substitution_description',
            'refill_sequence_number',
            'prescribed_quantity',
            'dispensed_quantity',
            'intended_duration',
            'actual_duration',
            'prescriber_code',
            'pharmacy_code',
            'pharmacy_zip',
            'return_to_stock_flag',
            'fill_status_code',
            'partial_fill_code',
            'government_funded_flag',
            'total_patient_pay_amt',
            'total_pay_amt',
            'cash_override_amt',
            'division_id',
            'prescription_code',
            'prescribed_product_commodity',
            'prescribed_product_subcommodity',
            'prescribed_product_therapeutic_class_code',
            'prescribed_product_therapeutic_class_description',
            'prescribed_product_generic_flag',
            'prescribed_product_generic_name',
            'prescribed_product_maintenance_drug_flag',
            'prescribed_product_compound_code',
            'prescribed_product_strength_code',
            'prescribed_product_strength_quantity',
            'prescribed_product_ndc',
            'dispensed_product_description',
            'dispensed_product_commodity',
            'dispensed_product_subcommodity',
            'dispensed_product_therapeutic_class_code',
            'dispensed_product_therapeutic_class_description',
            'dispensed_product_generic_flag',
            'dispensed_product_generic_name',
            'dispensed_product_maintenance_drug_flag',
            'dispensed_product_compound_code',
            'dispensed_product_strength_code',
            'dispensed_product_strength_quantity',
            'dispensed_product_ndc',
            'pdc_diabetes',
            'pdc_rasa',
            'pdc_statins',
            'payer_type',
            'hvjoinkey'
        ]
    ),
}

PDC_SCHEMA = SourceTable(
    'csv',
    separator='|',
    columns=[
        'med_phm_fill_fid',
        'pdc_diabetes',
        'pdc_rasa',
        'pdc_statins'
    ]
)
