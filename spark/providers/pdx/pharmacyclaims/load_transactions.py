import spark.helpers.postprocessor as postprocessor
import spark.helpers.records_loader as records_loader

def load(runner, input_path_prefix):
    '''
    Load in the transactions to an in memory table.
    '''
    for table, columns in TABLES.items():
        df = records_loader \
                .load(runner, input_path_prefix, columns, 'csv', '|')

        postprocessor.compose(
            postprocessor.trimmify,
            postprocessor.nullify
        )(df).createOrReplaceTempView(table)


TABLES = {
    'pdx_transactions': [
	       'pharmacy_ncpdp_number',
	       'supplier_defined_store_number',
	       'mail_service_number',
	       'pharmacy_zip_code',
	       'prescription_number_filler',
	       'date_written',
	       'date_filled',
	       'date_delivered_fulfilled',
	       'new_refill_counter',
	       'dispensed_ndc_number',
	       'dispensed_drug_name',
	       'prescribed_ndc_number',
	       'prescribed_drug_name',
	       'partial_fill_status',
	       'quantity_dispensed',
	       'days_supply',
	       'ingredient_cost',
	       'ingredient_cost_paid',
	       'dispensing_fee',
	       'dispensing_fee_paid',
	       'copay_coinsurance_amount',
	       'total_amount_paid_by_patient',
	       'reimbursed_amount',
	       'basis_of_ingredient_cost_submitted',
	       'basis_of_ingredient_cost_reimbursed',
	       'total_transaction_price_collected',
	       'payment_type',
	       'indicator_for_coupon_type',
	       'coupon_face_value',
	       'coupon_id',
	       'indicator_for_vouchers_free_goods_discounts_or_indigent_programs',
	       'plan_code',
	       'indicator_for_discount_cash_card_program',
	       'group_number',
	       'bank_identification_number',
	       'number_of_refills_authorized',
	       'refills_remaining',
	       'dispensed_as_written',
	       'prescriber_last_name',
	       'prescriber_first_name',
	       'prescriber_middle_initial',
	       'prescriber_dea_number',
	       'state_license_number_for_prescriber',
	       'state_of_assigned_number_for_prescriber',
	       'prescriber_npi',
	       'prescriber_city',
	       'prescriber_state',
	       'prescriber_zip_code',
	       'prescriber_phone_number',
	       'pharmacy_npi',
	       'patient_birth_year',
	       'coordination_of_benefits_counter',
	       'patient_gender_code',
	       'patient_zip_code',
	       'patient_location_code',
	       'deidentified_patient_code',
	       'origin_of_rx',
	       'indicator_for_e_prescribed_transaction',
	       'level_of_service',
	       'central_fill_flag',
	       'claim_indicator',
	       'processor_control_number',
	       'compound_code',
	       'chain_code',
	       'diagnosis_code',
	       'injectable_units_dispensed',
	       'hin_number',
	       'medicare_part_d_flag',
	       'refill_change_flag',
	       'unit_of_measure',
	       'medical_benefit_claim_indicator',
	       'filler',
	       'prescriber_id_qualifier',
	       'person_code',
	       'patient_relationship_code',
	       'ncpdp_patient_id_qualifier',
	       'cardholder_id',
	       'ncpdp_patient_id',
	       'patient_last_name',
	       'patient_first_name',
	       'patient_street_address',
	       'patient_zip',
	       'patient_date_of_birth',
               'hvJoinKey'
    ]
}

