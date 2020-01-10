from spark.helpers.source_table import SourceTable

TABLE_CONF = {
    'txn': SourceTable(
        'csv',
        separator='|',
        columns=[
            'first_name',
            'last_name',
            'date_of_birth',
            'pt_zip_address',
            'patient_gender_code',
            'pt_street_address',
            'pt_city_address',
            'pt_state_address',
            'pt_phone_number',
            'pt_email',
            'cardholder_id',
            'group_id',
            'pcn',
            'switch_transaction_number',
            'date_of_fill',
            'date_rx_written',
            'date_of_injury',
            'date_switched',
            'type_column',
            'transaction_response_status',
            'reject_codes',
            'reject_codes_2',
            'reject_codes_3',
            'reject_codes_4',
            'reject_codes_5',
            'diagnosis_code',
            'diagnosis_code_qualifier',
            'product_service_id',
            'product_service_qualifier',
            'rx_number',
            'bin',
            'fill_number',
            'number_of_refills_authorized',
            'quantity_dispensed',
            'unit_of_measure',
            'days_supply',
            'pharmacy_npi',
            'service_number',
            'plan',
            'compound_code',
            'unit_dose_indicator',
            'daw_code',
            'rx_origin_code',
            'submission_clarification_code',
            'orig_prescribed_prod_svc_code',
            'orig_prescribed_prod_svc_qualifier',
            'original_prescribed_quantity',
            'pa_number',
            'level_of_service',
            'reason_for_service_code',
            'professional_service_code',
            'result_of_service_code',
            'prescriber_id',
            'prescriber_last_name',
            'prescriber_first_name',
            'prescriber_street_address',
            'prescriber_city_address',
            'prescriber_state_address',
            'prescriber_zip_address',
            'primary_care_provider_id',
            'cob_other_payment_count',
            'other_payer_amount_recognized_raw',
            'amount_applied_to_periodic_deductible',
            'amount_exceeding_periodic_benefit_maximum',
            'accumulated_deductible_paid',
            'remaining_deductible_amount',
            'remaining_benefit_amount',
            'amount_of_copay_coinsurance',
            'basis_cost_determination',
            'ingredient_cost',
            'dispensing_fee',
            'incentive_amount',
            'gross_amount_due',
            'professional_service_fee',
            'pt_paid_amount',
            'other_amount_claimed_qualifier',
            'other_amount_claimed_submitted',
            'basis_of_reimbursement_determination',
            'ingredient_cost_paid',
            'dispensing_fee_paid',
            'incentive_amount_paid',
            'total_amount_paid',
            'professional_service_fee_paid_raw',
            'patient_pay_amount',
            'other_amount_paid_qualifier',
            'other_amount_paid',
            'tax_exempt_indicator',
            'coupon_type',
            'coupon_number',
            'coupon_value_amount',
            'service_qualifier',
            'provider_id_duplicate',
            'provider_id_qualifier',
            'prescriber_id_duplicate',
            'prescriber_id_qualifier',
            'primary_care_provider_id_qualifier',
            'other_payer_coverage_type',
            'other_payer_id',
            'other_payer_id_qualifier',
            'other_payer_date',
            'other_coverage_code',
            'other_coverage_code_duplicate',
            'plan_description',
            'place_of_service_code',
            'hvjoinkey'
        ]
    )
}