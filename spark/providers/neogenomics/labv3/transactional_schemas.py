from spark.helpers.source_table import SourceTable

TABLE_CONF = {
    'meta': SourceTable(
        'csv',
        separator='|',
        columns=[
            'header',
            'source',
            'patientid_hashed',
            'accession_number_hashed',
            'case_number_hashed',
            'test_orderid_hashed',
            'allowance',
            'bill_type',
            'billing_icd_codes',
            'billing_price',
            'biomarker_threshold',
            'body_site',
            'claim_id',
            'copay',
            'cpt_code',
            'cpt_modifier_id',
            'deductible',
            'denials_reason_code',
            'disease_stage',
            'disease_threshold',
            'insurance_company_id',
            'insurance_company_name',
            'level_of_service',
            'methodology',
            'methodology_threshold',
            'ordering_practice_address_line_1',
            'ordering_practice_city',
            'ordering_practice_name',
            'ordering_practice_state',
            'ordering_practice_zip_code',
            'ordering_provider_first_name',
            'ordering_provider_last_name',
            'ordering_provider_npi_number',
            'panel_code',
            'panel_name',
            'patient_pay',
            'payor_group_name',
            'payor_paid_amount',
            'performing_organization_address_line_1',
            'performing_organization_city',
            'performing_organization_name',
            'performing_organization_state',
            'performing_organization_zip_code',
            'plan_pay',
            'reason_for_refereal',
            'receiving_organization_address_line_1',
            'receiving_organization_city',
            'receiving_organization_name',
            'receiving_organization_state',
            'receiving_organization_zip_code',
            'referring_practice_address_line_1',
            'referring_practice_city',
            'referring_practice_name',
            'referring_practice_state',
            'referring_practice_zip_code',
            'referring_provider_first_name',
            'referring_provider_last_name',
            'referring_provider_npi_number',
            'billing_status',
            'revenue_code',
            'specimen_collection_date',
            'specimen_prep_method',
            'specimen_type',
            'test_manufacturer',
            'test_cancellation_date',
            'test_code',
            'test_created_date',
            'test_kit',
            'test_name',
            'test_instrument',
            'test_relevant_clinical_information',
            'test_report_date',
            'test_status',
            'upstream_reflux',
        ]
    ),
    'results': SourceTable(
        'csv',
        separator='|',
        columns=[
            'header',
            'source',
            'test_orderid_hashed',
            'result_code',
            'result_name',
            'result_comments',
            'result_reference_range',
            'result_units_of_measure',
            'result_value',
        ]
    )
}
