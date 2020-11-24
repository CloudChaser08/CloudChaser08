from spark.helpers.source_table import SourceTable

TABLE_CONF = {
    'transaction': SourceTable(
        'fixedwidth',
        separator='|',
        columns=[
            ('creation_date', 8),
            ('creation_time', 6),
            ('version_number', 2),
            ('transaction_code', 2),
            ('processor_control_number', 10),
            ('service_provider_id', 15),
            ('service_provider_id_qualifier', 2),
            ('date_of_service', 8),
            ('date_of_birth', 8),
            ('patient_gender_code', 1),
            ('patient_first_name', 25),
            ('patient_last_name', 35),
            ('patient_street_address', 30),
            ('paitent_city', 30),
            ('patient_state', 2),
            ('patient_zip', 5),
            ('patient_id_qualifier', 2),
            ('patient_id', 20),
            ('provider_id', 15),
            ('provider_id_qualifier', 2),
            ('prescriber_id', 15),
            ('primary_care_provider_id', 15),
            ('prescriber_last_name', 25),
            ('prescriber_id_qualifier', 2),
            ('cardholder_id', 30),
            ('group_id', 18),
            ('patient_relationship_code', 1),
            ('prescription_service_reference_number', 12),
            ('fill_number', 5),
            ('days_supply', 5),
            ('compound_code', 1),
            ('product_service_id', 19),
            ('dispense_as_written', 1),
            ('date_prescription_written', 8),
            ('number_of_refills_authorized', 5),
            ('level_of_service', 2),
            ('unit_dose_indicator', 1),
            ('product_service_id_qualifier', 2),
            ('quantity_dispensed', 11),
            ('prescription_service_reference_number_qualifier', 1),
            ('unit_of_measure', 2),
            ('diagnosis_code', 15),
            ('diagnosis_code_qualifier', 2),
            ('response_code', 1),
            ('reject_code_1', 3),
            ('reject_code_2', 3),
            ('reject_code_3', 3),
            ('reject_code_4', 3),
            ('reject_code_5', 3),
            ('unique_patient_id', 15),
            ('pharmacy_claim_id', 44),
            ('pharmacy_claim_ref_id', 44),
            ('taxonomy_code', 10),
            ('hvjoinkey', 36)
        ]
    )
}
