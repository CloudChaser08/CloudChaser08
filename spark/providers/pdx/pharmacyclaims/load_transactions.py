import spark.helpers.postprocessor as postprocessor
from pyspark.sql.functions import input_file_name

def load(runner, input_path_prefix):
    '''
    Load in the transactions to an in memory table.
    '''
    for table, columns in TABLES.items():
        df = runner.sqlContext.read.text(input_path_prefix)

        cols = []
        index = 1
        for col, width in columns:
            cols.append(df.value.substr(index, width).alias(col))
            index = index + width

        df = df.select(*cols).withColumn('data_set', input_file_name()).cache()
        df.count()

        postprocessor.compose(
            postprocessor.trimmify,
            postprocessor.nullify
        )(df).createOrReplaceTempView(table)


TABLES = {
    'pdx_transactions': [
        ('pharmacy_ncpdp_number', 7),
        ('supplier_defined_store_number', 7),
        ('mail_service_number', 4),
        ('pharmacy_zip_code', 9),
        ('prescription_number_filler', 15),
        ('date_written', 8),
        ('date_filled', 8),
        ('date_delivered_fulfilled', 8),
        ('new_refill_counter', 2),
        ('dispensed_ndc_number', 11),
        ('dispensed_drug_name', 28),
        ('prescribed_ndc_number', 11),
        ('prescribed_drug_name', 28),
        ('partial_fill_status', 1),
        ('quantity_dispensed', 10),
        ('days_supply', 3),
        ('ingredient_cost', 8),
        ('ingredient_cost_paid', 8),
        ('dispensing_fee', 8),
        ('dispensing_fee_paid', 8),
        ('copay_coinsurance_amount', 8),
        ('total_amount_paid_by_patient', 8),
        ('reimbursed_amount', 8),
        ('basis_of_ingredient_cost_submitted', 1),
        ('basis_of_ingredient_cost_reimbursed', 1),
        ('total_transaction_price_collected', 8),
        ('payment_type', 1),
        ('indicator_for_coupon_type', 2),
        ('coupon_face_value', 8),
        ('coupon_id', 15),
        ('indicator_for_vouchers_free_goods_discounts_or_indigent_programs', 2),
        ('plan_code', 9),
        ('indicator_for_discount_cash_card_program', 2),
        ('group_number', 15),
        ('bank_identification_number', 6),
        ('number_of_refills_authorized', 2),
        ('refills_remaining', 2),
        ('dispensed_as_written', 1),
        ('prescriber_last_name', 15),
        ('prescriber_first_name', 10),
        ('prescriber_middle_initial', 1),
        ('prescriber_dea_number', 9),
        ('state_license_number_for_prescriber', 15),
        ('state_of_assigned_number_for_prescriber', 2),
        ('prescriber_npi', 10),
        ('prescriber_city', 15),
        ('prescriber_state', 2),
        ('prescriber_zip_code', 5),
        ('prescriber_phone_number', 10),
        ('pharmacy_npi', 10),
        ('patient_birth_year', 4),
        ('coordination_of_benefits_counter', 2),
        ('patient_gender_code', 1),
        ('patient_zip_code', 5),
        ('patient_location_code', 2),
        ('deidentified_patient_code', 40),
        ('origin_of_rx', 1),
        ('indicator_for_e_prescribed_transaction', 1),
        ('level_of_service', 1),
        ('central_fill_flag', 1),
        ('claim_indicator', 1),
        ('processor_control_number', 10),
        ('compound_code', 1),
        ('chain_code', 2),
        ('diagnosis_code', 10),
        ('injectable_units_dispensed', 8),
        ('hin_number', 9),
        ('medicare_part_d_flag', 1),
        ('refill_change_flag', 1),
        ('unit_of_measure', 2),
        ('medical_benefit_claim_indicator', 1),
        ('filler', 6),
        ('prescriber_id_qualifier', 2),
        ('person_code', 3),
        ('patient_relationship_code', 1),
        ('ncpdp_patient_id_qualifier', 2),
        ('cardholder_id', 20),
        ('ncpdp_patient_id', 20),
        ('patient_last_name', 15),
        ('patient_first_name', 12),
        ('patient_street_address', 30),
        ('patient_zip', 5),
        ('patient_date_of_birth', 10),
        ('hvJoinKey', 36)
    ]
}
