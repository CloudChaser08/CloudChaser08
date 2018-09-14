from spark.helpers.source_table import SourceTable

TABLE_CONF = {
    'auroradx_transactions' : SourceTable(
	'csv',
	separator='|',
	columns=[
	    'patient_object_id',
	    'accession_id',
	    'patient_first_name',
	    'patient_last_name',
	    'patient_middle_name',
	    'patient_address_line_1',
	    'patient_address_line_2',
	    'patient_city',
	    'patient_state',
	    'patient_zip_code',
	    'patient_social_security_number',
	    'patient_date_of_birth',
	    'patient_gender',
	    'bill_type',
	    'icd_code',
	    'icd_code_description',
	    'final_diagnosis',
	    'performing_lab_object_id',
	    'performing_lab_name',
	    'performing_lab_address_line_1',
	    'performing_lab_address_line_2',
	    'performing_lab_city',
	    'performing_lab_state',
	    'performing_lab_zip_code',
	    'ordering_practice_name',
	    'ordering_practice_id',
	    'ordering_practice_address_line_1',
	    'ordering_practice_address_line_2',
	    'ordering_practice_city',
	    'ordering_practice_state',
	    'ordering_practice_zipcode',
	    'ordering_provider_id',
	    'ordering_provider_npi',
	    'ordering_provider_first_name',
	    'ordering_provider_last_name',
	    'ordering_provider_middle_initial',
	    'ordering_provider_degree',
	    'ordering_provider_specialty',
	    'specimen_id',
	    'specimen_number',
	    'comment',
	    'microscopic_description',
	    'specimen_source',
	    'specimen_location',
	    'clinical_information',
	    'test_cpt_code',
	    'test_code',
	    'test_name',
	    'procedure_name',
	    'accession_datetime',
	    'date_of_service',
	    'received_datetime',
	    'final_signout_datetime',
	    'case_number',
	    'hvjoinkey',
	    'part_process_date'
	]
    ),
}
