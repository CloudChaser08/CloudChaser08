from spark.helpers.source_table import SourceTable

TABLE_CONF = {
    'ALLERGEN': SourceTable(
        'csv',
        separator='|',
        columns=[
            'allergen_id',
            'description'
        ]
    ),
    'ALLERGY': SourceTable(
        'csv',
        separator='|',
        columns=[
            'allergy_id',
            'allergen_id',
            'patient_id',
            'medication_id',
            'start_date',
            'stop_date',
            'created_at',
            'is_active'
        ]
    ),
    'APPOINTMENT': SourceTable(
        'csv',
        separator='|',
        columns=[
            'appointment_id',
            'transcript_id',
            'patient_id',
            'provider_id',
            'start_time',
            'end_time',
            'is_cancelled',
            'appointment_type'
        ]
    ),
    'DIAGNOSIS': SourceTable(
        'csv',
        separator='|',
        columns=[
            'diagnosis_id',
            'patient_id',
            'provider_id',
            'icd9',
            'is_active',
            'start_date',
            'start_date_source',
            'stop_date',
            'last_modified',
            'created_at'
        ]
    ),
    'DIAGNOSIS_ICD10': SourceTable(
        'csv',
        separator='|',
        columns=[
            'diagnosis_id',
            'icd10'
        ]
    ),
    'DIAGNOSIS_ICD9': SourceTable(
        'csv',
        separator='|',
        columns=[
            'diagnosis_id',
            'icd9'
        ]
    ),
    'DIAGNOSIS_SNOMED': SourceTable(
        'csv',
        separator='|',
        columns=[
            'diagnosis_id',
            'concept_id',
            'source'
        ]
    ),
    'ENCCAT': SourceTable(
        'csv',
        separator='|',
        columns=[
            'enccat_id',
            'name'
        ]
    ),
    'ENCOUNTER': SourceTable(
        'csv',
        separator='|',
        columns=[
            'encounter_id',
            'transcript_id',
            'enctype_id',
            'reason_code_id',
            'result_code_id',
            'result_value',
            'created_at'
        ]
    ),
    'ENCOUNTEREVENTREASONCODE': SourceTable(
        'csv',
        separator='|',
        columns=[
            'reason_code_id',
            'reasondescription',
            'concept_id'
        ]
    ),
    'ENCOUNTEREVENTRESULTCODE': SourceTable(
        'csv',
        separator='|',
        columns=[
            'result_code_id',
            'resultdescription',
            'concept_id'
        ]
    ),
    'ENCTYPE': SourceTable(
        'csv',
        separator='|',
        columns=[
            'enctype_id',
            'enccat_id',
            'code_id',
            'code',
            'code_name',
            'is_active',
            'name'
        ]
    ),
    'E_ICD10': SourceTable(
        'csv',
        separator='|',
        columns=[
            'icd10',
            'icd10_noperiod',
            'billable',
            'short_name',
            'long_name',
            'vers'
        ]
    ),
    'E_ICD9': SourceTable(
        'csv',
        separator='|',
        columns=[
            'icd9',
            'icd9_noperiod',
            'billable',
            'short_name',
            'long_name',
            'vers'
        ]
    ),
    'E_LOINC': SourceTable(
        'csv',
        separator='|',
        columns=[
            'loinc_num',
            'component',
            'property',
            'time_aspct',
            'system',
            'scale_typ',
            'method_typ',
            'class',
            'last_modified',
            'chng_type',
            'comments',
            'status',
            'consumer_name',
            'molar_mass',
            'classtype',
            'formula',
            'species',
            'exmpl_answers',
            'survey_quest_text',
            'survey_quest_src',
            'unitsrequired',
            'submitted_units',
            'relatednames2',
            'shortname',
            'order_obs',
            'cdisc_common_tests',
            'hl7_field_subfield_id',
            'external_copyright_notice',
            'example_units',
            'long_common_name',
            'hl7_v2_datatype',
            'hl7_v3_datatype',
            'curated_range_and_units',
            'document_section',
            'example_ucum_units',
            'example_si_ucum_units',
            'status_reason',
            'status_text',
            'change_reason_public',
            'common_test_rank',
            'common_order_rank',
            'common_si_test_rank',
            'hl7_attachment_structure'
        ]
    ),
    'E_SNOMED_CONCEPT': SourceTable(
        'csv',
        separator='|',
        columns=[
            'conceptid',
            'conceptstatus',
            'fullyspecifiedname',
            'ctv3id',
            'snomedid',
            'isprimitive'
        ]
    ),
    'INSURANCE': SourceTable(
        'csv',
        separator='|',
        columns=[
            'insurance_id',
            'patient_id',
            'payer_id',
            'insuranceplan_id',
            'created_at',
            'order_of_benefit',
            'insurance_payment_type',
            'is_active',
            'last_modified'
        ]
    ),
    'INSURANCEPLAN': SourceTable(
        'csv',
        separator='|',
        columns=[
            'insuranceplan_id',
            'payer_id',
            'insuranceplan_name',
            'insurance_plan_type',
            'insurance_payment_type_option',
            'practice_id',
            'is_active'
        ]
    ),
    'LAB_RESULT': SourceTable(
        'csv',
        separator='|',
        columns=[
            'laborder_id',
            'patient_id',
            'provider_id',
            'vendor_id',
            'loinc_num',
            'result_status',
            'report_date',
            'observed_at',
            'obs_quan',
            'obs_qual',
            'unit',
            'is_abnormal',
            'created_at',
            'last_modified',
            'transcript_id'
        ]
    ),
    'LASTUPDATED': SourceTable(
        'csv',
        separator='|',
        columns=[
            'lastupdated'
        ]
    ),
    'MEDCLASS': SourceTable(
        'csv',
        separator='|',
        columns=[
            'medclass_id',
            'medclass_description',
            'level_code'
        ]
    ),
    'MEDICATION': SourceTable(
        'csv',
        separator='|',
        columns=[
            'medication_id',
            'ndc_formatted',
            'rxnorm_cui',
            'trade_name_id',
            'trade_name',
            'generic_name',
            'rx_or_otc',
            'route',
            'strength',
            'doseform'
        ]
    ),
    'MED_MEDCLASS': SourceTable(
        'csv',
        separator='|',
        columns=[
            'medication_id',
            'medclass_id'
        ]
    ),
    'PATIENT': SourceTable(
        'csv',
        separator='|',
        columns=[
            'patient_id',
            'gender',
            'birth_year',
            'insurance_type',
            'state',
            'zip',
            'ethnicity',
            'has_mobile_phone',
            'mobile_phone_opt_in',
            'has_email',
            'email_opt_in',
            'is_complete',
            'is_active',
            'practice_id',
            'preferred_provider_id',
            'consistent_patient_id'
        ]
    ),
    'PATIENT_RACE': SourceTable(
        'csv',
        separator='|',
        columns=[
            'patient_race_id',
            'patient_id',
            'race_id',
            'last_modified'
        ]
    ),
    'PATIENT_SMOKE': SourceTable(
        'csv',
        separator='|',
        columns=[
            'patient_smoke_id',
            'patient_id',
            'smoke_id',
            'effective_date',
            'last_modified',
            'created_at'
        ]
    ),
    'PAYER': SourceTable(
        'csv',
        separator='|',
        columns=[
            'payer_id',
            'name',
            'practice_id',
            'is_active',
            'last_modified'
        ]
    ),
    'PHARMACY': SourceTable(
        'csv',
        separator='|',
        columns=[
            'pharmacy_id',
            'pharm_type_id',
            'name',
            'state',
            'zip',
            'is_active',
            'last_modified'
        ]
    ),
    'PRACTICE': SourceTable(
        'csv',
        separator='|',
        columns=[
            'practice_id',
            'state',
            'zip',
            'created_at'
        ]
    ),
    'PRESCRIPTION': SourceTable(
        'csv',
        separator='|',
        columns=[
            'prescription_id',
            'provider_id',
            'patient_id',
            'medication_id',
            'diagnosis_id',
            'discontinued_reason',
            'start_date',
            'stop_date',
            'dos',
            'num_refills',
            'quantity',
            'dispensed_as_written',
            'frequency',
            'daily_admin',
            'erx',
            'last_modified',
            'pharmacy_id',
            'c_daily_admin',
            'c_dose_amount',
            'c_dose_amount_unit',
            'c_duration',
            'c_duration_days',
            'c_frequency',
            'c_max_daily_admin',
            'c_min_daily_admin',
            'c_prn',
            'c_quantity',
            'c_quantity_unit',
            'days_supply',
            'max_daily_dose',
            'refill_as_needed',
            'unit',
            'created_at'
        ]
    ),
    'PROVIDER': SourceTable(
        'csv',
        separator='|',
        columns=[
            'provider_id',
            'primary_specialty_id',
            'secondary_specialty_id',
            'is_active',
            'is_complete',
            'practice_id',
            'edit_level',
            'created_at',
            'gender',
            'derived_specialty',
            'derived_provider_type',
            'derived_ama_taxonomy',
            'consistent_provider_id'
        ]
    ),
    'RACE': SourceTable(
        'csv',
        separator='|',
        columns=[
            'race_id',
            'race_name'
        ]
    ),
    'SMOKE': SourceTable(
        'csv',
        separator='|',
        columns=[
            'smoke_id',
            'concept_id',
            'description'
        ]
    ),
    'SPECIALTY': SourceTable(
        'csv',
        separator='|',
        columns=[
            'specialty_id',
            'name',
            'provider_type',
            'npi_classification'
        ]
    ),
    'TRANSCRIPT': SourceTable(
        'csv',
        separator='|',
        columns=[
            'transcript_id',
            'patient_id',
            'provider_id',
            'signed_by_provider_id',
            'signed_by_time',
            'dos',
            'weight',
            'height',
            'c_bmi',
            'systolic_bp',
            'diastolic_bp',
            'pulse',
            'resp_rate',
            'note_type',
            'last_modified'
        ]
    ),
    'TRANSCRIPT_ALLERGY': SourceTable(
        'csv',
        separator='|',
        columns=[
            'transcript_id',
            'allergy_id',
            'is_active',
            'last_modified'
        ]
    ),
    'TRANSCRIPT_DIAGNOSIS': SourceTable(
        'csv',
        separator='|',
        columns=[
            'diagnosis_id',
            'transcript_id',
            'relationship_source',
            'order_by',
            'last_modified'
        ]
    ),
    'TRANSCRIPT_PRESCRIPTION': SourceTable(
        'csv',
        separator='|',
        columns=[
            'transcript_id',
            'prescription_id',
            'order_by',
            'is_active',
            'last_modified'
        ]
    ),
    'VACCINATION': SourceTable(
        'csv',
        separator='|',
        columns=[
            'vaccination_id',
            'patient_id',
            'vaccine_id',
            'administered_provider_id',
            'last_modified_provider_id',
            'status',
            'rejection_reason',
            'date_administered',
            'vaccinemanufacturer_id',
            'created_at'
        ]
    ),
    'VACCINE': SourceTable(
        'csv',
        separator='|',
        columns=[
            'vaccine_id',
            'name',
            'description',
            'cvx_code'
        ]
    ),
    'VACCINEMANUFACTURER': SourceTable(
        'csv',
        separator='|',
        columns=[
            'vaccinemanufacturer_id',
            'vaccinemanufacturer_name',
            'mvx_code'
        ]
    ),
    'VENDOR': SourceTable(
        'csv',
        separator='|',
        columns=[
            'vendor_id',
            'name'
        ]
    )
}

