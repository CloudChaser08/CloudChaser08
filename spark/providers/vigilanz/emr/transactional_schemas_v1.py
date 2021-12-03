"""
vigilanz emr source schema
"""
from spark.helpers.source_table import SourceTable

TABLE_CONF = {
    'diagnosis': SourceTable(
        'csv',
        separator='|',
        trimmify_nullify=False,
        columns=[
            'row_id',
            'client_id',
            'encounter_id',
            'icd_code',
            'icd_revision'
        ]
    ),
    'encounter': SourceTable(
        'csv',
        separator='|',
        trimmify_nullify=False,
        columns=[
            'row_id',
            'client_id',
            'encounter_id',
            'admission_date',
            'discharge_date',
            'discharge_disposition'
        ]
    ),
    'lab': SourceTable(
        'csv',
        separator='|',
        columns=[
            'row_id',
            'client_id',
            'encounter_id',
            'collection_date_time',
            'release_date_time',
            'lab_name',
            'result',
            'unit_of_measure',
            'reference_range',
            'abnormal_flag'
        ]
    ),
    'medication_administration': SourceTable(
        'csv',
        separator='|',
        columns=[
            'row_id',
            'client_id',
            'encounter_id',
            'drug_name',
            'administration_date_time',
            'dose',
            'dose_unit',
            'route',
            'ndc_code',
            'status'
        ]
    ),
    'medication': SourceTable(
        'csv',
        separator='|',
        columns=[
            'row_id',
            'client_id',
            'encounter_id',
            'drug_name',
            'start_date_time',
            'stop_date_time',
            'dose',
            'dose_unit',
            'route',
            'frequency',
            'ndc_code'
        ]
    ),
    'microbiology': SourceTable(
        'csv',
        separator='|',
        columns=[
            'row_id',
            'client_id',
            'encounter_id',
            'collection_date_time',
            'release_date_time',
            'organism',
            'micro_detail_id',
            'antibiotic',
            'sensitivity',
            'dilution'
        ]
    ),
    'vital': SourceTable(
        'csv',
        separator='|',
        columns=[
            'row_id',
            'client_id',
            'encounter_id',
            'collection_date_time',
            'release_date_time',
            'vital_name',
            'result',
            'unit_of_measure',
            'reference_range',
            'abnormal_flag'
        ]
    )
}
