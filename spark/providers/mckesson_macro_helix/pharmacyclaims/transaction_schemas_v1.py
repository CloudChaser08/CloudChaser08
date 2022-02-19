"""
mckesson macro helix Tables
"""
from spark.helpers.source_table import SourceTable

TABLE_CONF = {
    'txn': SourceTable(
        'csv',
        separator='|',
        confirm_schema=True,
        columns=[
            'row_id',
            'service_date',
            'mr_num',
            'visit_num',
            'first_name',
            'last_name',
            'birth_date',
            'patient_gender',
            'patient_zip',
            'patient_type',
            'ndc',
            'bupp',
            'packages',
            'quantity',
            'inpatient_flag',
            'medicaid_flag',
            'orphan_drug_flag',
            'insurance',
            'insurance_2',
            'insurance_3',
            'jcode',
            'gross_charge',
            'hospital_state',
            'hospital_zip',
            'dx_01',
            'dx_02',
            'dx_03',
            'dx_04',
            'dx_05',
            'dx_06',
            'dx_07',
            'dx_08',
            'dx_09',
            'dx_10',
            'dx_11',
            'dx_12',
            'dx_13',
            'dx_14',
            'dx_15',
            'dx_16',
            'dx_17',
            'dx_18',
            'dx_19',
            'dx_20',
            'dx_21',
            'dx_22',
            'dx_23',
            'dx_24',
            'mystery_column',  # TODO: find out what this is
            'hvJoinKey'
        ]
    )
}
