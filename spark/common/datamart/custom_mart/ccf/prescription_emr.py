"""
perscreption emr schema
"""

from pyspark.sql.types import *
from spark.common.schema import Schema

schema = Schema(
    name='schema_v1',
    data_type='custom_mart',
    output_directory='ccf/2021-08-31/prescription_emr/',
    distribution_key='record_id',
    provider_partition_column='part_provider',
    date_partition_column='part_mth',
    schema_structure=StructType([
        StructField('record_id', StringType(), True),
        StructField('crt_dt', StringType(), True),
        StructField('data_set_nm', StringType(), True),
        StructField('hvid', StringType(), True),
        StructField('ptnt_birth_yr', StringType(), True),
        StructField('ptnt_age_num', StringType(), True),
        StructField('ptnt_gender_cd', StringType(), True),
        StructField('ptnt_state_cd', StringType(), True),
        StructField('ptnt_zip3_cd', StringType(), True),
        StructField('deidentified_master_patient_id', StringType(), True),
        StructField('deidentified_patient_id', StringType(), True),
        StructField('data_source', StringType(), True),
        StructField('med_id', StringType(), True),
        StructField('visit_encounter_id', StringType(), True),
        StructField('medication_name', StringType(), True),
        StructField('drug_code', StringType(), True),
        StructField('drug_code_sys_nm', StringType(), True),
        StructField('src_drug_code', StringType(), True),
        StructField('src_drug_code_sys_nm', StringType(), True),
        StructField('src_drug_code_concept_name', StringType(), True),
        StructField('med_action_concept_name', StringType(), True),
        StructField('route_of_medication', StringType(), True),
        StructField('medication_domain', StringType(), True),
        StructField('med_start_date', StringType(), True),
        StructField('med_end_date', StringType(), True),
        StructField('med_discont_start_date', StringType(), True),
        StructField('dose_of_medication', StringType(), True),
        StructField('medication_strength', StringType(), True),
        StructField('med_strength_unit_of_measure', StringType(), True),
        StructField('medication_quantity', StringType(), True),
        StructField('med_quantity_uom', StringType(), True),
        StructField('med_form', StringType(), True),
        StructField('medication_treatment_course', StringType(), True),
        StructField('unit_of_measure_for_medication', StringType(), True),
        StructField('medication_frequence', StringType(), True),
        StructField('frequence_unit_of_measure', StringType(), True),
        StructField('medication_admin_duration', StringType(), True),
        StructField('med_admin_duration_uom', StringType(), True),
        StructField('generic_medicine_flag', StringType(), True),
        StructField('substitute_med_indication_flag', StringType(), True),
        StructField('place_of_service', StringType(), True),
        StructField('medication_refills', StringType(), True),
        StructField('prmy_src_tbl_nm', StringType(), True)
    ])
)
