"""labtests registry model"""
from pyspark.sql.types import *
from spark.common.schema import Schema
from spark.common.utility.output_type import DataType

# pylint: disable=duplicate-code

schema_v10 = StructType([
    StructField('record_id', LongType(), True),
    StructField('claim_id', StringType(), True),
    StructField('hvid', StringType(), True),
    StructField('created', DateType(), True),
    StructField('model_version', StringType(), True),
    StructField('data_set', StringType(), True),
    StructField('data_feed', StringType(), True),
    StructField('data_vendor', StringType(), True),
    StructField('source_version', StringType(), True),
    StructField('date_service', DateType(), True),
    StructField('date_specimen', DateType(), True),
    StructField('date_report', DateType(), True),
    StructField('time_report', StringType(), True),
    StructField('loinc_code', StringType(), True),
    StructField('lab_id', StringType(), True),
    StructField('test_id', StringType(), True),
    StructField('test_number', StringType(), True),
    StructField('test_ordered_local_id', StringType(), True),
    StructField('test_ordered_std_id', StringType(), True),
    StructField('test_ordered_name', StringType(), True),
    StructField('result_id', StringType(), True),
    StructField('result', StringType(), True),
    StructField('result_name', StringType(), True),
    StructField('result_unit_of_measure', StringType(), True),
    StructField('result_desc', StringType(), True),
    StructField('result_comments', StringType(), True),
    StructField('ref_range', StringType(), True),
    StructField('abnormal_flag', StringType(), True),
    StructField('fasting_status', StringType(), True),
    StructField('diagnosis_code', StringType(), True),
    StructField('diagnosis_code_qual', StringType(), True),
    StructField('diagnosis_code_priority', StringType(), True),
    StructField('procedure_code', StringType(), True),
    StructField('procedure_code_qual', StringType(), True),
    StructField('procedure_modifier_1', StringType(), True),
    StructField('procedure_modifier_2', StringType(), True),
    StructField('procedure_modifier_3', StringType(), True),
    StructField('procedure_modifier_4', StringType(), True),
    StructField('payer_id', StringType(), True),
    StructField('payer_id_qual', StringType(), True),
    StructField('payer_name', StringType(), True),
    StructField('payer_parent_name', StringType(), True),
    StructField('payer_org_name', StringType(), True),
    StructField('payer_type', StringType(), True),
    StructField('lab_other_id', StringType(), True),
    StructField('lab_other_qual', StringType(), True),
    StructField('ordering_other_id', StringType(), True),
    StructField('ordering_other_qual', StringType(), True),
    StructField('ordering_specialty', StringType(), True),
    StructField('logical_delete_reason', StringType(), True),
    StructField('vendor_record_id', StringType(), True),
    StructField('part_opportunity_id', StringType(), True)
])

data_type = DataType.LAB_TESTS
schemas = {
    'schema_v10': Schema(name='schema_v10',
                         schema_structure=schema_v10,
                         distribution_key='record_id',
                         data_type=DataType(data_type).value,
                         provider_partition_column='part_provider',
                         date_partition_column='part_best_date',
                         output_directory='registry/' + DataType(data_type).value + '/2022-03-18/'
                         )
}
