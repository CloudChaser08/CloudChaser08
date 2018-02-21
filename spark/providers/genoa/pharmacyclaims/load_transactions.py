from pyspark.sql.types import StructField, StructType, StringType
from pyspark.storagelevel import StorageLevel
from pyspark.sql.functions import *

import spark.helpers.udf.general_helpers as gen_helpers

def load(spark, sqlContext, input_path, is_new_schema):
    columns = [
        'patient_last_name',
        'patient_first_name',
        'patient_date_of_birth',
        'patient_gender',
        'patient_address_zip',
        'member_id',
        'sub_client_id',
        'patient_address_line_1',
        'patient_address_city',
        'patient_address_state',
        'patient_id',
        'bin_number',
        'processor_control_number',
        'transaction_count_',
        'service_provider_id',
        'service_provider_id_qualifier',
        'date_of_service',
        'patient_location',
        'patient_id_qualifier',
        'prescriber_id',
        'prescriber_last_name',
        'prescriber_id_qualifier',
        'patient_relationship_code',
        'prescription_service_reference_number',
        'fill_number',
        'days_supply',
        'compound_code',
        'product_service_id',
        'number_of_refills_authorized',
        'product_service_id_qualifier',
        'quantity_dispensed',
        'prescription_service_reference_number_qualifier',
        'unit_of_measure',
        'ingredient_cost_paid',
        'dispensing_fee_paid',
        'total_amount_paid',
        'amount_of_copay_coinsurance',
        'pharmacy_location__postal_code_',
        'payer_id',
        'payer_id_qualifier',
        'plan_identification',
        'plan_name',
        'hv_join_key'
    ]

    partitions = int(spark.conf.get('spark.sql.shuffle.partitions'))

    if is_new_schema:
        columns = columns[:-1] + ['transaction_code', 'response_code', 'sales_key', 'hv_join_key']

    get_set_id = udf(lambda x: gen_helpers.remove_split_suffix(x).replace('Genoa_',''))

    schema = StructType([StructField(c, StringType(), True) for c in columns])
    df = sqlContext.read.csv(input_path, schema=schema, sep='|') \
        .withColumn('input_file_name', get_set_id(input_file_name()))

    df.repartition(partitions) \
        .persist_and_track('genoa_rx_raw', StorageLevel.MEMORY_ONLY_SER) \
        .createOrReplaceTempView('genoa_rx_raw')

    if not is_new_schema:
        for c in ['transaction_code', 'response_code', 'sales_key']:
            df = df.withColumn(c, lit(''))

    df.createOrReplaceTempView('genoa_rx_raw')
