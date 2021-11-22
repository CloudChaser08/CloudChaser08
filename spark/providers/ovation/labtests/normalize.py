"""
ovation normalize
"""
import argparse
import spark.providers.ovation.labtests.transactional_schemas_v1 as source_schema_v1
import spark.providers.ovation.labtests.transactional_schemas_v2 as source_schema_v2
import spark.providers.ovation.labtests.transactional_schemas_v3 as source_schema_v3
import spark.providers.ovation.labtests.transactional_schemas_v4 as source_schema_v4
import spark.providers.ovation.labtests.transactional_schemas_v5 as source_schema_v5
import spark.providers.ovation.labtests.transactional_schemas_v6 as source_schema_v6

from spark.common.marketplace_driver import MarketplaceDriver
from spark.common.lab_common_model import schemas as labtest_schemas
from pyspark.sql.types import StringType
from pyspark.sql.functions import lit
import spark.common.utility.logger as logger

v1_to_v3_cutoff_date = '2020-09-21'
v2_cuttoff_start_date = '2020-12-04'
v2_cuttoff_end_date = '2021-02-16'
v2_cuttoff_special_date = ['2021-03-01']
v4_cutoff_date = ['2021-02-17','2021-11-21']
v5_cutoff_date = ['2021-02-18']
v6_cutoff_date = ['2021-05-03', '2021-05-06', '2021-05-11']


if __name__ == "__main__":

    # ------------------------ Provider specific configuration -----------------------
    provider_name = 'ovation'
    output_table_names_to_schemas = {
        'labtest_ovation_covid19': labtest_schemas['schema_v9'],
    }
    provider_partition_name = 'ovation'

    # ------------------------ Common for all providers -----------------------

    # Parse input arguments
    parser = argparse.ArgumentParser()
    parser.add_argument('--date', type=str)
    parser.add_argument('--end_to_end_test', default=False, action='store_true')
    args = parser.parse_known_args()[0]
    date_input = args.date
    end_to_end_test = args.end_to_end_test

    # 23 columns-initial schema: missing columns ('lab_id', 'instrument', 'kit_name' and 'loinc_code')
    is_schema_v1 = date_input < v1_to_v3_cutoff_date  # have 23 columns

    # 26 Columns-from v1 added: 'instrument', 'kit_name' and 'loinc_code' (lab_id is missing)**
    is_schema_v2 = \
        v2_cuttoff_start_date <= date_input <= v2_cuttoff_end_date or date_input in v2_cuttoff_special_date

    # 28 Columns-from v1 added: 'lab_id', 'instrument', 'kit_name', 'loinc_code' and 'column28'
    is_schema_v4 = date_input in v4_cutoff_date

    # 28 Columns-from v1 added: 'instrument', 'kit_name', 'loinc_code', 'column28' and 'column29' (lab_id is missing)**
    is_schema_v5 = date_input in v5_cutoff_date

    # 29 Columns-from v1 added: 'lab_id', 'instrument', 'kit_name', 'loinc_code', 'column28' and 'column29'
    is_schema_v6 = date_input in v6_cutoff_date

    # 27 Columns-from v1 added: 'lab_id', 'instrument', 'kit_name' and 'loinc_code'
    is_schema_v3 = \
        not is_schema_v2 and date_input >= v1_to_v3_cutoff_date \
        and date_input not in v4_cutoff_date and date_input not in v5_cutoff_date \
        and date_input not in v6_cutoff_date

    if is_schema_v1:
        source_table_schemas = source_schema_v1
    elif is_schema_v2:
        source_table_schemas = source_schema_v2
    elif is_schema_v4:
        source_table_schemas = source_schema_v4
    elif is_schema_v5:
        source_table_schemas = source_schema_v5
    elif is_schema_v6:
        source_table_schemas = source_schema_v6
    else:
        source_table_schemas = source_schema_v3

    # Create and run driver
    driver = MarketplaceDriver(
        provider_name,
        provider_partition_name,
        source_table_schemas,
        output_table_names_to_schemas,
        date_input,
        end_to_end_test,
        vdr_feed_id=188,
        use_ref_gen_values=True,
        unload_partition_count=2,
        output_to_transform_path=False
    )
    driver.init_spark_context()

    column_length = len(driver.spark.read.csv(driver.input_path, sep='|').columns)
    logger.log("-transactions data have {} columns".format(column_length))
    if column_length == 27:
        driver.source_table_schemas = source_schema_v3
    elif column_length == 29:
        driver.source_table_schemas = source_schema_v6
    else:
        raise ValueError('Unexpected column length in transaction: {}'.format(str(column_length)))

    driver.load()
    if is_schema_v1:
        logger.log('Adding missing columns for Schema Version 1')
        txn = driver.spark.table('txn')
        txn = txn.withColumn('lab_id', lit(None).cast(StringType())) \
            .withColumn('instrument', lit(None).cast(StringType())) \
            .withColumn('kit_name', lit(None).cast(StringType())) \
            .withColumn('loinc_code', lit(None).cast(StringType()))
        txn.createOrReplaceTempView('txn')
    elif is_schema_v2 or is_schema_v5:
        logger.log('Adding missing columns for Schema Version 2 & 5')
        txn = driver.spark.table('txn')
        txn = txn.withColumn('lab_id', lit(None).cast(StringType()))
        txn.createOrReplaceTempView('txn')
    driver.transform()
    driver.save_to_disk()
    driver.stop_spark()
    driver.log_run()
    driver.copy_to_output_path()
