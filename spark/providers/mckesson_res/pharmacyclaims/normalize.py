#! /usr/bin/python
import argparse

from pyspark.sql.functions import lit, col

from spark.common.utility import logger
from spark.common.pharmacyclaims import schemas
from spark.common.marketplace_driver import MarketplaceDriver

import spark.helpers.privacy.pharmacyclaims as pharm_priv
import spark.providers.mckesson_res.pharmacyclaims.transaction_schemas_v1 as old_schema
import spark.providers.mckesson_res.pharmacyclaims.transaction_schemas_v2 as new_schema

class MckessonResMarketplaceDriver(MarketplaceDriver):

    def __init__(
            self,
            provider_name,
            provider_partition_name,
            source_table_schema,
            output_table_names_to_schemas,
            date_input,
            end_to_end_test=False,
            test=False,
            load_date_explode=True,
            output_to_transform_path=False,
            unload_partition_count=20,
            vdr_feed_id=None,
            use_ref_gen_values=False,
            count_transform_sql=False
    ):
        self.schema_type = None
        super(MckessonResMarketplaceDriver, self).__init__(
            provider_name,
            provider_partition_name,
            source_table_schema,
            output_table_names_to_schemas,
            date_input,
            end_to_end_test,
            test,
            load_date_explode,
            output_to_transform_path,
            unload_partition_count,
            vdr_feed_id,
            use_ref_gen_values,
            count_transform_sql
        )

    def load(self, extra_payload_cols=None, cache_tables=True, payloads=True):
        # I feel like this can be done more efficiently
        # We just neeed the first row, after all
        unlabeled_input = self.runner.sqlContext.read.csv(self.input_path, sep='|')

        """
        Mckesson has 2 schemas - the old schema had 119 columns, the
        new schema has 118. There is no date cutoff for when the new
        schema starts and the old ends, so the best way to choose the
        correct schema is to simply count the columns.
        """
        if len(unlabeled_input.columns) == 118:
            self.schema_type, self.source_table_schema = 'new_schema', new_schema
        elif len(unlabeled_input.columns) == 119:
            self.schema_type, self.source_table_schema = 'old_schema', old_schema
        else:
            raise ValueError('Unexpected column length in transaction: {}'.format(str(len(unlabeled_input.columns))))

        super().load(
            extra_payload_cols=['hvJoinKey', 'claimId'], 
            cache_tables=cache_tables, 
            payloads=payloads
        )

        # txn is the name of the table inside 0_mckesson_res.sql
        if self.schema_type == "new_schema":
            logger.log('Adding columns for new schema')

            txn = self.spark.table('txn')
            txn = txn.withColumn(
                'DispensingFeePaid', lit(None)
            ).withColumn(
                'FillerTransactionTime', col('ClaimTransactionTime')
            )
            txn.createOrReplaceTempView('txn')

    def unload(self, data_frame, schema_obj, columns, table):

        new_output = pharm_priv.filter(data_frame)

        new_output.createOrReplaceTempView(table)

        super().unload(new_output, schema_obj, columns, table)


def run(date_input, end_to_end_test=False, test=False, spark_in=None):
    "Runs the normalization function"
    provider_name = 'mckesson_res'
    output_table_names_to_schemas = {
        'mckesson_res_pharmacyclaims': schemas['schema_v6'],
    }
    provider_partition_name = 'mckesson_res'
    vdr_feed_id = '36'

    source_table_schemas = None

    # Create and run driver
    driver = MckessonResMarketplaceDriver(
        provider_name=provider_name,
        provider_partition_name=provider_partition_name,
        source_table_schema=source_table_schemas,
        output_table_names_to_schemas=output_table_names_to_schemas,
        date_input=date_input,
        end_to_end_test=end_to_end_test,
        test=test,
        vdr_feed_id=vdr_feed_id
    )

    if spark_in is not None:
        driver.spark = spark_in['spark']
        driver.sql_context = spark_in['sqlContext']
        driver.runner = spark_in['runner']

    if test:
        driver.input_path = './test/providers/mckesson_res/pharmacyclaims/resources/input-res'
        driver.matching_path = './test/providers/mckesson_res/pharmacyclaims/resources/matching-res'
        driver.output_path = './test/providers/mckesson_res/pharmacyclaims/resources/output'

    driver.run()


if __name__ == "__main__":
    # Parse input arguments
    parser = argparse.ArgumentParser()
    parser.add_argument('--date', type=str)
    parser.add_argument('--end_to_end_test', default=False, action='store_true')
    args = parser.parse_args()

    _date_input = args.date
    _end_to_end_test = args.end_to_end_test

    run(_date_input, _end_to_end_test)
