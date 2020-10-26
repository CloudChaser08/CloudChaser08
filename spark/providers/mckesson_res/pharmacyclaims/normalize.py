#! /usr/bin/python
import spark.helpers.privacy.pharmacyclaims as pharm_priv

import argparse
from pyspark.sql.functions import lit, col
from spark.common.utility import logger

from spark.common.pharmacyclaims_common_model import schemas
import spark.providers.mckesson_res.pharmacyclaims.transaction_schemas_v1 as old_schema
import spark.providers.mckesson_res.pharmacyclaims.transaction_schemas_v2 as new_schema
from spark.common.marketplace_driver import MarketplaceDriver


schema = schemas['schema_v6']
schema_obj = schema.schema_structure

# OUTPUT_PATH_TEST = 's3://salusv/testing/dewey/airflow/e2e/mckesson_res/pharmacyclaims/spark-output/'
# OUTPUT_PATH_PRODUCTION = 's3a://salusv/warehouse/parquet/pharmacyclaims/2018-02-05/'
FEED_ID = '36'
VENDOR_ID = '119'


# 1. get paths 

# 2. normalize 
# - a. load 
#   - 1. fill in columns
#   - 2. check columns
#   - 3. compose trimmify nullify
#   - 4. createOrReplaceTempView

# - b. payloader load  
# - c. run normalize sql
# - d. schema enforcer apply schema

# - e. process and unload 
#   - 1. compose 
#           nullify,
#           add universal columns, 
#           pharm_priv filter
#   - 2. normalized_records_unloader partition and rename vs unload

# 3. logger log run details
# 4. normalized_records_unloader distcp 

class MckessonMarketplaceDriver(MarketplaceDriver):
    schema_type: str = "new_schema"

    def load(self, **kwargs):
        # I feel like this can be done more efficiently. 
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
            raise ValueError('Unexpected column length in transaction data: {}'.format(str(len(unlabeled_input.columns))))
        
        super().load(extra_payload_cols=['hvJoinKey', 'claimId'], **kwargs)

        # txn is the name of the table inside 0_mckesson_res.sql
        if self.schema_type == "new_schema":
            logger.log('Adding columns for new schema')

            txn = self.spark.table('mckesson_res_pharmacyclaims')
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
    # ------------------------ Provider specific configuration -----------------------
    
    provider_name = 'mckesson_res'
    output_table_names_to_schemas = {
        'mckesson_res_pharmacyclaims': schemas['schema_v6'],
    }
    provider_partition_name = 'mckesson_res'
    vdr_feed_id = '36'

    source_table_schemas = None

    # Create and run driver
    driver = MckessonMarketplaceDriver(
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

    # ------------------------ Common for all providers -----------------------

    # Parse input arguments
    parser = argparse.ArgumentParser()
    parser.add_argument('--date', type=str)
    parser.add_argument('--end_to_end_test', default=False, action='store_true')
    args = parser.parse_args()

    date_input = args.date
    end_to_end_test = args.end_to_end_test

    run(date_input, end_to_end_test)
    

