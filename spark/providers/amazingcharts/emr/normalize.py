"""
amazingcharts emr normalize
"""
import argparse
import spark.helpers.file_utils as file_utils
import spark.helpers.postprocessor as postprocessor
import spark.helpers.external_table_loader as external_table_loader
from spark.common.marketplace_driver import MarketplaceDriver
import spark.common.utility.logger as logger
from spark.common.emr.clinical_observation import schemas as clinical_observation_schemas
from spark.common.emr.diagnosis import schemas as diagnosis_schemas
from spark.common.emr.encounter import schemas as encounter_schemas
from spark.common.emr.lab_result import schemas as lab_result_schema
from spark.common.emr.medication import schemas as medication_schemas
from spark.common.emr.procedure import schemas as procedure_schemas
from spark.common.emr.vital_sign import schemas as vital_sign_schemas
import spark.providers.amazingcharts.emr.transactional_schemas_v1 as transactional_schemas_v1
import spark.providers.amazingcharts.emr.transactional_schemas_v2 as transactional_schemas_v2

from pyspark.sql.functions import col, split, explode


def run(date_input, test=False, end_to_end_test=False, spark=None, runner=None):

    # ------------------------ Provider specific configuration -----------------------
    provider_name = 'amazingcharts'

    output_table_names_to_schemas = {
        'amazingcharts_clinical_observation': clinical_observation_schemas['schema_v11'],
        'amazingcharts_diagnosis': diagnosis_schemas['schema_v10'],
        'amazingcharts_encounter': encounter_schemas['schema_v10'],
        'amazingcharts_lab_result': lab_result_schema['schema_v8'],
        'amazingcharts_medication': medication_schemas['schema_v11'],
        'amazingcharts_procedure': procedure_schemas['schema_v12'],
        'amazingcharts_vital_sign': vital_sign_schemas['schema_v7'],
    }

    provider_partition_name = '5'

    # ------------------------ Common for all providers -----------------------

    # New layout after 2018-07-25, but we already got it once on 2018-07-24
    if date_input >= '2018-01-01':
        source_table_schemas = transactional_schemas_v2
    else:
        source_table_schemas = transactional_schemas_v1

    # Create and run driver
    driver = MarketplaceDriver(
        provider_name,
        provider_partition_name,
        source_table_schemas,
        output_table_names_to_schemas,
        date_input,
        end_to_end_test,
        load_date_explode=False,
        unload_partition_count=10,
        vdr_feed_id=5,
        use_ref_gen_values=True,
        output_to_transform_path=False
    )

    script_path = __file__
    if test:
        driver.input_path = file_utils.get_abs_path(
            script_path, '../../../test/providers/amazingcharts/emr/resources/input/'
        ) + '/'

        driver.matching_path = file_utils.get_abs_path(
            script_path, '../../../test/providers/amazingcharts/emr/resources/payload/'
        ) + '/'
    elif end_to_end_test:
        driver.input_path = 's3://salusv/testing/dewey/airflow/e2e/amazingcharts/input/{}/'.format(
            date_input.replace('-', '/')
        )

        driver.matching_path = 's3://salusv/testing/dewey/airflow/e2e/amazingcharts/payload/{}/'.format(
            date_input.replace('-', '/')
        )

    conf_parameters = {
        'spark.executor.memoryOverhead': 512,
        'spark.driver.memoryOverhead': 512
    }

    if not test:
        driver.init_spark_context(conf_parameters=conf_parameters)
    else:
        driver.spark = spark
        driver.runner = runner

    driver.load()

    logger.log(' -trimmify-nullify matching_payload data')
    v_matching_sql = """ select * 
                    from
                        (   select  *
                                ,row_number() over(partition by personID order by age) as rownumber 
                            from matching_payload
                        ) t
                    where rownumber = 1
        """
    matching_payload_df = driver.spark.sql(v_matching_sql)
    cleaned_matching_payload_df = (
        postprocessor.compose(postprocessor.trimmify, postprocessor.nullify)(matching_payload_df))
    cleaned_matching_payload_df.createOrReplaceTempView("matching_payload")

    v_proc_sql = """
        SELECT
            enc.*,
            TRIM(
                REGEXP_REPLACE(
                    REGEXP_REPLACE(
                        UPPER(cpt_code),
                        '(\\([^)]*\\))|(\\bLOW\\b)|(\\bMEDIUM\\b)|(\\bHIGH\\b)|(\\bCOMPLEXITY\\b)|\\<|\\>',
                        ''
                    ),
                    '[^A-Z0-9]',
                    ' '    
                )
            ) AS proc_cd,
        FROM 
            f_encounter enc
        WHERE LENGTH(TRIM(COALESCE(enc.cpt_code, ''))) != 0
            AND
            TRIM(UPPER(COALESCE(enc.practice_key, 'empty'))) <> 'PRACTICE_KEY'
    """
    normalized_procedure_1 = \
        driver.spark.sql(v_proc_sql).withColumn('proc_cd', explode(split(col('proc_cd'), '\s+'))). \
            where("length(proc_cd) != 0").cache()

    normalized_procedure_1.createOrReplaceTempView('f_encounter_explode')

    if not test:
        logger.log('Loading external table: gen_ref_whtlst')
        external_table_loader.load_analytics_db_table(
            driver.runner.sqlContext, 'dw', 'gen_ref_whtlst', 'gen_ref_whtlst')

    driver.transform()

    if not test:
        driver.save_to_disk()
        driver.stop_spark()
        driver.log_run()
        driver.copy_to_output_path()
    logger.log('Done')


def main(args):
    run(args.date, end_to_end_test=args.airflow_test)


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--date', type=str)
    parser.add_argument('--airflow_test', default=False, action='store_true')
    args = parser.parse_known_args()[0]
    main(args)