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
import spark.providers.amazingcharts.emr.transactional_schemas_v3 as transactional_schemas_v3

from pyspark.sql.window import Window
from pyspark.sql.functions import col, lit, row_number, split, explode


def run(date_input, test=False, end_to_end_test=False, spark=None, runner=None):

    # ------------------------ Provider specific configuration -----------------------
    provider_name = 'amazingcharts'

    output_table_names_to_schemas = {
        'amazingcharts_clin_obsn_final_norm': clinical_observation_schemas['schema_v7'],
        'amazingcharts_diagnosis_final_norm': diagnosis_schemas['schema_v7'],
        'amazingcharts_encounter_final_norm': encounter_schemas['schema_v7'],
        'amazingcharts_lab_result_final_norm': lab_result_schema['schema_v7'],
        'amazingcharts_medication_final_norm': medication_schemas['schema_v7'],
        'amazingcharts_procedure_final_norm': procedure_schemas['schema_v8']
     }

    provider_partition_name = '5'

    # ------------------------ Common for all providers -----------------------

    # New layout after 2018-07-25, but we already got it once on 2018-07-24
    if date_input >= '2021-08-31':
        source_table_schemas = transactional_schemas_v3
    elif date_input >= '2018-01-01':
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
        end_to_end_test=end_to_end_test,
        test=test,
        load_date_explode=False,
        unload_partition_count=15,
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
        driver.output_path = file_utils.get_abs_path(
            script_path, '../../../test/providers/amazingcharts/emr/resources/output/'
        ) + '/'
    elif end_to_end_test:
        driver.input_path = 's3://salusv/testing/dewey/airflow/e2e/amazingcharts/input/{}/'.format(
            date_input.replace('-', '/')
        )

        driver.matching_path = 's3://salusv/testing/dewey/airflow/e2e/amazingcharts/payload/{}/'.format(
            date_input.replace('-', '/')
        )

    conf_parameters = {
        'spark.default.parallelism': 4000,
        'spark.sql.shuffle.partitions': 4000,
        'spark.executor.memoryOverhead': 1024,
        'spark.driver.memoryOverhead': 1024,
        'spark.driver.extraJavaOptions': '-XX:+UseG1GC',
        'spark.executor.extraJavaOptions': '-XX:+UseG1GC',
        'spark.sql.autoBroadcastJoinThreshold': 10485760
    }

    if not test:
        driver.init_spark_context(conf_parameters=conf_parameters)
    else:
        driver.spark = spark
        driver.runner = runner

    driver.load()

    logger.log(' -trimmify-nullify d_drug data')
    d_drug_df = driver.spark.table('d_drug')
    cleaned_d_drug_df = (
        postprocessor.compose(postprocessor.trimmify, postprocessor.nullify)(d_drug_df))
    cleaned_d_drug_df.distinct().createOrReplaceTempView("d_drug")

    logger.log(' -trimmify-nullify d_multum_to_ndc data')
    ndc_sql = """
    SELECT distinct max(ndc) as ndc, multum_id, touch_date 
        FROM d_multum_to_ndc 
        WHERE (multum_id, touch_date) 
        IN 
            (
            SELECT multum_id, max(touch_date) as touch_date 
            FROM d_multum_to_ndc 
            GROUP BY multum_id
            ) 
        GROUP BY multum_id, touch_date
    """
    d_multum_to_ndc_df = driver.spark.sql(ndc_sql)
    cleaned_d_multum_to_ndc_df = (
        postprocessor.compose(postprocessor.trimmify, postprocessor.nullify)(d_multum_to_ndc_df))
    cleaned_d_multum_to_ndc_df.distinct().createOrReplaceTempView("d_multum_to_ndc")
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
            cln.*,
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
            ) AS proc_cd
        FROM
            f_clinical_note cln
        WHERE LENGTH(TRIM(COALESCE(cln.cpt_code, ''))) != 0
            AND
            TRIM(UPPER(COALESCE(cln.practice_key, 'empty'))) <> 'PRACTICE_KEY'
    """
    proc_1 = driver.spark.sql(v_proc_sql).\
        withColumn('proc_cd', explode(split(col('proc_cd'), '\s+'))).where("length(proc_cd) != 0")

    proc_1.createOrReplaceTempView('f_clinical_note_explode')

    if not test:
        logger.log('Loading external table: gen_ref_whtlst')
        external_table_loader.load_analytics_db_table(
            driver.runner.sqlContext, 'dw', 'gen_ref_whtlst', 'gen_ref_whtlst')

    driver.transform()
    driver.save_to_disk()

    if not test:
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