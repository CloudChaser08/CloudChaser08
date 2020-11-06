#! /usr/bin/python
import argparse
import datetime
from dateutil.relativedelta import relativedelta
import subprocess
import logging

import pyspark.sql.functions as F
from pyspark.sql.utils import AnalysisException
from pyspark.sql import Window

from spark.runner import Runner
from spark.spark_setup import init
import spark.providers.allscripts.emr.transaction_schemas as transaction_schemas
from spark.common.emr.encounter import schema_v7 as encounter_schema
from spark.common.emr.diagnosis import schema_v7 as diagnosis_schema
from spark.common.emr.procedure import schema_v9 as procedure_schema
from spark.common.emr.provider_order import schema_v7 as provider_order_schema
from spark.common.emr.lab_order import schema_v6 as lab_order_schema
from spark.common.emr.lab_result import schema_v7 as lab_result_schema
from spark.common.emr.medication import schema_v7 as medication_schema
from spark.common.emr.clinical_observation import schema_v7 as clinical_observation_schema
from spark.common.emr.vital_sign import schema_v7 as vital_sign_schema
import spark.helpers.file_utils as file_utils
import spark.helpers.explode as explode
import spark.helpers.multithreaded_s3_transfer as multi_s3_transfer
import spark.helpers.payload_loader as payload_loader
import spark.helpers.schema_enforcer as schema_enforcer
import spark.helpers.external_table_loader as external_table_loader
import spark.helpers.postprocessor as postprocessor
import spark.helpers.normalized_records_unloader as normalized_records_unloader
import spark.helpers.udf.post_normalization_cleanup as post_norm_cleanup
import spark.helpers.privacy.common as priv_common
import spark.helpers.privacy.emr.encounter as encounter_priv
import spark.helpers.privacy.emr.diagnosis as diagnosis_priv
import spark.helpers.privacy.emr.procedure as procedure_priv
import spark.helpers.privacy.emr.provider_order as provider_order_priv
import spark.helpers.privacy.emr.lab_order as lab_order_priv
import spark.helpers.privacy.emr.lab_result as lab_result_priv
import spark.helpers.privacy.emr.medication as medication_priv
import spark.helpers.privacy.emr.clinical_observation as clinical_observation_priv
import spark.helpers.privacy.emr.vital_sign as vital_sign_priv

import spark.providers.allscripts.emr.udf as allscripts_udf

from spark.common.utility import logger, get_spark_runtime, get_spark_time
from spark.common.utility.output_type import DataType, RunType
from spark.common.utility.run_recorder import RunRecorder
from spark.common.utility.spark_state import SparkState


script_path = __file__

FEED_ID = '25'
VENDOR_ID = '35'

OUTPUT_PATH_TEST = 's3://salusv/testing/dewey/airflow/e2e/allscripts/emr/spark-output/'
OUTPUT_PATH_PRODUCTION = 's3://salusv/warehouse/parquet/emr/2017-08-23/'

transaction_paths = []
matching_paths = []
hadoop_times = []
spark_times = []


def run(spark, runner, date_input, explicit_input_path=None, explicit_matching_path=None,
        model=None, test=False, airflow_test=False):
    date_input = '-'.join(date_input.split('-')[:2])
    date_obj = datetime.date(*[int(el) for el in (date_input + '-01').split('-')])
    batch_id = date_obj.strftime('HV_%b%y')

    matching_date = '2017-09' if date_input <= '2017-09' else date_input

    max_cap = (date_obj + relativedelta(months=1) - relativedelta(days=1)).strftime('%Y-%m-%d')
    """
    HV will remove the MAX CAP scrubbing for the “data_captr_dt”  and remove the transformation 
    for “rec_stat_cd” from all the EMR tables.  This way the table will have the original source 
    data. These changes are reflected in the latest mapping document.  These changes are applicable 
    to Veradigm(Allscripts) EMR only
    """
    max_of_max_cap = '9999-12-31'
    BACKFILL_MAX_CAP = '2018-09-30'

    if test:
        input_path = file_utils.get_abs_path(
            script_path, '../../../test/providers/allscripts/emr/resources/input/{}/'.format(
                date_input.replace('-', '/')
            )
        ) + '/'
        matching_path = file_utils.get_abs_path(
            script_path, '../../../test/providers/allscripts/emr/resources/matching/{}/'.format(
                matching_date.replace('-', '/')
            )
        ) + '/'
        backfill_path = file_utils.get_abs_path(
            script_path, '../../../test/providers/allscripts/emr/resources/input/2016/12/'
        ) + '/'
    elif airflow_test:
        input_path = 's3a://salusv/testing/dewey/airflow/e2e/allscripts/emr/out/{}/'.format(
            date_input.replace('-', '/')
        )
        matching_path = 's3a://salusv/testing/dewey/airflow/e2e/allscripts/emr/payload/{}/'.format(
            matching_date.replace('-', '/')
        )
        backfill_path = 's3a://salusv/testing/dewey/airflow/e2e/allscripts/emr/out/2018/10/'
    else:
        input_path = 's3a://salusv/incoming/emr/allscripts/{}/'.format(
            date_input.replace('-', '/')
        ) if not explicit_input_path else explicit_input_path
        matching_path = 's3a://salusv/matching/payload/emr/allscripts/{}/'.format(
            matching_date.replace('-', '/')
        ) if not explicit_matching_path else explicit_matching_path
        backfill_path = 's3a://salusv/incoming/emr/allscripts/2018/09/'

    runner.sqlContext.registerFunction(
        'remove_last_chars', allscripts_udf.remove_last_chars
    )

    if not test:
        external_table_loader.load_ref_gen_ref(runner.sqlContext)

    explode.generate_exploder_table(spark, 2, 'diag_exploder')
    explode.generate_exploder_table(spark, 2, 'proc_exploder')
    explode.generate_exploder_table(spark, 4, 'medication_exploder')
    explode.generate_exploder_table(spark, 3, 'clin_obsn_exploder')

    for table in transaction_schemas.all_tables:
        if table.name in {'vitals_backfill_tier1', 'vitals_backfill_tier2',
                          'results_backfill_tier1', 'results_backfill_tier2'}:
            raw_table = runner.sqlContext.read.csv(
                backfill_path + table.name + '/', sep='|', quote='', schema=table.schema
            )
            postprocessor.compose(
                postprocessor.trimmify, postprocessor.nullify
            )(raw_table).createOrReplaceTempView(table.name)
        elif table.name in {'providers', 'patientdemographics', 'clients', 'clients2'}:
            # In the 2019/02 batch, Allscript sent us a clients table with a different schema
            # For that batch only, use that schema for the clients table
            if date_input == '2019-02':
                if table.name == 'clients':
                    continue
                if table.name == 'clients2':
                    table.name = 'clients'
            elif table.name == 'clients2':
                continue

            raw_table = runner.sqlContext.read.csv(
                input_path + table.name + '/', sep='|', quote='', schema=table.schema
            )
            postprocessor.compose(
                postprocessor.trimmify, postprocessor.nullify
            )(raw_table).createOrReplaceTempView('transactional_' + table.name)
        else:
            window = Window.partitionBy(*table.pk).orderBy(F.col('recordeddttm').desc())
            raw_table = runner.sqlContext.read.csv(
                input_path + table.name + '/', sep='|', quote='', schema=table.schema
            )

            raw_table = raw_table.withColumn('input_file_name', F.input_file_name()).persist()

            raw_table = postprocessor.compose(
                postprocessor.trimmify, postprocessor.nullify
            )(raw_table)

            # deduplicate based on natural key
            raw_table = raw_table.withColumn('row_num', F.row_number().over(window))\
                    .where(F.col('row_num') == 1)

            # add non-skewed provider columns
            for column in table.skewed_columns:
                raw_table = raw_table.withColumn(
                    'hv_{}'.format(column), F.when(
                        F.col(column).isNull(),
                        F.concat(F.lit('nojoin_'), F.rand())
                    ).otherwise(F.col(column))
                )

            raw_table.createOrReplaceTempView('transactional_' + table.name)

    # Tier1 and tier2 backfill data was delivered in slightly different layouts
    # Align and merge them
    t1 = spark.table('vitals_backfill_tier1')
    t2 = spark.table('vitals_backfill_tier2')
    # backfill does not impact data before 2014-04 or after 2018-09
    if date_input < '2014-04' or date_input > '2018-09':
        t1 = spark.createDataFrame([], schema=t1.schema)
        t2 = spark.createDataFrame([], schema=t2.schema)
    t1.union(t2.select(*t1.columns)).createOrReplaceTempView('vitals_backfill')

    t1 = spark.table('results_backfill_tier1')
    t2 = spark.table('results_backfill_tier2')
    # backfill does not impact data before 2014-04 or after 2018-09
    if date_input < '2014-04' or date_input > '2018-09':
        t1 = spark.createDataFrame([], schema=t1.schema)
        t2 = spark.createDataFrame([], schema=t2.schema)
    t1.union(t2.select(*t1.columns)).createOrReplaceTempView('results_backfill')

    payload_loader.load(runner, matching_path, extra_cols=['personId', 'claimId'])

    normalized_encounter = schema_enforcer.apply_schema(
        runner.run_spark_script(
            'normalize_encounter_app.sql', [
                ['max_cap', max_cap],
                ['batch_id', batch_id, False]
            ], return_output=True, source_file_path=script_path
        ), encounter_schema, columns_to_keep=['allscripts_date_partition']
    ).union(schema_enforcer.apply_schema(
        runner.run_spark_script(
            'normalize_encounter_enc.sql', [
                ['max_cap', max_cap],
                ['batch_id', batch_id, False]
            ], return_output=True, source_file_path=script_path
        ), encounter_schema, columns_to_keep=['allscripts_date_partition']
    ))
    normalized_diagnosis = runner.run_spark_script(
        'normalize_diagnosis.sql', [
            ['max_cap', max_cap],
            ['batch_id', batch_id, False]
        ], return_output=True, source_file_path=script_path
    )
    normalized_procedure = schema_enforcer.apply_schema(
        runner.run_spark_script(
            'normalize_procedure_ord.sql', [
                ['max_cap', max_cap],
                ['batch_id', batch_id, False]
            ], return_output=True, source_file_path=script_path
        ), procedure_schema, columns_to_keep=['allscripts_date_partition']
    ).union(schema_enforcer.apply_schema(
        runner.run_spark_script(
            'normalize_procedure_vac.sql', [
                ['max_cap', max_cap],
                ['batch_id', batch_id, False]
            ], return_output=True, source_file_path=script_path
        ), procedure_schema, columns_to_keep=['allscripts_date_partition']
    ))
    normalized_provider_order = schema_enforcer.apply_schema(
        runner.run_spark_script(
            'normalize_provider_order_ord.sql', [
                ['max_cap', max_cap],
                ['batch_id', batch_id, False]
            ], return_output=True, source_file_path=script_path
        ), provider_order_schema, columns_to_keep=['allscripts_date_partition']
    )
    normalized_lab_order = runner.run_spark_script(
        'normalize_lab_order.sql', [
            ['max_cap', max_cap],
            ['batch_id', batch_id, False]
        ], return_output=True, source_file_path=script_path
    )
    normalized_lab_result = runner.run_spark_script(
        'normalize_lab_result.sql', [
            ['max_cap', max_cap],
            ['backfill_max_cap', BACKFILL_MAX_CAP],
            ['batch_id', batch_id, False]
        ], return_output=True, source_file_path=script_path
    )
    normalized_medication = runner.run_spark_script(
        'normalize_medication.sql', [
            ['max_cap', max_cap],
            ['batch_id', batch_id, False]
        ], return_output=True, source_file_path=script_path
    )
    normalized_clinical_observation = runner.run_spark_script(
        'normalize_clinical_observation.sql', [
            ['max_cap', max_cap],
            ['batch_id', batch_id, False]
        ], return_output=True, source_file_path=script_path
    )
    normalized_vital_sign = runner.run_spark_script(
        'normalize_vital_sign.sql', [
            ['max_cap', max_cap],
            ['backfill_max_cap', BACKFILL_MAX_CAP],
            ['batch_id', batch_id, False]
        ], return_output=True, source_file_path=script_path
    )

    normalized_tables = [
        {
            'name': 'medication',
            'data': normalized_medication,
            'privacy': medication_priv,
            'schema': medication_schema,
            'model_version': '07',
            'join_key': 'hv_medctn_id',
            'date_caps': [
                ('enc_dt', 'EARLIEST_VALID_SERVICE_DATE', max_cap),
                ('medctn_admin_dt', 'EARLIEST_VALID_SERVICE_DATE', max_cap),
                ('medctn_start_dt', 'EARLIEST_VALID_SERVICE_DATE', max_cap),
                ('medctn_end_dt', 'EARLIEST_VALID_SERVICE_DATE', max_cap),
                ('data_captr_dt', 'EARLIEST_VALID_SERVICE_DATE', max_of_max_cap)
            ]
        }, {
            'name': 'lab_result',
            'data': normalized_lab_result,
            'privacy': lab_result_priv,
            'schema': lab_result_schema,
            'model_version': '07',
            'join_key': 'hv_lab_result_id',
            'date_caps': [
                ('enc_dt', 'EARLIEST_VALID_SERVICE_DATE', max_cap),
                ('lab_test_execd_dt', 'EARLIEST_VALID_SERVICE_DATE', max_cap),
                ('lab_result_dt', 'EARLIEST_VALID_SERVICE_DATE', max_cap),
                ('data_captr_dt', 'EARLIEST_VALID_SERVICE_DATE', max_of_max_cap) # Max cap is applied in SQL
            ]
        }, {
            'name': 'encounter',
            'data': normalized_encounter,
            'privacy': encounter_priv,
            'schema': encounter_schema,
            'model_version': '07',
            'join_key': 'hv_enc_id',
            'date_caps': [
                ('enc_start_dt', 'EARLIEST_VALID_SERVICE_DATE', '9999-12-31'),
                ('enc_end_dt', 'EARLIEST_VALID_SERVICE_DATE', '9999-12-31'),
                ('data_captr_dt', 'EARLIEST_VALID_SERVICE_DATE', max_of_max_cap)
            ]
        }, {
            'name': 'diagnosis',
            'data': normalized_diagnosis,
            'privacy': diagnosis_priv,
            'schema': diagnosis_schema,
            'model_version': '07',
            'join_key': 'hv_diag_id',
            'date_caps': [
                ('enc_dt', 'EARLIEST_VALID_SERVICE_DATE', max_cap),
                ('diag_dt', 'EARLIEST_VALID_DIAGNOSIS_DATE', max_cap),
                ('diag_onset_dt', 'EARLIEST_VALID_DIAGNOSIS_DATE', max_cap),
                ('diag_resltn_dt', 'EARLIEST_VALID_DIAGNOSIS_DATE', max_cap),
                ('data_captr_dt', 'EARLIEST_VALID_SERVICE_DATE', max_of_max_cap)
            ],
            'update_whitelists': lambda whitelists: whitelists + [{
                'column_name': 'diag_snomed_cd',
                'domain_name': 'SNOMED',
                'whitelist_col_name': 'gen_ref_cd'
            }]
        }, {
            'name': 'procedure',
            'data': normalized_procedure,
            'privacy': procedure_priv,
            'schema': procedure_schema,
            'model_version': '09',
            'join_key': 'hv_proc_id',
            'date_caps': [
                ('enc_dt', 'EARLIEST_VALID_SERVICE_DATE', max_cap),
                ('proc_dt', 'EARLIEST_VALID_SERVICE_DATE', max_cap),
                ('data_captr_dt', 'EARLIEST_VALID_SERVICE_DATE', max_of_max_cap)
            ],
            'update_whitelists': lambda whitelists: whitelists + [{
                'column_name': 'proc_snomed_cd',
                'domain_name': 'SNOMED',
                'whitelist_col_name': 'gen_ref_cd'
            }]
        }, {
            'name': 'provider_order',
            'data': normalized_provider_order,
            'privacy': provider_order_priv,
            'schema': provider_order_schema,
            'model_version': '07',
            'join_key': 'hv_prov_ord_id',
            'date_caps': [
                ('enc_dt', 'EARLIEST_VALID_SERVICE_DATE', max_cap),
                ('prov_ord_dt', 'EARLIEST_VALID_SERVICE_DATE', max_cap),
                ('prov_ord_complt_dt', 'EARLIEST_VALID_SERVICE_DATE', max_cap),
                ('data_captr_dt', 'EARLIEST_VALID_SERVICE_DATE', max_of_max_cap)
            ]
        }, {
            'name': 'lab_order',
            'data': normalized_lab_order,
            'privacy': lab_order_priv,
            'schema': lab_order_schema,
            'model_version': '06',
            'join_key': 'hv_lab_ord_id',
            'date_caps': [
                ('enc_dt', 'EARLIEST_VALID_SERVICE_DATE', max_cap),
                ('lab_ord_dt', 'EARLIEST_VALID_SERVICE_DATE', max_cap),
                ('data_captr_dt', 'EARLIEST_VALID_SERVICE_DATE', max_of_max_cap)
            ],
            'additional_transformer': priv_common.Transformer(
                lab_ord_alt_cd=[
                    priv_common.TransformFunction(post_norm_cleanup.clean_up_procedure_code, ['lab_ord_alt_cd'])
                ]
            )
        }, {
            'name': 'clinical_observation',
            'data': normalized_clinical_observation,
            'privacy': clinical_observation_priv,
            'schema': clinical_observation_schema,
            'model_version': '07',
            'join_key': 'hv_clin_obsn_id',
            'date_caps': [
                ('enc_dt', 'EARLIEST_VALID_SERVICE_DATE', max_cap),
                ('clin_obsn_dt', 'EARLIEST_VALID_SERVICE_DATE', max_cap),
                ('clin_obsn_onset_dt', 'EARLIEST_VALID_DIAGNOSIS_DATE', max_cap),
                ('data_captr_dt', 'EARLIEST_VALID_SERVICE_DATE', max_of_max_cap)
            ],
            'update_whitelists': lambda whitelists: whitelists + [{
                'column_name': 'clin_obsn_snomed_cd',
                'domain_name': 'SNOMED',
                'whitelist_col_name': 'gen_ref_cd'
            }]
        }, {
            'name': 'vital_sign',
            'data': normalized_vital_sign,
            'privacy': vital_sign_priv,
            'schema': vital_sign_schema,
            'model_version': '07',
            'join_key': 'hv_vit_sign_id',
            'date_caps': [
                ('enc_dt', 'EARLIEST_VALID_SERVICE_DATE', max_cap),
                ('vit_sign_dt', 'EARLIEST_VALID_SERVICE_DATE', max_cap),
                ('data_captr_dt', 'EARLIEST_VALID_SERVICE_DATE', max_of_max_cap) # Max cap is applied in SQL
            ]
        }
    ]

    for table in ([t for t in normalized_tables if t['name'] == model] if model else normalized_tables):
        postprocessor.compose(
            postprocessor.trimmify, postprocessor.nullify,
            schema_enforcer.apply_schema_func(table['schema'], cols_to_keep=['allscripts_date_partition']),
            postprocessor.add_universal_columns(
                feed_id=FEED_ID, vendor_id=VENDOR_ID, filename=None,
                model_version_number=table['model_version'],

                # rename defaults
                record_id='row_id', created='crt_dt', data_set='data_set_nm',
                data_feed='hvm_vdr_feed_id', data_vendor='hvm_vdr_id',
                model_version='mdl_vrsn_num'
            ),
            table['privacy'].filter(
                runner.sqlContext, update_whitelists=table.get('update_whitelists', lambda x: x),
                additional_transformer=table.get('additional_transformer')
            ),
            *(
                [
                    postprocessor.apply_date_cap(
                        runner.sqlContext, date_col, max_cap_date, FEED_ID, domain_name
                    ) for (date_col, domain_name, max_cap_date) in table['date_caps']
                ] + [
                    schema_enforcer.apply_schema_func(table['schema'], cols_to_keep=['allscripts_date_partition'])
                ]
            )
        )(table['data']).createOrReplaceTempView('normalized_{}'.format(table['name']))

        hvm_historical_date = postprocessor.coalesce_dates(
            runner.sqlContext, FEED_ID, datetime.date(1901, 1, 1),
            'HVM_AVAILABLE_HISTORY_START_DATE',
            'EARLIEST_VALID_SERVICE_DATE'
        )

        new_data = runner.sqlContext.table(
            'normalized_{}'.format(table['name'])
        ).alias('new_data').cache_and_track('new_data')

        normalized_records_unloader.unload(
            spark, runner, new_data, 'allscripts_date_partition', max_cap,
            FEED_ID, provider_partition_name='part_hvm_vdr_feed_id',
            date_partition_name='part_mth', hvm_historical_date=datetime.datetime(
                hvm_historical_date.year, hvm_historical_date.month, hvm_historical_date.day
            ), staging_subdir=table['name'], test_dir=(file_utils.get_abs_path(
                script_path, '../../../test/providers/allscripts/emr/resources/output/'
            ) if test else None), unload_partition_count=50, skip_rename=True,
            distribution_key='row_id'
        )

    if not test and not airflow_test:
        transaction_paths.append(input_path)
        matching_paths.append(matching_path)


def main(args):
    # init
    models = args.models.split(',') if args.models else [
        'medication', 'lab_result', 'provider_order', 'lab_order', 'encounter',
        'diagnosis', 'procedure', 'clinical_observation', 'vital_sign'
    ]

    if args.airflow_test:
        output_path = OUTPUT_PATH_TEST
    elif args.output_path:
        output_path = args.output_path
    else:
        output_path = OUTPUT_PATH_PRODUCTION

    for model in models:
        spark, sqlContext = init("Allscripts EMR {}".format(model))
        runner = Runner(sqlContext)

        run(spark, runner, args.date, explicit_input_path=args.input_path,
            explicit_matching_path=args.matching_path, model=model,
            airflow_test=args.airflow_test)

        if args.airflow_test:
            spark.stop()
            normalized_records_unloader.distcp(output_path)
        else:
            spark_times.append(get_spark_time())
            spark.stop()
            hadoop_times.append(normalized_records_unloader.timed_distcp(output_path))

    if not args.airflow_test:
        total_spark_time = sum(spark_times)
        total_hadoop_time = sum(hadoop_times)

        combined_trans_paths = ','.join(transaction_paths)
        combined_matching_paths = ','.join(matching_paths)

        logger.log_run_details(
            provider_name='AllScripts',
            data_type=DataType.EMR,
            data_source_transaction_path=combined_trans_paths,
            data_source_matching_path=combined_matching_paths,
            output_path=OUTPUT_PATH_PRODUCTION,
            run_type=RunType.MARKETPLACE,
            input_date=args.date
        )

        RunRecorder().record_run_details(total_spark_time, total_hadoop_time)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('--date', type=str)
    parser.add_argument('--airflow_test', default=False, action='store_true')
    parser.add_argument('--models', type=str, default=None)
    parser.add_argument('--input_path', type=str)
    parser.add_argument('--matching_path', type=str)
    parser.add_argument('--output_path', type=str)
    args = parser.parse_args()
    main(args)
