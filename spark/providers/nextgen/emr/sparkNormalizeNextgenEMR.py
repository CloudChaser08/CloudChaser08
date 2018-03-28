import argparse
import time as time_module
from subprocess import check_output
from datetime import datetime, date, time
from spark.runner import Runner
from spark.spark_setup import init
import spark.helpers.file_utils as file_utils
import spark.helpers.payload_loader as payload_loader
import spark.helpers.postprocessor as postprocessor
import spark.helpers.explode as explode
import spark.helpers.normalized_records_unloader as normalized_records_unloader
import spark.helpers.external_table_loader as external_table_loader
from spark.helpers.privacy.emr import                   \
    encounter as priv_encounter,                        \
    clinical_observation as priv_clinical_observation,  \
    procedure as priv_procedure,                        \
    lab_result as priv_lab_result,                      \
    diagnosis as priv_diagnosis,                        \
    medication as priv_medication,                      \
    lab_order as priv_lab_order,                        \
    provider_order as priv_provider_order,              \
    vital_sign as priv_vital_sign

import logging

LAST_RESORT_MIN_DATE = datetime(1900, 1, 1)
S3_ENCOUNTER_REFERENCE    = 's3a://salusv/reference/nextgen/encounter_deduped/'
S3_DEMOGRAPHICS_REFERENCE = 's3a://salusv/reference/nextgen/demographics_orc/'

def run(spark, runner, date_input, test=False, airflow_test=False):
    org_num_partitions = spark.conf.get('spark.sql.shuffle.partitions')

    runner.sqlContext.sql('SET mapreduce.output.fileoutputformat.compress.codec=org.apache.hadoop.io.compress.GzipCodec')
    runner.sqlContext.sql('SET hive.exec.compress.output=true')
    runner.sqlContext.sql('SET mapreduce.output.fileoutputformat.compress=true')

    script_path = __file__

    if test:
        input_root_path = file_utils.get_abs_path(
            script_path, '../../../test/providers/nextgen/emr/resources/input/'
        ) + '/'
        input_path = file_utils.get_abs_path(
            script_path, '../../../test/providers/nextgen/emr/resources/input/'
        ) + '/'
        demo_reference_path = file_utils.get_abs_path(
            script_path, '../../../test/providers/nextgen/emr/resources/reference/demo/'
        ) + '/'
        enc_reference_path = file_utils.get_abs_path(
            script_path, '../../../test/providers/nextgen/emr/resources/reference/enc/'
        ) + '/'
# NOTE: No matching data yet
#        matching_path = file_utils.get_abs_path(
#            script_path, '../../../test/providers/nextgen/emr/resources/matching/'
#        ) + '/'
    elif airflow_test:
        input_root_path = 's3://salusv/testing/dewey/airflow/e2e/nextgen/emr/input/'
        input_path = 's3://salusv/testing/dewey/airflow/e2e/nextgen/emr/input/{}/'.format(
            date_input.replace('-', '/')
        )
        demo_reference_path = S3_DEMOGRAPHICS_REFERENCE
        enc_reference_path  = S3_ENCOUNTER_REFERENCE
# NOTE: No matching data yet
#        matching_path = 's3://salusv/testing/dewey/airflow/e2e/nextgen/emr/payload/{}/'.format(
#            date_input.replace('-', '/')
#        )
    else:
        input_root_path = 's3a://salusv/incoming/emr/nextgen/'
        input_path = 's3a://salusv/incoming/emr/nextgen/{}/'.format(
            date_input.replace('-', '/')
        )
        demo_reference_path = S3_DEMOGRAPHICS_REFERENCE
        enc_reference_path  = S3_ENCOUNTER_REFERENCE
# NOTE: No matching data yet
#        matching_path = 's3a://salusv/matching/payload/emr/nextgen/{}/'.format(
#            date_input.replace('-', '/')
#        )

    external_table_loader.load_icd_diag_codes(runner.sqlContext)
    external_table_loader.load_icd_proc_codes(runner.sqlContext)
    external_table_loader.load_hcpcs_codes(runner.sqlContext)
    external_table_loader.load_cpt_codes(runner.sqlContext)
    external_table_loader.load_loinc_codes(runner.sqlContext)
    external_table_loader.load_ref_gen_ref(runner.sqlContext)
    logging.debug("Loaded external tables")

    min_date = runner.sqlContext.sql("SELECT gen_ref_1_dt FROM ref_gen_ref WHERE hvm_vdr_feed_id = 35 AND gen_ref_domn_nm = 'EARLIEST_VALID_SERVICE_DATE'").take(1)
    min_date = min_date[0].gen_ref_1_dt.isoformat().split("T")[0] if len(min_date) > 0 else None
    max_date = date_input
    min_diag_date = runner.sqlContext.sql("SELECT gen_ref_1_dt FROM ref_gen_ref WHERE hvm_vdr_feed_id = 35 AND gen_ref_domn_nm = 'EARLIEST_VALID_DIAGNOSIS_DATE'").take(1)
    min_diag_date = min_diag_date[0].gen_ref_1_dt.isoformat().split("T")[0] if len(min_diag_date) > 0 else None
    logging.debug("Loaded min dates")

    runner.run_spark_script('../../../common/emr/clinical_observation_common_model_v4.sql', [
        ['table_name', 'clinical_observation_common_model', False],
        ['additional_columns', [
            ['part_mth', 'string']
        ]],
        ['properties', '', False]
    ])
    runner.run_spark_script('../../../common/emr/diagnosis_common_model_v5.sql', [
        ['table_name', 'diagnosis_common_model', False],
        ['additional_columns', []],
        ['properties', '', False]
    ])
    runner.run_spark_script('../../../common/emr/encounter_common_model_v4.sql', [
        ['table_name', 'encounter_common_model', False],
        ['additional_columns', []],
        ['properties', '', False]
    ])
    runner.run_spark_script('../../../common/emr/lab_order_common_model_v3.sql', [
        ['table_name', 'lab_order_common_model', False],
        ['additional_columns', [
            ['part_mth', 'string']
        ]],
        ['properties', '', False]
    ])
    runner.run_spark_script('../../../common/emr/lab_result_common_model_v4.sql', [
        ['table_name', 'lab_result_common_model', False],
        ['additional_columns', [
            ['part_mth', 'string']
        ]],
        ['properties', '', False]
    ])
    runner.run_spark_script('../../../common/emr/medication_common_model_v4.sql', [
        ['table_name', 'medication_common_model', False],
        ['additional_columns', [
            ['part_mth', 'string'],
            ['row_num',  'string']
        ]],
        ['properties', '', False]
    ])
    runner.run_spark_script('../../../common/emr/procedure_common_model_v4.sql', [
        ['table_name', 'procedure_common_model', False],
        ['additional_columns', []],
        ['properties', '', False]
    ])
    runner.run_spark_script('../../../common/emr/provider_order_common_model_v4.sql', [
        ['table_name', 'provider_order_common_model', False],
        ['additional_columns', [
            ['part_mth', 'string']
        ]],
        ['properties', '', False]
    ])
    runner.run_spark_script('../../../common/emr/vital_sign_common_model_v4.sql', [
        ['table_name', 'vital_sign_common_model', False],
        ['additional_columns', [
            ['part_mth', 'string']
        ]],
        ['properties', '', False]
    ])
    logging.debug("Created common model tables")

    explode.generate_exploder_table(spark, 20, 'lab_order_exploder')
    explode.generate_exploder_table(spark, 4, 'lipid_exploder')
    explode.generate_exploder_table(spark, 15, 'vital_signs_exploder')
    explode.generate_exploder_table(spark, 5, 'medication_exploder')
    logging.debug("Created exploder tables")

# NOTE: No matching data yet
#    payload_loader.load(runner, matching_path, ['hvJoinKey', 'claimId'])

    runner.run_spark_script('load_transactions.sql', [
        ['input_root_path', input_root_path],
        ['input_path', input_path],
        ['s3_encounter_reference', enc_reference_path],
        ['s3_demographics_reference', demo_reference_path],
    ])
    logging.debug("Loaded transactions data")

    runner.run_spark_script('deduplicate_transactions.sql')

    transaction_tables = [
        'demographics_local', 'encounter_dedup', 'vitalsigns', 'lipidpanel',
        'allergy', 'substanceusage', 'diagnosis', 'order', 'laborder',
        'labresult', 'medicationorder', 'procedure', 'extendeddata'
    ]

    # trim and nullify all incoming transactions tables
    for table in transaction_tables:
        postprocessor.compose(
            postprocessor.trimmify, postprocessor.nullify
        )(runner.sqlContext.sql('select * from {}'.format(table))).createTempView(table)

    runner.run_spark_script('normalize_encounter.sql', [
        ['min_date', min_date],
        ['max_date', max_date]
    ])
    logging.debug("Normalized encounter")
    runner.run_spark_script('normalize_diagnosis.sql', [
        ['min_date', min_date],
        ['max_date', max_date],
        ['diag_min_date', min_diag_date]
    ])
    logging.debug("Normalized diagnosis")
    runner.run_spark_script('normalize_procedure.sql', [
        ['min_date', min_date],
        ['max_date', max_date]
    ])
    logging.debug("Normalized procedure")
    runner.run_spark_script('normalize_lab_order.sql', [
        ['min_date', min_date],
        ['max_date', max_date]
    ])
    logging.debug("Normalized lab order")
    runner.run_spark_script('normalize_lab_result.sql', [
        ['min_date', min_date],
        ['max_date', max_date]
    ])
    logging.debug("Normalized lab result")
    runner.run_spark_script('normalize_medication.sql', [
        ['min_date', min_date],
        ['max_date', max_date]
    ])
    # The row_num column is generated inside the normalize_medication.sql
    # script in order to ensure that when we run a distinct to remove
    # duplicates, we maintain  at least 1 normalized row per source row
    runner.sqlContext.sql('SELECT * FROM medication_common_model_bak').drop('row_num').createOrReplaceTempView('medication_common_model')
    logging.debug("Normalized medication")
    runner.run_spark_script('normalize_provider_order.sql', [
        ['min_date', min_date],
        ['max_date', max_date]
    ])
    logging.debug("Normalized provider order")
    runner.run_spark_script('normalize_clinical_observation.sql', [
        ['min_date', min_date],
        ['max_date', max_date],
        ['partitions', org_num_partitions, False]
    ])
    logging.debug("Normalized clinical observation")
    runner.run_spark_script('normalize_vital_sign.sql', [
        ['min_date', min_date],
        ['max_date', max_date]
    ])
    logging.debug("Normalized vital sign")

    def update_encounter_whitelists(whitelists):
        return whitelists + [{
            'column_name': 'enc_typ_nm',
            'domain_name': 'emr_enc.enc_typ_nm'
        }]

    def update_diagnosis_whitelists(whitelists):
        return whitelists + [{
            'column_name': 'diag_stat_desc',
            'domain_name': 'emr_diag.diag_stat_desc',
            'whitelist_col_name': 'gen_ref_itm_desc'
        }]

    def update_lab_order_whitelists(whitelists):
        return whitelists + [{
            'column_name': 'lab_ord_snomed_cd',
            'domain_name': 'SNOMED',
            'whitelist_col_name': 'gen_ref_cd'
        }, {
            'column_name': 'lab_ord_alt_cd',
            'domain_name': 'emr_lab_ord.lab_ord_alt_cd',
            'whitelist_col_name': 'gen_ref_cd'
        }, {
            'column_name': 'lab_ord_test_nm',
            'domain_name': 'emr_lab_ord.lab_ord_test_nm'
        }, {
            'column_name': 'rec_stat_cd',
            'domain_name': 'emr_lab_ord.rec_stat_cd',
            'whitelist_col_name': 'gen_ref_cd'
        }]

    def update_lab_result_whitelists(whitelists):
        return whitelists + [{
            'column_name': 'lab_test_nm',
            'domain_name': 'emr_lab_result.lab_test_nm',
            'clean_up_freetext_fn': lambda x: x.upper() if x else None
        }, {
            'column_name': 'lab_result_nm',
            'domain_name': 'emr_lab_result.lab_result_nm',
            'clean_up_freetext_fn': lambda x: x.upper() if x else None
        }, {
            'column_name': 'lab_test_snomed_cd',
            'domain_name': 'SNOMED',
            'whitelist_col_name': 'gen_ref_cd'
        }, {
            'column_name': 'lab_test_vdr_cd',
            'domain_name': 'emr_lab_result.lab_test_vdr_cd',
            'whitelist_col_name': 'gen_ref_cd'
        }, {
            'column_name': 'rec_stat_cd',
            'domain_name': 'emr_lab_result.rec_stat_cd',
            'whitelist_col_name': 'gen_ref_cd'
        }]

    def update_medication_whitelists(whitelists):
        return whitelists + [{
            'column_name': 'medctn_admin_sig_cd',
            'domain_name': 'emr_medctn.medctn_admin_sig_cd',
            'whitelist_col_name': 'gen_ref_cd'
        }, {
            'column_name': 'medctn_admin_sig_txt',
            'domain_name': 'emr_medctn.medctn_admin_sig_txt',
            'whitelist_col_name': 'gen_ref_itm_desc'
        }]

    def update_clinical_observation_whitelists(whitelists):
        return whitelists + [{
            'column_name': 'clin_obsn_typ_cd',
            'domain_name': 'substanceusage.clinicalrecordtypecode',
            'whitelist_col_name': 'gen_ref_cd',
            'feed_id': '35',
            'comp_col_names': ['clin_obsn_typ_cd_qual']
        }, {
            'column_name': 'clin_obsn_typ_nm',
            'domain_name': 'substanceusage.clinicalrecorddescription',
            'feed_id': '35'
        }, {
            'column_name': 'clin_obsn_nm',
            'domain_name': 'emr_clin_obsn.clin_obsn_nm',
            'clean_up_freetext_fn': lambda x: x.upper() if x else None
        }, {
            'column_name': 'clin_obsn_result_desc',
            'domain_name': 'emr_clin_obsn.clin_obsn_desc',
            'clean_up_freetext_fn': lambda x: x.upper() if x else None
        }]

    normalized_tables = [
        {
            'table_name'    : 'clinical_observation_common_model',
            'script_name'   : 'emr/clinical_observation_common_model_v4.sql',
            'data_type'     : 'clinical_observation',
            'date_column'   : 'part_mth',
            'privacy_filter': priv_clinical_observation,
            'filter_args'   : [update_clinical_observation_whitelists]
        },
        {
            'table_name'    : 'diagnosis_common_model',
            'script_name'   : 'emr/diagnosis_common_model_v5.sql',
            'data_type'     : 'diagnosis',
            'date_column'   : 'enc_dt',
            'privacy_filter': priv_diagnosis,
            'filter_args'   : [update_diagnosis_whitelists]
        },
        {
            'table_name'    : 'encounter_common_model',
            'script_name'   : 'emr/encounter_common_model_v4.sql',
            'data_type'     : 'encounter',
            'date_column'   : 'enc_start_dt',
            'privacy_filter': priv_encounter,
            'filter_args'   : [update_encounter_whitelists]
        },
        {
            'table_name'    : 'medication_common_model',
            'script_name'   : 'emr/medication_common_model_v4.sql',
            'data_type'     : 'medication',
            'date_column'   : 'part_mth',
            'privacy_filter': priv_medication,
            'filter_args'   : [update_medication_whitelists]
        },
        {
            'table_name'    : 'procedure_common_model',
            'script_name'   : 'emr/procedure_common_model_v4.sql',
            'data_type'     : 'procedure',
            'date_column'   : 'proc_dt',
            'privacy_filter': priv_procedure
        },
        {
            'table_name'    : 'lab_result_common_model',
            'script_name'   : 'emr/lab_result_common_model_v4.sql',
            'data_type'     : 'lab_result',
            'date_column'   : 'part_mth',
            'privacy_filter': priv_lab_result,
            'filter_args'   : [update_lab_result_whitelists]
        },
        {
            'table_name'    : 'lab_order_common_model',
            'script_name'   : 'emr/lab_order_common_model_v3.sql',
            'data_type'     : 'lab_order',
            'date_column'   : 'part_mth',
            'privacy_filter': priv_lab_order,
            'filter_args'   : [update_lab_order_whitelists]
        },
        {
            'table_name'    : 'provider_order_common_model',
            'script_name'   : 'emr/provider_order_common_model_v4.sql',
            'data_type'     : 'provider_order',
            'date_column'   : 'part_mth',
            'privacy_filter': priv_provider_order
        },
        {
            'table_name'    : 'vital_sign_common_model',
            'script_name'   : 'emr/vital_sign_common_model_v4.sql',
            'data_type'     : 'vital_sign',
            'date_column'   : 'part_mth',
            'privacy_filter': priv_vital_sign
        }
    ]

    min_hvm_date = runner.sqlContext.sql("SELECT gen_ref_1_dt FROM ref_gen_ref WHERE hvm_vdr_feed_id = 35 AND gen_ref_domn_nm = 'HVM_AVAILABLE_HISTORY_START_DATE'").take(1)

    if len(min_hvm_date) > 0:
        historical_date = datetime.combine(min_hvm_date[0].gen_ref_1_dt, time(0))
    elif min_date is not None:
        historical_date = datetime.strptime(min_date, '%Y-%m-%d')
    else:
        historical_date = LAST_RESORT_MIN_DATE

    for table in normalized_tables:
        filter_args = [runner.sqlContext] + table.get('filter_args', [])
        postprocessor.compose(
            postprocessor.add_universal_columns(
                feed_id='35', vendor_id='118', filename=None,

                # rename defaults
                record_id='row_id', created='crt_dt', data_set='data_set_nm',
                data_feed='hvm_vdr_feed_id', data_vendor='hvm_vdr_id',
                model_version='mdl_vrsn_num'
            ),

            table['privacy_filter'].filter(*filter_args)
        )(
            runner.sqlContext.sql('select * from {}'.format(table['table_name']))
        ).createOrReplaceTempView(table['table_name'])

        columns = filter(lambda x: x != 'part_mth', map(lambda x: x.name, \
                      runner.sqlContext.sql('SELECT * FROM {}'.format(table['table_name'])).schema.fields))
        if not test:
            normalized_records_unloader.partition_and_rename(
                spark, runner, 'emr', table['script_name'], '35',
                table['table_name'], table['date_column'], date_input,
                staging_subdir='{}/'.format(table['data_type']),
                distribution_key='row_id', provider_partition='part_hvm_vdr_feed_id',
                date_partition='part_mth', columns=columns, hvm_historical_date=historical_date
            )
        logging.debug("Cleaned up {}".format(table['table_name']))


def main(args):
    # init
    spark, sqlContext = init("Nextgen EMR")

    # initialize runner
    runner = Runner(sqlContext)

    run(spark, runner, args.date, airflow_test=args.airflow_test)

    spark.stop()

    if args.airflow_test:
        output_path = 's3://salusv/testing/dewey/airflow/e2e/nextgen/emr/spark-output/'
    else:
        output_path = 's3://salusv/warehouse/parquet/emr/2017-08-23/'
        try:
            check_output(['hadoop', 'fs', '-ls', '/user/hive/warehouse/encounter_dedup'])
            check_output(['aws', 's3', 'rm', '--recursive',
                        S3_ENCOUNTER_REFERENCE.replace("s3a://", "s3://")])
            check_output(['s3-dist-cp', '--src', '/user/hive/warehouse/encounter_dedup',
                        '--dest', S3_ENCOUNTER_REFERENCE])
        except:
            logging.warn("Something went wrong in persisting the new distinct encounter data")

        try:
            check_output(['hadoop', 'fs', '-ls', '/user/hive/warehouse/demographics_local'])
            check_output(['aws', 's3', 'rm', '--recursive',
                        S3_DEMOGRAPHICS_REFERENCE.replace("s3a://", "s3://")])
            check_output(['s3-dist-cp', '--src', '/user/hive/warehouse/demographics_local',
                        '--dest', S3_DEMOGRAPHICS_REFERENCE])
        except:
            logging.warn("Something went wrong in persisting the new demographics data")


        try:
            check_output(['hadoop', 'fs', '-ls', '/staging/encounter/'])
            check_output(['aws', 's3', 'rm', '--recursive',
                        output_dir + 'encounter/part_hvm_vdr_feed_id=35/'])
        except:
            logging.warn("Something went wrong in removing the old normalized encounter data")


    normalized_records_unloader.distcp(output_path)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('--date', type=str)
    parser.add_argument('--airflow_test', default=False, action='store_true')
    args = parser.parse_args()
    main(args)
