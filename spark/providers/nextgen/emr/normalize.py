import argparse
from subprocess import check_output
from datetime import datetime, time
from spark.runner import Runner
from spark.spark_setup import init
import spark.helpers.file_utils as file_utils
import spark.helpers.payload_loader as payload_loader
import spark.helpers.postprocessor as postprocessor
import spark.helpers.explode as explode
import spark.helpers.normalized_records_unloader as normalized_records_unloader
import spark.helpers.external_table_loader as external_table_loader
import spark.helpers.schema_enforcer as schema_enforcer
from spark.helpers.privacy.common import Transformer, TransformFunction
import spark.helpers.udf.post_normalization_cleanup as post_norm_cleanup
from spark.providers.nextgen.emr import load_transactions
from spark.providers.nextgen.emr import prepare_demographics
from spark.providers.nextgen.emr import deduplicate_transactions
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
from spark.common.emr.clinical_observation import schema_v4 as clinical_observation_schema
from spark.common.emr.diagnosis import schema_v5 as diagnosis_schema
from spark.common.emr.encounter import schema_v9 as encounter_schema
from spark.common.emr.lab_order import schema_v3 as lab_order_schema
from spark.common.emr.lab_result import schema_v8 as lab_result_schema
from spark.common.emr.medication import schema_v4 as medication_schema
from spark.common.emr.procedure import schema_v11 as procedure_schema
from spark.common.emr.provider_order import schema_v9 as provider_order_schema
from spark.common.emr.vital_sign import schema_v4 as vital_sign_schema

import pyspark.sql.functions as FN

import logging

from spark.common.utility.output_type import DataType, RunType
from spark.common.utility.run_recorder import RunRecorder
from spark.common.utility import logger


LAST_RESORT_MIN_DATE = datetime(1900, 1, 1)
S3_ENCOUNTER_REFERENCE = 's3://salusv/reference/nextgen/encounter_deduped/'
S3_DEMOGRAPHICS_REFERENCE = 's3://salusv/reference/nextgen/demographics_orc/'
S3_CROSSWALK_REFERENCE = 's3://salusv/reference/nextgen/crosswalk/'

HDFS_ENCOUNTER_REFERENCE = '/user/hive/warehouse/encounter_dedup'
HDFS_DEMOGRAPHICS_REFERENCE = '/user/hive/warehouse/demographics_local'

OUTPUT_PATH_TEST = 's3://salusv/testing/dewey/airflow/e2e/nextgen/emr/spark-output/'
OUTPUT_PATH_PRODUCTION = 's3://salusv/warehouse/parquet/emr/2017-08-23/'


def run(spark, runner, date_input, input_file_path, payload_path, normalize_encounter=True,
        demo_ref=S3_DEMOGRAPHICS_REFERENCE, enc_ref=S3_ENCOUNTER_REFERENCE,
        test=False, airflow_test=False):
    runner.sqlContext.sql(
        'SET mapreduce.output.fileoutputformat.compress.codec=org.apache.hadoop.io.compress.GzipCodec')
    runner.sqlContext.sql('SET hive.exec.compress.output=true')
    runner.sqlContext.sql('SET mapreduce.output.fileoutputformat.compress=true')

    script_path = __file__

    if test:
        input_path = file_utils.get_abs_path(
            script_path, '../../../test/providers/nextgen/emr/resources/input/'
        ) + '/'
        demo_reference_path = file_utils.get_abs_path(
            script_path, '../../../test/providers/nextgen/emr/resources/reference/demo/'
        ) + '/'
        enc_reference_path = file_utils.get_abs_path(
            script_path, '../../../test/providers/nextgen/emr/resources/reference/enc/'
        ) + '/'
        matching_path = file_utils.get_abs_path(
            script_path, '../../../test/providers/nextgen/emr/resources/matching/'
        ) + '/'
        crosswalk_path = file_utils.get_abs_path(
            script_path, '../../../test/providers/nextgen/emr/resources/reference/crosswalk/'
        ) + '/'
    elif airflow_test:
        input_path = 's3://salusv/testing/dewey/airflow/e2e/nextgen/emr/input/{}/'.format(
            date_input.replace('-', '/')
        )
        demo_reference_path = demo_ref
        enc_reference_path = enc_ref
        matching_path = 's3://salusv/testing/dewey/airflow/e2e/nextgen/emr/payload/{}/'.format(
            date_input.replace('-', '/')
        )
        crosswalk_path = S3_CROSSWALK_REFERENCE
    else:
        input_path = 's3a://salusv/incoming/emr/nextgen/{}/'.format(
            date_input.replace('-', '/')
        ) if input_file_path is None else input_file_path
        demo_reference_path = demo_ref
        enc_reference_path = enc_ref
        matching_path = 's3a://salusv/matching/payload/emr/nextgen/{}/'.format(
            date_input.replace('-', '/')
        ) if payload_path is None else payload_path
        crosswalk_path = S3_CROSSWALK_REFERENCE

    external_table_loader.load_icd_diag_codes(runner.sqlContext)
    external_table_loader.load_icd_proc_codes(runner.sqlContext)
    external_table_loader.load_hcpcs_codes(runner.sqlContext)
    external_table_loader.load_cpt_codes(runner.sqlContext)
    external_table_loader.load_loinc_codes(runner.sqlContext)
    external_table_loader.load_ref_gen_ref(runner.sqlContext)
    logging.debug("Loaded external tables")

    min_date = postprocessor.get_gen_ref_date(runner.sqlContext, 35, 'EARLIEST_VALID_SERVICE_DATE')
    min_date = min_date.isoformat().split("T")[0] if min_date is not None else None
    max_date = date_input
    min_diag_date = postprocessor\
        .get_gen_ref_date(runner.sqlContext, 35, 'EARLIEST_VALID_DIAGNOSIS_DATE')
    min_diag_date = min_diag_date.isoformat().split("T")[0] if min_diag_date is not None else None
    logging.debug("Loaded min dates")

    explode.generate_exploder_table(spark, 20, 'lab_order_exploder')
    explode.generate_exploder_table(spark, 4, 'lipid_exploder')
    explode.generate_exploder_table(spark, 15, 'vital_signs_exploder')
    explode.generate_exploder_table(spark, 5, 'medication_exploder')
    explode.generate_exploder_table(spark, 2, 'procedure_2_exploder')
    logging.debug("Created exploder tables")

    payload_loader.load(runner, matching_path, ['hvJoinKey'], load_file_name=True, allow_empty=True)

    load_transactions.load(runner, input_path, enc_reference_path, demo_reference_path, test=test)
    logging.debug("Loaded transactions data")

    # Append HVIDs to the demographics table
    prepare_demographics.prepare(spark, runner, crosswalk_path)
    deduplicate_transactions.deduplicate(runner, test=test)

    transaction_tables = [
        'demographics_local', 'encounter_dedup', 'vitalsigns', 'lipidpanel',
        'allergy', 'substanceusage', 'diagnosis', 'order', 'laborder',
        'labresult', 'medicationorder', 'procedure', 'extendeddata'
    ]

    # trim and nullify all incoming transactions tables
    for table in transaction_tables:
        postprocessor.compose(
            postprocessor.trimmify, postprocessor.nullify
        )(runner.sqlContext.table(table)).createOrReplaceTempView(table)

    runner.sqlContext.table(
        'demographics_local').cache_and_track('demographics_local')\
        .createOrReplaceTempView('demographics_local')

    runner.sqlContext.table('demographics_local').count()

    normalized = {}
    if normalize_encounter:
        normalized['encounter'] = runner.run_spark_script('normalize_encounter.sql', [
            ['min_date', min_date],
            ['max_date', max_date]
        ], return_output=True)
        logging.debug("Normalized encounter")

    normalized['diagnosis'] = runner.run_spark_script('normalize_diagnosis.sql', [
        ['min_date', min_date],
        ['max_date', max_date],
        ['diag_min_date', min_diag_date]
    ], return_output=True)
    logging.debug("Normalized diagnosis")

    runner.run_spark_script('normalize_procedure_2_prenorm.sql',
                            [
                                ['min_date', min_date],
                                ['max_date', max_date]
                            ],
                            return_output=True)\
        .createOrReplaceTempView('procedure_order_real_actcode')
    normalized['procedure'] = schema_enforcer.apply_schema(
        runner.run_spark_script('normalize_procedure_1.sql', [
            ['min_date', min_date],
            ['max_date', max_date]
        ], return_output=True), procedure_schema
    ) \
        .union(
            schema_enforcer.apply_schema(
                runner.run_spark_script('normalize_procedure_2.sql', [
                    ['min_date', min_date],
                    ['max_date', max_date],
                ], return_output=True), procedure_schema
            )
        )
    logging.debug("Normalized procedure")

    normalized['lab_order'] = runner.run_spark_script('normalize_lab_order.sql', [
        ['min_date', min_date],
        ['max_date', max_date]
    ], return_output=True)
    logging.debug("Normalized lab order")

    normalized['lab_result'] = schema_enforcer.apply_schema(
        runner.run_spark_script('normalize_lab_result_1.sql', [
            ['min_date', min_date],
            ['max_date', max_date]
            ], return_output=True), lab_result_schema, columns_to_keep=['part_mth']) \
        .union(schema_enforcer.apply_schema(
            runner.run_spark_script('normalize_lab_result_2.sql', [
                ['min_date', min_date],
                ['max_date', max_date]
            ], return_output=True), lab_result_schema, columns_to_keep=['part_mth']))
    logging.debug("Normalized lab result")

    runner.sqlContext.table('medicationorder') \
        .withColumn('row_num', FN.monotonically_increasing_id()) \
        .createOrReplaceTempView('medicationorder')
    icd_diag_codes = runner.sqlContext.table('icd_diag_codes')
    tmp_medication = runner.run_spark_script('normalize_medication.sql', [
        ['min_date', min_date],
        ['max_date', max_date]
    ], return_output=True)
    p1 = tmp_medication.where('medctn_diag_cd IS NULL')
    p2 = tmp_medication.where('medctn_diag_cd IS NOT NULL') \
        .join(icd_diag_codes, tmp_medication.medctn_diag_cd == icd_diag_codes.code, 'left_outer') \
        .select(*(
            [tmp_medication[c] for c in tmp_medication.columns if c != 'medctn_diag_cd'] + 
            [icd_diag_codes.code.alias('medctn_diag_cd')]
        )).select(*p1.columns)
    #  Re-order or else the union will not work correctly

    # The row_num column is generated inside the normalize_medication.sql
    # script in order to ensure that when we run a distinct to remove
    # duplicates, we maintain at least 1 normalized row per source row
    normalized['medication'] = p1.union(p2).distinct().drop('row_num') \
        .withColumn('medctn_diag_dt', FN.coalesce(FN.col('medctn_ord_dt'), FN.col('enc_dt')))
    # For whitelisting purposes
    logging.debug("Normalized medication")

    runner.run_spark_script('normalize_provider_order_prenorm.sql', [
        ['min_date', min_date],
        ['max_date', max_date]
    ], return_output=True).createOrReplaceTempView('ord_clean_actcodes')
    normalized['provider_order'] = runner.run_spark_script('normalize_provider_order.sql', [
        ['min_date', min_date],
        ['max_date', max_date]
    ], return_output=True) \
        .withColumn('prov_ord_diag_dt', FN.coalesce(FN.col('prov_ord_dt'), FN.col('enc_dt')))
    # For whitelisting purposes
    logging.debug("Normalized provider order")

    cln_obs1 = runner.run_spark_script('normalize_clinical_observation_1.sql', [
        ['min_date', min_date],
        ['max_date', max_date]
    ], return_output=True)
    cln_obs3 = runner.run_spark_script('normalize_clinical_observation_3.sql', [
        ['min_date', min_date],
        ['max_date', max_date]
    ], return_output=True)
    normalized['clinical_observation'] = \
        clean_and_union_cln_obs(runner.sqlContext, cln_obs1, cln_obs3)
    logging.debug("Normalized clinical observation")

    runner.run_spark_script('normalize_vital_sign_prenorm.sql', [
        ['min_date', min_date],
        ['max_date', max_date]
    ], return_output=True).createOrReplaceTempView('vitalsigns_w_msrmt')
    normalized['vital_sign'] = runner.run_spark_script('normalize_vital_sign.sql', [
        ['min_date', min_date],
        ['max_date', max_date]
    ], return_output=True)
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

    def update_provider_order_whitelists(whitelists):
        return whitelists + [{
            'column_name': 'prov_ord_alt_cd',
            'domain_name': 'emr_prov_ord.prov_ord_alt_cd',
            'whitelist_col_name': 'gen_ref_cd'
        }, {
            'column_name': 'prov_ord_alt_nm',
            'domain_name': 'emr_prov_ord.prov_ord_alt_nm',
            'whitelist_col_name': 'gen_ref_itm_nm'
        }, {
            'column_name': 'prov_ord_alt_desc',
            'domain_name': 'emr_prov_ord.prov_ord_alt_desc',
            'whitelist_col_name': 'gen_ref_itm_desc'
        }, {
            'column_name': 'prov_ord_diag_nm',
            'domain_name': 'emr_prov_ord.prov_ord_diag_nm',
            'whitelist_col_name': 'gen_ref_itm_nm'
        }, {
            'column_name': 'prov_ord_rsn_cd',
            'domain_name': 'emr_prov_ord.prov_ord_rsn_cd',
            'whitelist_col_name': 'gen_ref_cd',
            'comp_col_names': ['prov_ord_rsn_cd_qual']
        }, {
            'column_name': 'prov_ord_rsn_nm',
            'domain_name': 'emr_prov_ord.prov_ord_rsn_nm',
            'whitelist_col_name': 'gen_ref_itm_nm',
        }, {
            'column_name': 'prov_ord_stat_cd',
            'domain_name': 'emr_prov_ord.prov_ord_stat_cd',
            'whitelist_col_name': 'gen_ref_cd',
            'comp_col_names': ['prov_ord_stat_cd_qual']
        }, {
            'column_name': 'prov_ord_complt_rsn_cd',
            'domain_name': 'emr_prov_ord.prov_ord_complt_rsn_cd',
            'whitelist_col_name': 'gen_ref_cd'
        }, {
            'column_name': 'prov_ord_cxld_rsn_cd',
            'domain_name': 'emr_prov_ord.prov_ord_cxld_rsn_cd',
            'whitelist_col_name': 'gen_ref_cd'
        }, {
            'column_name': 'prov_ord_result_desc',
            'domain_name': 'emr_prov_ord.prov_ord_result_desc',
            'whitelist_col_name': 'gen_ref_itm_desc'
        }, {
            'column_name': 'prov_ord_trtmt_typ_cd',
            'domain_name': 'emr_prov_ord.prov_ord_trtmt_typ_cd',
            'whitelist_col_name': 'gen_ref_cd',
            'comp_col_names': ['prov_ord_trtmt_typ_cd_qual']
        }, {
            'column_name': 'prov_ord_rfrd_speclty_cd',
            'domain_name': 'emr_prov_ord.prov_ord_rfrd_speclty_cd',
            'whitelist_col_name': 'gen_ref_cd',
            'comp_col_names': ['prov_ord_rfrd_speclty_cd_qual']
        }, {
            'column_name': 'prov_ord_specl_instrs_desc',
            'domain_name': 'emr_prov_ord.prov_ord_specl_instrs_desc',
            'whitelist_col_name': 'gen_ref_itm_desc'
        }]

    provider_order_transformer = Transformer(
        prov_ord_diag_cd=[
            TransformFunction(post_norm_cleanup.clean_up_diagnosis_code,
                              ['prov_ord_diag_cd', 'prov_ord_diag_cd_qual', 'prov_ord_diag_dt'])
        ]
    )

    def update_procedure_whitelists(whitelists):
        return whitelists + [{
            'column_name': 'proc_stat_cd',
            'domain_name': 'emr_proc.proc_stat_cd',
            'whitelist_col_name': 'gen_ref_cd',
            'comp_col_names': ['proc_stat_cd_qual']
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
        }]

    medication_transformer = Transformer(
        medctn_diag_cd=[
            TransformFunction(post_norm_cleanup.clean_up_diagnosis_code,
                              ['medctn_diag_cd', 'medctn_diag_cd_qual', 'medctn_diag_dt'])
        ]
    )

    normalized_tables = [
        {
            'schema': clinical_observation_schema,
            'data_type': 'clinical_observation',
            'date_column': 'part_mth',
            'privacy_filter': priv_clinical_observation,
            'partitions': 100
        },
        {
            'schema': diagnosis_schema,
            'data_type': 'diagnosis',
            'date_column': 'enc_dt',
            'privacy_filter': priv_diagnosis,
            'filter_args': [update_diagnosis_whitelists]
        },
        {
            'schema': medication_schema,
            'data_type': 'medication',
            'date_column': 'part_mth',
            'privacy_filter': priv_medication,
            'filter_args': [update_medication_whitelists, medication_transformer],
            'cols_to_keep': ['medctn_diag_dt'],
            'cols_to_drop': 'medctn_diag_dt'
        },
        {
            'schema': procedure_schema,
            'data_type': 'procedure',
            'date_column': 'proc_dt',
            'privacy_filter': priv_procedure,
            'filter_args': [update_procedure_whitelists]
        },
        {
            'schema': lab_result_schema,
            'data_type': 'lab_result',
            'date_column': 'part_mth',
            'privacy_filter': priv_lab_result,
            'filter_args': [update_lab_result_whitelists]
        },
        {
            'schema': lab_order_schema,
            'data_type': 'lab_order',
            'date_column': 'part_mth',
            'privacy_filter': priv_lab_order,
            'filter_args': [update_lab_order_whitelists]
        },
        {
            'schema': provider_order_schema,
            'data_type': 'provider_order',
            'date_column': 'part_mth',
            'privacy_filter': priv_provider_order,
            'filter_args': [update_provider_order_whitelists, provider_order_transformer],
            'cols_to_keep': ['prov_ord_diag_dt'],
            'cols_to_drop': 'prov_ord_diag_dt',
            'partitions': 40
        },
        {
            'schema': vital_sign_schema,
            'data_type': 'vital_sign',
            'date_column': 'part_mth',
            'privacy_filter': priv_vital_sign
        }
    ]

    if normalize_encounter:
        normalized_tables.append({
            'schema': encounter_schema,
            'data_type': 'encounter',
            'date_column': 'enc_start_dt',
            'privacy_filter': priv_encounter,
            'filter_args': [update_encounter_whitelists]
        })

    min_hvm_date = postprocessor.\
        get_gen_ref_date(runner.sqlContext, 35, 'HVM_AVAILABLE_HISTORY_START_DATE')

    if min_hvm_date is not None:
        historical_date = datetime.combine(min_hvm_date, time(0))
    elif min_date is not None:
        historical_date = datetime.strptime(min_date, '%Y-%m-%d')
    else:
        historical_date = LAST_RESORT_MIN_DATE

    for table in normalized_tables:
        filter_args = [runner.sqlContext] + table.get('filter_args', [])
        cols_to_keep = table.get('cols_to_keep', []) + \
                       (['part_mth'] if table['date_column'] == 'part_mth' else [])
        df = postprocessor.compose(
            postprocessor.add_universal_columns(
                feed_id='35', vendor_id='118', filename=None,

                # rename defaults
                record_id='row_id', created='crt_dt', data_set='data_set_nm',
                data_feed='hvm_vdr_feed_id', data_vendor='hvm_vdr_id',
                model_version='mdl_vrsn_num'
            ),
            schema_enforcer.apply_schema_func(table['schema'], cols_to_keep=cols_to_keep),
            table['privacy_filter'].filter(*filter_args),
            postprocessor.trimmify,
            postprocessor.nullify,
            schema_enforcer.apply_schema_func(table['schema'], cols_to_keep=cols_to_keep)
        )(
            normalized[table['data_type']]
        )

        df = df.drop(*table.get('cols_to_drop', []))

        df.createOrReplaceTempView(table['data_type'] + '_common_model')

        if not test:
            normalized_records_unloader.unload(
                spark, runner, df, table['date_column'], date_input,
                provider_partition_value='35',
                provider_partition_name='part_hvm_vdr_feed_id',
                date_partition_name='part_mth', 
                hvm_historical_date=historical_date,
                staging_subdir='{}/'.format(table['data_type']),
                columns=table['schema'].names, distribution_key='row_id',
                unload_partition_count=table.get('partitions', 20)
            )
        logging.debug("Cleaned up {}".format(table['data_type']))

    if not test:
        runner.sqlContext.table('encounter_dedup').coalesce(1000).write.orc(
            HDFS_ENCOUNTER_REFERENCE, compression='zlib', mode='overwrite')
        runner.sqlContext.table('demographics_local').coalesce(1000).write.orc(
            HDFS_DEMOGRAPHICS_REFERENCE, compression='zlib', mode='overwrite')

    if not test and not airflow_test:
        logger.log_run_details(
            provider_name='NextGen',
            data_type=DataType.EMR,
            data_source_transaction_path=input_path,
            data_source_matching_path=matching_path,
            output_path=OUTPUT_PATH_PRODUCTION,
            run_type=RunType.MARKETPLACE,
            input_date=date_input
        )


def clean_and_union_cln_obs(sqlc, cln_obs1, cln_obs3):
    substanceusage_whitelists = [{
        'column_name': 'clin_obsn_typ_cd',
        'domain_name': 'substanceusage.clinicalrecordtypecode',
        'whitelist_col_name': 'gen_ref_cd',
        'feed_id': '35',
        'comp_col_names': ['clin_obsn_typ_cd_qual']
    }, {
        'column_name': 'clin_obsn_typ_nm',
        'domain_name': 'substanceusage.clinicalrecorddescription',
        'feed_id': '35'
    }]

    extendeddata_whitelists = [{
        'column_name': 'clin_obsn_typ_cd',
        'domain_name': 'extendeddata.clinicalrecordtypecode',
        'whitelist_col_name': 'gen_ref_cd',
        'feed_id': '35',
        'comp_col_names': ['clin_obsn_typ_cd_qual']
    }, {
        'column_name': 'clin_obsn_typ_nm',
        'domain_name': 'extendeddata.clinicalrecorddescription',
        'feed_id': '35'
    }]

    p1 = postprocessor.compose(
        *[
            postprocessor.apply_whitelist(
                sqlc, whitelist['column_name'], whitelist['domain_name'],
                comp_col_names=whitelist.get('comp_col_names'),
                whitelist_col_name=whitelist.get('whitelist_col_name'),
                clean_up_freetext_fn=whitelist.get('clean_up_freetext_fn')
            )
            for whitelist in substanceusage_whitelists
        ]
    )(cln_obs1)

    p2 = postprocessor.compose(
        *[
            postprocessor.apply_whitelist(
                sqlc, whitelist['column_name'], whitelist['domain_name'],
                comp_col_names=whitelist.get('comp_col_names'),
                whitelist_col_name=whitelist.get('whitelist_col_name'),
                clean_up_freetext_fn=whitelist.get('clean_up_freetext_fn')
            )
            for whitelist in extendeddata_whitelists
        ]
    )(cln_obs3)

    # Reorder before unioning
    return p2.union(p1.select(*[p1[c] for c in p2.columns]))


def main(args):
    # init
    spark, sqlContext = init("Nextgen EMR")

    # initialize runner
    runner = Runner(sqlContext)

    if args.airflow_test:
        output_path = OUTPUT_PATH_TEST
    elif args.output_path:
        output_path = args.output_path
    else:
        output_path = OUTPUT_PATH_PRODUCTION

    run(spark, runner, args.date, args.input_path,
        args.payload_path, normalize_encounter=args.normalize_encounter,
        demo_ref=args.input_demo_ref, enc_ref=args.input_enc_ref,
        airflow_test=args.airflow_test)

    spark.stop()

    if not args.airflow_test and not args.output_path:
        try:
            check_output(['hadoop', 'fs', '-ls', HDFS_ENCOUNTER_REFERENCE])
            if args.output_enc_ref.startswith('s3://'):
                check_output(['aws', 's3', 'rm', '--recursive', args.output_enc_ref])
                check_output(['s3-dist-cp', '--src', HDFS_ENCOUNTER_REFERENCE,
                              '--dest', args.output_enc_ref])
            elif args.output_enc_ref.startswith('hdfs://'):
                check_output(['hadoop', 'fs', '-rm', '-r', '-f', args.output_enc_ref])
                check_output(['s3-dist-cp', '--src', HDFS_ENCOUNTER_REFERENCE,
                              '--dest', args.output_enc_ref])
            else:
                raise ValueError("Unexpected protocol in encounter output path")
            check_output(['hadoop', 'fs', '-rm', '-r', '-f', HDFS_ENCOUNTER_REFERENCE])
        except:
            logging.warning("Something went wrong in persisting the new distinct encounter data")

        try:
            check_output(['hadoop', 'fs', '-ls', HDFS_DEMOGRAPHICS_REFERENCE])
            if args.output_enc_ref.startswith('s3://'):
                check_output(['aws', 's3', 'rm', '--recursive', args.output_demo_ref])
                check_output(['s3-dist-cp', '--src', HDFS_DEMOGRAPHICS_REFERENCE,
                              '--dest', args.output_demo_ref])
            elif args.output_enc_ref.startswith('hdfs://'):
                check_output(['hadoop', 'fs', '-rm', '-r', '-f', args.output_demo_ref])
                check_output(['s3-dist-cp', '--src', HDFS_DEMOGRAPHICS_REFERENCE,
                              '--dest', args.output_demo_ref])
            else:
                raise ValueError("Unexpected protocol in demographics output path")
            check_output(['hadoop', 'fs', '-rm', '-r', '-f', HDFS_DEMOGRAPHICS_REFERENCE])
        except:
            logging.warning("Something went wrong in persisting the new demographics data")

        if args.normalize_encounter:
            try:
                check_output(['hadoop', 'fs', '-ls', '/staging/encounter/'])
                check_output(['aws', 's3', 'rm', '--recursive',
                              output_path + 'encounter/part_hvm_vdr_feed_id=35/'])
            except:
                logging.warning("Something went wrong in removing the old normalized encounter data")

    if args.airflow_test:
        normalized_records_unloader.distcp(output_path)
    else:
        hadoop_time = normalized_records_unloader.timed_distcp(output_path)
        RunRecorder().record_run_details(additional_time=hadoop_time)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('--date', type=str)
    parser.add_argument('--input_path', type=str)
    parser.add_argument('--payload_path', type=str)
    parser.add_argument('--output_path', type=str)
    parser.add_argument('--input_demo_ref', default=S3_DEMOGRAPHICS_REFERENCE, type=str)
    parser.add_argument('--output_demo_ref', default=S3_DEMOGRAPHICS_REFERENCE, type=str)
    parser.add_argument('--input_enc_ref', default=S3_ENCOUNTER_REFERENCE, type=str)
    parser.add_argument('--output_enc_ref', default=S3_ENCOUNTER_REFERENCE, type=str)
    parser.add_argument('--dont_normalize_encounter', default=True, action='store_false',
                        dest='normalize_encounter')
    parser.add_argument('--airflow_test', default=False, action='store_true')
    args = parser.parse_known_args()[0]
    main(args)
