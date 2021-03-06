"""
cardinal rhm normalize
"""
import argparse
import re
import pyspark.sql.functions as FN

from spark.runner import Runner
from spark.spark_setup import init
# from spark.common.medicalclaims_common_model import schema_v6 as schema
from spark.helpers.privacy.common import Transformer, TransformFunction
# import spark.helpers.external_table_loader as external_table_loader
import spark.helpers.schema_enforcer as schema_enforcer
import spark.helpers.privacy.common as common_priv
import spark.helpers.udf.post_normalization_cleanup as post_norm_cleanup

from spark.common.utility.output_type import DataType, RunType
from spark.common.utility.run_recorder import RunRecorder
from spark.common.utility import logger


def actcode_cleanup(actcode):
    if actcode is None:
        return None

    actcode = actcode.upper()

    actcode = re.sub('[^A-Z0-9]', ' ', actcode)
    actcode = actcode.split(' ')[0][:7]

    if len(actcode) > 0:
        return actcode
    return None


def run(spark, runner, date_input, test=False, airflow_test=False):
    encounter = spark.table('sample.cardinal_rheumatology_encounter_prelim')
    lab = spark.table('sample.cardinal_rheumatology_lab_prelim')

    encounter_schema = encounter.schema
    lab_schema = lab.schema

    encounter = \
        encounter.withColumn('const_weight', FN.lit('WEIGHT')) \
        .withColumn('const_weight_pounds', FN.lit('WEIGHT_POUNDS')) \
        .withColumn('const_null', FN.lit(None)) \
        .withColumn('const_height', FN.lit('HEIGHT')) \
        .withColumn('const_height_inches', FN.lit('HEIGHT_INCHES')) \
        .withColumn('const_bmi', FN.lit('BMI')) \
        .withColumn('const_bmi_index', FN.lit('BMI_INDEX'))

    encounter_transformer = Transformer(
        ptnt_birth_yr=[
            TransformFunction(post_norm_cleanup.cap_year_of_birth,
                              ['ptnt_age_num', 'enc_date', 'ptnt_birth_yr'])
        ],
        ptnt_age_num=[
            TransformFunction(post_norm_cleanup.cap_age, ['ptnt_age_num']),
            TransformFunction(post_norm_cleanup.validate_age,
                              ['ptnt_age_num', 'enc_date', 'ptnt_birth_yr'])
        ],
        ptnt_gender_cd=[
            TransformFunction(post_norm_cleanup.clean_up_gender, ['ptnt_gender_cd'])
        ],
        ptnt_state_cd=[
            TransformFunction(post_norm_cleanup.validate_state_code, ['ptntt_state_cd'])
        ],
        ptnt_zip3_cd=[
            TransformFunction(post_norm_cleanup.mask_zip_code, ['ptnt_zip3_cd'])
        ],
        weight_lb=[
            TransformFunction(post_norm_cleanup.clean_up_vital_sign,
                              ['const_weight', 'weight_lb', 'const_weight_pounds', 'ptnt_gender_cd'
                               , 'ptnt_age_num', 'ptnt_birth_yr', 'const_null', 'enc_date'])
        ],
        height_in=[
            TransformFunction(post_norm_cleanup.clean_up_vital_sign,
                              ['const_height', 'height_in', 'const_height_inches', 'ptnt_gender_cd',
                               'ptnt_age_num', 'ptnt_birth_yr', 'const_null', 'enc_date'])
        ],
        bmi_calc=[
            TransformFunction(post_norm_cleanup.clean_up_vital_sign,
                              ['const_bmi', 'bmi_calc', 'const_bmi_index',
                               'ptnt_gender_cd', 'ptnt_age_num',
                               'ptnt_birth_yr', 'const_null', 'enc_date'])
        ],
        primary_diagnosis_code_id=[
            TransformFunction(post_norm_cleanup.clean_up_diagnosis_code,
                              ['primary_diagnosis_code_id', 'const_null', 'enc_date'])
        ],
        med_ndc_id=[
            TransformFunction(post_norm_cleanup.clean_up_ndc_code, ['med_ndc_id'])
        ],
        order_actcode=[
            TransformFunction(actcode_cleanup, ['order_actcode'])
        ],
        order_ndc_id=[
            TransformFunction(post_norm_cleanup.clean_up_ndc_code, ['order_ndc_id'])
        ]
    )

    lab_transformer = Transformer(
        ptnt_birth_yr=[
            TransformFunction(post_norm_cleanup.cap_year_of_birth,
                              ['ptnt_age_num', 'enc_date', 'ptnt_birth_yr'])
        ],
        ptnt_age_num=[
            TransformFunction(post_norm_cleanup.cap_age, ['ptnt_age_num']),
            TransformFunction(post_norm_cleanup.validate_age,
                              ['ptnt_age_num', 'enc_date', 'ptnt_birth_yr'])
        ],
        ptnt_gender_cd=[
            TransformFunction(post_norm_cleanup.clean_up_gender, ['ptnt_gender_cd'])
        ],
        ptnt_state_cd=[
            TransformFunction(post_norm_cleanup.validate_state_code, ['ptntt_state_cd'])
        ],
        ptnt_zip3_cd=[
            TransformFunction(post_norm_cleanup.mask_zip_code, ['ptnt_zip3_cd'])
        ]
    )

    enc2 = common_priv.filter(encounter, encounter_transformer)
    lab2 = common_priv.filter(lab, lab_transformer)

    enc3 = schema_enforcer.apply_schema(enc2, encounter_schema)
    lab3 = schema_enforcer.apply_schema(lab2, lab_schema)

    enc3.repartition(10).write.\
        parquet('s3a://salusv/tmp/cardinal_rhm/processed/encounter/', compression='gzip')
    lab3.repartition(10).write.\
        parquet('s3a://salusv/tmp/cardinal_rhm/processed/lab/', compression='gzip')

    output_paths = ','.join(
        [
            's3a://salusv/tmp/cardinal_rhm/processed/encounter/',
            's3a://salusv/tmp/cardinal_rhm/processed/lab/'
        ])

    if not test and not airflow_test:
        logger.log_run_details(
            provider_name='Cardinal_Rheumatology',
            data_type=DataType.EMR,
            data_source_transaction_path="",
            data_source_matching_path="",
            output_path=output_paths,
            run_type=RunType.MARKETPLACE,
            input_date=date_input
        )


def main(args):
    spark, sql_context = init('Cardinal Rheumatology EMR')

    runner = Runner(sql_context)

    run(spark, runner, args.date, airflow_test=args.airflow_test)

    if not args.airflow_test:
        RunRecorder().record_run_details()


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    args = parser.parse_known_args()[0]
    main(args)
