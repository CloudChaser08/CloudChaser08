#! /usr/bin/python
import argparse
import time
import logging
import subprocess
from datetime import timedelta, datetime
from spark.runner import Runner
from spark.spark_setup import init
import spark.helpers.file_utils as file_utils
import spark.helpers.payload_loader as payload_loader
import spark.helpers.normalized_records_unloader as normalized_records_unloader


TODAY = time.strftime('%Y-%m-%d', time.localtime())
S3_EXPRESS_SCRIPTS_IN = 's3a://salusv/incoming/enrollmentrecords/express_scripts/'
S3_EXPRESS_SCRIPTS_OUT = 's3a://salusv/warehouse/parquet/enrollmentrecords/2017-03-22/'
S3_EXPRESS_SCRIPTS_ENROLLMENT_MATCHING = 's3a://salusv/matching/payload/enrollmentrecords/express_scripts/'
S3_EXPRESS_SCRIPTS_RX_MATCHING = 's3a://salusv/matching/payload/pharmacyclaims/esi/'

S3A_REF_PHI = 's3a://salusv/reference/express_scripts_phi/'
S3_REF_PHI = 's3://salusv/reference/express_scripts_phi/'
LOCAL_REF_PHI = '/local_phi/'


def run (spark, runner, date_input, test=False):
    setid = '10130X001_HV_RX_ENROLLMENT_D{}.txt'.format(date_input.replace('-',''))

    min_date = '2008-01-01'
    max_date = date_input

    # create helper tables
    runner.run_spark_script(file_utils.get_rel_path(
        __file__,
        '../../../common/zip3_to_state.sql'
    ))

    runner.run_spark_script(file_utils.get_rel_path(
            __file__,
            '../../../common/enrollment_common_model.sql'
        ), [
            ['table_name', 'enrollment_common_model', False],
            ['properties', '', False]
    ])

    date_path = date_input.replace('-', '/')

    if test:
        input_path = file_utils.get_rel_path(
            __file__,
            '../../../test/providers/express_scripts/enrollmentrecords/resources/input/'
        )
        matching_path = file_utils.get_rel_path(
            __file__,
            '../../../test/providers/express_scripts/enrollmentrecords/resources/matching/'
        )
        new_phi_path = file_utils.get_rel_path(
            __file__,
            '../../../test/providers/express_scripts/enrollmentrecords/resources/new_phi/'
        )
        ref_phi_path = file_utils.get_rel_path(
            __file__,
            '../../../test/providers/express_scripts/enrollmentrecords/resources/ref_phi/'
        )
        local_phi_path = '/tmp' + LOCAL_REF_PHI
    else:
        input_path     = S3_EXPRESS_SCRIPTS_IN + date_path + '/'
        matching_path  = S3_EXPRESS_SCRIPTS_ENROLLMENT_MATCHING + date_path + '/'
        new_phi_path   = S3_EXPRESS_SCRIPTS_RX_MATCHING + date_path + '/'
        ref_phi_path   = S3A_REF_PHI
        local_phi_path = 'hdfs://' + LOCAL_REF_PHI


    runner.run_spark_script(file_utils.get_rel_path(
            __file__,
            'load_enrollment_records.sql'
        ), [
            ['input_path', input_path]
    ])

    payload_loader.load(runner, new_phi_path, ['hvJoinKey', 'patientId'])

    runner.run_spark_query('ALTER TABLE matching_payload RENAME TO new_phi')

    runner.run_spark_script(file_utils.get_rel_path(
            __file__,
            'load_matching_payload.sql'
        ), [
            ['matching_path', matching_path] 
    ])

    if test:
        subprocess.check_call(['rm', '-r', local_phi_path])
        subprocess.check_call(['mkdir', '-p', local_phi_path])
    else:
        subprocess.check_call(['hadoop', 'fs', '-rm', '-r', '-f', local_phi_path])
        subprocess.check_call(['hadoop', 'fs', '-mkdir', local_phi_path])

    runner.run_spark_script(file_utils.get_rel_path(
            __file__,
            'load_and_combine_phi.sql'
        ), [
            ['local_phi_path', local_phi_path],
            ['s3_phi_path', ref_phi_path]
    ])

    runner.run_spark_script(file_utils.get_rel_path(
        __file__, 'normalize.sql'
    ), [
        ['filename', setid],
        ['today', TODAY],
        ['feedname', '16'],
        ['vendor', '17']
    ])

    if not test:
        normalized_records_unloader.partition_and_rename(spark, runner, 'enrollmentrecords', 'enrollment_common_model.sql',
            'express_scripts', 'enrollment_common_model', 'date_service', date_input, date_input[:-3])

def main(args):
    # init
    spark, sqlContext = init("Express Scripts")

    # initialize runner
    runner = Runner(sqlContext)

    run(spark, runner, args.date)

    spark.stop()

    normalized_records_unloader.distcp(S3_EXPRESS_SCRIPTS_OUT)

    # offload reference data
    subprocess.check_call(['aws', 's3', 'rm', '--recursive', S3_REF_PHI])
    subprocess.check_call(['s3-dist-cp', '--s3ServerSideEncryption', '--src', 'hdfs://' + LOCAL_REF_PHI, '--dest', S3A_REF_PHI])

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('--date', type=str)
    args = parser.parse_args()
    main(args)
