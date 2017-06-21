#!/usr/bin/python
import subprocess
import argparse
import time
import sys
import os
import logging
sys.path.append(os.path.abspath("../redshift_norm_common/"))
import create_date_validation_table as date_validator

TODAY = time.strftime('%Y-%m-%d', time.localtime())
S3_ABILITY_INPUT = 's3://salusv/incoming/medicalclaims/ability/'
S3_ABILITY_OUTPUT = 's3://salusv/warehouse/text/medicalclaims/ability/'
S3_ABILITY_MATCHING = 's3://salusv/matching/payload/medicalclaims/ability/'

parser = argparse.ArgumentParser()
parser.add_argument('--date', type=str, required=True)
parser.add_argument('--s3_credentials', type=str, required=True)
args, unknown = parser.parse_known_args()

input_path = S3_ABILITY_INPUT + args.date.replace('-', '/') + '/'
matching_path = S3_ABILITY_MATCHING + args.date.replace('-', '/') + '/'
output_path = S3_ABILITY_OUTPUT
db = 'dev'

psql = ['psql', '-p', '5439']

# generate date validation table
date_validator.generate(args.s3_credentials)

for product in [
        'ap', 'ses', 'ease'
]:

    setid = '{}_{}'.format(args.date.replace('-', '_'), product)
    input_prefix = input_path + setid + '_'

    try:
        subprocess.check_output(['aws', 's3', 'ls', input_prefix])
    except Exception as e:
        logging.warn('Prefix does not exist: ' + input_prefix)
        continue

    # create table for medical claims common model
    subprocess.call(' '.join(
        psql
        + ['-v', 'filename="\'' + setid + '\'"']
        + ['-v', 'today="\'' + TODAY + '\'"']
        + ['-v', 'feedname="\'15\'"']
        + ['-v', 'vendor="\'14\'"']
        + [db, '<', '../redshift_norm_common/medicalclaims_common_model.sql']
    ), shell=True)

    if product == 'ap' and args.date <= '2017-02-15':
        subprocess.call(' '.join(
            psql
            + ['-v', 'claimaffiliation_path="\'' + S3_ABILITY_INPUT
               + 'vwclaimaffiliation_correction_20170215'
               + '/ap_vwclaimaffiliation.txt.20140101_20170215' + '\'"']
            + ['-v', 'credentials="\'' + args.s3_credentials + '\'"']
            + [db, '<', 'load_vwclaimaffiliation.sql']
        ), shell=True)
    else:
        subprocess.call(' '.join(
            psql
            + ['-v', 'claimaffiliation_path="\''
               + input_prefix + 'vwclaimaffiliation' + '\'"']
            + ['-v', 'credentials="\'' + args.s3_credentials + '\'"']
            + [db, '<', 'load_vwclaimaffiliation.sql']
        ), shell=True)

    # load data
    subprocess.call(' '.join(
        psql
        + ['-v', 'header_path="\'' + input_prefix + 'record.vwheader' + '\'"']
        + ['-v', 'serviceline_path="\'' + input_prefix + 'vwserviceline.' + '\'"']
        + ['-v', 'servicelineaffiliation_path="\'' + input_prefix + 'vwservicelineaffiliation' + '\'"']
        + ['-v', 'diagnosis_path="\'' + input_prefix + 'vwdiagnosis' + '\'"']
        + ['-v', 'procedure_path="\'' + input_prefix + 'vwprocedurecode' + '\'"']
        + ['-v', 'billing_path="\'' + input_prefix + 'vwbilling' + '\'"']
        + ['-v', 'payer_path="\'' + input_prefix + 'record.vwpayer' + '\'"']
        + ['-v', 'credentials="\'' + args.s3_credentials + '\'"']
        + [db, '<', 'load_transactions.sql']
    ), shell=True)

    matching_prefix = matching_path + setid + '_'
    subprocess.call(' '.join(
        psql
        + ['-v', 'matching_path="\'' + matching_prefix + '\'"']
        + ['-v', 'credentials="\'' + args.s3_credentials + '\'"']
        + [db, '<', 'load_matching_payload.sql']
    ), shell=True)

    # normalize
    subprocess.call(' '.join(psql + [db, '<', 'normalize.sql']), shell=True)

    # privacy filtering
    subprocess.call(' '.join(psql + ['-v', 'table_name=medicalclaims_common_model'] +
                            ['-v', 'column_name=diagnosis_code'] +
                            ['-v', 'qual_column_name=diagnosis_code_qual'] +
                            ['-v', 'service_date_column_name=date_service'] +
                            [db, '<', '../redshift_norm_common/nullify_icd9_blacklist.sql']), shell=True)
    subprocess.call(' '.join(psql + ['-v', 'table_name=medicalclaims_common_model'] +
                            ['-v', 'column_name=diagnosis_code'] +
                            ['-v', 'qual_column_name=diagnosis_code_qual'] +
                            ['-v', 'service_date_column_name=date_service'] +
                            [db, '<', '../redshift_norm_common/nullify_icd10_blacklist.sql']), shell=True)
    subprocess.call(' '.join(psql + ['-v', 'table_name=medicalclaims_common_model'] +
                            ['-v', 'column_name=diagnosis_code'] +
                            ['-v', 'qual_column_name=diagnosis_code_qual'] +
                            ['-v', 'service_date_column_name=date_service'] +
                            [db, '<', '../redshift_norm_common/genericize_icd9.sql']), shell=True)
    subprocess.call(' '.join(psql + ['-v', 'table_name=medicalclaims_common_model'] +
                            ['-v', 'column_name=diagnosis_code'] +
                            ['-v', 'qual_column_name=diagnosis_code_qual'] +
                            ['-v', 'service_date_column_name=date_service'] +
                            [db, '<', '../redshift_norm_common/genericize_icd10.sql']), shell=True)
    subprocess.call(' '.join(psql + [db, '<', '../redshift_norm_common/scrub_place_of_service.sql']), shell=True)
    subprocess.call(' '.join(psql + [db, '<', '../redshift_norm_common/scrub_discharge_status.sql']), shell=True)
    subprocess.call(' '.join(psql + [db, '<', '../redshift_norm_common/nullify_drg_blacklist.sql']), shell=True)
    subprocess.call(' '.join(psql + ['-v', 'table_name=medicalclaims_common_model'] +
                            ['-v', 'column_name=patient_age'] +
                            [db, '<', '../redshift_norm_common/cap_age.sql']), shell=True)

    # unload to s3
    year_months = subprocess.check_output(' '.join(
        psql
        + [db, '-c', '\'select distinct substring(date_service, 0, 8) from medicalclaims_common_model\'']
    ), shell=True)

    for ym in map(
            lambda x: x.strip(),
            year_months.decode('utf-8').split('\n')[2:-3]
    ):
        subprocess.call(' '.join(
            psql
            + ['-v', 'output_path="\'' + output_path + ym + '/' + setid + '_' + '\'"']
            + ['-v', 'credentials="\'' + args.s3_credentials + '\'"']
            + ['-v', 'select_from_common_model_table="\'SELECT * FROM medicalclaims_common_model WHERE substring(date_service, 0, 8) =  \\\'' + ym + '\\\'\'"']
            + [db, '<', '../redshift_norm_common/unload_common_model.sql']
        ), shell=True)
