#! /usr/bin/python
import subprocess
import argparse
import time
import sys
sys.path.append(os.path.abspath("../redshift_norm_common/"))
import create_date_validation_table as date_validator

TODAY = time.strftime('%Y-%m-%d', time.localtime())

parser = argparse.ArgumentParser()
parser.add_argument('--header_path', type=str)
parser.add_argument('--serviceline_path', type=str)
parser.add_argument('--servicelineaffiliation_path', type=str)
parser.add_argument('--diagnosis_path', type=str)
parser.add_argument('--procedure_path', type=str)
parser.add_argument('--billing_path', type=str)
parser.add_argument('--payer_path', type=str)
parser.add_argument('--matching_path', type=str)
parser.add_argument('--output_path', type=str)
parser.add_argument('--database', type=str, nargs='?')
parser.add_argument('--setid', type=str)
parser.add_argument('--cluster_endpoint', type=str)
parser.add_argument('--s3_credentials', type=str)
parser.add_argument('--rs_user', type=str, nargs='?')
parser.add_argument('--rs_password', type=str, nargs='?')
args = parser.parse_args()

db = args.database if args.database else 'dev'

psql = ['psql', '-h', args.cluster_endpoint, '-p', '5439']
if args.rs_user:
    psql.append('-U')
    psql.append(args.rs_user)

# generate date validation table
date_validator.generate(psql, db, args.s3_credentials)

# create table for medical claims common model
subprocess.call(' '.join(
    psql
    + ['-v', 'filename="\'' + args.setid + '\'"']
    + ['-v', 'today="\'' + TODAY + '\'"']
    + ['-v', 'feedname="\'15\'"']
    + ['-v', 'vendor="\'14\'"']
    + [db, '<', '../redshift_norm_common/medicalclaims_common_model.sql']
), shell=True)

# load data
subprocess.call(' '.join(
    psql
    + ['-v', 'header_path="\'' + args.header_path + '\'"']
    + ['-v', 'serviceline_path="\'' + args.serviceline_path + '\'"']
    + ['-v', 'servicelineaffiliation_path="\'' + args.servicelineaffiliation_path + '\'"']
    + ['-v', 'diagnosis_path="\'' + args.diagnosis_path + '\'"']
    + ['-v', 'procedure_path="\'' + args.procedure_path + '\'"']
    + ['-v', 'billing_path="\'' + args.billing_path + '\'"']
    + ['-v', 'payer_path="\'' + args.payer_path + '\'"']
    + ['-v', 'credentials="\'' + args.s3_credentials + '\'"']
    + [db, '<', 'load_transactions.sql']
), shell=True)
subprocess.call(' '.join(
    psql
    + ['-v', 'matching_path="\'' + args.matching_path + '\'"']
    + ['-v', 'credentials="\'' + args.s3_credentials + '\'"']
    + [db, '<', 'load_matching_payload.sql']
), shell=True)

# normalize
subprocess.call(' '.join(psql + [db, '<', 'normalize.sql']), shell=True)

# privacy filtering
subprocess.call(' '.join(psql + ['-v', 'table_name=medicalclaims_common_model'] +
                        ['-v', 'column_name=diagnosis_code'] +
                        ['-v', 'qual_column_name=diagnosis_code_qual'] +
                        [db, '<', '../redshift_norm_common/nullify_icd9_blacklist.sql']), shell=True)
subprocess.call(' '.join(psql + ['-v', 'table_name=medicalclaims_common_model'] +
                        ['-v', 'column_name=diagnosis_code'] +
                        ['-v', 'qual_column_name=diagnosis_code_qual'] +
                        [db, '<', '../redshift_norm_common/nullify_icd10_blacklist.sql']), shell=True)
subprocess.call(' '.join(psql + ['-v', 'table_name=medicalclaims_common_model'] +
                        ['-v', 'column_name=diagnosis_code'] +
                        ['-v', 'qual_column_name=diagnosis_code_qual'] +
                        [db, '<', '../redshift_norm_common/genericize_icd9.sql']), shell=True)
subprocess.call(' '.join(psql + ['-v', 'table_name=medicalclaims_common_model'] +
                        ['-v', 'column_name=diagnosis_code'] +
                        ['-v', 'qual_column_name=diagnosis_code_qual'] +
                        [db, '<', '../redshift_norm_common/genericize_icd10.sql']), shell=True)
subprocess.call(' '.join(psql + [db, '<', '../redshift_norm_common/scrub_place_of_service.sql']), shell=True)
subprocess.call(' '.join(psql + [db, '<', '../redshift_norm_common/scrub_discharge_status.sql']), shell=True)
subprocess.call(' '.join(psql + [db, '<', '../redshift_norm_common/nullify_drg_blacklist.sql']), shell=True)
subprocess.call(' '.join(psql + ['-v', 'table_name=medicalclaims_common_model'] +
                        ['-v', 'column_name=patient_age'] +
                        [db, '<', '../redshift_norm_common/cap_age.sql']), shell=True)

# unload to s3
subprocess.call(' '.join(
    psql
    + ['-v', 'output_path="\'' + args.output_path + '\'"']
    + ['-v', 'credentials="\'' + args.s3_credentials + '\'"']
    + ['-v', 'select_from_common_model_table="\'SELECT * FROM medicalclaims_common_model\'"']
    + [db, '<', '../redshift_norm_common/unload_common_model.sql']
), shell=True)
