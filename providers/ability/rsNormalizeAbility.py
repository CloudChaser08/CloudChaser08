
#!/usr/bin/python
import subprocess
import argparse
import time
import sys
import os
sys.path.append(os.path.abspath("../redshift_norm_common/"))
import create_date_validation_table as date_validator

TODAY = time.strftime('%Y-%m-%d', time.localtime())

parser = argparse.ArgumentParser()
parser.add_argument('--header_path', type=str)
parser.add_argument('--serviceline_path', type=str)
parser.add_argument('--servicelineaffiliation_path', type=str)
parser.add_argument('--claimaffiliation_path', type=str)
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

# switch for loading the vwclaimaffiliation table
#
# note that the table must be loaded for this routine to run, but it
# is shared for the AP application up through 2017/02/15, so this
# switch is useful for loading the table only on the first run.
parser.add_argument('--load_claimaffiliation', default=False, action='store_true')

args = parser.parse_known_args()[0]

db = args.database if args.database else 'dev'

psql = ['psql', '-h', args.cluster_endpoint, '-p', '5439']
if args.rs_user:
    psql.append('-U')
    psql.append(args.rs_user)

# generate date validation table
date_validator.generate(args.s3_credentials)

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

if args.load_claimaffiliation:
    subprocess.call(' '.join(
        psql
        + ['-v', 'claimaffiliation_path="\'' + args.claimaffiliation_path + '\'"']
        + ['-v', 'credentials="\'' + args.s3_credentials + '\'"']
        + [db, '<', 'load_vwclaimaffiliation.sql']
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
        + ['-v', 'output_path="\'' + args.output_path + ym + '/' + args.setid + '\'"']
        + ['-v', 'credentials="\'' + args.s3_credentials + '\'"']
        + ['-v', 'select_from_common_model_table="\'SELECT * FROM medicalclaims_common_model WHERE substring(date_service, 0, 8) =  \\\'' + ym + '\\\'\'"']
        + [db, '<', '../redshift_norm_common/unload_common_model.sql']
    ), shell=True)
