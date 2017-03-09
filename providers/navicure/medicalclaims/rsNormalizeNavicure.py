#! /usr/bin/python
import subprocess
import re
import argparse
import time

TODAY = time.strftime('%Y-%m-%d', time.localtime())

parser = argparse.ArgumentParser()
parser.add_argument('--input_path', type=str)
parser.add_argument('--matching_path', type=str)
parser.add_argument('--output_path', type=str)
parser.add_argument('--database', type=str, nargs='?')
parser.add_argument('--setid', type=str)
parser.add_argument('--cluster_endpoint', type=str, nargs='?')
parser.add_argument('--s3_credentials', type=str)
parser.add_argument('--rs_user', type=str, nargs='?')
parser.add_argument('--rs_password', type=str, nargs='?')
parser.add_argument('--first_run', default=False, action='store_true')
args = parser.parse_args()

db = args.database if args.database else 'dev'

psql = ['psql', '-p', '5439']
if args.cluster_endpoint:
    psql.extend(['-h', args.cluster_endpoint])
if args.rs_user:
    psql.extend(['-U', args.rs_user, '-W'])

if args.first_run:
    subprocess.call(' '.join(psql + [db, '<', 'user_defined_functions.sql']), shell=True)
    subprocess.call(' '.join(psql + [db, '<', 'create_helper_tables.sql']), shell=True)
    subprocess.call(' '.join(psql + [db, '<', '../../redshift_norm_common/zip3_to_state.sql']), shell=True)

    # Create a table for valid dates and their correct format
    min_date = '2014-01-01'
    subprocess.call(' '.join(psql + [db, '<', '../../redshift_norm_common/prep_date_offset_table.sql']), shell=True)
    while True:
        res1 = subprocess.check_output(['psql', '-c', 'SELECT count(*) FROM tmp;']);
        res2 = subprocess.check_output(['psql', '-c', 'SELECT extract(\'days\' FROM (getdate() - \'' + min_date + '\'))::int;']);
        rows = int(res1.split("\n")[2].lstrip().rstrip())
        target = int(res2.split("\n")[2].lstrip().rstrip())
        if rows > target + 1:
            break
        else:
            subprocess.call(' '.join(psql + [db, '<', '../../redshift_norm_common/expand_date_offset_table.sql']), shell=True)
    subprocess.call(' '.join(psql + ['-v', 'min_valid_date="\'' + min_date + '\'"'] +
        [db, '<', '../../redshift_norm_common/create_date_formatting_table.sql']), shell=True)

subprocess.call(' '.join(psql + ['-v', 'filename="\'' + args.setid + '\'"'] + 
    ['-v', 'today="\'' + TODAY + '\'"'] +
    ['-v', 'feedname="\'navicure medical claims\'"'] +
    ['-v', 'vendor="\'navicure\'"'] +
    [db, '<', '../../redshift_norm_common/medicalclaims_common_model.sql']), shell=True)
subprocess.call(' '.join(psql + ['-v', 'input_path="\'' + args.input_path + '\'"'] +
    ['-v', 'credentials="\'' + args.s3_credentials + '\'"'] +
    [db, '<', 'load_transactions.sql']), shell=True)
subprocess.call(' '.join(psql + ['-v', 'matching_path="\'' + args.matching_path + '\'"'] +
    ['-v', 'credentials="\'' + args.s3_credentials + '\'"'] +
    [db, '<', 'load_matching_payload.sql']), shell=True)
subprocess.call(' '.join(psql + [db, '<', 'normalize_claims.sql']), shell=True)

# Privacy filtering
subprocess.call(' '.join(psql + ['-v', 'table_name=medicalclaims_common_model'] +
    ['-v', 'column_name=diagnosis_code'] +
    ['-v', 'qual_column_name=diagnosis_code_qual'] +
    ['-v', 'service_date_column_name=date_service'] +
    [db, '<', '../../redshift_norm_common/nullify_icd9_blacklist.sql']), shell=True)
subprocess.call(' '.join(psql + ['-v', 'table_name=medicalclaims_common_model'] +
    ['-v', 'column_name=diagnosis_code'] +
    ['-v', 'qual_column_name=diagnosis_code_qual'] +
    ['-v', 'service_date_column_name=date_service'] +
    [db, '<', '../../redshift_norm_common/nullify_icd10_blacklist.sql']), shell=True)
subprocess.call(' '.join(psql + ['-v', 'table_name=medicalclaims_common_model'] +
    ['-v', 'column_name=diagnosis_code'] +
    ['-v', 'qual_column_name=diagnosis_code_qual'] +
    ['-v', 'service_date_column_name=date_service'] +
    [db, '<', '../../redshift_norm_common/genericize_icd9.sql']), shell=True)
subprocess.call(' '.join(psql + ['-v', 'table_name=medicalclaims_common_model'] +
    ['-v', 'column_name=diagnosis_code'] +
    ['-v', 'qual_column_name=diagnosis_code_qual'] +
    ['-v', 'service_date_column_name=date_service'] +
    [db, '<', '../../redshift_norm_common/genericize_icd10.sql']), shell=True)
subprocess.call(' '.join(psql + [db, '<', '../../redshift_norm_common/scrub_place_of_service.sql']), shell=True)
subprocess.call(' '.join(psql + [db, '<', '../../redshift_norm_common/scrub_discharge_status.sql']), shell=True)
subprocess.call(' '.join(psql + [db, '<', '../../redshift_norm_common/nullify_drg_blacklist.sql']), shell=True)
subprocess.call(' '.join(psql + ['-v', 'table_name=medicalclaims_common_model'] +
    ['-v', 'column_name=patient_age'] +
    [db, '<', '../../redshift_norm_common/cap_age.sql']), shell=True)
    

subprocess.call(' '.join(psql + ['-v', 'output_path="\'' + args.output_path + '\'"'] +
    ['-v', 'credentials="\'' + args.s3_credentials + '\'"'] +
    ['-v', 'select_from_common_model_table="\'SELECT * FROM medicalclaims_common_model\'"'] +
    [db, '<', '../../redshift_norm_common/unload_common_model.sql']), shell=True)
