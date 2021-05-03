#! /usr/bin/python
import subprocess
import re
import argparse
import time

TODAY = time.strftime('%Y-%m-%d', time.localtime())

parser = argparse.ArgumentParser()
parser.add_argument('--date', type=str)
parser.add_argument('--setid', type=str)
parser.add_argument('--s3_credentials', type=str)
parser.add_argument('--iam_role', type=str)
parser.add_argument('--first_run', default=False, action='store_true')
parser.add_argument('--debug', default=False, action='store_true')
args = parser.parse_known_args()[0]

input_path = 's3://salusv/incoming/medicalclaims/navicure/{}/'.format(args.date.replace('-', '/'))
matching_path = 's3://salusv/matching/payload/medicalclaims/navicure/{}/'.format(args.date.replace('-', '/'))
output_path = 's3://salusv/warehouse/text/medicalclaims/navicure/{}/'.format(args.date.replace('-', '/'))

def run_psql_script(script, variables=[]):
    args = ['psql', '-f', script]
    for var in variables:
        args.append('-v')
        if (len(var) == 3 and var[2] == False):
       	    args.append("{}={}".format(var[0], var[1]))
        else:
            args.append("{}='{}'".format(var[0], var[1]))
    subprocess.check_call(args)

def run_psql_query(query, return_output=False):
    args = ['psql', '-c', query]
    if return_output:
        return subprocess.check_output(args)
    subprocess.check_call(args)

psql_scripts = []
psql_variables = []
def enqueue_psql_script(script, variables=[]):
    global psql_scripts, psql_variables
    psql_scripts.append(script)
    psql_variables.append(variables)

def execute_queue(debug=False):
    global psql_scripts, psql_variables
    for i in xrange(len(psql_scripts)):
        run_psql_script(psql_scripts[i], psql_variables[i])

if args.first_run:
    enqueue_psql_script('user_defined_functions.sql')
    enqueue_psql_script('create_helper_tables.sql')
    enqueue_psql_script('../../redshift_norm_common/zip3_to_state.sql')

    # Create a table for valid dates and their correct format
    min_date = '2014-01-01'
    run_psql_script('../../redshift_norm_common/prep_date_offset_table.sql')
    while True:
        res1 = run_psql_query('SELECT count(*) FROM tmp;', True)
        res2 = run_psql_query('SELECT extract(\'days\' FROM (getdate() - \'' + min_date + '\'))::int;', True)
        rows = int(res1.split("\n")[2].lstrip().rstrip())
        target = int(res2.split("\n")[2].lstrip().rstrip())
        if rows > target + 1:
            break
        else:
            run_psql_script('../../redshift_norm_common/expand_date_offset_table.sql')
    enqueue_psql_script('../../redshift_norm_common/create_date_formatting_table.sql', [
        ['min_valid_date', min_date]
    ])

credentials = args.iam_role
if args.s3_credentials:
    credentials = args.s3_credentials

enqueue_psql_script('../../redshift_norm_common/medicalclaims_common_model.sql', [
    ['filename', args.setid],
    ['today', TODAY],
    ['feedname', '24'],
    ['vendor', '34']
])
enqueue_psql_script('load_transactions.sql', [
    ['input_path', input_path],
    ['credentials', credentials]
])
enqueue_psql_script('load_matching_payload.sql', [
    ['matching_path', matching_path],
    ['credentials', credentials]
])
enqueue_psql_script('normalize_claims.sql')

# Privacy filtering
enqueue_psql_script('../../redshift_norm_common/nullify_icd9_blacklist.sql', [
    ['table_name', 'medicalclaims_common_model', False],
    ['column_name', 'diagnosis_code', False],
    ['qual_column_name', 'diagnosis_code_qual', False],
    ['service_date_column_name', 'date_service', False]
])
enqueue_psql_script('../../redshift_norm_common/nullify_icd10_blacklist.sql', [
    ['table_name', 'medicalclaims_common_model', False],
    ['column_name', 'diagnosis_code', False],
    ['qual_column_name', 'diagnosis_code_qual', False],
    ['service_date_column_name', 'date_service', False]
])
enqueue_psql_script('../../redshift_norm_common/genericize_icd9.sql', [
    ['table_name', 'medicalclaims_common_model', False],
    ['column_name', 'diagnosis_code', False],
    ['qual_column_name', 'diagnosis_code_qual', False],
    ['service_date_column_name', 'date_service', False]
])
enqueue_psql_script('../../redshift_norm_common/genericize_icd10.sql', [
    ['table_name', 'medicalclaims_common_model', False],
    ['column_name', 'diagnosis_code', False],
    ['qual_column_name', 'diagnosis_code_qual', False],
    ['service_date_column_name', 'date_service', False]
])
enqueue_psql_script('../../redshift_norm_common/scrub_place_of_service.sql')
enqueue_psql_script('../../redshift_norm_common/scrub_discharge_status.sql')
enqueue_psql_script('../../redshift_norm_common/nullify_drg_blacklist.sql')
enqueue_psql_script('../../redshift_norm_common/cap_age.sql', [
    ['table_name', 'medicalclaims_common_model', False],
    ['column_name', 'patient_age', False],
])
    
enqueue_psql_script('../../redshift_norm_common/unload_common_model.sql', [
    ['output_path', output_path],
    ['credentials', credentials],
    ['select_from_common_model_table', 'SELECT * FROM medicalclaims_common_model']
])

execute_queue(args.debug)
