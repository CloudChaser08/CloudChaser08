#! /usr/bin/python
import subprocess
import re
import argparse
import time

TODAY = time.strftime('%Y-%m-%d', time.localtime())
S3_ALLSCRIPTS_IN = 's3://salusv/incoming/medicalclaims/allscripts/'
S3_ALLSCRIPTS_OUT = 's3://salusv/warehouse/text/medicalclaims/allscripts/'
S3_ALLSCRIPTS_MATCHING = 's3://salusv/matching/payload/medicalclaims/allscripts/'

parser = argparse.ArgumentParser()
parser.add_argument('--date', type=str)
parser.add_argument('--setid', type=str)
parser.add_argument('--s3_credentials', type=str)
parser.add_argument('--first_run', default=False, action='store_true')
parser.add_argument('--debug', default=False, action='store_true')
args = parser.parse_known_args()

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

# DISABLED for now. Must resolve psql variable collision first
#    else:
#        v = reduce(lambda x,y: x + y, psql_variables, [])
#        with open('full_normalization_routine.sql', 'w') as fout:
#            subprocess.check_call(['cat'] + psql_scripts, stdout=fout)
#        run_psql_script('full_normalization_routine.sql', v)

if args.first_run:
    enqueue_psql_script('user_defined_functions.sql')
    enqueue_psql_script('create_helper_tables.sql')
    enqueue_psql_script('../../redshift_norm_common/zip3_to_state.sql')

    # Create a table for valid dates and their correct format
    min_date = '2012-01-01'
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
    run_psql_script('../../redshift_norm_common/create_date_formatting_table.sql', [
        ['min_valid_date', min_date]
    ])

date_path = args.date.replace('-', '/')

enqueue_psql_script('../../redshift_norm_common/medicalclaims_common_model.sql', [
    ['filename', args.setid],
    ['today', TODAY],
    ['feedname', '26'],
    ['vendor', '35']
])
enqueue_psql_script('load_transactions.sql', [
    ['header_path', S3_ALLSCRIPTS_IN + date_path + '/header/'],
    ['serviceline_path', S3_ALLSCRIPTS_IN + date_path + '/serviceline/'],
    ['credentials', args.s3_credentials]
])
enqueue_psql_script('load_matching_payload.sql', [
    ['matching_path', S3_ALLSCRIPTS_MATCHING + date_path + '/'],
    ['credentials', args.s3_credentials]
])
enqueue_psql_script('split_raw_transactions.sql')
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
    ['column_name', 'patient_age', False]
])

execute_queue(args.debug)

months = run_psql_query('SELECT DISTINCT regexp_replace(date_service, \'-..$\', \'\') FROM medicalclaims_common_model;', True)
for m in months.split("\n")[2:-3]:
    m = m.strip()
    if m == '':
        m = "NULL"
    print m
    if m != 'NULL':
        run_psql_script('../../redshift_norm_common/unload_common_model.sql', [
            ['output_path', S3_ALLSCRIPTS_OUT + m + '/' + args.date + '_'],
            ['credentials', args.s3_credentials],
            ['select_from_common_model_table', 'SELECT * FROM medicalclaims_common_model WHERE date_service LIKE \\\'{}%\\\''.format(m)]
        ])
    else:
        run_psql_script('../../redshift_norm_common/unload_common_model.sql', [
            ['output_path', S3_ALLSCRIPTS_OUT + m + '/' + args.date + '_'],
            ['credentials', args.s3_credentials],
            ['select_from_common_model_table', 'SELECT * FROM medicalclaims_common_model WHERE date_service IS NULL'.format(m)]
        ])
