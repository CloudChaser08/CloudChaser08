#! /usr/bin/python
import subprocess
import re
import argparse
import time
from datetime import datetime, timedelta

TODAY = time.strftime('%Y-%m-%d', time.localtime())
S3_EMDEON_IN = 's3://salusv/incoming/pharmacyclaims/emdeon/'
S3_EMDEON_WAREHOUSE = 's3://salusv/warehouse/text/pharmacyclaims/emdeon/'
S3_EMDEON_MATCHING = 's3://salusv/matching/payload/pharmacyclaims/emdeon/'

parser = argparse.ArgumentParser()
parser.add_argument('--date', type=str)
parser.add_argument('--setid', type=str)
parser.add_argument('--s3_credentials', type=str)
parser.add_argument('--first_run', default=False, action='store_true')
parser.add_argument('--debug', default=False, action='store_true')
args = parser.parse_known_args()[0]

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
    enqueue_psql_script('../../redshift_norm_common/zip3_to_state.sql')
    enqueue_psql_script('load_payer_mapping.sql', [
        ['credentials', args.s3_credentials]
    ])

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

file_date = datetime.strptime(args.date, '%Y-%m-%d')
run_psql_script('create_normalized_data_table.sql', [
    ['table', 'normalized_claims', False]
])
setid_path_to_unload = {}
for i in xrange(1, 15):
    d_path = (file_date - timedelta(days=i)).strftime('%Y/%m/%d')
    run_psql_script('load_normalized_data.sql', [
        ['table', 'normalized_claims', False],
        ['input_path', S3_EMDEON_WAREHOUSE + d_path + '/'],
        ['credentials', args.s3_credentials]
    ])
    res = run_psql_query('SELECT DISTINCT data_set FROM normalized_claims', True)
    setids = map(lambda x: x.replace(' ',''), res.split("\n")[2:-3])
    for setid in setids:
        if setid not in setid_path_to_unload:
            setid_path_to_unload[setid] = S3_EMDEON_WAREHOUSE + d_path + '/'

date_path = args.date.replace('-', '/')
setid_path_to_unload[args.setid] = S3_EMDEON_WAREHOUSE + date_path + '/'

enqueue_psql_script('../../redshift_norm_common/pharmacyclaims_common_model.sql', [
    ['filename', args.setid],
    ['today', TODAY],
    ['feedname', 'webmd pharmacy claims'],
    ['vendor', 'webmd']
])
enqueue_psql_script('load_transactions.sql', [
    ['input_path', S3_EMDEON_IN + date_path + '/'],
    ['credentials', args.s3_credentials]
])
enqueue_psql_script('load_matching_payload.sql', [
    ['matching_path', S3_EMDEON_MATCHING + date_path + '/'],
    ['credentials', args.s3_credentials]
])
enqueue_psql_script('normalize_pharmacy_claims.sql')

# Privacy filtering
enqueue_psql_script('../../redshift_norm_common/nullify_icd9_blacklist.sql', [
    ['table_name', 'pharmacyclaims_common_model', False],
    ['column_name', 'diagnosis_code', False],
    ['qual_column_name', 'diagnosis_code_qual', False],
    ['service_date_column_name', 'date_service', False]
])
enqueue_psql_script('../../redshift_norm_common/nullify_icd10_blacklist.sql', [
    ['table_name', 'pharmacyclaims_common_model', False],
    ['column_name', 'diagnosis_code', False],
    ['qual_column_name', 'diagnosis_code_qual', False],
    ['service_date_column_name', 'date_service', False]
])
enqueue_psql_script('../../redshift_norm_common/genericize_icd9.sql', [
    ['table_name', 'pharmacyclaims_common_model', False],
    ['column_name', 'diagnosis_code', False],
    ['qual_column_name', 'diagnosis_code_qual', False],
    ['service_date_column_name', 'date_service', False]
])
enqueue_psql_script('../../redshift_norm_common/genericize_icd10.sql', [
    ['table_name', 'pharmacyclaims_common_model', False],
    ['column_name', 'diagnosis_code', False],
    ['qual_column_name', 'diagnosis_code_qual', False],
    ['service_date_column_name', 'date_service', False]
])
enqueue_psql_script('../../redshift_norm_common/hash_rx_number.sql')
enqueue_psql_script('../../redshift_norm_common/nullify_sales_tax.sql')
enqueue_psql_script('../../redshift_norm_common/cap_age.sql', [
    ['table_name', 'pharmacyclaims_common_model', False],
    ['column_name', 'patient_age', False]
])

# If this script is being run to backfill missing data, we have to apply
# reversals from claims we received in batches after the current one
enqueue_psql_script('create_normalized_data_table.sql', [
    ['table', 'additional_claims', False]
])
for i in xrange(1, 15):
    d_path = (file_date + timedelta(days=i)).strftime('%Y/%m/%d')
    enqueue_psql_script('load_normalized_data.sql', [
        ['table', 'additional_claims', False],
        ['input_path', S3_EMDEON_WAREHOUSE + d_path + '/'],
        ['credentials', args.s3_credentials]
    ])

enqueue_psql_script('clean_out_reversed_claims.sql')

for setid, s3_path in setid_path_to_unload.iteritems():
    enqueue_psql_script('../../redshift_norm_common/unload_common_model.sql', [
        ['output_path', s3_path],
        ['credentials', args.s3_credentials],
        ['select_from_common_model_table', "SELECT * FROM normalized_claims WHERE data_set=\\\'" + setid + "\\\'"]
    ])

execute_queue(args.debug)
