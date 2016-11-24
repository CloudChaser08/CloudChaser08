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
parser.add_argument('--setid', type=str) # set id of the data we are normalizing
parser.add_argument('--unload_setid', type=str, nargs='?') # set id of the data we are unloading to s3
parser.add_argument('--cluster_endpoint', type=str, nargs='?')
parser.add_argument('--s3_credentials', type=str)
parser.add_argument('--rs_user', type=str, nargs='?')
parser.add_argument('--rs_password', type=str, nargs='?')
parser.add_argument('--create_reversal_table', default=False, action='store_true')
parser.add_argument('--first_run', default=False, action='store_true')
args = parser.parse_args()

db = args.database if args.database else 'dev'

psql = ['psql', '-p', '5439']
if args.cluster_endpoint:
    psql.extend(['-h', args.cluster_endpoint])
if args.rs_user:
    psql.extend(['-U', args.rs_user, '-W'])

def run_psql_script(script, variables=[]):
    args = psql + ['-f', script]
    
    for var in variables:
        args.append('-v')
        args.append("{}={}".format(var[0], var[1]))

    args.append(db)
    return subprocess.check_output(args)

def run_psql_command(command):
    args = psql + ['-c', command, db]
    return subprocess.check_output(args)

if args.first_run:
    run_psql_script('../../redshift_norm_common/zip3_to_state.sql')

    # Create a table for valid dates and their correct format
    min_date = '2009-01-01'
    run_psql_script('../../redshift_norm_common/prep_date_offset_table.sql')
    while True:
        res1 = run_psql_command('SELECT count(*) FROM tmp;')
        res2 = run_psql_command('SELECT extract(\'days\' FROM (getdate() - \'' + min_date + '\'))::int;')
        rows = int(res1.split("\n")[2].lstrip().rstrip())
        target = int(res2.split("\n")[2].lstrip().rstrip())
        if rows > target + 1:
            break
        else:
            run_psql_script('../../redshift_norm_common/expand_date_offset_table.sql')
    run_psql_script('../../redshift_norm_common/create_date_formatting_table.sql', [
        ['min_valid_date', "'{}'".format(min_date)]
    ])

run_psql_script('../../redshift_norm_common/pharmacyclaims_common_model.sql', [
    ['filename', "'{}'".format(args.setid)], ['today', "'{}'".format(TODAY)],
    ['feedname', "'genoa pharmacy claims'"], ['vendor', "'genoa'"] 
])
run_psql_script('load_transactions.sql', [
    ['input_path', "'{}'".format(args.input_path)],
    ['credentials', "'{}'".format(args.s3_credentials)]
])
run_psql_script('load_matching_payload.sql', [
    ['matching_path', "'{}'".format(args.matching_path)],
    ['credentials', "'{}'".format(args.s3_credentials)]
])
run_psql_script('normalize_pharmacy_claims.sql')

# Privacy filtering
run_psql_script('../../redshift_norm_common/hash_rx_number.sql')
run_psql_script('../../redshift_norm_common/nullify_sales_tax.sql')
run_psql_script('../../redshift_norm_common/cap_age.sql', [
    ['table_name', 'pharmacyclaims_common_model'], ['column_name', 'patient_age']
])

if args.create_reversal_table:
    run_psql_script('data_to_reverse_table.sql')

if args.unload_setid:
    run_psql_script('../../redshift_norm_common/unload_common_model.sql', [
        ['output_path', "'{}'".format(args.output_path)],
        ['credentials', "'{}'".format(args.s3_credentials)],
        ['select_from_common_model_table', "'SELECT * FROM pharmacyclaims_common_model WHERE data_set=\\\'{}\\\' '".format(args.unload_setid)]
    ])
        
    run_psql_script('delete_unloaded_claims.sql', [
        ['setid', "'{}'".format(args.unload_setid)]
    ])
