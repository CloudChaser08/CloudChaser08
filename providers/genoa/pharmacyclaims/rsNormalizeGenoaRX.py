#! /usr/bin/python
import subprocess
import re
import argparse
import time

TODAY = time.strftime('%Y-%m-%d', time.localtime())

parser = argparse.ArgumentParser()
parser.add_argument('--date', type=str)
parser.add_argument('--s3_credentials', type=str)
parser.add_argument('--create_reversal_table', default=False, action='store_true')
parser.add_argument('--first_run', default=False, action='store_true')
args = parser.parse_known_args()

psql = ['psql']

def run_psql_script(script, variables=[]):
    args = psql + ['-f', script]
    
    for var in variables:
        args.append('-v')
        args.append("{}={}".format(var[0], var[1]))

    return subprocess.check_output(args)

def run_psql_command(command):
    args = psql + ['-c', command]
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

run_psql_script('create_merged_transaction_table.sql')
run_psql_script('create_merged_payload_table.sql')

# Genoa sends us current month + a 90 day look back
# Deduplication logic for removing duplicates from just the current batch
# would be complicated. Renormalize all of their data instead. It's small
batches = subprocess.check_output(' '.join(['aws', 's3', 'ls', 's3://salusv/incoming/pharmacyclaims/genoa/', '--recursive', '|', 'grep', '-o', 'incoming.*', '|', 'sed', "'s/\...\.bz2//'", '|', 'uniq']), shell=True).split("\n")
run_psql_script('../../redshift_norm_common/pharmacyclaims_common_model.sql', [
    ['filename', "'UNKNOWN'".format()], ['today', "'{}'".format(TODAY)],
    ['feedname', "'21'"], ['vendor', "'20'"] 
])
for batch in batches:
    if batch.strip() == '':
        continue
    input_path    = 's3://salusv/' + batch.rsplit('/', 1)[0]
    print input_path
    setid         = batch.rsplit('/', 1)[1].replace('Genoa_', '')
    matching_path = input_path.replace('incoming', 'matching/payload')
    output_path   = input_path.replace('incoming', 'warehouse/text')
    run_psql_script('load_transactions.sql', [
        ['input_path', "'{}'".format(input_path)],
        ['credentials', "'{}'".format(args.s3_credentials)],
        ['setid', "'{}'".format(setid)]
    ])
    run_psql_script('load_matching_payload.sql', [
        ['matching_path', "'{}'".format(matching_path)],
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

run_psql_script('clean_out_reversals.sql')

if args.unload_setid:
    run_psql_script('../../redshift_norm_common/unload_common_model.sql', [
        ['output_path', "'{}'".format(args.output_path)],
        ['credentials', "'{}'".format(args.s3_credentials)],
        ['select_from_common_model_table', "'SELECT * FROM pharmacyclaims_common_model WHERE data_set=\\\'{}\\\' '".format(args.unload_setid)]
    ])
        
    run_psql_script('delete_unloaded_claims.sql', [
        ['setid', "'{}'".format(args.unload_setid)]
    ])
