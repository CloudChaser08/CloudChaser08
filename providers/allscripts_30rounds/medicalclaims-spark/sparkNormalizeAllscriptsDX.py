#! /usr/bin/python
import subprocess
import re
import argparse
import time
import logging
from pyspark.context import SparkContext
from pyspark.sql import HiveContext, SparkSession
spark = SparkSession.builder.master("yarn-client").appName("Allscripts Normalization").config('spark.sql.catalogImplementation', 'hive').getOrCreate()
spark.sparkContext.addFile('../../spark_norm_common/user_defined_functions.py')
spark.sparkContext.addFile('../../spark_norm_common/post_normalization_cleanup.py')
sqlContext = HiveContext(spark.sparkContext)

from user_defined_functions import get_diagnosis_with_priority, string_set_diff, uniquify
sqlContext.registerFunction('get_diagnosis_with_priority', get_diagnosis_with_priority)
sqlContext.registerFunction('string_set_diff', string_set_diff)
sqlContext.registerFunction('uniquify', uniquify)

from post_normalization_cleanup import clean_up_diagnosis_code, obscure_place_of_service, filter_due_to_place_of_service
sqlContext.registerFunction('filter_due_to_place_of_service', filter_due_to_place_of_service)
sqlContext.registerFunction('obscure_place_of_service', obscure_place_of_service)
sqlContext.registerFunction('clean_up_diagnosis_code', clean_up_diagnosis_code)

TODAY = time.strftime('%Y-%m-%d', time.localtime())
S3_ALLSCRIPTS_IN = 's3a://salusv/incoming/medicalclaims/allscripts_30rounds/'
S3_ALLSCRIPTS_OUT = 's3a://salusv/warehouse/text/medicalclaims/allscripts_30rounds/'
S3_ALLSCRIPTS_MATCHING = 's3a://salusv/matching/payload/medicalclaims/allscripts_30rounds/'

parser = argparse.ArgumentParser()
parser.add_argument('--date', type=str)
parser.add_argument('--setid', type=str)
parser.add_argument('--first_run', default=False, action='store_true')
parser.add_argument('--debug', default=False, action='store_true')
args = parser.parse_known_args()

def run_spark_script(script, variables=[]):
    content = ''
    with open(script) as inf:
        content = inf.read()
    for i in xrange(len(variables)):
        if len(variables[i]) != 3 or variables[i][2]:
            variables[i][1] = "'" + variables[i][1] + "'"
    k_vars = {variables[i][0] : variables[i][1] for i in xrange(len(variables))}
    content = content.format(**k_vars)
    for statement in content.split(';'):
        if len(statement.strip()) == 0:
            continue
        print "STATEMENT: " + statement
        sqlContext.sql(statement)

def run_spark_query(query, return_output=False):
    if return_output:
        return sqlContext.sql(query)
    sqlContext.sql(query)

psql_scripts = []
psql_variables = []
def enqueue_psql_script(script, variables=[]):
    global psql_scripts, psql_variables
    psql_scripts.append(script)
    psql_variables.append(variables)

def execute_queue(debug=False):
    global psql_scripts, psql_variables
    for i in xrange(len(psql_scripts)):
        run_spark_script(psql_scripts[i], psql_variables[i])

# DISABLED for now. Must resolve psql variable collision first
#    else:
#        v = reduce(lambda x,y: x + y, psql_variables, [])
#        with open('full_normalization_routine.sql', 'w') as fout:
#            subprocess.check_call(['cat'] + psql_scripts, stdout=fout)
#        run_spark_script('full_normalization_routine.sql', v)

if args.first_run:
#    enqueue_psql_script('user_defined_functions.sql')
    enqueue_psql_script('create_helper_tables.sql')
    enqueue_psql_script('../../spark_norm_common/zip3_to_state.sql')

date_path = args.date.replace('-', '/')

enqueue_psql_script('../../spark_norm_common/medicalclaims_common_model.sql')
enqueue_psql_script('load_transactions.sql', [
    ['input_path', S3_ALLSCRIPTS_IN + date_path + '/transactions/']
])
enqueue_psql_script('load_linking.sql', [
    ['input_path', S3_ALLSCRIPTS_IN + date_path + '/linking/']
])
enqueue_psql_script('load_matching_payload.sql', [
    ['matching_path', S3_ALLSCRIPTS_MATCHING + date_path + '/']
])
enqueue_psql_script('split_raw_transactions.sql', [
    ['max_allowed_date', args.date.replace('-', '')]
])
enqueue_psql_script('normalize_claims.sql', [
    ['filename', args.setid],
    ['today', TODAY],
    ['feedname', '26'],
    ['vendor', '35']
])

# Privacy filtering
enqueue_psql_script('../../spark_norm_common/medicalclaims_post_normalization_cleanup.sql')

execute_queue(args.debug)

res1 = run_spark_query("SELECT DISTINCT regexp_replace(date_service, '-..$', '') FROM medicalclaims_common_model", True)

months = map(lambda x: x.asDict().values()[0], res1.collect())
run_spark_script('../../spark_norm_common/unload_table.sql')
run_spark_script('../../spark_norm_common/unload_common_model.sql', [
    ['select_statement', "SELECT *, 'NULL' as magic_date FROM medicalclaims_common_model WHERE date_service is NULL", False]
])
run_spark_script('../../spark_norm_common/unload_common_model.sql', [
    ['select_statement', "SELECT *, regexp_replace(date_service, '-..$', '') as magic_date FROM medicalclaims_common_model WHERE date_service IS NOT NULL", False]
])

spark.sparkContext.stop()
