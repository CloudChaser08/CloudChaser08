#ye The DAG object; we'll need this to instantiate a DAG
from airflow import DAG

# Operators; we need this to operate!
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator

from airflow.hooks.hive_hooks import HiveServer2Hook

from airflow.models import Variable

import re
import os
import sys
import csv
import struct
from datetime import timedelta, datetime

SRC_PATH='http://www.accessdata.fda.gov/cder/'
SRC_FILE='ndctext.zip'
TMP_PATH='/tmp/ndc_'

if Variable.get('AIRFLOW_ENV', default_var='').find('prod') != -1:
    SCHEMA='default'
    S3_TEXT='s3://salusv/reference/ndc/'
    S3_PARQUET='s3a://salusv/reference/parquet/ndc/'
else:
    SCHEMA='dev'
    S3_TEXT='s3://healthveritydev/jcap/ndc/'
    S3_PARQUET='s3a://healthveritydev/jcap/parquet/ndc/'

hive = HiveServer2Hook(hiveserver2_conn_id='hive_analytics')

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

dag = DAG(
    'update_ndc', 
    default_args=default_args, 
    start_date=datetime(2017, 1, 20),
    schedule_interval='@daily' if Variable.get('AIRFLOW_ENV', default_var='').find('prod') != -1 else None,
)




def format_and_save(lines,file_name):
    f = open(file_name,'w')
    for code in lines:
        if code == "NDCPACKAGECODE":
            continue
        # unpack what we know is an adequate length string
        f.write(lines[code])

    f.close()


def get_lines(file_type, file_name):
    if file_type == 'product':
        field_names = ['PRODUCTID','PRODUCTNDC','PRODUCTTYPENAME','PROPRIETARYNAME',
                'PROPRIETARYNAMESUFFIX','NONPROPRIETARYNAME','DOSAGEFORMNAME','ROUTENAME',
                'STARTMARKETINGDATE','ENDMARKETINGDATE','MARKETINGCATEGORYNAME',
                'APPLICATIONNUMBER','LABELERNAME','SUBSTANCENAME','ACTIVE_NUMERATOR_STRENGTH',
                'ACTIVE_INGRED_UNIT','PHARM_CLASSES','DEASCHEDULE']
        code_column = 'PRODUCTNDC'
    elif file_type == 'package':
        field_names = ['PRODUCTID','PRODUCTNDC','NDCPACKAGECODE','PACKAGEDESCRIPTION']
        code_column = 'NDCPACKAGECODE'

    # load files into their dicts, key on code
    lines = {}

    fd = open(file_name,'r')
    reader = csv.DictReader(fd, delimiter="\t")
    for line in fd:
        reader = csv.DictReader([line],fieldnames=field_names,delimiter="\t")
        for row in reader:
            code = row[code_column]
            lines[code] = line

    fd.close
    return lines


def prepare_ndcfile(ds, file_type, yesterday, today, updated, **kwargs):
    codes_added    = 0
    codes_modified = 0
    codes_removed  = []

    old_file_name     = '/tmp/ndc_' + ds + '/' + yesterday
    new_file_name     = '/tmp/ndc_' + ds + '/' + today
    updated_file_name = '/tmp/ndc_' + ds + '/' + updated

    # read the files in
    old_lines = get_lines(file_type, old_file_name)
    new_lines = get_lines(file_type, new_file_name)

    # first, find diffs, update to new one if found
    for code in old_lines:
        if code not in new_lines:
            codes_removed.append(code)
            continue
        # this is going from char 47 forward, because there's a random hash that changes
        if(old_lines[code][47:] != new_lines[code][47:]):
            print("Line with code "+code+" differs, updating")
            print("Old line is "+old_lines[code][47:])
            print("New line is "+new_lines[code][47:])
            old_lines[code] = new_lines[code]
            codes_modified = codes_modified+1

    # next, find new codes that do not exist in old codes, add to old codes
    for code in new_lines:
        if code not in old_lines:
            #print("Code "+code+" does not exist, adding")
            old_lines[code] = new_lines[code]
            codes_added = codes_added+1

    format_and_save(old_lines,updated_file_name)
    print("Complete, added "+str(codes_added)+", updated "+str(codes_modified)+" removed "+str(len(codes_removed)))
    print("Codes removed (but we didn't actually remove them):")
    print(codes_removed)


def create_temp_tables(ds, schema, s3, **kwars):
    sqls = [
        """DROP TABLE IF EXISTS {}.temp_ref_ndc_product""".format(schema),
        """DROP TABLE IF EXISTS {}.temp_ref_ndc_package""".format(schema),

        """CREATE EXTERNAL TABLE {}.temp_ref_ndc_package (
            product_id          string,
            product_ndc         string,
            ndc_package_code    string,
            package_description string
        )
        ROW FORMAT DELIMITED
        FIELDS TERMINATED BY '\t'
        LINES  TERMINATED BY '\n'
        LOCATION '{}/{}/package/'""".format(schema, s3, ds),

        """CREATE EXTERNAL TABLE {}.temp_ref_ndc_product (
            product_id                string,
            product_ndc               string,
            product_type              string,
            proprietary_name          string,
            proprietary_name_suffix   string,
            nonproprietary_name       string,
            dosage_form_name          string,
            route_name                string,
            start_marketing_date      string,
            end_marketing_date        string,
            marketing_category_name   string,
            application_number        string,
            labeler_name              string,
            substance_name            string,
            active_numerator_strength string,
            active_ingred_unit        string,
            pharm_classes             string,
            dea_schedule              string
        )
        ROW FORMAT DELIMITED
        FIELDS TERMINATED BY '\t'
        LINES TERMINATED BY '\n'
        LOCATION '{}/{}/product/'""".format(schema, s3, ds)
    ]
    
    hive.get_records(sqls)


def create_new_ndc_table(ds, schema, s3, **kwargs):
    sqls = [
        """DROP TABLE IF EXISTS {}.ref_ndc_code_new""".format(schema),

        """CREATE EXTERNAL TABLE {}.ref_ndc_code_new(
            ndc_code                    string,
            package_description         string,
            product_type                string,
            proprietary_name            string,
            proprietary_name_suffix     string,
            nonproprietary_name         string,
            dosage_form_name            string,
            route_name                  string,
            start_marketing_date        string,
            end_marketing_date          string,
            marketing_category_name     string,
            labeler_name                string,
            substance_name              string,
            active_numerator_strength   string,
            active_ingred_unit          string,
            pharm_classes               string
        )
        STORED AS PARQUET
        LOCATION '{}/{}/'""".format(schema, s3, ds),

        """ INSERT INTO {}.ref_ndc_code_new
        SELECT a.ndc_code, a.package_description, b.product_type, b.proprietary_name, b.proprietary_name_suffix,
               b.nonproprietary_name, b.dosage_form_name, b.route_name, b.start_marketing_date, b.end_marketing_date,
               b.marketing_category_name, b.labeler_name, b.substance_name, b.active_numerator_strength, b.active_ingred_unit,
               b.pharm_classes
        FROM
          (SELECT n.*,
                CASE WHEN substr(ndc_package_code, 5, 1)='-' and substr(ndc_package_code, 10, 1)='-' then
                      CONCAT('0',replace(ndc_package_code,'-',''))
                   WHEN substr(ndc_package_code, 6, 1)='-' and substr(ndc_package_code, 10, 1)='-' then
                      CONCAT(substr(ndc_package_code, 1, 5),'0',replace(substr(ndc_package_code, 7, 6),'-',''))
                   WHEN substr(ndc_package_code, 6, 1)='-' and substr(ndc_package_code, 11, 1)='-' then
                      CONCAT(replace(substr(ndc_package_code, 1, 10),'-',''),'0',substr(ndc_package_code, 12, 1))
                   ELSE 'ERROR' end as ndc_code
            from ndc_package n
           ) a
           LEFT JOIN ndc_product b on a.product_ndc=b.product_ndc""".format(schema)
    ]

    hive.get_records(sqls)

def replace_old_table(ds, schema, s3, **kwargs):
    sqls = [
        """CREATE EXTERNAL TABLE IF NOT EXISTS {}.ref_ndc_code LIKE {}.ref_ndc_code_new""".format(schema, schema),
        """DROP TABLE {}.ref_ndc_code_new""".format(schema),
        """ALTER TABLE {}.ref_ndc_code SET LOCATION '{}/{}/'""".format(schema, s3, ds),

        """DROP TABLE IF EXISTS {}.temp_ref_ndc_product""".format(schema),
        """DROP TABLE IF EXISTS {}.temp_ref_ndc_package""".format(schema)
    ]

    hive.get_records(sqls)



fetch_yesterday = BashOperator(
    task_id='fetch_yesterday',
    params={ "TMP_PATH": TMP_PATH, "S3_TEXT": S3_TEXT},
    bash_command="""
        mkdir {{ params.TMP_PATH}}{{ ds }}
        /usr/local/bin/aws s3 cp --sse AES256 {{ params.S3_TEXT }}{{ yesterday_ds }}/product/product.txt {{ params.TMP_PATH }}{{ ds }}/yesterday_product.txt
        /usr/local/bin/aws s3 cp --sse AES256 {{ params.S3_TEXT }}{{ yesterday_ds }}/package/package.txt {{ params.TMP_PATH }}{{ ds }}/yesterday_package.txt
    """,
    dag=dag)

fetch_new = BashOperator(
    task_id='fetch_new',
    bash_command='cd /tmp/ndc_{{ ds }} && wget ' + SRC_PATH + SRC_FILE,
    dag=dag)

decompress_new = BashOperator(
    task_id='decompress_new',
    bash_command='cd /tmp/ndc_{{ ds }} && unzip -o ' + SRC_FILE,
    retries=3,
    dag=dag)

prepare_product = PythonOperator(
    task_id='prepare_product',
    op_kwargs = { "file_type": "product", "yesterday": "yesterday_product.txt", "today": "product.txt", "updated": "product_updated.txt" },
    python_callable=prepare_ndcfile,
    provide_context=True,
    dag=dag
)

prepare_package = PythonOperator(
    task_id='prepare_package',
    op_kwargs = { "file_type": "package", "yesterday": "yesterday_package.txt", "today": "package.txt", "updated": "package_updated.txt" },
    python_callable=prepare_ndcfile,
    provide_context=True,
    dag=dag
)

push_updated = BashOperator(
    task_id='push_updated',
    params={ "TMP_PATH": TMP_PATH, "S3_TEXT": S3_TEXT},
    bash_command="""
        /usr/local/bin/aws s3 cp --sse AES256 {{ params.TMP_PATH }}{{ ds }}/product_updated.txt {{ params.S3_TEXT }}{{ ds }}/product/product.txt
        /usr/local/bin/aws s3 cp --sse AES256 {{ params.TMP_PATH }}{{ ds }}/package_updated.txt {{ params.S3_TEXT }}{{ ds }}/package/package.txt
    """, 
    dag=dag
) 

create_temp = PythonOperator(
    task_id='create_temp',
    op_kwargs = { "schema": SCHEMA, "s3": S3_TEXT },
    python_callable=create_temp_tables,
    provide_context=True,
    dag=dag
)

create_new = PythonOperator(
    task_id='create_new',
    op_kwargs = { "schema": SCHEMA, "s3": S3_PARQUET },
    python_callable=create_new_ndc_table,
    provide_context=True,
    dag=dag
)

replace_old = PythonOperator(
    task_id='replace_old',
    op_kwargs = { "schema": SCHEMA, "s3": S3_PARQUET },
    python_callable=replace_old_table,
    provide_context=True,
    dag=dag
)

fetch_new.set_upstream(fetch_yesterday)
decompress_new.set_upstream(fetch_new)

prepare_package.set_upstream(decompress_new)
prepare_product.set_upstream(decompress_new)

push_updated.set_upstream([prepare_package, prepare_product])

create_temp.set_upstream(push_updated)
create_new.set_upstream(create_temp)

replace_old.set_upstream(create_new)


