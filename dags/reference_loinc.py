#ye The DAG object; we'll need this to instantiate a DAG
from airflow import DAG

# Operators; we need this to operate!
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator

from airflow.models import Variable

import scrapy
from scrapy.spiders import Spider

import re
import os
import sys
from json import loads
import struct
from datetime import timedelta, datetime

if sys.modules.get('util.hv_datadog'):
    del sys.modules['util.hv_datadog']
from util.hv_datadog import hv_datadog, start_dag_op, end_dag_op

if sys.modules.get('util.hive'):
    del sys.modules['util.hive']
from util.hive import hive_execute

TMP_PATH='/tmp/loinc_'
FILES_OF_INTEREST=['loinc.csv','map_to.csv','source_organization.csv']

if Variable.get('AIRFLOW_ENV', default_var='').find('prod') != -1:
    SCHEMA='default'
    S3_TEXT='s3://salusv/reference/loinc/'
    S3_PARQUET='s3a://salusv/reference/parquet/loinc/'
    AIRFLOW_ENV='prod'
else:
    SCHEMA='dev'
    S3_TEXT='s3://healthveritydev/jcap/loinc/'
    S3_PARQUET='s3a://healthveritydev/jcap/parquet/loinc/'
    AIRFLOW_ENV='dev'


dd = hv_datadog(env=AIRFLOW_ENV, keys=loads(Variable.get('DATADOG_KEYS')))

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'on_success_callback': dd.dd_eventer,
    'on_retry_callback':   dd.dd_eventer,
    'on_failure_callback': dd.dd_eventer
}

dag = DAG(
    'reference_loinc',
    default_args=default_args,
    start_date=datetime(2017, 2, 6),
    schedule_interval='@monthly' if Variable.get('AIRFLOW_ENV', default_var='').find('prod') != -1 else None,
)

class FiledownloadItem(scrapy.Item):
    file_urls = scrapy.Field()
    files = scrapy.Field()

class loinc_spider(Spider):
    name = "loinc"

    def start_requests(self):
        url = 'https://loinc.org/login_form'
        yield scrapy.Request(url=url, callback=self.parse)

    def parse(self, response):
        self.log("Parsing Login Form")
        return scrapy.FormRequest.from_response(
            response,
            formname='login_form',
            formdata={'__ac_name': 'healthverity', '__ac_password': 'XwoBMtT6GUZBmn8XjyV7'},
            callback=self.after_login
        )


    def after_login(self, response):
        self.log("Submitted Login Form")
        # check login succeed before going on
        if "Login failed" in response.body:
            self.logger.error("Login failed")
            return
        # We've successfully authenticated, let's have some fun!
        else:
            return scrapy.Request(url="https://loinc.org/downloads/files/loinc-table-csv-text-format",
                            callback=self.agree_terms)

        self.agree_terms()

    def agree_terms(self, response):
        self.log("Agreeing to Terms")
        if "Please Review the Copyright and Terms of Use" not in response.body:
            self.log("Copyright form not found")
            return scrapy.Request(url="https://loinc.org/downloads/files/loinc-table-csv-text-format/gotoCopyrightedFile",
                            callback=self.get_download_link)
        else:
            return scrapy.FormRequest.from_response(
                response,
                formname='edit_form',
                formdata={"loinc-and-relma-copyright-and-terms-of-use":"I Accept These Terms and Conditions"},
                callback=self.agree
            )

        self.agree()

    def agree(self, response):
        self.log("Terms Agreed To")
        if "You have not reviewed and agreed to" in response.body:
            self.log("Terms NOT agreed to apparently")
            return scrapy.Request(url="https://loinc.org/downloads/files/loinc-table-csv-text-format",
                            callback=self.agree_terms)
        else:
            return FiledownloadItem(
                        file_urls=[
                            'http://loinc.org/downloads/files/loinc-table-csv-text-format/loinc-table-file-csv-text-format/download'
                        ]
                    )


def scrape_loinc(tomorrow_ds, **kwargs):
    """Setups item signal and run the spider"""
    # set up crawler
    from scrapy.crawler import CrawlerProcess

    # shut off log
    crawler = CrawlerProcess()
    crawler.settings.set('LOG_ENABLED', False)
    crawler.settings.set('ITEM_PIPELINES','{"scrapy.pipelines.files.FilesPipeline":1}')
    crawler.settings.set('FILES_STORE', TMP_PATH + tomorrow_ds)

    # schedule spider
    crawler.crawl(loinc_spider())

    # start engine scrapy/twisted
    crawler.start()




def create_temp_tables(tomorrow_ds, schema, s3, **kwars):

    sqls = [
        """DROP TABLE IF EXISTS {}.temp_ref_loinc""".format(schema),
        """
        CREATE EXTERNAL TABLE {}.temp_ref_loinc (
            loinc_num string,
            component string,
            property string,
            time_aspct string,
            loinc_system string,
            scale_type string,
            method_type string,
            loinc_class string,
            versionlastchanged string,
            chng_type string,
            definitiondescription string,
            status string,
            consumer_name string,
            classtype double,
            formula string,
            species string,
            exmpl_answers string,
            survey_quest_text string,
            survey_quest_src string,
            unitsrequired string,
            submitted_units string,
            relatednames2 string,
            shortname string,
            order_obs string,
            cdisc_common_tests string,
            hl7_field_subfield_id string,
            external_copyright_notice string,
            example_units string,
            long_common_name string,
            unitsandrange string,
            document_section string,
            example_ucum_units string,
            example_si_ucum_units string,
            status_reason string,
            status_text string,
            change_reason_public string,
            common_test_rank double,
            common_order_rank double,
            common_si_test_rank double,
            hl7_attachment_structure string,
            external_copyright_link string,
            paneltype string,
            askatorderentry string,
            associatedobservations string,
            versionfirstreleased string,
            validhl7attachmentrequest string
        )
        ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
        STORED AS TEXTFILE
        LOCATION '{}{}/loinc.csv/'
        tblproperties ('skip.header.line.count'='1')""".format(schema, s3, tomorrow_ds)
    ]

    hive_execute(sqls)

def create_new_loinc_table(tomorrow_ds, schema, s3, **kwargs):
    sqls = [
        """DROP TABLE IF EXISTS {}.ref_loinc_new""".format(schema),

        """CREATE EXTERNAL TABLE {}.ref_loinc_new (
              loinc_num string,
              component string,
              property string,
              time_aspct string,
              loinc_system string,
              scale_type string,
              method_type string,
              loinc_class string,
              versionlastchanged string,
              chng_type string,
              definitiondescription string,
              status string,
              consumer_name string,
              classtype double,
              formula string,
              species string,
              exmpl_answers string,
              survey_quest_text string,
              survey_quest_src string,
              unitsrequired string,
              submitted_units string,
              relatednames2 string,
              shortname string,
              order_obs string,
              cdisc_common_tests string,
              hl7_field_subfield_id string,
              external_copyright_notice string,
              example_units string,
              long_common_name string,
              unitsandrange string,
              document_section string,
              example_ucum_units string,
              example_si_ucum_units string,
              status_reason string,
              status_text string,
              change_reason_public string,
              common_test_rank double,
              common_order_rank double,
              common_si_test_rank double,
              hl7_attachment_structure string,
              external_copyright_link string,
              paneltype string,
              askatorderentry string,
              associatedobservations string
            )
            STORED AS PARQUET""".format(schema),

        """CREATE EXTERNAL TABLE IF NOT EXISTS {}.ref_loinc LIKE {}.ref_loinc_new""".format(schema, schema),

        """ALTER TABLE {}.ref_loinc_new SET LOCATION '{}{}/'""".format(schema, s3, tomorrow_ds),

        """ INSERT INTO {}.ref_loinc_new
                SELECT loinc_num, component, property, time_aspct, loinc_system, scale_type, method_type, loinc_class,
                       versionlastchanged, chng_type, definitiondescription, status, consumer_name, classtype,
                       formula, species, exmpl_answers, survey_quest_text, survey_quest_src, unitsrequired, 
                       submitted_units, relatednames2, shortname, order_obs, cdisc_common_tests, hl7_field_subfield_id,
                       external_copyright_notice, example_units, long_common_name, unitsandrange, document_section,
                       example_ucum_units, example_si_ucum_units, status_reason, status_text, change_reason_public,
                       common_test_rank, common_order_rank, common_si_test_rank, hl7_attachment_structure,
                       external_copyright_link, paneltype, askatorderentry, associatedobservations
                  FROM {}.temp_ref_loinc
                UNION DISTINCT
                SELECT loinc_num, component, property, time_aspct, loinc_system, scale_type, method_type, loinc_class,
                       versionlastchanged, chng_type, definitiondescription, status, consumer_name, classtype,
                       formula, species, exmpl_answers, survey_quest_text, survey_quest_src, unitsrequired, 
                       submitted_units, relatednames2, shortname, order_obs, cdisc_common_tests, hl7_field_subfield_id,
                       external_copyright_notice, example_units, long_common_name, unitsandrange, document_section,
                       example_ucum_units, example_si_ucum_units, status_reason, status_text, change_reason_public,
                       common_test_rank, common_order_rank, common_si_test_rank, hl7_attachment_structure,
                       external_copyright_link, paneltype, askatorderentry, associatedobservations
                  FROM {}.ref_loinc
           """.format(schema, schema, schema)
    ]

    hive_execute(sqls)

def replace_old_table(tomorrow_ds, schema, s3, **kwargs):
    sqls = [
        """DROP TABLE {}.ref_loinc_new""".format(schema),
        """ALTER TABLE {}.ref_loinc SET LOCATION '{}{}/'""".format(schema, s3, tomorrow_ds),

        """DROP TABLE IF EXISTS {}.temp_ref_loinc""".format(schema)
    ]

    hive_execute(sqls)

start_run = start_dag_op(dag, dd)
end_run   = end_dag_op(dag, dd)


create_tmp_dir = BashOperator(
    task_id='create_tmp_dir',
    params={ "TMP_PATH": TMP_PATH},
    bash_command='mkdir -p {{ params.TMP_PATH }}{{ tomorrow_ds }}',
    retries=3,
    dag=dag)

fetch_new = PythonOperator(
    task_id='fetch_new',
    python_callable=scrape_loinc,
    provide_context=True,
    dag=dag
)

decompress_new = BashOperator(
    task_id='decompress_new',
    params={ "TMP_PATH": TMP_PATH},
    bash_command='cd {{ params.TMP_PATH }}{{ tomorrow_ds }}/full && unzip -o * ' + ' '.join(FILES_OF_INTEREST),
    retries=3,
    dag=dag)

push_updated = []
for f in FILES_OF_INTEREST:
    push_updated.append(
        BashOperator(
            task_id='push_updated_' + f,
            params={ "TMP_PATH": TMP_PATH, "S3_TEXT": S3_TEXT, "FOI": f},
            bash_command="""
                gzip {{ params.TMP_PATH }}{{ tomorrow_ds }}/full/{{ params.FOI }}
                /usr/local/bin/aws s3 cp --sse AES256 {{ params.TMP_PATH }}{{ tomorrow_ds }}/full/{{ params.FOI }}.gz {{ params.S3_TEXT }}{{ tomorrow_ds }}/{{ params.FOI }}/{{ params.FOI}}.gz
            """, 
            dag=dag
        )
    )

cleanup_temp = BashOperator(
    task_id='cleanup_temp',
    params={ "TMP_PATH": TMP_PATH},
    bash_command='rm -rf {{ params.TMP_PATH }}{{ tomorrow_ds }}',
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
    op_kwargs = { "schema": SCHEMA, "s3": S3_PARQUET},
    python_callable=create_new_loinc_table,
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

start_run.set_downstream(create_tmp_dir)

fetch_new.set_upstream(create_tmp_dir)
decompress_new.set_upstream(fetch_new)

for i in push_updated:
    i.set_upstream(decompress_new)
    i.set_downstream(cleanup_temp)
    i.set_downstream(create_temp)

create_new.set_upstream(create_temp)
replace_old.set_upstream(create_new)

end_run.set_upstream(replace_old)

