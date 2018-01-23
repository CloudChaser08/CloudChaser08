#ye The DAG object; we'll need this to instantiate a DAG
import common.HVDAG as HVDAG

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

import util.hv_datadog
import util.hive

for m in [util.hv_datadog, util.hive, HVDAG]:
    reload(m)

from util.hv_datadog import hv_datadog, start_dag_op, end_dag_op
from util.hive import hive_execute

TMP_PATH='/tmp/loinc_'
FILES_OF_INTEREST=['loinc.csv','map_to.csv','source_organization.csv']

if Variable.get('AIRFLOW_ENV', default_var='').find('prod') != -1:
    SCHEMA='default'
    S3_TEXT='salusv/reference/loinc/'
    S3_PARQUET='salusv/reference/parquet/loinc/'
    S3_PARQUET_MAP='salusv/reference/parquet/loinc_map/'
    AIRFLOW_ENV='prod'
else:
    SCHEMA='dev'
    S3_TEXT='healthveritydev/jcap/loinc/'
    S3_PARQUET='healthveritydev/jcap/parquet/loinc/'
    S3_PARQUET_MAP='healthveritydev/jcap/parquet/loinc_map/'
    AIRFLOW_ENV='dev'

REF_LOINC_COLUMNS = [
            ['loinc_num', 'string'],
            ['component', 'string'],
            ['property', 'string'],
            ['time_aspct', 'string'],
            ['loinc_system', 'string'],
            ['scale_type', 'string'],
            ['method_type', 'string'],
            ['loinc_class', 'string'],
            ['versionlastchanged', 'string'],
            ['chng_type', 'string'],
            ['definitiondescription', 'string'],
            ['status', 'string'],
            ['consumer_name', 'string'],
            ['classtype', 'double'],
            ['formula', 'string'],
            ['species', 'string'],
            ['exmpl_answers', 'string'],
            ['survey_quest_text', 'string'],
            ['survey_quest_src', 'string'],
            ['unitsrequired', 'string'],
            ['submitted_units', 'string'],
            ['relatednames2', 'string'],
            ['shortname', 'string'],
            ['order_obs', 'string'],
            ['cdisc_common_tests', 'string'],
            ['hl7_field_subfield_id', 'string'],
            ['external_copyright_notice', 'string'],
            ['example_units', 'string'],
            ['long_common_name', 'string'],
            ['unitsandrange', 'string'],
            ['document_section', 'string'],
            ['example_ucum_units', 'string'],
            ['example_si_ucum_units', 'string'],
            ['status_reason', 'string'],
            ['status_text', 'string'],
            ['change_reason_public', 'string'],
            ['common_test_rank', 'double'],
            ['common_order_rank', 'double'],
            ['common_si_test_rank', 'double'],
            ['hl7_attachment_structure', 'string'],
            ['external_copyright_link', 'string'],
            ['paneltype', 'string'],
            ['askatorderentry', 'string'],
            ['associatedobservations', 'string'],
            ['versionfirstreleased', 'string'],
            ['validhl7attachmentrequest', 'string']
];

REF_LOINC_SCHEMA = ',\n'.join([col[0] + ' ' + col[1] for col in REF_LOINC_COLUMNS])

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

dag = HVDAG.HVDAG(
    'reference_loinc',
    default_args=default_args,
    start_date=datetime(2017, 2, 6),
    schedule_interval='@monthly' if Variable.get('AIRFLOW_ENV', default_var='').find('prod') != -1 else None,
)

class loinc_spider(Spider):
    name = "loinc"

    def start_requests(self):
        url = 'https://loinc.org/login_form'
        yield scrapy.Request(url=url, callback=self.parse)

    def parse(self, response):
        self.log("Parsing Login Form")
        return scrapy.FormRequest.from_response(
            response,
            formname='loginform',
            formdata={'log': 'healthverity', 'pwd': 'XwoBMtT6GUZBmn8XjyV7'},
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
	    self.log("We have successfully logged in, let's have some fun.")
            return scrapy.Request(url="https://loinc.org/download/loinc-table-file-csv",
                            callback=self.agree_terms)

        self.agree_terms()

    def agree_terms(self, response):
        self.log("Agreeing to Terms")
        if "Please review the following Copyright and Terms of Use" not in response.body:
            self.log("Copyright form not found")
            return scrapy.Request(url="https://loinc.org/download/loinc-table-file-csv",
                            callback=self.download_file)
        else:
            return scrapy.FormRequest(
                url='https://loinc.org/download/loinc-table-file-csv/',
                formdata={"tc_accepted":"1", "tc_submit":"download"},
                callback=self.download_file
            )

        self.download_file()

    def download_file(self, response):
        self.log("Terms Agreed To")
        if "You have not reviewed and agreed to" in response.body:
            self.log("Terms NOT agreed to apparently")
            return scrapy.Request(url="https://loinc.org/download/loinc-table-file-csv",
                            callback=self.agree_terms)
        else:
            self.log("Downloading zip into " + self.settings.attributes['FILES_STORE'].value + '/full/loinc.zip')
            with open(self.settings.attributes['FILES_STORE'].value + '/full/loinc.zip', 'wb') as f:
                f.write(response.body)


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




def create_temp_tables(tomorrow_ds, schema, s3, ref_loinc_schema, **kwargs):

    sqls = [
        """DROP TABLE IF EXISTS {}.temp_ref_loinc_string""".format(schema),
        """
        CREATE EXTERNAL TABLE {}.temp_ref_loinc_string (
            {}
        )
        ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
        STORED AS TEXTFILE
        LOCATION 's3a://{}{}/loinc.csv/'
        tblproperties ('skip.header.line.count'='1')""".format(schema, ref_loinc_schema, s3, tomorrow_ds),

        """DROP TABLE IF EXISTS {}.temp_ref_loinc""".format(schema),
        """
        CREATE TABLE {}.temp_ref_loinc (
            {}
        )
        """.format(schema, ref_loinc_schema),

        """
        INSERT INTO {}.temp_ref_loinc SELECT * FROM {}.temp_ref_loinc_string
        """.format(schema, schema),

        """
        DROP TABLE {}.temp_ref_loinc_string
        """.format(schema),

        """DROP TABLE IF EXISTS {}.temp_ref_loinc_map_to""".format(schema),
        """
        CREATE EXTERNAL TABLE {}.temp_ref_loinc_map_to (
            loinc_num string,
            map_to string,
            comment string
        )
        ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
        STORED AS TEXTFILE
        LOCATION 's3a://{}{}/map_to.csv/'
        tblproperties ('skip.header.line.count'='1')""".format(schema, s3, tomorrow_ds)

    ]

    hive_execute(sqls)

def create_new_loinc_table(tomorrow_ds, schema, s3_loinc, s3_map, ref_loinc_schema, **kwargs):
    sqls = [
        """DROP TABLE IF EXISTS {}.ref_loinc_new""".format(schema),

        """CREATE EXTERNAL TABLE {}.ref_loinc_new (
                {}
            )
            STORED AS PARQUET
            LOCATION 's3n://{}{}/'""".format(schema, ref_loinc_schema, s3_loinc, tomorrow_ds),

        """CREATE EXTERNAL TABLE IF NOT EXISTS {0}.ref_loinc like {0}.ref_loinc_new STORED AS PARQUET""".format(schema),


        # We need to strip any non-numeric characters out of the LOINC codes
        """ INSERT INTO {0}.ref_loinc_new
                select regexp_replace(a.loinc_num, '[^0-9]', '') as loinc_num, {1} from (
                    SELECT * FROM {0}.temp_ref_loinc t
                    UNION DISTINCT
                    SELECT * FROM {0}.ref_loinc r WHERE r.loinc_num NOT IN
                        (SELECT regexp_replace(loinc_num, '[^0-9]', '') FROM {0}.temp_ref_loinc)
                ) a
           """.format(schema, ', '.join([c[0] for c in REF_LOINC_COLUMNS[1:]])),

        """DROP TABLE IF EXISTS {}.ref_loinc_map_to_new""".format(schema),

        """CREATE EXTERNAL TABLE {}.ref_loinc_map_to_new (
              loinc_num string,
              map_to string,
              comment string
            )
            STORED AS PARQUET
            LOCATION 's3n://{}{}/'""".format(schema, s3_map, tomorrow_ds),

        # We need to strip any non-numeric characters out of the LOINC codes
        """INSERT INTO {0}.ref_loinc_map_to_new
            SELECT regexp_replace(loinc_num, '[^0-9]', '') as loinc_num,
            regexp_replace(map_to, '[^0-9]', '') as map_to,
            comment
            FROM {0}.temp_ref_loinc_map_to""".format(schema)
    ]

    hive_execute(sqls)

def replace_old_table(tomorrow_ds, schema, s3_loinc, s3_map, **kwargs):
    sqls = [
        """DROP TABLE {}.ref_loinc_new""".format(schema),
        """ALTER TABLE {}.ref_loinc SET LOCATION 's3a://{}{}/'""".format(schema, s3_loinc, tomorrow_ds),

        """CREATE EXTERNAL TABLE IF NOT EXISTS {0}.ref_loinc_map_to LIKE {0}.ref_loinc_map_to_new""".format(schema),
        """DROP TABLE {}.ref_loinc_map_to_new""".format(schema),
        """ALTER TABLE {}.ref_loinc_map_to SET LOCATION 's3a://{}{}/'""".format(schema, s3_map, tomorrow_ds),

        """DROP TABLE IF EXISTS {}.temp_ref_loinc""".format(schema),
        """DROP TABLE IF EXISTS {}.temp_ref_loinc_map_to""".format(schema)
    ]

    hive_execute(sqls)

start_run = start_dag_op(dag, dd)
end_run   = end_dag_op(dag, dd)


create_tmp_dir = BashOperator(
    task_id='create_tmp_dir',
    params={ "TMP_PATH": TMP_PATH},
    bash_command='mkdir -p {{ params.TMP_PATH }}{{ tomorrow_ds }}/full',
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
                /usr/local/bin/aws s3 cp --sse AES256 {{ params.TMP_PATH }}{{ tomorrow_ds }}/full/{{ params.FOI }}.gz s3://{{ params.S3_TEXT }}{{ tomorrow_ds }}/{{ params.FOI }}/{{ params.FOI}}.gz
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
    op_kwargs = { "schema": SCHEMA, "s3": S3_TEXT, "ref_loinc_schema": REF_LOINC_SCHEMA},
    python_callable=create_temp_tables,
    provide_context=True,
    dag=dag
)

create_new = PythonOperator(
    task_id='create_new',
    op_kwargs = { "schema": SCHEMA, "s3_loinc": S3_PARQUET, "s3_map": S3_PARQUET_MAP, "ref_loinc_schema": REF_LOINC_SCHEMA},
    python_callable=create_new_loinc_table,
    provide_context=True,
    dag=dag
)

replace_old = PythonOperator(
    task_id='replace_old',
    op_kwargs = { "schema": SCHEMA, "s3_loinc": S3_PARQUET, "s3_map": S3_PARQUET_MAP},
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

