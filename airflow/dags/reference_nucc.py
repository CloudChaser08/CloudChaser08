import airflow.macros as macros

import common.HVDAG as HVDAG

import util.emr_utils as emr_utils

from airflow.models import Variable

from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator

import scrapy
from scrapy.spiders import Spider

from datetime import datetime, timedelta

for m in [HVDAG, emr_utils]:
    reload(m)

from util.hive import hive_execute

TMP_DIR = '/tmp/nucc_'

# EMR Cluster related fields
EMR_CLUSTER_NAME = 'nucc_taxonomy_{}'
EMR_NUM_NODES = '2'
EMR_NODE_TYPE = 'm4.xlarge'
EMR_EBS_VOLUME_SIZE = '10'

if Variable.get('AIRFLOW_ENV', default_var='').find('prod') != -1:
    SCHEMA = 'default'
    S3_TEXT = 'salusv/reference/nucc/'
    S3_PARQUET = 'salusv/reference/parquet/nucc/'
    AIRFLOW_ENV = 'prod'
else:
    SCHEMA = 'dev'
    S3_TEXT = 'salusv/testing/dewey/airflow/e2e/reference/nucc/'
    S3_PARQUET = 'salusv/testing/dewey/airflow/e2e/reference/parquet/nucc/'
    AIRFLOW_ENV = 'dev'

REF_NUCC_SCHEMA = '''
                code              string,
                taxonomy_type     string,
                classification    string,
                specialization    string,
                definition        string,
                notes             string,
                version           string
'''

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

dag = HVDAG.HVDAG(
    'reference_nucc',
    default_args = default_args,
    start_date = datetime(2009, 1, 8),
    # Run on the 8th day of the month every six months
#     schedule_interval = '0 0 8 */6 *' if Variable.get('AIRFLOW_ENV', default_var='').find('prod') != -1 else None,
    schedule_interval = '0 0 8 */6 *'
)


def scrape_nucc(ds, **kwargs):
    '''
    Our little crawler that will go out
    and download the file that we need.
    '''
    class nucc_spider(Spider):
        name = 'nucc'
    
        def start_requests(self):
            self.log("Scrapy spider is launching.")
            url = 'http://www.nucc.org/index.php/code-sets-mainmenu-41/provider-taxonomy-mainmenu-40/csv-mainmenu-57'
            yield scrapy.Request(url=url, callback=self.parse)
    
    
        def parse(self, response):
            self.log("Began parsing a page")
    
            # Parse out the relevant info using XPath Queries
            # Info: https://www.w3.org/TR/xpath/
            file_locs = response.xpath('//div[@class="content-wrapper"]/ul/li/a/@href').extract()
            file_desc = response.xpath('//div[@class="content-wrapper"]/ul/li/a/text()').extract()
            files = zip(file_locs, file_desc)
    
            # Filter out for the date that we want
            relevant_file_list = filter(lambda x: self.settings['EXPECTED_DATE'] in x[1], files)
    
            # Sanity checking
            if len(relevant_file_list) is 0:
                self.logger.error('No relevant file found.')
                self.log('Expected date: ' + self.settings['EXPECTED_DATE'])
                self.log('Files: ' + str(files))
                raise Exception('No relevant file found.')
            elif len(relevant_file_list) is not 1:
                self.logger.error('Found more than one relevant file.')
                raise Exception('Found more than one relevant file.')
            else:
                relevant_file, desc = relevant_file_list[0]

                # XCom the version for later tasks
                version = desc.replace(',','').split(' ')[1]
                kwargs['ti'].xcom_push(key = 'version', value = version)

                # Go download the file
                return scrapy.Request(url=response.urljoin(relevant_file),
                                    callback=self.download_file)
    

        def download_file(self, response):
            self.log('Downloading csv file')
            file_loc = self.settings['TMP_DIR'] + \
                    '/nucc-' + self.settings['EXPECTED_DATE'].replace('/','-') + '.csv'
            # XCom the file_loc for later tasks
            kwargs['ti'].xcom_push(key = 'file_loc', value = file_loc)

            # Write out the contents of the HTTP response to a CSV file
            with open(file_loc, 'wb') as f:
                f.write(response.body)


    from scrapy.crawler import CrawlerProcess

    crawler = CrawlerProcess()

    crawler.settings.set('LOG_ENABLED', False)
    
    date_parts = ds.split('-')
    expected_date_nucc_format = date_parts[1].lstrip('0') + '/1/' + date_parts[0][2:]
    crawler.settings.set('EXPECTED_DATE', expected_date_nucc_format)

    crawler.settings.set('TMP_DIR', TMP_DIR + ds)
    crawler.crawl(nucc_spider())

    crawler.start()


def do_create_emr_cluster(ds, **kwargs):
    cluster_name = EMR_CLUSTER_NAME.format(ds)

    global EMR_NUM_NODES, EMR_NODE_TYPE, EMR_EBS_VOLUME_SIZE
    EMR_NUM_NODES = kwargs.get('emr_num_nodes', EMR_NUM_NODES)
    EMR_NODE_TYPE = kwargs.get('emr_node_type', EMR_NODE_TYPE)
    EMR_EBS_VOLUME_SIZE = kwargs.get('emr_ebs_volume_size', EMR_EBS_VOLUME_SIZE)

    #TODO: Remove when done testing.
    print (cluster_name, EMR_NUM_NODES, EMR_NODE_TYPE, EMR_EBS_VOLUME_SIZE)

    emr_utils.create_emr_cluster(
        cluster_name, EMR_NUM_NODES, EMR_NODE_TYPE,
        EMR_EBS_VOLUME_SIZE, 'reference load', True)


def do_execute_queries(ds, ds_nodash, schema, s3_text, s3_parquet, ref_nucc_schema, **kwargs):
    sqls = [
        '''DROP TABLE IF EXISTS {}.temp_ref_nucc_{}'''.format(schema, ds_nodash),
        '''
        CREATE EXTERNAL TABLE {}.temp_ref_nucc_{} (
            {}
        )
        ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
        STORED AS TEXTFILE
        LOCATION 's3a://{}{}/'
        '''.format(schema, ds_nodash, ref_nucc_schema, s3_text, ds),
        ''' CREATE EXTERNAL TABLE IF NOT EXISTS {}.ref_nucc (
                {}
            )
            STORED AS PARQUET
        '''.format(schema, ref_nucc_schema),
        ''' CREATE EXTERNAL TABLE {}.ref_nucc_new_{} (
                {}
            )
            STORED AS PARQUET
            LOCATION 's3n://{}{}/'
        '''.format(schema, ds_nodash, ref_nucc_schema, s3_parquet, ds),
        ''' INSERT INTO {0}.ref_nucc_new_{1}
            SELECT * FROM (
                SELECT * FROM {0}.temp_ref_nucc_{1}
                UNION
                SELECT * FROM {0}.ref_nucc
            ) a
        '''.format(schema, ds_nodash),
        ''' ALTER TABLE {}.ref_nucc SET LOCATION 's3a://{}{}/' '''.format(schema, s3_parquet, ds),
        ''' DROP TABLE {}.temp_ref_nucc_{} '''.format(schema, ds_nodash),
        ''' DROP TABLE {}.ref_nucc_new_{} '''.format(schema, ds_nodash)
    ]

    cluster_name = EMR_CLUSTER_NAME.format(ds)
    emr_utils.run_hive_queries(cluster_name, sqls)


def do_delete_cluster(ds, **kwargs):
    cluster_name = EMR_CLUSTER_NAME.format(ds)

    emr_utils.delete_emr_cluster(cluster_name)


### Operators ###

create_tmp_dir = BashOperator(
    task_id = 'create_tmp_dir',
    params = { 'TMP_DIR': TMP_DIR },
    bash_command = 'mkdir -p {{ params.TMP_DIR }}{{ ds }}',
    retries = 3,
    dag = dag
)

fetch_csv = PythonOperator(
    task_id = 'fetch_csv',
    python_callable=scrape_nucc,
    provide_context=True,
    dag=dag
)

remove_csv_header = BashOperator(
    task_id = 'remove_csv_header',
    bash_command = '''
        sed -i '1d' {{ ti.xcom_pull(task_ids = 'fetch_csv', key = 'file_loc') }}
    ''',
    retries = 3,
    dag = dag
)

append_version_to_csv = BashOperator( task_id = 'append_version_to_csv',
    bash_command = '''
    perl -pi -e 's/\\r\\n/,{{ ti.xcom_pull(task_ids = 'fetch_csv', key = 'version') }}\\n/g' {{ ti.xcom_pull(task_ids = 'fetch_csv', key = 'file_loc') }}
    ''',
    retires = 3,
    dag = dag
)

push_csv_to_s3 = BashOperator(
    task_id = 'push_csv_to_s3',
    params = { 'TMP_DIR': TMP_DIR, 'S3_TEXT': S3_TEXT },
    bash_command = '''/usr/local/bin/aws s3 cp --sse AES256 {{ ti.xcom_pull(task_ids = 'fetch_csv', key = 'file_loc') }} s3://{{ params.S3_TEXT }}{{ ds }}/nucc.csv''',
    retries = 3,
    dag = dag
)

delete_tmp_dir = BashOperator(
    task_id = 'delete_tmp_dir',
    params = { 'TMP_DIR': TMP_DIR },
    bash_command = 'rm -r {{ params.TMP_DIR }}{{ ds }}',
    retries = 3,
    dag = dag
)

create_emr_cluster = PythonOperator(
    task_id = 'create_emr_cluster',
    python_callable = do_create_emr_cluster,
    provide_context = True,
    dag = dag
)

execute_queries = PythonOperator(
    task_id = 'execute_queries',
    op_kwargs = { 
        'schema': SCHEMA,
        's3_text': S3_TEXT,
        's3_parquet': S3_PARQUET,
        'ref_nucc_schema': REF_NUCC_SCHEMA
    },
    python_callable = do_execute_queries,
    provide_context = True,
    dag = dag
)

delete_cluster = PythonOperator(
    task_id = 'delete_cluster',
    python_callable = do_delete_cluster,
    provide_context = True,
    dag = dag
)

### DAG structure ###

fetch_csv.set_upstream(create_tmp_dir)
remove_csv_header.set_upstream(fetch_csv)
append_version_to_csv.set_upstream(remove_csv_header)
push_csv_to_s3.set_upstream(append_version_to_csv)

delete_tmp_dir.set_upstream(push_csv_to_s3)

create_emr_cluster.set_upstream(push_csv_to_s3)
execute_queries.set_upstream(create_emr_cluster)
delete_cluster.set_upstream(execute_queries)

