import common.HVDAG as HVDAG

from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator

from airflow.models import Variable

import scrapy
from scrapy.spiders import Spider

from datetime import datetime, timedelta

for m in [HVDAG]:
    reload(m)

TMP_DIR = '/tmp/nucc_'

if Variable.get('AIRFLOW_ENV', default_var='').find('prod') != -1:
    SCHEMA = 'default'
    S3_TEXT = 'salusv/reference/nucc/'
    S3_PARQUET = 'salusv/reference/parquet/nucc/'
    S3_PARQUET_MAP = 'salusv/reference/parquet/nucc_map'
    AIRFLOW_ENV = 'prod'
else:
    SCHEMA = 'dev'
    S3_TEXT = 'salusv/...'
    S3_PARQUET = 'salusv/...'
    S3_PARQUET_MAP = 'salusv/...'
    AIRFLOW_ENV = '...'

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
    schedule_interval = '0 12 * * *' if Variable.get('AIRFLOW_ENV', default_var='').find('prod') != -1 else None,
)


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
        elif len(relevant_file_list) is not 1:
            self.logger.error('Found more than one relevant file.')
        else:
            # Create a Scrapy Item (downloadable_csv) to be processed
            # by our defined Scrapy Item Pipeline
            relevant_file = relevant_file_list[0]
            return scrapy.Request(url=response.urljoin(relevant_file[0]),
                                callback=self.download_file)


    def download_file(self, response):
        self.log('Downloading csv file')
        with open(self.settings['TMP_DIR'] + \
                '/nucc_' + \
                self.settings['EXPECTED_DATE'].replace('/','-') + \
                '.csv', 'wb') as f:
            f.write(response.body)


def scrape_nucc(ds, **kwargs):
    from scrapy.crawler import CrawlerProcess

    crawler = CrawlerProcess()
    crawler.settings.set('LOG_ENABLED', True)
    
    date_parts = ds.split('-')
    expected_date_nucc_format = date_parts[1].lstrip('0') + '/1/' + date_parts[0].lstrip('20')
    crawler.settings.set('EXPECTED_DATE', expected_date_nucc_format)

    crawler.settings.set('TMP_DIR', TMP_DIR + ds)
    crawler.crawl(nucc_spider())

    crawler.start()


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

#TODO: place correct paths for S3 dev env and test.
push_csv_to_s3 = BashOperator(
    task_id = 'push_csv_to_s3',
    params = { 'TMP_DIR': TMP_DIR },
    bash_command = '/usr/local/bin/aws s3 cp --sse --recursive AES256 {{ params.TMP_DIR }}{{ ds }} s3://{{ params.S3_TEXT }}{{ ds }}',
    retries = 3,
    dag = dag
)


fetch_csv.set_upstream(create_tmp_dir)
