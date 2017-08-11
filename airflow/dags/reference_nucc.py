# import common.HVDAG as HVDAG
# 
# from airflow.operators.bash_operator import BashOperator
# from airflow.operators.python_operator import PythonOperator

import scrapy
from scrapy.item import Item
from scrapy.spiders import Spider

import csv

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
    crawler.settings.set('EXPECTED_DATE', ds)
    crawler.settings.set('TMP_DIR', '/tmp/joe/nucc')
    crawler.crawl(nucc_spider())

    crawler.start()


if __name__ == '__main__':
    scrape_nucc('7/1/17')
