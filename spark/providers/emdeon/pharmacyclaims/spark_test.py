#! /usr/bin/python
import os
import argparse
import time
from spark.runner import Runner
from spark.spark_setup import init
# import spark.helpers.create_date_validation_table as date_validator


def get_rel_path(relative_filename):
    return os.path.abspath(
        os.path.join(
            os.path.dirname(__file__),
            relative_filename
        )
    )


# init
spark, sqlContext = init("Emdeon RX")

# initialize runner
runner = Runner(sqlContext)

TODAY = time.strftime('%Y-%m-%d', time.localtime())
S3_EMDEON_IN = 's3a://salusv/warehouse/text/pharmacyclaims/emdeon/'
S3_EMDEON_WAREHOUSE = 's3://salusv/warehouse/text/pharmacyclaims/2017-02-28/part_provider=emdeon/'

parser = argparse.ArgumentParser()
parser.add_argument('--date', type=str)
args = parser.parse_known_args()

runner.run_spark_script(get_rel_path('load_warehouse_data.sql'), [
    ['input_path', S3_EMDEON_WAREHOUSE + 'part_best_date=2013-01/']
])

sqlContext.sql("CREATE TEMPORARY FUNCTION extract_date2 AS 'generalUdfs.extractDate'")
sqlContext.sql('DROP TABLE IF EXISTS java_udf')
sqlContext.sql('CREATE TABLE java_udf (d date)')
print(time.time())
sqlContext.sql("INSERT INTO java_udf SELECT date_service FROM normalized_claims")
print(time.time())
sqlContext.sql('SELECT d, count(*) FROM java_udf GROUP BY d ORDER BY count(*) DESC LIMIT 10').show()

sqlContext.sql('DROP TABLE IF EXISTS java_udf')
sqlContext.sql('CREATE TABLE java_udf (d date)')
print(time.time())
sqlContext.sql("INSERT INTO java_udf SELECT date_service FROM normalized_claims")
print(time.time())
sqlContext.sql('SELECT d, count(*) FROM java_udf GROUP BY d ORDER BY count(*) DESC LIMIT 10').show()

sqlContext.sql('DROP TABLE IF EXISTS java_udf')
sqlContext.sql('CREATE TABLE java_udf (d date)')
print(time.time())
sqlContext.sql("INSERT INTO java_udf SELECT extract_date(date_service, '%Y-%m-%d', cast('2012-01-01' as date), "
               "cast('2013-02-01' as date)) FROM normalized_claims")
print(time.time())
sqlContext.sql('SELECT d, count(*) FROM java_udf GROUP BY d ORDER BY count(*) DESC LIMIT 10').show()

sqlContext.sql('DROP TABLE IF EXISTS java_udf')
sqlContext.sql('CREATE TABLE java_udf (d date)')
print(time.time())
sqlContext.sql("INSERT INTO java_udf SELECT extract_date(date_service, '%Y-%m-%d', "
               "cast('2012-01-01' as date), cast('2013-02-01' as date)) FROM normalized_claims")
print(time.time())
sqlContext.sql('SELECT d, count(*) FROM java_udf GROUP BY d ORDER BY count(*) DESC LIMIT 10').show()

sqlContext.sql('DROP TABLE IF EXISTS java_udf')
sqlContext.sql('CREATE TABLE java_udf (d date)')
print(time.time())
sqlContext.sql("INSERT INTO java_udf SELECT extract_date2(date_service, 'yyyy-MM-dd', cast('2012-01-01' as date)"
               ", cast('2013-02-01' as date)) FROM normalized_claims")
print(time.time())
sqlContext.sql('SELECT d, count(*) FROM java_udf GROUP BY d ORDER BY count(*) DESC LIMIT 10').show()

sqlContext.sql('DROP TABLE IF EXISTS java_udf')
sqlContext.sql('CREATE TABLE java_udf (d date)')
print(time.time())
sqlContext.sql("INSERT INTO java_udf SELECT extract_date2(date_service, 'yyyy-MM-dd', cast('2012-01-01' as date)"
               ", cast('2013-02-01' as date)) FROM normalized_claims")
print(time.time())
sqlContext.sql('SELECT d, count(*) FROM java_udf GROUP BY d ORDER BY count(*) DESC LIMIT 10').show()

spark.sparkContext.stop()
