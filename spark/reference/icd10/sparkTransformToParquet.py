import argparse
import pyspark.sql.functions as F
import spark.helpers.postprocessor as postprocessor
from spark.spark_setup import init

def run(spark, year):
    PCS_INPUT = 's3://salusv/incoming/reference/icd10/pcs/{}/'.format(year)
    PCS_OUTPUT = 's3://salusv/reference/parquet/icd10/{}/pcs/'.format(year)
    CM_INPUT = 's3://salusv/incoming/reference/icd10/cm/{}/'.format(year)
    CM_OUTPUT = 's3://salusv/reference/parquet/icd10/{}/cm/'.format(year)

    pcs = spark.read.text(PCS_INPUT)
    cm = spark.read.text(CM_INPUT)

    postprocessor.compose(
        postprocessor.trimmify,
        postprocessor.nullify
    )
    (
        pcs.select(
            pcs.value.substr(1, 5).alias('ordernum'),
            pcs.value.substr(7, 7).alias('code'),
            pcs.value.substr(15, 1).alias('header'),
            pcs.value.substr(17, 60).alias('short_description'),
            pcs.value.substr(78, 323).alias('long_description')
        )
    ).repartition(1).write.parquet(PCS_OUTPUT)

    postprocessor.compose(
        postprocessor.trimmify,
        postprocessor.nullify
    )
    (
        cm.select(
            cm.value.substr(1, 5).alias('ordernum'),
            cm.value.substr(7, 7).alias('code'),
            cm.value.substr(15, 1).alias('header'),
            cm.value.substr(17, 60).alias('short_description'),
            cm.value.substr(78, 323).alias('long_description')
        )
    ).repartition(1).write.parquet(CM_OUTPUT)

def main(args):
    spark, sqlContext = init('Reference ICD10')

    run(spark, args.year)

    spark.stop()


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--year', type=str)
    args = parser.parse_args()
    main(args)
