import argparse
import pyspark.sql.functions as F
from spark.runner import Runner
from spark.spark_setup import init
import spark.helpers.records_loader as records_loader
import file_schemas as file_schemas

def run(spark, runner, year):
    CPT_INPUT = 's3://salusv/incoming/reference/cpt/{}/'.format(year)
    CPT_OUTPUT = 's3://salusv/reference/parquet/cpt/{}/'.format(year)

    records_loader.load_and_clean_all_v2(runner, CPT_INPUT, file_schemas)

    cpt_long = spark.table('long')
    cpt_short = spark.table('short')
    cpt_pla = spark.table('pla')
    cpt_mod = spark.table('mod')

    cpt_short_long = cpt_long.join(cpt_short, cpt_long.code == cpt_short.code, 'full') \
                             .select(F.col('code'), F.col('short_description'),
                                     F.col('long_description')
                                    )
    cpt = cpt_short_long.union(
            cpt_pla.select(F.col('code'), F.col('short_description'), F.col('long_description')))

    cpt_plus_modifiers = cpt.union(
        cpt_mod.select(F.col('code'), F.lit(None).alias('short_description'),
                       F.col('long_description')
                      )
    )
    cpt_plus_modifiers.repartition(1).write.parquet(CPT_OUTPUT)


def main(args):
    spark, sqlContext = init('Reference CPT')

    runner = Runner(sqlContext)

    run(spark, runner, args.year)

    spark.stop()


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--year', type=str)
    args = parser.parse_args()
    main(args)
