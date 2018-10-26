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

    '''
    The rules for creating the cpt table are as follows:
        1. Start with the long description and short description table.  Join them on the cpt code to
           get a base set of cpt_codes that contain: code, short_desc, long_desc
        2. Union the CPT-PLA codes to the base set created in step 1.
        3. Union the Modifiers to get the final set of cpt_codes.  The Modifiers table only
           has a long description column, so set the short_desc to NULL when performing the union

    NOTE: This logic IS subject to change year by year, so make sure to always verify with
          analytics that the logic remains the same based on the files we recived.
    '''
    cpt_short_long = cpt_long.join(cpt_short, cpt_long.long_code == cpt_short.short_code, 'full') \
                             .select(F.col('short_code').alias('code'), F.col('short_description'),
                                     F.col('long_description')
                                    )
    cpt = cpt_short_long.union(
            cpt_pla.select(F.col('pla_code').alias('code'), F.col('short_description'), F.col('long_description')))

    cpt_plus_modifiers = cpt.union(
        cpt_mod.select(F.col('mod_code').alias('code'), F.lit(None).alias('short_description'),
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
