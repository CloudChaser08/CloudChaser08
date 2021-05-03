from datetime import datetime
import argparse
import pyspark.sql.functions as F
from spark.runner import Runner
from spark.spark_setup import init
import spark.helpers.external_table_loader as external_table_loader
import spark.helpers.records_loader as records_loader
import spark.reference.cpt.file_schemas as file_schemas


def run(spark, runner, year):
    timestamp = datetime.now().strftime('%Y%m%d%H%M%S')
    CPT_INPUT = 's3://salusv/incoming/reference/cpt/{}/'.format(year)
    CPT_OUTPUT = 's3://salusv/reference/parquet/cpt/{}/{}/'.format(year, timestamp)

    """
        NOTE: To prep SHORTU.txt, you need to convert to utf-8, replace the first space with a tab,
        then convert back to latin-1 (the default encoding).
        Steps from shell:
        1. mv SHORTU orig_SHORTU.txt
        2. iconv -f latin1 -t utf-8 orig_SHORTU.txt | sed 's/ /<press Ctrl-V then Tab>/' | iconv 
        -f utf-8 -t latin1 > SHORTU.txt 
        "\t" will not work to input tabs
    """

    records_loader.load_and_clean_all_v2(runner, CPT_INPUT, file_schemas)
    external_table_loader.load_analytics_db_table(
        runner.sqlContext,
        'default',
        'ref_cpt',
        'cpt_codes'
    )

    cpt_long = spark.table('long')
    cpt_short = spark.table('short')
    cpt_pla = spark.table('pla')
    cpt_mod = spark.table('mod')

    """
    The rules for creating the cpt table are as follows:
        1. Start with the long description and short description table.  Join them on the cpt code to
           get a base set of cpt_codes that contain: code, short_desc, long_desc
        2. Union the CPT-PLA codes to the base set created in step 1.
        3. Union the Modifiers to the cpt_code set.  The Modifiers table only
           has a long description column, so set the short_desc to NULL when performing the union
        4. Take missing rows from current ref_cpt table and union them to get the final set
           of cpt_codes
        5. Filter out any codes that are not of length 2 (modifiers) or length 5 (codes)

    NOTES: This logic IS subject to change year by year, so make sure to always verify with
          analytics that the logic remains the same based on the files we recived.
         
        The 2020 'short' file was not tab seperated. We had to manually update the file to match the
        expected format. 
        
        Once the parquet is in place, run the following to create the table:
        CREATE EXTERNAL TABLE ref_cpt(`code` STRING, `short_description` STRING, `long_description` STRING)
            ROW FORMAT SERDE 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
            WITH SERDEPROPERTIES (
              'serialization.format' = '1'
            )
            STORED AS
              INPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat'
              OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
            LOCATION '<path_to_output_parquet>'
    """

    cpt_short_long = cpt_long.join(
        cpt_short, cpt_long.long_code == cpt_short.short_code, 'full')\
        .select(F.col('short_code').alias('code'), F.col('short_description'), F.col('long_description'))
    cpt = cpt_short_long.union(
        cpt_pla.select(F.col('pla_code').alias('code'), F.col('short_description'), F.col('long_description')))

    cpt_plus_modifiers = cpt.union(
        cpt_mod.select(F.col('mod_code').alias('code'), F.lit(None).alias('short_description'),
                       F.col('long_description')))

    current_cpt_table = spark.table('cpt_codes')
    missing_current_cpt_codes = current_cpt_table.join(
        cpt_plus_modifiers, current_cpt_table.code == cpt_plus_modifiers.code, 'leftanti')

    cpt_all = cpt_plus_modifiers.union(missing_current_cpt_codes)

    cpt_final = cpt_all.where((F.length(F.col('code')) == 2) | (F.length(F.col('code')) == 5))

    cpt_final.repartition(1).write.parquet(CPT_OUTPUT)


def main(args):
    spark, sql_context = init('Reference CPT')

    runner = Runner(sql_context)

    run(spark, runner, args.year)

    spark.stop()


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--year', type=str)
    args = parser.parse_known_args()[0]
    main(args)
