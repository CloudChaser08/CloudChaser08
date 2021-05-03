import argparse

from spark.runner import Runner
from spark.spark_setup import init

import spark.helpers.file_utils as file_utils


def run(spark, runner, output_location):
    script_path = __file__

    sql_script = file_utils.get_abs_path(
        script_path, '../pull_ndc_ref.sql'
    )

    runner.run_spark_script(
        sql_script,
        []
    )

    marketplace_ndc = runner.sqlContext.sql('select * from marketplace_ndc_new')

    marketplace_ndc.repartition(1).write.parquet(output_location)


def main(args):
    spark, sql_context = init('Marketplace NDC')

    runner = Runner(sql_context)

    run(spark, runner, args.output_loc)

    spark.stop()


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--output_loc', type=str)
    args = parser.parse_known_args()
    main(args)
