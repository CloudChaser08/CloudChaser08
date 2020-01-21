#! /usr/bin/python
import argparse

from spark.runner import Runner
from spark.spark_setup import init

from spark.common.marketplace_driver import MarketplaceDriver
from spark.common.lab_common_model import schemas as labtest_schemas
import spark.helpers.external_table_loader as external_table_loader
import spark.providers.ambry_genetics.labtests.transactional_schemas as source_table_schemas

from spark.common.utility.output_type import DataType, RunType
from spark.common.utility.run_recorder import RunRecorder
from spark.common.utility import logger


def run(spark, runner, date_input, test=False, airflow_test=False):
    script_path = __file__

    output_table_names_to_schemas = {
        'ambry_labtest_rnd3': labtest_schemas['schema_v8'],
    }

    driver = MarketplaceDriver(
        "ambry_genetics",
        "ambry",
        source_table_schemas,
        output_table_names_to_schemas,
        date_input,
        airflow_test
    )

    driver.init_spark_context()

    if not test:
        external_table_loader.load_ref_gen_ref(runner.sqlContext)

    driver.run()


def main(args):
    spark, sqlContext = init('AmbryGenetics')

    runner = Runner(sqlContext)

    run(spark, runner, args.date, airflow_test=args.airflow_test)

    spark.stop()


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--date', type=str)
    parser.add_argument('--airflow_test', default=False, action='store_true')
    args = parser.parse_args()
    main(args)

