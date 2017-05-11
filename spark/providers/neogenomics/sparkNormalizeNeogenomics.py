import argparse
import time
from datetime import datetime
from pyspark.sql.functions import monotonically_increasing_id
from spark.runner import Runner
from spark.spark_setup import init
import spark.helpers.file_utils as file_utils
import spark.helpers.payload_loader as payload_loader
import spark.helpers.normalized_records_unloader as normalized_records_unloader
import spark.providers.neogenomics.udf as neo_udf

TODAY = time.strftime('%Y-%m-%d', time.localtime())
output_path = 's3://salusv/warehouse/parquet/labtests/2017-05-10/'


def run(spark, runner, date_input, test=False):

    runner.sqlContext.registerFunction(
        'clean_neogenomics_diag_list', neo_udf.clean_neogenomics_diag_list
    )

    date_obj = datetime.strptime(date_input, '%Y-%m-%d')

    setid = 'TestMeta_' + date_obj.strftime('%Y%m%d') + '.dat'

    script_path = __file__

    if test:
        input_path = file_utils.get_rel_path(
            script_path, '../../test/providers/neogenomics/resources/input/'
        )
        matching_path = file_utils.get_rel_path(
            script_path, '../../test/providers/neogenomics/resources/matching/'
        )
    else:
        input_path = 's3a://salusv/incoming/labtests/neogenomics/{}/'.format(
            date_input.replace('-', '/')
        )
        matching_path = 's3a://salusv/matching/payload/labtests/neogenomics/{}/'.format(
            date_input.replace('-', '/')
        )

    min_date = '2010-01-01'
    max_date = date_input

    # create helper tables
    runner.run_spark_script(file_utils.get_rel_path(
        script_path,
        'create_helper_tables.sql'
    ))

    runner.run_spark_script(file_utils.get_rel_path(
        script_path,
        '../../common/lab_common_model_v3.sql'
    ), [
        ['table_name', 'lab_common_model', False],
        ['properties', '', False]
    ])

    payload_loader.load(runner, matching_path, ['personId'])

    runner.run_spark_script(
        file_utils.get_rel_path(
            script_path, 'load_transactions.sql'
        ), [
            ['input_path', input_path]
        ]
    )

    runner.run_spark_script(file_utils.get_rel_path(
        script_path, 'normalize.sql'
    ), [
        ['filename', setid],
        ['today', TODAY],
        ['feedname', '32'],
        ['vendor', '78'],
        ['min_date', min_date],
        ['max_date', max_date]
    ])

    # add in primary key
    runner.sqlContext.sql('select * from lab_common_model').withColumn(
        'record_id', monotonically_increasing_id()
    ).createTempView(
        'lab_common_model'
    )

    if not test:
        normalized_records_unloader.partition_and_rename(
            spark, runner, 'lab', 'lab_common_model_v3.sql', 'neogenomics',
            'lab_common_model', 'date_service', date_input
        )


def main(args):
    # init
    spark, sqlContext = init("Neogenomics")

    # initialize runner
    runner = Runner(sqlContext)

    run(spark, runner, args.date)

    spark.stop()

    normalized_records_unloader.distcp(output_path)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('--date', type=str)
    args = parser.parse_args()
    main(args)
