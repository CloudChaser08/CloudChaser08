from spark.runner import Runner
from spark.spark_setup import init
from nppes_schema import *


def run(spark, runner, date_input, num_output_files, test=False, airflow_test=False):
    file_path = args.nppes_csv_path
    S3_PARQUET_LOCATION = args.s3_parquet_loc

    # Load monthly replacement file into dataframe with schema
    df = runner.sqlContext.read.csv(file_path, header=True, schema=nppes_schema)

    # write parquet files to s3 location
    df.repartition(num_output_files).write.mode('overwrite').parquet(S3_PARQUET_LOCATION)


def main(args):
    # Initialize Spark
    spark, sqlContext = init("NPPES")

    # Initialize the Spark Runner
    runner = Runner(sqlContext)

    # Run the spark routine
    run(spark, runner, args.date, airflow_test=args.airflow_test,
        num_output_files=args.num_output_files)

    # Tell spark to shutdown
    spark.stop()


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--s3_parquet_loc', type=str)
    parser.add_argument('--airflow_test', default=False, action='store_true')
    parser.add_argument('--num_output_files', default=20, type=int)
    args = parser.parse_args()
    main(args)
