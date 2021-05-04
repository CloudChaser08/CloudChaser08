import argparse
from spark.helpers import external_table_loader
from spark.runner import Runner
from spark.spark_setup import init


def run(spark, runner, input_file_path, output_location, partitions, test=False, airflow_test=False):
    # Load current warehouse table into dataframe
    nppes_warehouse = external_table_loader._get_table_as_df(runner.sqlContext, 'default', 'ref_nppes')

    # get current table schema
    nppes_schema = nppes_warehouse.schema

    # Load monthly replacement file into dataframe with schema
    df = runner.sqlContext.read.csv(input_file_path, schema=nppes_schema)
    df = df.filter(df['npi'] != 'NPI')  # ignore file header

    # Get rows with npi that are not in monthly replacement file
    warehouse_diff = nppes_warehouse.join(df, ["npi"], "leftanti")

    # Add missing npi rows to new dataset
    nppes_total = df.union(warehouse_diff)

    # write parquet files to s3 location
    nppes_total.repartition(partitions).write.parquet(output_location)


def main(args):
    # Initialize Spark
    spark, sql_context = init("NPPES")

    # Initialize the Spark Runner
    runner = Runner(sql_context)

    # Run the spark routine
    run(spark, runner, input_file_path=args.nppes_csv_path,
        output_location=args.s3_parquet_loc, airflow_test=args.airflow_test,
        partitions=args.partitions)

    # Tell spark to shutdown
    spark.stop()


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--nppes_csv_path', type=str)
    parser.add_argument('--s3_parquet_loc', type=str)
    parser.add_argument('--airflow_test', default=False, action='store_true')
    parser.add_argument('--partitions', default=20, type=int)
    args = parser.parse_known_args()[0]
    main(args)
