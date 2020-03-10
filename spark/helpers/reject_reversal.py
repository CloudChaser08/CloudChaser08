from datetime import datetime as dt
from pyspark.sql.types import StringType, StructType, StructField
import subprocess
import re


def create_empty_dataframe(spark,
                         part_provider_field,
                         date_partition_field,
                         output_schema,
                         table_name='previous_run_from_transformed'):

    # Create an empty dataframe that matches the expected output schema
    # This is useful for starting brand new providers who have reject and reversal logic.
    st = StructType([StructField(date_partition_field, StringType(), True)] +
                    [StructField(part_provider_field, StringType(), True)] +
                    output_schema.fields)
    spark.createDataFrame(spark.sparkContext.emptyRDD(), st).createOrReplaceTempView(table_name)


def load_previous_run_from_transformed(spark,
                                       date_input,
                                       output_path,
                                       part_provider_field,
                                       part_provider,
                                       date_partition_field,
                                       output_schema,
                                       table_name='previous_run_from_transformed'):

    full_output_path = output_path + part_provider_field + '=' + part_provider + '/'
    s3_list_command = ['aws', 's3', 'ls', full_output_path]

    def load_previous_run(file_date):
        df = spark.read.parquet(full_output_path + 'part_file_date=' + file_date + '/')
        df.createOrReplaceTempView(table_name)

    def get_previous_part_file_dates():
        # create a datetime from date_input
        vdr_file_date = dt.strptime(date_input, '%Y-%m-%d')

        # list the existing transformed location
        s3_existing_part_file_dates = subprocess.check_output(s3_list_command)

        # parse the dates from the existing part_file_dates
        existing_part_file_dates = [
            dt.strptime(re.sub('[^0-9-]+', '', row), "%Y-%m-%d") for row in
            s3_existing_part_file_dates.decode().split("\n")
            if 'part_file_date' in row
        ]

        # find all dates prior to current vdr_file_date
        file_dates = [date for date in existing_part_file_dates
                      if date < vdr_file_date]

        return file_dates

    previous_part_file_dates = get_previous_part_file_dates()
    if previous_part_file_dates:
        max_file_date = str(max(previous_part_file_dates).date())
        load_previous_run(max_file_date)
    else:
        create_empty_dataframe(
            spark,
            part_provider_field,
            date_partition_field,
            output_schema,
            table_name)
