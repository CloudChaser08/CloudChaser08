
import argparse
import csv
import datetime
import os

import spark.helpers.file_utils as file_utils
import spark.helpers.postprocessor as postprocessor
from spark.spark_setup import init

# This is the normalization script for the NCPDP reference data. 
# Right now, only the `Provider Information` (mas.txt) and `Provider Information Transactions` (trn.txt) files are being parsed.
# These contain data on pharmacy and non-pharmacy dispensing site records with geographic and licensing information.
# More information can be found in "s3://salusv/sample/ncpdp/NCPDP dataQ Implementation Guide v3.1.pdf"

INCOMING_DATEFORMAT = "%Y%m%d"

# We can't use a generalized fixed width file parser because the first and last rows have different content.
def ncpdp_fixed_width_to_parquet(spark, input_filename, output_path, parse_path):
    """
    Converts a fixed width file found in `input_filename` to a parquet file in `output_path` using the parsing params found in `parse_path`.
    Also deletes the first and last rows which are special to NCPDP and are not standard.
    """
    os.makedirs(output_path, exist_ok=True)

    df = spark.read.text(f"{input_filename}.txt")
    
    with open(file_utils.get_abs_path(__file__, f"{parse_path}.csv"), encoding="utf-8") as parse_file:
        parsing_data = csv.reader(parse_file, delimiter='|')

        # have to skip the header
        next(parsing_data)

        cleaned = [(row[0].strip(), int(row[1]), int(row[2]), row[3].strip()) for row in parsing_data]
        df = postprocessor.parse_fixed_width_columns(df=df, columns=cleaned)

    path_items = os.path.split(parse_path) 

    internal_table_name = f"ref_ncpdp_{path_items[-2]}_{path_items[-1]}"
    df.createOrReplaceTempView(internal_table_name)

    # top and bottom row are different data
    # but have pseudo id of 9999999
    # "9999999M010901202081801 Copyright 2020 National Council for Prescription Drug Programs, All Rights Reserved "
    # Where "09012020" is the date and 81801 is the row count
    sql = f"select * from {internal_table_name} where ncpdp_prov_id != '9999999'"

    res_df = spark.sql(sql)

    # The output paths are templated by date, so if it runs again for the same date, it should overwrite the old version for that date. 
    res_df.repartition(1).write.parquet(output_path, mode='overwrite')


def run(spark, input_path, output_path, date_in):
    date = datetime.datetime.strptime(date_in, '%Y-%m-%d').date()
    date_format = date.strftime(INCOMING_DATEFORMAT)

    tables = {
        "master": ["mas"],
        "transactions": ["trn"]
    }

    for table_type, tables in tables.items():
        for table in tables:
            parse_path = f"{table_type}/{table}"
            input_filename = os.path.join(input_path, parse_path).format(date=date_format)
            output_location = os.path.join(output_path, parse_path).format(date=date_format)
            ncpdp_fixed_width_to_parquet(spark=spark, input_filename=input_filename, output_path=output_location, parse_path=parse_path)

  
def main(args):
    spark, sqlContext = init('Reference NCPDP')
    
    input_path = 's3://salusv/incoming/reference/ncpdp/{date}/'
    output_path = 's3://salusv/reference/parquet/ncpdp/{date}/'

    run(spark, input_path=input_path, output_path=output_path, date_in=args.date)

    spark.stop()


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--date', type=str)
    args = parser.parse_args()
    main(args)
