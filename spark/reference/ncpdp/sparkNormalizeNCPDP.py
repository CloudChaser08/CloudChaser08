
import argparse
import csv
import datetime
import os

import spark.helpers.file_utils as file_utils
import spark.helpers.postprocessor as postprocessor
from spark.spark_setup import init

# This is the normalization script for the NCPDP reference data. 
# Right now, only the `Provider Information` (mas.txt)
# and `Provider Information Transactions` (trn.txt) files are being parsed.
# These contain data on pharmacy and non-pharmacy dispensing site records with geographic and licensing information.
# More information can be found in "s3://salusv/sample/ncpdp/NCPDP dataQ Implementation Guide v3.1.pdf"

# create temp master
# insert added (A), changed (C), deleted (D) transaction records into temp master
# insert old master excluding all entries from transaction records into temp master
# delete old master
# export new master


INCOMING_DATEFORMAT = "%Y%m%d"


# We can't use a generalized fixed width file parser because the first and last rows have different content.
def ncpdp_fixed_width_to_parquet(spark, input_path, output_path, transaction_table, master_table):
    """
    Converts a fixed width file found in `input_filename` to a parquet file in `output_path` using the parsing
    params found in `parse_path`. Also deletes the first and last rows which are special to NCPDP and are not standard.
    """
    
    master_output = os.path.join(output_path, "master", master_table)
    transaction_output = os.path.join(output_path, "transactions", transaction_table)

    # prepare the output paths 
    for path in [master_output, transaction_output]:
        os.makedirs(path, exist_ok=True)

    transaction_input = os.path.join(input_path, "transactions", transaction_table + ".txt")
    df = spark.read.text(transaction_input)
    
    transaction_parse_file = file_utils.get_abs_path(__file__, os.path.join("transactions", transaction_table + ".csv"))
    
    with open(transaction_parse_file, encoding="utf-8") as parse_file:
        parsing_data = csv.reader(parse_file, delimiter='|')

        # have to skip the header
        next(parsing_data)

        cleaned = [(row[0].strip(), int(row[1]), int(row[2]), row[3].strip()) for row in parsing_data]
        df = postprocessor.parse_fixed_width_columns(df=df, columns=cleaned)

    internal_table_name = "ref_ncpdp_transactions_{table}".format(table=transaction_table)
    df.createOrReplaceTempView(internal_table_name)

    # top and bottom row are different data
    # but have pseudo id of 9999999
    # "9999999M010901202081801 Copyright 2020 National Council for Prescription Drug Programs, All Rights Reserved "
    # Where "09012020" is the date and 81801 is the row count
    sql = "select * from {internal_table_name} where ncpdp_prov_id != '9999999'".format(
        internal_table_name=internal_table_name)
    res_df = spark.sql(sql)

    # The output paths are templated by date, so if it runs again for the same date,
    # it should overwrite the old version for that date.
    res_df.repartition(1).write.parquet(transaction_output, mode='overwrite')

    # drop the columns only relevant to the transaction table
    columns_to_drop = ['transaction_cd', 'transaction_dt']
    master_df = res_df.drop(*columns_to_drop)

    # create the temporary new master table
    temp_master_name = "ref_ncpdp_master_{table}_temp".format(table=master_table)
    master_df.createOrReplaceTempView(temp_master_name)

    # select things in the master that aren't already in the transaction 
    # union with transaction data
    old_master_sql = """
        select mas.* from ref_ncpdp_master_{table} as mas 
        left join {temp_master_name} as trn on trn.ncpdp_prov_id=mas.ncpdp_prov_id 
        where trn.ncpdp_prov_id is NULL
        UNION
        select * from {temp_master_name}
    """.format(table=master_table, temp_master_name=temp_master_name)

    final_df = spark.sql(old_master_sql)

    # now add those items into the master output
    final_df.repartition(1).write.parquet(master_output, mode='overwrite')


def run(spark, input_path, output_path, date, date_prev, prev_master_path=None):
    
    date_format = date.strftime(INCOMING_DATEFORMAT)
    prev_date_format = date_prev.strftime(INCOMING_DATEFORMAT)

    master_tables = [
        "mas"
    ]
    if prev_master_path is None:
        prev_master_path = output_path
     
    # load all the old master tables. should be in the same location as the output
    for table in master_tables:
        old_master_path = os.path.join(prev_master_path, "master/{table}/").format(date=prev_date_format, table=table)
        df = spark.read.format("parquet").load(old_master_path)
        df.createOrReplaceTempView("ref_ncpdp_master_{table}".format(table=table))

    transaction_tables = [
        ("trn", "mas")
    ]

    # process all the transaction files and their corresponding master files
    for transaction_table, master_table in transaction_tables:
        input_path_with_date = input_path.format(date=date_format) 
        output_path_with_date = output_path.format(date=date_format)

        ncpdp_fixed_width_to_parquet(
            spark=spark, 
            input_path=input_path_with_date, 
            output_path=output_path_with_date, 
            transaction_table=transaction_table, 
            master_table=master_table
        )


def get_last_month(date):
    # get last month, fixed day since timedelta doesn't do months
    year, month, day = date.timetuple()[:3]
    new_month = ((month - 1) % 12) or 12
    date_prev = datetime.date(year - (new_month // 12), new_month, 1)
    
    return date_prev


def main(args):
    spark, sql_context = init('Reference NCPDP')
    
    input_path = 's3://salusv/incoming/reference/ncpdp/{date}/'
    output_path = 's3://salusv/reference/parquet/ncpdp/{date}/'
    
    date = datetime.datetime.strptime(args.date, '%Y-%m-%d').date()
    date_prev = get_last_month(date=date)

    run(spark, input_path=input_path, output_path=output_path, date=date, date_prev=date_prev)

    spark.stop()


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--date', type=str)
    args = parser.parse_known_args()[0]
    main(args)
