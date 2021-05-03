import argparse

import spark.helpers.postprocessor as postprocessor
from pyspark.sql import Window
from pyspark.sql.functions import *
from spark.helpers import external_table_loader
from spark.runner import Runner
from spark.spark_setup import init


def extract_strings(dataframe):
    """
    Cut strings in list of whole strings according to schema and assign proper column names
    """
    new_df = dataframe.select(
        dataframe.value.substr(1, 5).alias('hcpc'),
        dataframe.value.substr(6, 4).alias('seqnum'),
        dataframe.value.substr(11, 1).alias('recid'),
        dataframe.value.substr(12, 79).alias('long_description'),
        dataframe.value.substr(92, 28).alias('short_description'),
        dataframe.value.substr(120, 2).alias('price1'),
        dataframe.value.substr(122, 2).alias('price2'),
        dataframe.value.substr(124, 2).alias('price3'),
        dataframe.value.substr(126, 2).alias('price4'),
        dataframe.value.substr(128, 1).alias('multi_pi'),
        dataframe.value.substr(129, 6).alias('cim1'),
        dataframe.value.substr(135, 6).alias('cim2'),
        dataframe.value.substr(141, 6).alias('cim3'),
        dataframe.value.substr(147, 8).alias('mcm1'),
        dataframe.value.substr(155, 8).alias('mcm2'),
        dataframe.value.substr(163, 8).alias('mcm3'),
        dataframe.value.substr(171, 10).alias('statute'),
        dataframe.value.substr(181, 3).alias('labcert1'),
        dataframe.value.substr(184, 3).alias('labcert2'),
        dataframe.value.substr(187, 3).alias('labcert3'),
        dataframe.value.substr(190, 3).alias('labcert4'),
        dataframe.value.substr(193, 3).alias('labcert5'),
        dataframe.value.substr(196, 3).alias('labcert6'),
        dataframe.value.substr(199, 3).alias('labcert7'),
        dataframe.value.substr(202, 3).alias('labcert8'),
        dataframe.value.substr(205, 5).alias('xref1'),
        dataframe.value.substr(210, 5).alias('xref2'),
        dataframe.value.substr(215, 5).alias('xref3'),
        dataframe.value.substr(220, 5).alias('xref4'),
        dataframe.value.substr(225, 5).alias('xref5'),
        dataframe.value.substr(230, 1).alias('cov'),
        dataframe.value.substr(231, 2).alias('asc_grp'),
        dataframe.value.substr(233, 8).alias('asc_dt'),
        dataframe.value.substr(241, 3).alias('opps'),
        dataframe.value.substr(244, 1).alias('opps_pi'),
        dataframe.value.substr(245, 8).alias('opps_dt'),
        dataframe.value.substr(253, 4).alias('procnote'),
        dataframe.value.substr(257, 3).alias('betos'),
        dataframe.value.substr(261, 1).alias('tos1'),
        dataframe.value.substr(262, 1).alias('tos2'),
        dataframe.value.substr(263, 1).alias('tos3'),
        dataframe.value.substr(264, 1).alias('tos4'),
        dataframe.value.substr(265, 1).alias('tos5'),
        dataframe.value.substr(266, 3).alias('anest_bu'),
        dataframe.value.substr(269, 8).alias('add_dt'),
        dataframe.value.substr(277, 8).alias('act_eff_dt'),
        dataframe.value.substr(285, 8).alias('term_dt'),
        dataframe.value.substr(293, 1).alias('action_cd')

    )

    return new_df


def trim_data(dataframe):
    return (postprocessor.compose(
        postprocessor.trimmify,
        postprocessor.nullify
    )(dataframe))


def combine_new_and_old_tables(hcpcs, current_hcpcs_codes_table):
    """
    We take row data, break it by columns and then append data from sql table
    where codes exists in old table but not a new one.
    """
    # We only need lines 320 charater long. There's some comments in the document.
    row_data = hcpcs.where(length(col("value")) == 320)

    trimmed_df = trim_data(extract_strings(row_data))

    trimmed_current_table = trim_data(current_hcpcs_codes_table)

    missing_hcpcs_codes = trimmed_current_table.join(trimmed_df, trimmed_current_table.hcpc == trimmed_df.hcpc,
                                                     'leftanti')

    return trimmed_df.union(missing_hcpcs_codes)


def concatenate_multiline_descriptions(all_hcpcs_codes):
    """
    First we combine all descriptions in the list, so max seqnum will have maximum of
    descriptions combines (full description).  See this example
    https://stackoverflow.com/questions/45131481/aggregation-function-collect-list-or-collect-set-over-window
    Then we filter only rows with max seqnum dropping other rows
    And finally convert arrays to strings, returning just codes, max seqnums and descriptions
    """
    collected_lists = all_hcpcs_codes.withColumn('long_description_list', collect_list('long_description').over(
        Window.partitionBy('hcpc').orderBy('seqnum'))).select('hcpc', 'seqnum', 'long_description',
                                                              'long_description_list')

    max_aggr = collected_lists.groupBy('hcpc').agg(max('seqnum').alias('seqnum'))

    combined_descriptions = collected_lists.join(max_aggr, ['hcpc', 'seqnum'])

    long_descs = combined_descriptions.withColumn('long_description', concat_ws(' ', 'long_description_list'))

    return long_descs.select(['hcpc', 'seqnum', 'long_description'])


def get_first_rows(all_hcpcs_codes):
    """
    :param all_hcpcs_codes: All hcpcs codes from two combined tables
    :return: All rows where seqnum is minimal within that set of rows.
    We cut out long descriptions and seqnum since we are getting those from other dataset
    """
    min_seqnums = all_hcpcs_codes.groupBy('hcpc').agg(min('seqnum').alias('seqnum'))

    first_rows = all_hcpcs_codes.join(min_seqnums, ['hcpc', 'seqnum'])

    return first_rows.drop('long_description').drop('seqnum')


def run(spark, runner, args):
    """
    Schema source: https://www.cms.gov/Medicare/Coding/HCPCSReleaseCodeSets/Alpha-Numeric-HCPCS-Items/2018-HCPCS-Record-Layout-.html
    File: https://www.cms.gov/Medicare/Coding/HCPCSReleaseCodeSets/Alpha-Numeric-HCPCS-Items/2018-Alpha-Numeric-HCPCS-File-.html?DLPage=1&DLEntries=10&DLSort=0&DLSortDir=descending
    """
    hcpcs = spark.read.text(args.incoming)

    external_table_loader.load_analytics_db_table(runner.sqlContext, "default", "ref_hcpcs", "ref_hcpcs")

    current_hcpcs_codes_table = spark.table('ref_hcpcs')

    all_hcpcs_codes = combine_new_and_old_tables(hcpcs, current_hcpcs_codes_table)

    just_descriptions = concatenate_multiline_descriptions(all_hcpcs_codes)

    first_rows_no_desc = get_first_rows(all_hcpcs_codes)

    full_dataset = first_rows_no_desc.join(just_descriptions, ['hcpc'])

    full_dataset.repartition(args.partitions).write.parquet(args.s3_parquet_loc, mode='overwrite')


def main(args):
    spark, sql_context = init('Reference HCPCS')
    runner = Runner(sql_context)
    run(spark, runner, args)

    spark.stop()


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--incoming', type=str)
    parser.add_argument('--s3_parquet_loc', type=str)
    parser.add_argument('--partitions', default=20, type=int)
    args = parser.parse_known_args()
    main(args)
