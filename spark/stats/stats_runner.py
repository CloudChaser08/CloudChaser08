import argparse
import os

import spark.spark_setup as spark_setup

import spark.stats.config.reader.config_reader as config_reader
import spark.helpers.stats.utils as stat_utils
import spark.stats.processor as processor

def run(spark, sqlContext, provider_name, quarter, start_date, \
        end_date, earliest_date, output_dir):

    all_stats = processor.run_marketplace_stats(spark, sqlContext, \
                provider_name, quarter, start_date, end_date, earliest_date)

    os.makedirs(output_dir)
    for key, stat in all_stats.items():
        if stat:
            with open(output_dir + provider_name + '_' + key + '.csv', 'w') as f:
                for row in stat:
                    for col, value in row.asDict().items():
                        f.write(col + ',' + str(value) + '\n')

    return all_stats


def main(args):
    # Parse out the cli args
    provider_name = args.provider_name
    quarter = args.quarter
    start_date = args.start_date
    end_date = args.end_date
    earliest_date = args.earliest_date
    output_dir = args.output_dir

    # Get the directory of provider configs
    config_dir = '/'.join(__file__.split('/')[:-1]) + '/config/'

    # set up spark
    spark, sqlContext = spark_setup \
                        .init('{} marketplace stats'.format(provider_name))

    # Calculate marketplace stats
    all_stats = run(spark, sqlContext, provider_name, quarter, \
                    start_date, end_date, earliest_date, config_dir)


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--provider_name', type = str)
    parser.add_argument('--quarter', type = str)
    parser.add_argument('--start_date', type = str)
    parser.add_argument('--end_date', type = str)
    parser.add_argument('--earliest_date', type = str)
    parser.add_argument('--output_dir', type = str)
    args = parser.parse_args()
    main(args)

