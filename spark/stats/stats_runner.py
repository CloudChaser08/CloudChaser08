import argparse

import spark.spark_setup as spark_setup

import spark.stats.config.reader.config_reader as config_reader
import spark.stats.processor.processor as processor

def run(spark, sqlContext, provider_name, quarter, start_date, \
        end_date, earliest_date, get_data_func, get_provider_conf_func, \
        output_dir):

    all_stats_df = processor.run_marketplace_stats(spark, sqlContext, \
                provider_name, quarter, start_date, end_date, earliest_date, \
                get_data_func, get_provider_conf_func)

    for key, df in all_stats_df.items():
        if df:
            df.coalesce(1).write.option('header', 'true').csv(output_dir + provider_name + '_' + quarter + '_' + key + '.csv')

    return all_stats_df


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

    # Set up function arguments
    get_data_func = processor._get_all_data
    get_provider_conf_func = lambda x: config_reader.get_provider_config( \
                                    x, config_dir, 'providers.json')

    # Calculate marketplace stats
    all_stats_df = run(spark, sqlContext, provider_name, quarter, \
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

