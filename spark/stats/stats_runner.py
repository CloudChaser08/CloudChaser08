import argparse
import os
import logging
import inspect

import spark.spark_setup as spark_setup

import spark.stats.processor as processor

def run(spark, sqlContext, provider, quarter, start_date, end_date, provider_config, output_dir):
    # Calculate marketplace stats
    marketplace_stats = processor.run_marketplace_stats(
            spark, sqlContext, quarter, start_date,
            end_date, provider_config
        )

    # Calculate epi calcs
    epi_calcs = processor.get_epi_calcs(provider_config)

    stats = dict(marketplace_stats, **epi_calcs)

    # Write out results
    feed_id = provider_config['datafeed_id']
    for key, stat in stats.items():
        if stat:
            with open(output_dir + '/' + feed_id + '_' + key + '.csv', 'w') as f:
                # Write out the header
                cols = [str(c) for c in stat[0].keys()]
                f.write(','.join(cols) + '\n')
                for row in stat:
                    # Write out each row
                    f.write(','.join([str(row[c]) for c in cols]) + '\n')


def main(args):
    # Parse out the cli args
    feed_id = args.feed_id
    quarter = args.quarter
    start_date = args.start_date
    end_date = args.end_date
    output_dir = args.output_dir

    # Get the providers config
    this_file = inspect.getmodule(inspect.stack()[1][0]).__file__
    config_file = file_utils.get_abs_path(this_file, 'config/providers.json')
    provider_conf = config_reader.get_provider_config(
                                    config_file, feed_id)

    # Create output directory
    output_dir = output_dir[:-1] if output_dir.endswith('/') else output_dir
    try:
        os.makedirs(output_dir)
    except OSError:
        logging.warn("Output dir already exists")

    # set up spark
    spark, sqlContext = spark_setup \
                        .init('Feed {} marketplace stats'.format(feed_id))

    # Calculate stats
    run(spark, sqlContext, provider, quarter, start_date, end_date, provider_config)


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--feed_id', type = str)
    parser.add_argument('--quarter', type = str)
    parser.add_argument('--start_date', type = str)
    parser.add_argument('--end_date', type = str)
    parser.add_argument('--output_dir', type = str)
    args = parser.parse_args()
    main(args)

