import argparse
import inspect

import spark.spark_setup as spark_setup
import spark.helpers.file_utils as file_utils
import spark.stats.config.reader.config_reader as config_reader
import spark.stats.stats_writer as stats_writer
import spark.stats.processor as processor

ALL_STATS = [
    'key_stats', 'longitudinality', 'year_over_year', 'fill_rates', 'top_values', 'epi'
]

def run(spark, sqlContext, quarter, start_date, end_date, provider_config, stats_to_calculate=ALL_STATS):

    # Calculate epi calcs
    epi_calcs = processor.get_epi_calcs(provider_config) if 'epi' in stats_to_calculate else {}

    if provider_config['datatype'] == 'emr':
        marketplace_stats = dict([
            (model_conf['datatype'], processor.run_marketplace_stats(
                spark, sqlContext, quarter, start_date, end_date,
                dict([it for it in provider_config.items() + model_conf.items()]),
                stats_to_calculate
            )) for model_conf in provider_config['models']
        ])

        for model_conf in provider_config['models']:
            stats_writer.write_to_s3(
                marketplace_stats[model_conf['datatype']],
                dict([it for it in provider_config.items() + model_conf.items()]),
                quarter
            )
        stats_writer.write_to_s3(epi_calcs, provider_config, quarter)

        stats = dict(marketplace_stats, **epi_calcs)

    else:
        # Calculate marketplace stats
        marketplace_stats = processor.run_marketplace_stats(
            spark, sqlContext, quarter, start_date, end_date, provider_config, stats_to_calculate
        )

        stats = dict(marketplace_stats, **epi_calcs)

        stats_writer.write_to_s3(stats, provider_config, quarter)

    return stats


def main(args):
    # Parse out the cli args
    feed_id = args.feed_id
    quarter = args.quarter
    start_date = args.start_date
    end_date = args.end_date
    stats = args.stats

    if not stats:
        stats = ALL_STATS

    # Get the providers config
    this_file = inspect.getframeinfo(inspect.stack()[1][0]).filename
    config_file = file_utils.get_abs_path(this_file, 'config/providers.json')
    provider_conf = config_reader.get_provider_config(config_file, feed_id)

    # set up spark
    spark, sqlContext = spark_setup \
                        .init('Feed {} marketplace stats'.format(feed_id))

    # Calculate stats
    run(spark, sqlContext, quarter, start_date, end_date, provider_conf, stats)

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--feed_id', type = str)
    parser.add_argument('--quarter', type = str)
    parser.add_argument('--start_date', type = str)
    parser.add_argument('--end_date', type = str)
    parser.add_argument('--stats', nargs = '+', default = None)
    args = parser.parse_args()
    main(args)
