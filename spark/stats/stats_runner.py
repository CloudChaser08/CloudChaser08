import argparse
import inspect

import spark.spark_setup as spark_setup
import spark.helpers.file_utils as file_utils
import spark.stats.config.reader.config_reader as config_reader
import spark.stats.stats_writer as stats_writer
import spark.stats.processor as processor

ALL_STATS = [
    'key_stats', 'longitudinality', 'year_over_year', 'fill_rate', 'top_values', 'epi'
]
EMR_ENCOUNTER_STATS = ['longitudinality', 'year_over_year']

def run(spark, sqlContext, quarter, start_date, end_date, provider_config, stats_to_calculate=ALL_STATS):

    epi_calcs = processor.get_epi_calcs(provider_config) if 'epi' in stats_to_calculate else {}

    if provider_config['datatype'] == 'emr':
        if 'emr_enc' not in [m['datatype'] for m in provider_config['models']]:
            emr_encounter_stats = []
        else:
            emr_encounter_stats = EMR_ENCOUNTER_STATS

        union_level_stats = [stat for stat in stats_to_calculate if stat in provider_config and stat not in emr_encounter_stats]
        model_level_stats = [
            stat for stat in stats_to_calculate
            for model_conf in provider_config['models']
            if stat in model_conf
        ]
        encounter_level_stats = [
            stat for stat in stats_to_calculate if stat in provider_config and stat in emr_encounter_stats
        ]

        model_level_marketplace_stats = dict([
            (model_conf['datatype'], processor.run_marketplace_stats(
                spark, sqlContext, quarter, start_date, end_date,
                dict([it for it in provider_config.items() + model_conf.items()]),
                model_level_stats
            )) for model_conf in provider_config['models']
        ])
        union_level_marketplace_stats = processor.run_marketplace_stats(
            spark, sqlContext, quarter, start_date, end_date, provider_config, union_level_stats
        )
        encounter_level_marketplace_stats = processor.run_marketplace_stats(
            spark, sqlContext, quarter, start_date, end_date,
            dict([it for it in provider_config.items() + [m for m in provider_config['models'] if m['datatype'] == 'emr_enc'][0].items()]),
            encounter_level_stats
        ) if encounter_level_stats else {}

        for model_conf in provider_config['models']:
            stats_writer.write_to_s3(
                model_level_marketplace_stats[model_conf['datatype']],
                dict([it for it in provider_config.items() + model_conf.items()]),
                quarter
            )
        stats_writer.write_to_s3(dict(
            union_level_marketplace_stats, **dict(encounter_level_marketplace_stats, **epi_calcs)
        ), provider_config, quarter)

        stats = dict(
            model_level_marketplace_stats, **dict(union_level_marketplace_stats, **dict(encounter_level_marketplace_stats, **epi_calcs))
        )

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
    feed_ids = args.feed_ids.split()
    quarter = args.quarter
    start_date = args.start_date
    end_date = args.end_date
    stats = args.stats

    if not stats:
        stats = ALL_STATS

    for feed_id in feed_ids:
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
    parser.add_argument('--feed_ids', type = str, nargs='+')
    parser.add_argument('--quarter', type = str)
    parser.add_argument('--start_date', type = str)
    parser.add_argument('--end_date', type = str)
    parser.add_argument('--stats', nargs = '+', default = None)
    args = parser.parse_args()
    main(args)
