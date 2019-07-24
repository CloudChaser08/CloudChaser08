import argparse
import copy
import inspect

import spark.spark_setup as spark_setup
import spark.helpers.file_utils as file_utils
import spark.stats.config.reader.config_reader as config_reader
import spark.stats.stats_writer as stats_writer
import spark.stats.processor as processor
from spark.stats.data_layout.sql_generator import generate_data_layout_version_sql


ALL_STATS = ['key_stats', 'longitudinality', 'year_over_year', 'fill_rate', 'top_values', 'epi']
EMR_ENCOUNTER_STATS = ['longitudinality', 'year_over_year']


def run(spark, sqlContext, quarter, start_date, end_date, provider_conf, stats_to_calculate=ALL_STATS):
    """ Run stats for the given provider """

    epi_calcs = {}
    if 'epi' in stats_to_calculate:
        epi_calcs = processor.get_epi_calcs(provider_conf)

    if provider_conf['datatype'] == 'emr':
        # Get the EMR encounter config
        # Only if an EMR encounter config is provided do we need the encounter stats
        emr_encounter_conf = None
        for model_conf in provider_conf['models']:
            if model_conf['datatype'] == 'emr_enc':
                emr_encounter_conf = copy.deepcopy(model_conf)
        emr_encounter_stats = EMR_ENCOUNTER_STATS if emr_encounter_conf else []

        # All stats for all models in provider_conf['models']
        model_conf_stats = set()
        for model_conf in provider_conf['models']:
            for model_conf_stat in model_conf:
                model_conf_stats.add(model_conf_stat)

        # Get the stats to run for each type
        encounter_level_stats = []
        union_level_stats = []
        model_level_stats = []
        for stat in stats_to_calculate:
            if stat in provider_conf:
                if stat in emr_encounter_stats:
                    encounter_level_stats.append(stat)
                else:
                    union_level_stats.append(stat)

            if stat in model_conf_stats:
                model_level_stats.append(stat)

        # Run stats at a per-model level
        model_level_marketplace_stats = {}
        for model_conf in provider_conf['models']:
            datatype_name = model_conf['datatype']

            # Conf for this model is the standard top-level conf updated with model_conf
            model_specific_conf = copy.deepcopy(provider_conf)
            model_specific_conf.update(model_conf)
            model_specific_stats = processor.run_marketplace_stats(
                spark, sqlContext, quarter, start_date, end_date,
                model_specific_conf, model_level_stats
            )

            model_level_marketplace_stats[datatype_name] = model_specific_stats
            stats_writer.write_to_s3(model_specific_stats, model_specific_conf, quarter)

        # Run stats at an encounter level
        encounter_level_marketplace_stats = {}
        if encounter_level_stats:
            encounter_specific_conf = copy.deepcopy(provider_conf)
            encounter_specific_conf.update(emr_encounter_conf)

            encounter_level_marketplace_stats = processor.run_marketplace_stats(
                spark, sqlContext, quarter, start_date, end_date,
                encounter_specific_conf, encounter_level_stats
            )

        # Run stats at a union level
        union_level_marketplace_stats = processor.run_marketplace_stats(
            spark, sqlContext, quarter, start_date, end_date, provider_conf, union_level_stats
        )
        union_level_marketplace_stats.update(encounter_level_marketplace_stats)
        union_level_marketplace_stats.update(epi_calcs)
        stats_writer.write_to_s3(union_level_marketplace_stats, provider_conf, quarter)

        # Create master dict of stats to return
        stats = copy.deepcopy(model_level_marketplace_stats)
        stats.update(union_level_marketplace_stats)
        stats.update(encounter_level_marketplace_stats)
        stats.update(epi_calcs)
    else:
        # Calculate marketplace stats
        stats = processor.run_marketplace_stats(
            spark, sqlContext, quarter, start_date, end_date, provider_conf, stats_to_calculate
        )
        stats.update(epi_calcs)
        stats_writer.write_to_s3(stats, provider_conf, quarter)

    # Generate SQL for data_layout
    generate_data_layout_version_sql(provider_conf, stats, 'FAKE VERSION NAME', quarter)

    return stats


def main(args):
    """ Main method for stats_runner.py """

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

    # Set up spark
    spark, sqlContext = spark_setup.init('Feed {} marketplace stats'.format(feed_id))

    # Calculate stats
    run(spark, sqlContext, quarter, start_date, end_date, provider_conf, stats)


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--feed_id', type=str)
    parser.add_argument('--quarter', type=str)
    parser.add_argument('--start_date', type=str)
    parser.add_argument('--end_date', type=str)
    parser.add_argument('--stats', nargs='+', default=None)
    main(parser.parse_args())
