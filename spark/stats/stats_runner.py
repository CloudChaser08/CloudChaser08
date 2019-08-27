import argparse
import inspect

import spark.spark_setup as spark_setup
import spark.helpers.file_utils as file_utils
import spark.stats.config.reader.config_reader as config_reader
import spark.stats.processor as processor
from spark.stats.data_layout import generate_data_layout_version_sql

from spark.stats.models.results import (
    ProviderStatsResult,
    StatsResult
)

ALL_STATS = {
    'key_stats', 'longitudinality', 'year_over_year', 'fill_rate', 'top_values', 'epi'
}
EMR_ENCOUNTER_STATS = {'longitudinality', 'year_over_year'}


def _get_encounter_model_config(provider_config):
    """ Merges the provider config with the emr encounter model if available,
        otherwise returns None
    """
    for model in provider_config.models:
        if model.datatype == 'emr_enc':
            return provider_config.merge_provider_model(model)
    return None


def run(spark, sql_context, start_date, end_date, provider_config):
    """ Runs all stats for a provider between the start and end dates,
        returning a result dictionary
    """
    df_provider = processor.DataframeProvider(
        spark=spark,
        sql_context=sql_context,
        provider_conf=provider_config,
        start_date=start_date,
        end_date=end_date
    )

    # If there's an encounter model available, use that to calculate
    # longitudinality and year over year, otherwise use the provider config
    enc_prov_config = provider_config
    enc_df_provider = df_provider
    for model in provider_config.models:
        if model.datatype == 'emr_enc':
            enc_prov_config = provider_config.merge_provider_model(model)
            enc_df_provider = df_provider.reconfigure(enc_prov_config)
            break

    # Run common stats
    results = StatsResult(
        epi_calcs=processor.get_epi_calcs(provider_config),
        key_stats=processor.run_key_stats(
            provider_config, start_date, end_date, df_provider
        ),
        top_values=processor.run_top_values(
            provider_config, df_provider
        ),
        fill_rate=processor.run_fill_rates(
            provider_config, df_provider
        ),
        longitudinality=processor.run_longitudinality(
            enc_prov_config, end_date, enc_df_provider # use encounter config/dataframe
        ),
        year_over_year=processor.run_year_over_year(
            enc_prov_config, end_date, enc_df_provider  # use encounter config/dataframe
        )
    )

    # Calculate fill rate and top values for nested models
    model_results = {}
    for model in provider_config.models:
        model_prov_config = provider_config.merge_provider_model(model)
        model_df_provider = df_provider.reconfigure(model_prov_config)
        model_results[model.datatype] = StatsResult(
            fill_rate=processor.run_fill_rates(model_prov_config, model_df_provider),
            top_values=processor.run_top_values(model_prov_config, model_df_provider)
        )

    return ProviderStatsResult(
        results=results,
        model_results=model_results,
        config=provider_config
    )


def main(args):
    # Parse out the cli args
    feed_ids = args.feed_ids
    quarter = args.quarter
    start_date = args.start_date
    end_date = args.end_date
    stats = set(args.stats or ALL_STATS)

    for feed_id in feed_ids:
        # Get the providers config
        this_file = inspect.getframeinfo(inspect.stack()[1][0]).filename
        config_file = file_utils.get_abs_path(this_file, 'config/providers.json')

        # set up spark
        spark, sql_context = spark_setup.init(
            'Feed {} marketplace stats'.format(feed_id)
        )

        provider_conf = config_reader.get_provider_config(
            sql_context, config_file, feed_id
        )

        # Calculate stats
        stats = run(spark, sql_context, start_date, end_date, provider_conf)

        # Generate SQL for new data_layout version.
        # 'quarter' is used as the version name
        generate_data_layout_version_sql(stats, quarter)


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--feed_ids', type = str, nargs='+')
    parser.add_argument('--quarter', type = str)
    parser.add_argument('--start_date', type = str)
    parser.add_argument('--end_date', type = str)
    parser.add_argument('--stats', nargs = '+', default = None)
    args = parser.parse_args()
    main(args)
