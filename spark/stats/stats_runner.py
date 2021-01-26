import argparse
# import inspect
import json
import os

import boto3

import spark.spark_setup as spark_setup
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

S3_OUTPUT_BUCKET = "healthveritydev"
S3_OUTPUT_KEY = "marketplace_stats/json/{}/{}/{}"


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
        key_stats=processor.run_key_stats(provider_config, start_date, end_date, df_provider),
        top_values=processor.run_top_values(provider_config, df_provider),
        fill_rate=processor.run_fill_rates(provider_config, df_provider),
        longitudinality=processor.run_longitudinality(enc_prov_config, end_date, enc_df_provider),
        year_over_year=processor.run_year_over_year(enc_prov_config, end_date, enc_df_provider)
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


def write_summary_file_to_s3(stats, version):
    """
    Upload summary json file to s3
    """
    summary_json = stats.to_dict()
    summary_json['version'] = version

    datafeed_id = stats.config.datafeed_id
    filename = '{}_stats_summary_{}.json'.format(datafeed_id, version)
    output_file = 'output/{}/{}/{}'.format(datafeed_id, version, filename)

    try:
        os.makedirs(os.path.dirname(output_file))
    except OSError:
        # version_name directory already exists, which isn't a problem
        pass

    with open(output_file, 'w+') as summary:
        summary.write(json.dumps(summary_json))
    boto3.client('s3').upload_file(
        output_file,
        S3_OUTPUT_BUCKET,
        S3_OUTPUT_KEY.format(datafeed_id, version, filename)
    )


def main(args):
    # Parse out the cli args
    feed_ids = args.feed_ids
    version = args.version
    start_date = args.start_date
    end_date = args.end_date
    stats = set(args.stats or ALL_STATS)

    for feed_id in feed_ids:

        # set up spark
        spark, sql_context = spark_setup.init(
            'Feed {} marketplace stats'.format(feed_id)
        )

        provider_conf = config_reader.get_provider_config(sql_context, feed_id)

        # Calculate stats
        stats = run(spark, sql_context, start_date, end_date, provider_conf)

        # generate and write json summary to s3
        write_summary_file_to_s3(stats, version)

        # Generate SQL for new data_layout version.
        generate_data_layout_version_sql(stats, version)


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--feed_ids', type=str, nargs='+')
    parser.add_argument('--version', type=str)
    parser.add_argument('--start_date', type=str)
    parser.add_argument('--end_date', type=str)
    parser.add_argument('--stats', nargs='+', default=None)
    args = parser.parse_args()
    main(args)
