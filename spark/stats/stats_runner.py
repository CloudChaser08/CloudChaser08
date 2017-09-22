import argparse

import spark.spark_setup as spark_setup

import spark.stats.config.reader.config_reader as config_reader

def main(args):
    provider_name = args.provider_name

    # Get the config for the provider
    provider_conf = config_reader.get_provider_config(provider_name, 'config/providers.json')

    print provider_conf


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--provider_name', type = str)
    parser.add_argument('--quarter', type = str)
    parser.add_argument('--start_date', type = str)
    parser.add_argument('--end_date', type = str)
    parser.add_argument('--earliest_date', type = str)
    args = parser.parse_args()
    main(args)
