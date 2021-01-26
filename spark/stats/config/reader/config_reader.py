"""
    Reads in Provider configuration that is required to run stats
"""
import json

import boto3

from spark.stats.models import Provider
from ...datamodel import get_table_metadata

_S3_CLIENT = boto3.client('s3')
_S3_BUCKET = 'healthveritydev'
_S3_KEY = 'marketplace_stats/config/providers.json'


def _load_providers_json_from_s3():
    return json.load(
        _S3_CLIENT.get_object(Bucket=_S3_BUCKET, Key=_S3_KEY)['Body']
    )


def _get_base_provider_config_for_feed(feed_id):
    """
        Gets the base provider configuration for a datafeed
    """
    providers_config_json = _load_providers_json_from_s3()
    return _extract_provider_conf(feed_id, providers_config_json)


def _extract_provider_conf(feed_id, provider_config_json):
    """
        Extract a single Provider object from the providers JSON file that
        has a matching feed_id, raising an error if that provider does not
        exist in the file
    """

    for provider in provider_config_json['providers']:
        if provider.get('datafeed_id') == feed_id:
            return Provider(**provider)

    raise ValueError(
        'Feed {} is not in the providers config file'.format(feed_id)
    )


def _fill_table_meta(conf, sql_context):
    """ Fills in all columns for the model """
    return conf.copy_with(
        table=get_table_metadata(sql_context, conf.datatype)
    )


def get_provider_config(sql_context, feed_id):
    """
        Read the providers config files and each associated stat calc config
        file and combine them into one provider config object.
        :param sql_context: A Spark SQLContext object
        :param feed_id: The id of the provider feed
        :paramproviders_conf_file: Absolute path of the location of the
                                    config file with all provider configs
        :return: A Provider config object
    """

    # Gets the provider config for only this feed
    provider_conf = _get_base_provider_config_for_feed(feed_id)

    # Gets the provider config for only this feed
    if provider_conf.datatype == 'emr':
        provider_conf = provider_conf.copy_with(
            models=[_fill_table_meta(m, sql_context) for m in provider_conf.models]
        )
    else:
        provider_conf = _fill_table_meta(provider_conf, sql_context)

    return provider_conf
