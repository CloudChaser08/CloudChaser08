"""
    Reads in Provider configuration that is required to run stats
"""
import json
import logging
import os
from contextlib import closing

import boto3
import psycopg2
from spark.helpers.file_utils import get_abs_path
from spark.stats.config.dates import dates as provider_dates
from spark.stats.models import (
    Provider, Column, FillRateConfig, TopValuesConfig, EPICalcsConfig,
    KeyStatsConfig, LongitudinalityConfig, YearOverYearConfig
)

SSM = boto3.client('ssm')

SSM_PARAM_NAME = 'dev-marketplace-rds_ro_db_conn'
PG_CONN_DETAILS = json.loads(
    SSM.get_parameters(
        Names=[SSM_PARAM_NAME],
        WithDecryption=True
    )['Parameters'][0]['Value']
)

PG_HOST = PG_CONN_DETAILS['host']
PG_PASSWORD = PG_CONN_DETAILS['password']
PG_DB = PG_CONN_DETAILS['database']
PG_USER = PG_CONN_DETAILS['user']

# map from emr datatype (table) name to the name of each datatype in
# the marketplace db
EMR_DATATYPE_NAME_MAP = {
    'emr_enc': 'Encounter',
    'emr_diag': 'Diagnosis',
    'emr_clin_obsn': 'Clinical Observation',
    'emr_proc': 'Procedure',
    'emr_prov_ord': 'Provider Order',
    'emr_lab_test': 'Lab Test',
    'emr_medctn': 'Medication'
}

def _get_config_from_json(filename):
    '''
    Reads a config file as json and stores it in a Python dict.
    Input:
        - filename: Absolute path of the location for the file
    Output:
        - data: the config represented as a Python dict
    '''

    with open(filename, 'r') as conf:
        data = json.loads(conf.read())
        return data


def _get_column_map_from_db(query):
    """
        Runs query and returns a mapping dict of <name> => Column pairs
    """
    conn = psycopg2.connect(
        host=PG_HOST,
        database=PG_DB,
        user=PG_USER,
        password=PG_PASSWORD
    )

    with closing(conn.cursor()) as cursor:
        cursor.execute(query)
        results = cursor.fetchall()
    return {
        res[0]: Column(name=res[0], field_id=res[1], sequence=res[2])
        for res in results
    }


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


def _get_top_values_config(datafeed_id):
    """ Gets the top values configuration from the database """
    get_columns_sql = """
        select f.physical_name as name, f.id as field_id, f.sequence as sequence
            from marketplace_datafield f
            join marketplace_datatable t on t.id = f.datatable_id
            join marketplace_datamodel m on m.id = t.datamodel_id
            join marketplace_datafeed_datamodels dm on dm.datamodel_id = m.id
        where dm.datafeed_id = {} and m.is_supplemental = 'f' and f.top_values= 't';
    """.format(datafeed_id)

    return TopValuesConfig(columns=_get_column_map_from_db(get_columns_sql))


def _get_fill_rate_config(datafeed_id, emr_datatype=None):
    """ Gets the fill rate configuration from the database """
    get_columns_sql = """
        select f.physical_name as name, f.id as field_id, f.sequence as sequence
            from marketplace_datafield f
            join marketplace_datatable t on t.id = f.datatable_id
            join marketplace_datamodel m on m.id = t.datamodel_id
            join marketplace_datafeed_datamodels dm on dm.datamodel_id = m.id
        where dm.datafeed_id = {} and m.is_supplemental = 'f' {};
    """.format(
        datafeed_id,
        "and t.name = '{}'".format(EMR_DATATYPE_NAME_MAP[emr_datatype]) if emr_datatype else ''
    )

    return FillRateConfig(columns=_get_column_map_from_db(get_columns_sql))


def _fill_date_fields(conf):
    """ Fills in date_field from the provider_dates mapping """
    if not conf.date_fields:
        conf = conf.copy_with(
            date_fields=provider_dates[conf.datatype]
        )
    return conf


def _fill_fill_rate(conf):
    """ Fills in the fill rate config """
    if conf.fill_rate:
        # Only use datatype if it is an emr data type
        datatype = conf.datatype if conf.datatype.startswith('emr') else None
        return conf.copy_with(
            fill_rate_conf=_get_fill_rate_config(conf.datafeed_id, datatype)
        )
    return conf

def _fill_top_values(conf):
    """ Fills in the top values config """
    if conf.top_values:
        return conf.copy_with(
            top_values_conf=_get_top_values_config(conf.datafeed_id)
        )
    return conf

def _fill_epi_calcs(conf):
    """ Fills in the EPI calculations config """
    if conf.epi_calcs:
        return conf.copy_with(epi_calcs_conf=EPICalcsConfig(
            fields=['age', 'gender', 'state', 'region']
        ))
    return conf

def _fill_key_stats(providers_conf_file):
    """
        Creates a key stats function that fills out the key stats
        configuration provided from a static config file, located relative to
        the providers_conf_file
    """
    def _fill(conf):
        if conf.key_stats:
            file_name = (
                conf.key_stats_conf_file
                or os.path.join(conf.datatype, 'key_stats.json')
            )
            file_path = get_abs_path(providers_conf_file, file_name)
            return conf.copy_with(
                key_stats_conf=KeyStatsConfig(
                    **_get_config_from_json(file_path)
                )
            )
        return conf
    return _fill


def _fill_longitudinality(providers_conf_file):
    """
        Creates a fill function that fills out the longitudinality
        configuration provided from a static config file, located relative to
        the providers_conf_file
    """
    def _fill(conf):
        if conf.longitudinality:
            file_name = (
                conf.longitudinality_conf_file
                or os.path.join(conf.datatype, 'longitudinality.json')
            )
            file_path = get_abs_path(providers_conf_file, file_name)
            return conf.copy_with(
                longitudinality_conf=LongitudinalityConfig(
                    **_get_config_from_json(file_path)
                )
            )
        return conf
    return _fill


def _fill_year_over_year(providers_conf_file):
    """
        Creates a fill function that fills out the year-over-year
        configuration provided from a static config file, located relative to
        the providers_conf_file
    """
    def _fill(conf):
        if conf.year_over_year:
            file_name = (
                conf.year_over_year_conf_file
                or os.path.join(conf.datatype, 'year_over_year.json')
            )
            file_path = get_abs_path(providers_conf_file, file_name)
            return conf.copy_with(
                year_over_year_conf=YearOverYearConfig(
                    **_get_config_from_json(file_path)
                )
            )
        return conf
    return _fill


def _pipe_fills(*fills):
    """
        Creates a single fill function that passes a configuration object
        through the provided fill functions, and returns the new configuration
    """
    def _fill(conf):
        for fill in fills:
            conf = fill(conf)
        return conf
    return _fill


def get_provider_config(providers_conf_file, feed_id):
    """
        Read the providers config files and each associated stat calc config
        file and combine them into one provider config object.
        :param feed_id: The id of the provider feed
        :param providers_conf_file: Absolute path of the location of the
                                    config file with all provider configs
        :return: A Provider config object
    """
    provider_file_json = _get_config_from_json(providers_conf_file)

    # Gets the provider config for only this feed
    provider_conf = _extract_provider_conf(feed_id, provider_file_json)

    # Gets the provider config for only this feed
    if provider_conf.datatype == 'emr':
        fill_provider_model = _pipe_fills(
            _fill_fill_rate,
            _fill_top_values,
            _fill_date_fields,
        )
        provider_conf = provider_conf.copy_with(
            models=[fill_provider_model(m) for m in provider_conf.models]
        )
    else:
        provider_conf = _pipe_fills(
            _fill_fill_rate,
            _fill_top_values,
            _fill_epi_calcs,
            _fill_longitudinality(providers_conf_file),
            _fill_year_over_year(providers_conf_file),
            _fill_key_stats(providers_conf_file)
        )(provider_conf)

    return _fill_date_fields(provider_conf)
