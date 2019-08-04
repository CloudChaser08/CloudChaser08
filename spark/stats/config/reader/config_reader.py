import json
import logging
import os
from contextlib import closing

import boto3
import psycopg2
from spark.helpers.file_utils import get_abs_path
from spark.stats.config.dates import dates as provider_dates

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
emr_datatype_name_map = {
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


def _get_config_from_db(query):
    '''
    Runs query on marketplace prod and returns a results dict.
    Input:
        - query: The query to run
    Output:
        - data: the config represented as a Python dict
    '''
    conn = psycopg2.connect(
        host=PG_HOST,
        database=PG_DB,
        user=PG_USER,
        password=PG_PASSWORD
    )

    with closing(conn.cursor()) as cursor:
        cursor.execute(query)
        results = cursor.fetchall()
    return dict([
        (res[0], {'field_id': res[1], 'sequence': res[2]})
        for res in results
    ])


def _extract_provider_conf(feed_id, providers_conf):
    '''
    Get a specific providers config from the config file with all
    provider configs.
    Input:
        - feed_id: The id of the provider feed
        - providers_conf: A Python dict containing configs
                          for every provider
    Output:
        - _ : A Python dict with the config for 'feed_id'
    '''

    conf = [x for x in providers_conf['providers'] if x['datafeed_id'] == feed_id]
    if len(conf) == 0:
        raise Exception('Feed {} is not in the providers config file'.format(feed_id))
    return conf[0]


def _get_top_values_columns(datafeed_id):

    get_columns_sql = """
        select f.physical_name as name, f.id as field_id, f.sequence as sequence
            from marketplace_datafield f
            join marketplace_datatable t on t.id = f.datatable_id
            join marketplace_datamodel m on m.id = t.datamodel_id
            join marketplace_datafeed_datamodels dm on dm.datamodel_id = m.id
        where dm.datafeed_id = {} and m.is_supplemental = 'f' and f.top_values= 't';
    """.format(datafeed_id)

    return _get_config_from_db(get_columns_sql)


def _get_fill_rate_columns(datafeed_id, emr_datatype=None):
    get_columns_sql = """
        select f.physical_name as name, f.id as field_id, f.sequence as sequence
            from marketplace_datafield f
            join marketplace_datatable t on t.id = f.datatable_id
            join marketplace_datamodel m on m.id = t.datamodel_id
            join marketplace_datafeed_datamodels dm on dm.datamodel_id = m.id
        where dm.datafeed_id = {} and m.is_supplemental = 'f' {};
    """.format(
        datafeed_id,
        "and t.name = '{}'".format(emr_datatype_name_map[emr_datatype]) if emr_datatype else ''
    )

    return _get_config_from_db(get_columns_sql)


def _fill_in_dates(conf):
    if not conf.get('date_field'):
        conf['date_field'] = provider_dates[conf['datatype']]

    return conf


def _fill_in_conf_dict(conf, feed_id, providers_conf_file):
    # configure stats whose configurations come from the marketplace db
    if conf.get('fill_rate'):
        conf['fill_rate_conf'] = {
            "columns": _get_fill_rate_columns(
                conf['datafeed_id'], conf['datatype'] if conf['datatype'].startswith('emr') else None
            )
        }

    if conf.get('top_values'):
        conf['top_values_conf'] = {
            "columns": _get_top_values_columns(conf['datafeed_id']),
            "max_values": 10
        }

    # epi doesn't require any additional configurations
    if conf.get('epi_calcs'):
        conf['epi_calcs_conf'] = {}

    # configure stats whose configurations do not come from the marketplace db
    no_db_stat_calcs = ['key_stats', 'longitudinality', 'year_over_year']
    for calc in no_db_stat_calcs:
        if not conf.get(calc):
            continue

        if calc + '_conf_file' not in conf:
            logging.info('No config for {} found in feed {} config, falling back to default.'.format(calc, feed_id))
            conf_file_loc = get_abs_path(providers_conf_file,
                                        conf['datatype'] + '/' + calc + '.json')
            conf[calc + '_conf'] = _get_config_from_json(conf_file_loc)
        elif conf[calc + '_conf_file']:
            conf_file_loc = get_abs_path(providers_conf_file, conf[calc + '_conf_file'])
            conf[calc + '_conf'] = _get_config_from_json(conf_file_loc)

    return conf


def get_provider_config(providers_conf_file, feed_id):
    '''
    Read the providers config files and each associated stat calc config file
    and combine them into one provider config object.
    Input:
        - feed_id: The id of the provider feed
        - providers_conf_file: Absolute path of the location of the
                               config file with all provider configs.
    Output:
        - provider_conf: A python dict of the providers config with
                         each associated stat calcs config embedded
    '''
    providers_conf = _get_config_from_json(providers_conf_file)

    if 'providers' not in providers_conf:
        raise Exception('{} does not contain providers list'.format(providers_conf))

    provider_conf = _extract_provider_conf(feed_id, providers_conf)

    # Check that datatype is specified
    if 'datatype' not in provider_conf or provider_conf['datatype'] is None:
        raise Exception('datatype is not specified for feed {}'.format(feed_id))
    elif provider_conf['datatype'] == 'emr':
        provider_conf['models'] = [
            _fill_in_dates(_fill_in_conf_dict(dict(
                list(model_conf.items()) + [('datafeed_id', provider_conf['datafeed_id'])]
            ), feed_id, providers_conf_file))
            for model_conf in provider_conf['models']
        ]
    else:
        provider_conf = _fill_in_conf_dict(provider_conf, feed_id, providers_conf_file)

    return _fill_in_dates(provider_conf)
