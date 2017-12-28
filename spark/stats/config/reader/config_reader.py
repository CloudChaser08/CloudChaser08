import json
import logging
import os
from contextlib import closing
import psycopg2
from spark.helpers.file_utils import get_abs_path


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
        host='pg-prod.healthverity.com',
        database='config',
        user='hvreadonly',
        password=os.environ.get('PGPASSWORD')
    )

    with closing(conn.cursor()) as cursor:
        cursor.execute(query)
        results = cursor.fetchall()
    return dict(results)


def _extract_provider_conf(provider_name, providers_conf):
    '''
    Get a specific providers config from the config file with all
    provider configs.
    Input:
        - provider_name: The name of the provider
        - providers_conf: A Python dict containing configs
                          for every provider
    Output:
        - _ : A Python dict with the config for 'provider_name'
    '''

    conf = list(filter(lambda x: x['name'] == provider_name, providers_conf['providers']))
    if len(conf) == 0:
        raise Exception('{} is not in the providers config file'.format(provider_name))
    return conf[0]


def _get_top_values_columns(datafeed_id):

    get_columns_sql = """
        select f.physical_name, f.id as field_id
            from marketplace_datafield f
            join marketplace_datatable t on t.id = f.datatable_id
            join marketplace_datamodel m on m.id = t.datamodel_id
            join marketplace_datafeed_datamodels dm on dm.datamodel_id = m.id
        where dm.datafeed_id = {} and m.is_supplemental = 'f' and f.top_values= 't';
    """.format(datafeed_id)

    return _get_config_from_db(get_columns_sql)


def _get_fill_rate_columns(datafeed_id):

    get_columns_sql = """
        select f.physical_name, f.id as field_id
            from marketplace_datafield f
            join marketplace_datatable t on t.id = f.datatable_id
            join marketplace_datamodel m on m.id = t.datamodel_id
            join marketplace_datafeed_datamodels dm on dm.datamodel_id = m.id
        where dm.datafeed_id = {} and m.is_supplemental = 'f';
    """.format(datafeed_id)

    return _get_config_from_db(get_columns_sql)


def get_provider_config(providers_conf_file, provider_name):
    '''
    Read the providers config files and each associated stat calc config file
    and combine them into one provider config object.
    Input:
        - provider_name: The name of the provider
        - providers_conf_file: Absolute path of the location of the
                               config file with all provider configs.
    Output:
        - provider_conf: A python dict of the providers config with
                         each associated stat calcs config embedded
    '''
    providers_conf = _get_config_from_json(providers_conf_file)

    if 'providers' not in providers_conf:
        raise Exception('{} does not contain providers list'.format(providers_conf))

    provider_conf = _extract_provider_conf(provider_name, providers_conf)

    # Check that datatype is specified
    if 'datatype' not in provider_conf or provider_conf['datatype'] is None:
        raise Exception('datatype is not specified for provider {}'.format(provider_name))

    # configure stats whose configurations come from the marketplace db
    if provider_conf['fill_rate']:
        provider_conf['fill_rate_conf'] = {
            "columns": _get_fill_rate_columns(provider_conf['datafeed_id'])
        }

    if provider_conf['top_values']:
        provider_conf['top_values_conf'] = {
            "columns": _get_top_values_columns(provider_conf['datafeed_id']),
            "max_values": 10
        }

    # configure stats whose configurations do not come from the marketplace db
    no_db_stat_calcs = ['key_stats', 'longitudinality', 'year_over_year', 'epi_calcs']
    for calc in no_db_stat_calcs:
        if calc + '_conf_file' not in provider_conf:
            logging.info('No config for {} found in {} config, falling back to default.'.format(calc, provider_name))
            conf_file_loc = get_abs_path(providers_conf_file,
                                        provider_conf['datatype'] + '/' + calc + '.json')
            provider_conf[calc + '_conf'] = _get_config_from_json(conf_file_loc)
        elif provider_conf[calc + '_conf_file']:
            conf_file_loc = get_abs_path(providers_conf_file, provider_conf[calc + '_conf_file'])
            provider_conf[calc + '_conf'] = _get_config_from_json(conf_file_loc)

    return provider_conf
