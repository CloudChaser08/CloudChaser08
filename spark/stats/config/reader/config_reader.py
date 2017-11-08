import json
import logging
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


def get_provider_config(providers_conf_file, provider_name):
    '''
    Read the providers config files and each associated stat calc config file
    and combine them into one provider config object.
    Input:
        - provider_name: The name of the provider
        - providers_conf_file: Absolute path of the location of the
                               config file with all provider configs.
    Output:
        - out: A function that creates the provider config 
               for the inputed provider_name
    '''
    providers_conf = _get_config_from_json(providers_conf_file)

    if 'providers' not in providers_conf:
        raise Exception('{} does not contain providers list'.format(providers_conf))

    provider_conf = _extract_provider_conf(provider_name, providers_conf)

    # Check that datatype is specified
    if not 'datatype' in provider_conf or provider_conf['datatype'] == None:
        raise Exception('datatype is not specified for provider {}'.format(provider_name))

    # Get each individual stat calcs config based on the config path in them
    stat_calcs = ['fill_rate', 'key_stats', 'top_values', \
                 'longitudinality', 'year_over_year', 'epi_calcs']
    for calc in stat_calcs:
        if calc not in provider_conf:
            logging.info('No config for {} found in {} config, falling back to default.'.format(calc, provider_name))
            conf_file_loc = get_abs_path(providers_conf_file, 
                                        provider_conf['datatype'] + '/' + calc + '.json')
            provider_conf[calc] = _get_config_from_json(conf_file_loc)
        elif provider_conf[calc]:
            conf_file_loc = get_abs_path(providers_conf_file, provider_conf[calc])
            provider_conf[calc] = _get_config_from_json(conf_file_loc)
    
    return provider_conf


