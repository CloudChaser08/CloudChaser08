import json
import os

def _get_config_from_json(filename):
    '''
    * Put doc here *
    '''

    with open(filename, 'r') as conf:
        data = json.loads(conf.read())
        return data


def _extract_provider_conf(provider_name, providers_conf):
    '''
    * Put doc here *
    '''

    conf = list(filter(lambda x: x['name'] == provider_name, providers_conf['providers']))
    if len(conf) == 0:
        raise Exception('{} is not in the providers config file'.format(provider_name))
    return conf[0]


def get_provider_config(provider_name, providers_conf_file):
    '''
    * Put doc here *
    '''

    config_dir = '/'.join(os.path.abspath(providers_conf_file).split('/')[:-1])
    
    providers_conf = _get_config_from_json(providers_conf_file)

    if 'providers' not in providers_conf:
        raise Exception('{} does not contain providers list'.format(providers_conf))

    provider_conf = _extract_provider_conf(provider_name, providers_conf)

    # Get each individual stat calcs config based on the config path in them
    stat_calcs = ['fill_rate', 'key_stats', 'top_values', \
                 'longitudinality', 'year_over_year', 'epi_calcs']
    for calc in stat_calcs:
        if calc not in provider_conf:
            raise Exception('No config for {} found in {} config'.format(calc, provider_name))
        if provider_conf[calc]:
            conf_file_loc = config_dir + '/' + provider_conf[calc]
            provider_conf[calc] = _get_config_from_json(conf_file_loc)
    
    return provider_conf



