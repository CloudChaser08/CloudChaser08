import boto3
from functools import reduce

S3_OUTPUT_DIR = "s3://healthveritydev/marketplace_stats/sql_scripts/{}/"

KEYSTATS_UPDATE_SQL_TEMPLATE = "UPDATE marketplace_datafeed SET {} = '{}' WHERE id = '{}';"

LONGITUDINALITY_DELETE_SQL_TEMPLATE = "DELETE FROM marketplace_longitudinalityreportitem WHERE datafeed_id = '{}';"
LONGITUDINALITY_INSERT_SQL_TEMPLATE = "INSERT INTO marketplace_longitudinalityreportitem " \
                                      "(duration, value, average, std_dev, datafeed_id) values ('{}', '{}', '{}', '{}', '{}');"

YOY_DELETE_SQL_TEMPLATE = "DELETE FROM marketplace_yearoveryearreportitem WHERE datafeed_id = '{}';"
YOY_INSERT_SQL_TEMPLATE = "INSERT INTO marketplace_yearoveryearreportitem " \
                          "(startyear, value, datafeed_id) values ('{}', '{}', '{}');"

EPI_AGE_DELETE_SQL_TEMPLATE = "DELETE FROM marketplace_agereportitem WHERE datafeed_id = '{}';"
EPI_AGE_INSERT_SQL_TEMPLATE = "INSERT INTO marketplace_agereportitem (value, age, datafeed_id) " \
                              "values ('{}', '{}', '{}');"
EPI_GENDER_DELETE_SQL_TEMPLATE = "DELETE FROM marketplace_genderreportitem WHERE datafeed_id = '{}';"
EPI_GENDER_INSERT_SQL_TEMPLATE = "INSERT INTO marketplace_genderreportitem (value, gender, datafeed_id) " \
                                 "values ('{}', '{}', '{}');"
EPI_STATE_DELETE_SQL_TEMPLATE = "DELETE FROM marketplace_georeportitem WHERE datafeed_id = '{}';"
EPI_STATE_INSERT_SQL_TEMPLATE = "INSERT INTO marketplace_georeportitem (value, state, datafeed_id) " \
                                "values ('{}', '{}', '{}');"
EPI_REGION_DELETE_SQL_TEMPLATE = "DELETE FROM marketplace_regionreportitem WHERE datafeed_id = '{}';"
EPI_REGION_INSERT_SQL_TEMPLATE = "INSERT INTO marketplace_regionreportitem (value, region, datafeed_id) " \
                                 "values ('{}', '{}', '{}');"

FILL_RATE_INSERT_SQL_TEMPLATE = "INSERT INTO marketplace_datafeedfield " \
                                "(datafield_id, data_feed_id, fill_rate) " \
                                "VALUES ('{datafield_id}', '{data_feed_id}', '{fill_rate}') " \
                                "ON CONFLICT (datafield_id, data_feed_id) DO UPDATE " \
                                "SET fill_rate = '{fill_rate}';"

TOP_VALS_INSERT_SQL_TEMPLATE = "INSERT INTO marketplace_datafeedfield " \
                               "(datafield_id, data_feed_id, top_values) " \
                               "VALUES ('{datafield_id}', '{data_feed_id}', '{top_values}') " \
                               "ON CONFLICT (datafield_id, data_feed_id) DO UPDATE " \
                               "SET top_values = '{top_values}';"


def _generate_queries(stats, provider_conf):
    """
    Generate queries based on given stats
    """

    queries = {}

    for stat_name, stat_value in stats.items():
        stat_queries = []

        if stat_name == 'key_stats' and stat_value:
            for key_stat in stat_value:
                stat_queries.append(KEYSTATS_UPDATE_SQL_TEMPLATE.format(
                    key_stat['field'], key_stat['value'], provider_conf['datafeed_id']
                ))
        elif stat_name == 'longitudinality' and stat_value:
            stat_queries.append(LONGITUDINALITY_DELETE_SQL_TEMPLATE.format(provider_conf['datafeed_id']))
            for longitudinality_stat in stat_value:
                stat_queries.append(LONGITUDINALITY_INSERT_SQL_TEMPLATE.format(
                    longitudinality_stat['duration'], longitudinality_stat['value'],
                    longitudinality_stat['average'], longitudinality_stat['std_dev'], provider_conf['datafeed_id']
                ))
        elif stat_name == 'year_over_year' and stat_value:
            stat_queries.append(YOY_DELETE_SQL_TEMPLATE.format(provider_conf['datafeed_id']))
            for yoy_stat in stat_value:
                stat_queries.append(YOY_INSERT_SQL_TEMPLATE.format(
                    yoy_stat['year'], yoy_stat['count'], provider_conf['datafeed_id']
                ))
        elif stat_name == 'epi' and stat_value:
            stat_queries.append(EPI_AGE_DELETE_SQL_TEMPLATE.format(provider_conf['datafeed_id']))
            stat_queries.append(EPI_GENDER_DELETE_SQL_TEMPLATE.format(provider_conf['datafeed_id']))
            stat_queries.append(EPI_STATE_DELETE_SQL_TEMPLATE.format(provider_conf['datafeed_id']))
            stat_queries.append(EPI_REGION_DELETE_SQL_TEMPLATE.format(provider_conf['datafeed_id']))

            for epi_category in stat_value:
                if epi_category['field'] == 'region':
                    query = EPI_REGION_INSERT_SQL_TEMPLATE
                elif epi_category['field'] == 'state':
                    query = EPI_STATE_INSERT_SQL_TEMPLATE
                elif epi_category['field'] == 'age':
                    query = EPI_AGE_INSERT_SQL_TEMPLATE
                elif epi_category['field'] == 'gender':
                    query = EPI_GENDER_INSERT_SQL_TEMPLATE

                for epi_stat in epi_category:
                    stat_queries.append(
                        query.format(epi_stat['value'], epi_stat['field'], provider_conf['datafeed_id'])
                    )

        elif stat_name == 'top_values' and stat_value:
            columns = set([r['column'] for r in stat_value])

            for column in columns:
                top_values_string = reduce(lambda x1, x2: x1 + ', ' + x2, [
                    '{} ({})'.format(r['value'], r['count'])
                    for r in stat_value if r['column'] == column
                ])
                name_id_dict = provider_conf['top_values_conf']['columns']

                stat_queries.append(TOP_VALS_INSERT_SQL_TEMPLATE.format(
                    datafield_id=name_id_dict[column], data_feed_id=provider_conf['datafeed_id'],
                    top_values=top_values_string
                ))

        elif stat_name == 'fill_rates' and stat_value:
            name_id_dict = provider_conf['fill_rate_conf']['columns']

            for field_dict in stat_value:
                stat_queries.append(FILL_RATE_INSERT_SQL_TEMPLATE.format(
                    datafield_id=name_id_dict[field_dict['field']], data_feed_id=provider_conf['datafeed_id'],
                    fill_rate=str(float(field_dict['fill']) * 100)
                ))

        queries[stat_name] = stat_queries

    return queries


def _write_queries(queries, datafeed_id, quarter, identifier=None):
    """
    Write given queries to s3
    """
    for stat_name, stat_queries in queries.items():
        filename = '{}_{}{}.sql'.format(datafeed_id, '{}_'.format(identifier) if identifier else '', stat_name)
        with open(filename, 'w') as query_output:
            query_output.write('BEGIN;\n')
            query_output.writelines([(q + '\n') for q in stat_queries])
            query_output.write('COMMIT;\n')
        boto3.client('s3').upload_file(
            filename, S3_OUTPUT_DIR.split('/')[2], '/'.join(S3_OUTPUT_DIR.format(quarter).split('/')[3:]) + filename
        )


def write_to_s3(stats, provider_conf, quarter):
    """
    Generate SQL scripts that are used to export given
    stats dictionary to the marketplace DB.

    Those scripts are saved to S3_OUTPUT_DIR.
    """
    if provider_conf['datatype'].startswith('emr'):
        for model_conf in provider_conf['models']:
            queries = _generate_queries(stats[model_conf['datatype']], model_conf)
            _write_queries(queries, provider_conf['datafeed_id'], quarter, identifier=model_conf['datatype'])
    else:
        queries = _generate_queries(stats, provider_conf)
        _write_queries(queries, provider_conf['datafeed_id'], quarter)
