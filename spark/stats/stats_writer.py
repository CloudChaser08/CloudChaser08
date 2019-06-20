import boto3
import os
from functools import reduce

S3_OUTPUT_DIR = "s3://healthveritydev/marketplace_stats/sql_scripts/{}/"

KEYSTATS_UPDATE_SQL_TEMPLATE = "UPDATE marketplace_datafeed SET {} = '{}' WHERE id = '{}';"

LONGITUDINALITY_DELETE_SQL_TEMPLATE = "DELETE FROM marketplace_longitudinalityreportitem WHERE datafeed_id = '{}';"
LONGITUDINALITY_INSERT_SQL_TEMPLATE = "INSERT INTO marketplace_longitudinalityreportitem " \
                                      "(duration, value, average, std_dev, datafeed_id) values ('{}', '{}', '{}', '{}', '{}');"

YOY_DELETE_SQL_TEMPLATE = "DELETE FROM marketplace_yearoveryearreportitem WHERE datafeed_id = '{}';"
YOY_INSERT_SQL_TEMPLATE = "INSERT INTO marketplace_yearoveryearreportitem " \
                          "(startyear, value, datafeed_id) values ('{}', '{}', '{}');"

EPI_AGE_INSERT_SQL_TEMPLATE = "INSERT INTO marketplace_agereportitem (value, age, datafeed_id) " \
                              "VALUES ('{value}', '{field}', '{datafeed_id}') ON CONFLICT (age, datafeed_id) " \
                              "DO UPDATE SET value = '{value}';"

EPI_GENDER_INSERT_SQL_TEMPLATE = "INSERT INTO marketplace_genderreportitem (value, gender, datafeed_id) " \
                                 "VALUES ('{value}', '{field}', '{datafeed_id}') ON CONFLICT (gender, datafeed_id) " \
                                 "DO UPDATE SET value = '{value}';"


EPI_STATE_INSERT_SQL_TEMPLATE = "INSERT INTO marketplace_georeportitem (value, state, datafeed_id) " \
                                "VALUES ('{value}', '{field}', '{datafeed_id}') ON CONFLICT (state, datafeed_id) " \
                                "DO UPDATE SET value = '{value}';"


EPI_REGION_INSERT_SQL_TEMPLATE = "INSERT INTO marketplace_regionreportitem (value, division, datafeed_id) " \
                                 "VALUES ('{value}', '{field}', '{datafeed_id}') ON CONFLICT (division, datafeed_id) " \
                                 "DO UPDATE SET value = '{value}';"

FILL_RATE_INSERT_SQL_TEMPLATE = "INSERT INTO marketplace_datafeedfield " \
                                "(name, sequence, datafield_id, data_feed_id, fill_rate, unique_to_data_feed) " \
                                "VALUES ('{name}', '{sequence}', '{datafield_id}', '{data_feed_id}', '{fill_rate}', false) " \
                                "ON CONFLICT (datafield_id, data_feed_id) DO UPDATE " \
                                "SET fill_rate = '{fill_rate}';"

TOP_VALS_DELETE_SQL_TEMPLATE = "UPDATE marketplace_datafeedfield SET top_values = NULL "\
                               "WHERE data_feed_id = {data_feed_id};"

TOP_VALS_INSERT_SQL_TEMPLATE = "INSERT INTO marketplace_datafeedfield " \
                               "(name, sequence, datafield_id, data_feed_id, top_values, unique_to_data_feed) " \
                               "VALUES ('{name}', '{sequence}', '{datafield_id}', '{data_feed_id}', '{top_values}', false) " \
                               "ON CONFLICT (datafield_id, data_feed_id) DO UPDATE " \
                               "SET top_values = '{top_values}';"

REGION_MAP = {
    "CT": "NEW_ENGLAND",
    "ME": "NEW_ENGLAND",
    "NH": "NEW_ENGLAND",
    "RI": "NEW_ENGLAND",
    "VT": "NEW_ENGLAND",
    "MA": "NEW_ENGLAND",

    "NJ": "MIDDLE_ATLANTIC",
    "NY": "MIDDLE_ATLANTIC",
    "PA": "MIDDLE_ATLANTIC",

    "IL": "EAST_NORTH_CENTRAL",
    "IN": "EAST_NORTH_CENTRAL",
    "MI": "EAST_NORTH_CENTRAL",
    "OH": "EAST_NORTH_CENTRAL",
    "WI": "EAST_NORTH_CENTRAL",

    "IA": "WEST_NORTH_CENTRAL",
    "KS": "WEST_NORTH_CENTRAL",
    "MN": "WEST_NORTH_CENTRAL",
    "MO": "WEST_NORTH_CENTRAL",
    "ND": "WEST_NORTH_CENTRAL",
    "SD": "WEST_NORTH_CENTRAL",
    "NE": "WEST_NORTH_CENTRAL",

    "DE": "SOUTH_ATLANTIC",
    "DC": "SOUTH_ATLANTIC",
    "FL": "SOUTH_ATLANTIC",
    "GA": "SOUTH_ATLANTIC",
    "MD": "SOUTH_ATLANTIC",
    "NC": "SOUTH_ATLANTIC",
    "SC": "SOUTH_ATLANTIC",
    "VA": "SOUTH_ATLANTIC",
    "WV": "SOUTH_ATLANTIC",

    "AL": "EAST_SOUTH_CENTRAL",
    "KY": "EAST_SOUTH_CENTRAL",
    "MS": "EAST_SOUTH_CENTRAL",
    "TN": "EAST_SOUTH_CENTRAL",
    "AR": "WEST_SOUTH_CENTRAL",
    "LA": "WEST_SOUTH_CENTRAL",
    "OK": "WEST_SOUTH_CENTRAL",
    "TX": "WEST_SOUTH_CENTRAL",

    "AZ": "MOUNTAIN",
    "CO": "MOUNTAIN",
    "ID": "MOUNTAIN",
    "MT": "MOUNTAIN",
    "NV": "MOUNTAIN",
    "NM": "MOUNTAIN",
    "UT": "MOUNTAIN",
    "WY": "MOUNTAIN",

    "AK": "PACIFIC",
    "CA": "PACIFIC",
    "HI": "PACIFIC",
    "OR": "PACIFIC",
    "WA": "PACIFIC"
}

VALID_AGES = [
    '0-17',
    '18-44',
    '45-64',
    '65+'
]

VALID_GENDERS = [
    'M',
    'F',
]


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

        elif stat_name in ['age', 'gender', 'state', 'region'] and stat_value:
            if stat_name == 'region':

                # format regions correctly, remove invalid regions
                stat_value = [
                    {
                        'field': stat['field'].replace('_', ' ').title(),
                        'value': float(stat['value'])
                    } for stat in stat_value if stat['field'].upper() in REGION_MAP.values()
                ]

                query = EPI_REGION_INSERT_SQL_TEMPLATE
            elif stat_name == 'state':

                # format states correctly, remove invalid states
                stat_value = [
                    {
                        'field': stat['field'].upper(),
                        'value': float(stat['value'])
                    } for stat in stat_value if stat['field'].upper() in REGION_MAP.keys()
                ]

                query = EPI_STATE_INSERT_SQL_TEMPLATE
            elif stat_name == 'age':

                # remove invalid ages
                stat_value = [
                    {
                        'field': stat['field'],
                        'value': float(stat['value'])
                    } for stat in stat_value if stat['field'] in VALID_AGES
                ]

                query = EPI_AGE_INSERT_SQL_TEMPLATE
            elif stat_name == 'gender':

                # remove invalid genders
                stat_value = [
                    {
                        'field': stat['field'],
                        'value': float(stat['value'])
                    } for stat in stat_value if stat['field'] in VALID_GENDERS
                ]

                query = EPI_GENDER_INSERT_SQL_TEMPLATE

            # convert stat to percentages
            total_value = sum([stat['value'] for stat in stat_value])
            for stat in stat_value:
                stat['value'] = (stat['value'] / total_value) * 100

            # append to queries list
            for epi_stat in stat_value:
                epi_stat['datafeed_id'] = provider_conf['datafeed_id']
                stat_queries.append(
                    query.format(**epi_stat)
                )

        elif stat_name == 'top_values' and stat_value:
            columns = set([r['column'] for r in stat_value])

            name_id_dict = provider_conf['top_values_conf']['columns']

            stat_queries.append(TOP_VALS_DELETE_SQL_TEMPLATE.format(
                data_feed_id=provider_conf["datafeed_id"]))

            for column in columns:
                top_values_string = reduce(lambda x1, x2: x1 + ', ' + x2, [
                    '{} ({}:{})'.format(r['value'].encode('utf-8'), r['count'], r['percentage'])
                    for r in stat_value if r['column'] == column
                ])

                stat_queries.append(TOP_VALS_INSERT_SQL_TEMPLATE.format(
                    name=column, datafield_id=name_id_dict[column]['field_id'], sequence=name_id_dict[column]['sequence'],
                    data_feed_id=provider_conf['datafeed_id'], top_values=top_values_string
                ))

        elif stat_name == 'fill_rate' and stat_value:
            name_id_dict = provider_conf['fill_rate_conf']['columns']

            for field_dict in stat_value:
                stat_queries.append(FILL_RATE_INSERT_SQL_TEMPLATE.format(
                    name=field_dict['field'], datafield_id=name_id_dict[field_dict['field']]['field_id'],
                    sequence=name_id_dict[field_dict['field']]['sequence'], data_feed_id=provider_conf['datafeed_id'],
                    fill_rate=str(float(field_dict['fill'] or 0) * 100)
                ))

        queries[stat_name] = stat_queries

    return queries


def _write_queries(queries, datafeed_id, quarter, identifier):
    """
    Write given queries to s3.

    Queries will first be written to output/<quarter>/ in case the s3 write fails.
    """
    output_dir = 'output/{}/'.format(quarter)

    try:
        os.makedirs(output_dir)
    except:
        pass

    for stat_name, stat_queries in queries.items():
        if len(stat_queries) > 0:
            filename = '{}_{}_{}.sql'.format(datafeed_id, identifier, stat_name)
            with open(output_dir + filename, 'w') as query_output:
                query_output.write('BEGIN;\n')
                query_output.writelines([(q + '\n') for q in stat_queries])
                query_output.write('COMMIT;\n')
            boto3.client('s3').upload_file(
                output_dir + filename, S3_OUTPUT_DIR.split('/')[2],
                '/'.join(S3_OUTPUT_DIR.format(quarter).split('/')[3:]) + filename
            )


def write_to_s3(stats, provider_conf, quarter):
    """
    Generate SQL scripts that are used to export given
    stats dictionary to the marketplace DB.

    Those scripts are saved to S3_OUTPUT_DIR.
    """
    queries = _generate_queries(stats, provider_conf)
    _write_queries(queries, provider_conf['datafeed_id'], quarter, provider_conf['datatype'])
