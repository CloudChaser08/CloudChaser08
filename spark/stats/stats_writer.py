import psycopg2
import os
import logging


MARKETPLACE_CONNECTION_PROD_CONFIG = {
    'host': 'pg-prod.healthverity.com',
    'database': 'config',
    'user': 'hvreadonly',
    'password': os.environ.get('PGPASSWORD')
}

MARKETPLACE_CONNECTION_DEV_CONFIG = {
    'host': 'pg-dev.healthverity.com',
    'database': 'config',
    'user': 'hvreadonly',
    'password': os.environ.get('PGPASSWORD')
}

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


def _generate_queries(stats, datafeed_id):
    """
    Generate queries based on given stats
    """

    queries = {}

    for stat_name, stat_value in stats.items():
        stat_queries = []

        if stat_name == 'key_stats':
            for key_stat in stat_value:
                stat_queries.append(KEYSTATS_UPDATE_SQL_TEMPLATE.format(
                    key_stat['field'], key_stat['value'], datafeed_id
                ))
        elif stat_name == 'longitudinality':
            stat_queries.append(LONGITUDINALITY_DELETE_SQL_TEMPLATE.format(datafeed_id))
            for longitudinality_stat in stat_value:
                stat_queries.append(LONGITUDINALITY_INSERT_SQL_TEMPLATE.format(
                    longitudinality_stat['duration'], longitudinality_stat['value'],
                    longitudinality_stat['average'], longitudinality_stat['std_dev'], datafeed_id
                ))
        elif stat_name == 'year_over_year':
            stat_queries.append(YOY_DELETE_SQL_TEMPLATE.format(datafeed_id))
            for yoy_stat in stat_value:
                stat_queries.append(YOY_INSERT_SQL_TEMPLATE.format(
                    yoy_stat['year'], yoy_stat['count'], datafeed_id
                ))
        elif stat_name == 'epi':
            stat_queries.append(EPI_AGE_DELETE_SQL_TEMPLATE.format(datafeed_id))
            stat_queries.append(EPI_GENDER_DELETE_SQL_TEMPLATE.format(datafeed_id))
            stat_queries.append(EPI_STATE_DELETE_SQL_TEMPLATE.format(datafeed_id))
            stat_queries.append(EPI_REGION_DELETE_SQL_TEMPLATE.format(datafeed_id))

            for epi_stat in stat_value:

        elif stat_name == 'fill_rate':
            pass

        queries[stat_name] = stat_queries

    return queries


def _write_queries(queries, output_dir, datafeed_id):
    for stat_name, stat_queries in queries.items():
        with open('{}_{}.sql'.format(datafeed_id, stat_name), 'w') as query_output:
            query_output.write('BEGIN;')
            query_output.writelines(stat_queries)
            query_output.write('COMMIT;')


def write_to_db(stats, sql_scripts_output_dir, datafeed_id, dev=True):
    """
    Generate and execute SQL scripts that are used to export given
    stats dictionary to the marketplace DB (dev by default).

    SQL Scripts will be saved to the given sql_scripts_output_dir.
    """

    queries = _generate_queries(stats, datafeed_id)

    _write_queries(queries)

    conn = psycopg2.connect(**(
        MARKETPLACE_CONNECTION_DEV_CONFIG if dev else MARKETPLACE_CONNECTION_PROD_CONFIG
    ))

    cursor = conn.cursor()

    for stat_name, stat_queries in queries.items():
        try:
            for query in stat_queries:
                cursor.execute(query)
            cursor.commit()
        except:
            logging.error('Could not commit stats for {}.'.format(stat_name))
            cursor.rollback()

    cursor.close()
    conn.close()
