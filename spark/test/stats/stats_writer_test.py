import spark.stats.stats_writer as stats_writer

PROVIDER_CONF = {
    'datafeed_id': 'TEST_DF',
    'top_values_conf': {
        'columns': {
            'test_column_1': 1,
            'test_column_2': 2,
            'test_column_3': 3
        },
        'max_values': 10
    },
    'fill_rate_conf': {
        'columns': {
            'test_column_1': 1,
            'test_column_2': 2,
            'test_column_3': 3
        }
    }
}


def test__generate_queries():
    test_stats = {
        'key_stats': [
            {
                'field': 'myfield',
                'value': 'myvalue',
            }
        ],
        'longitudinality': [
            {
                'duration': 'myduration',
                'value': 'myvalue',
                'average': 'myaverage',
                'std_dev': 'mystd_dev',
            }
        ],
        'year_over_year': [
            {
                'year': 'myyear',
                'count': 'mycount'
            }
        ],
        'top_values': [
            {
                'column': 'test_column_1',
                'value': 'test_value_1',
                'count': '1000'
            }, {
                'column': 'test_column_1',
                'value': 'test_value_2',
                'count': '2000'
            }, {
                'column': 'test_column_2',
                'value': '0',
                'count': '10000'
            }, {
                'column': 'test_column_3',
                'value': '99',
                'count': '90918'
            }
        ],
        'fill_rates': [
            {'field': 'test_column_1', 'fill': '0.92'},
            {'field': 'test_column_2', 'fill': '0'},
            {'field': 'test_column_3', 'fill': '1'}
        ]
    }

    queries = stats_writer._generate_queries(test_stats, PROVIDER_CONF)

    key_stats_queries = queries['key_stats']
    longitudinality_queries = queries['longitudinality']
    year_over_year_queries = queries['year_over_year']
    top_values_queries = sorted(queries['top_values'])
    fill_rate_queries = sorted(queries['fill_rates'])

    assert len(key_stats_queries) == 1
    assert key_stats_queries[0] == "UPDATE marketplace_datafeed SET myfield = 'myvalue' WHERE id = 'TEST_DF';"

    assert len(longitudinality_queries) == 2
    assert longitudinality_queries[0] == "DELETE FROM marketplace_longitudinalityreportitem WHERE datafeed_id = 'TEST_DF';"
    assert longitudinality_queries[1] == "INSERT INTO marketplace_longitudinalityreportitem (duration, value, average, std_dev, datafeed_id) " \
        "values ('myduration', 'myvalue', 'myaverage', 'mystd_dev', 'TEST_DF');"

    assert len(year_over_year_queries) == 2
    assert year_over_year_queries[0] == "DELETE FROM marketplace_yearoveryearreportitem WHERE datafeed_id = 'TEST_DF';"
    assert year_over_year_queries[1] == "INSERT INTO marketplace_yearoveryearreportitem (startyear, value, datafeed_id) " \
        "values ('myyear', 'mycount', 'TEST_DF');"

    assert len(top_values_queries) == 3
    assert top_values_queries[0] == "INSERT INTO marketplace_datafeedfield (datafield_id, data_feed_id, top_values) " \
        "VALUES ('1', 'TEST_DF', 'test_value_1 (1000), test_value_2 (2000)') " \
        "ON CONFLICT (datafield_id, data_feed_id) DO UPDATE " \
        "SET top_values = 'test_value_1 (1000), test_value_2 (2000)';"
    assert top_values_queries[1] == "INSERT INTO marketplace_datafeedfield (datafield_id, data_feed_id, top_values) " \
        "VALUES ('2', 'TEST_DF', '0 (10000)') " \
        "ON CONFLICT (datafield_id, data_feed_id) DO UPDATE " \
        "SET top_values = '0 (10000)';"
    assert top_values_queries[2] == "INSERT INTO marketplace_datafeedfield (datafield_id, data_feed_id, top_values) " \
        "VALUES ('3', 'TEST_DF', '99 (90918)') " \
        "ON CONFLICT (datafield_id, data_feed_id) DO UPDATE " \
        "SET top_values = '99 (90918)';"

    assert len(fill_rate_queries) == 3
    assert fill_rate_queries[0] == "INSERT INTO marketplace_datafeedfield (datafield_id, data_feed_id, fill_rate) " \
        "VALUES ('1', 'TEST_DF', '92.0') ON CONFLICT (datafield_id, data_feed_id) DO UPDATE SET fill_rate = '92.0';"
    assert fill_rate_queries[1] == "INSERT INTO marketplace_datafeedfield (datafield_id, data_feed_id, fill_rate) " \
        "VALUES ('2', 'TEST_DF', '0.0') ON CONFLICT (datafield_id, data_feed_id) DO UPDATE SET fill_rate = '0.0';"
    assert fill_rate_queries[2] == "INSERT INTO marketplace_datafeedfield (datafield_id, data_feed_id, fill_rate) " \
        "VALUES ('3', 'TEST_DF', '100.0') ON CONFLICT (datafield_id, data_feed_id) DO UPDATE SET fill_rate = '100.0';"
