import spark.stats.stats_writer as stats_writer


PROVIDER_CONF = {
    'datafeed_id': 'TEST_DF',
    'top_values_conf': {
        'columns': {
            'test_column_1': {
                'field_id': 1,
                'sequence': 0,
                'category': 'TODO',
                'description': 'Test description 1',
                'table_id': 1,
                'table_name': 'Test table 1',
                'table_desc': 'Test table description 1',
                'table_seq': 0,
                'field_type_name': 'String'
            },
            'test_column_2': {
                'field_id': 2,
                'sequence': 0,
                'category': 'TODO',
                'description': 'Test description 2',
                'table_id': 1,
                'table_name': 'Test table 1',
                'table_desc': 'Test table description 1',
                'table_seq': 0,
                'field_type_name': 'Number'
            },
            'test_column_3': {
                'field_id': 3,
                'sequence': 0,
                'category': 'TODO',
                'description': 'Test description 3',
                'table_id': 1,
                'table_name': 'Test table 2',
                'table_desc': 'Test table description 2',
                'table_seq': 1,
                'field_type_name': 'String'
            }
        },
        'max_values': 10
    },
    'fill_rate_conf': {
        'columns': {
            'test_column_1': {
                'field_id': 1,
                'sequence': 0,
                'category': 'TODO',
                'description': 'Test description 1',
                'table_id': 1,
                'table_name': 'Test table 1',
                'table_desc': 'Test table description 1',
                'table_seq': 0,
                'field_type_name': 'String'
            },
            'test_column_2': {
                'field_id': 2,
                'sequence': 0,
                'category': 'TODO',
                'description': 'Test description 2',
                'table_id': 1,
                'table_name': 'Test table 1',
                'table_desc': 'Test table description 1',
                'table_seq': 0,
                'field_type_name': 'Number'
            },
            'test_column_3': {
                'field_id': 3,
                'sequence': 0,
                'category': 'TODO',
                'description': 'Test description 3',
                'table_id': 1,
                'table_name': 'Test table 2',
                'table_desc': 'Test table description 2',
                'table_seq': 1,
                'field_type_name': 'String'
            }
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
                'count': '1000',
                'percentage': 0.01
            }, {
                'column': 'test_column_1',
                'value': 'test_value_2',
                'count': '2000',
                'percentage': 0.02
            }, {
                'column': 'test_column_2',
                'value': '0',
                'count': '10000',
                'percentage': 0.1
            }, {
                'column': 'test_column_3',
                'value': '99',
                'count': '90918',
                'percentage': 0.90918
            }
        ],
        'fill_rate': [
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
    fill_rate_queries = sorted(queries['fill_rate'])
    version_queries = queries['version']

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

    assert len(top_values_queries) == 4
    assert top_values_queries[0] == "INSERT INTO marketplace_datafeedfield (name, sequence, datafield_id, data_feed_id, top_values, unique_to_data_feed) " \
        "VALUES ('test_column_1', '0', '1', 'TEST_DF', 'test_value_1 (1000:0.01), test_value_2 (2000:0.02)', false) " \
        "ON CONFLICT (datafield_id, data_feed_id) DO UPDATE " \
        "SET top_values = 'test_value_1 (1000:0.01), test_value_2 (2000:0.02)';"
    assert top_values_queries[1] == "INSERT INTO marketplace_datafeedfield (name, sequence, datafield_id, data_feed_id, top_values, unique_to_data_feed) " \
        "VALUES ('test_column_2', '0', '2', 'TEST_DF', '0 (10000:0.1)', false) " \
        "ON CONFLICT (datafield_id, data_feed_id) DO UPDATE " \
        "SET top_values = '0 (10000:0.1)';"
    assert top_values_queries[2] == "INSERT INTO marketplace_datafeedfield (name, sequence, datafield_id, data_feed_id, top_values, unique_to_data_feed) " \
        "VALUES ('test_column_3', '0', '3', 'TEST_DF', '99 (90918:0.90918)', false) " \
        "ON CONFLICT (datafield_id, data_feed_id) DO UPDATE " \
        "SET top_values = '99 (90918:0.90918)';"
    assert top_values_queries[3] == "UPDATE marketplace_datafeedfield SET top_values = NULL " \
        "WHERE data_feed_id = TEST_DF;"
    assert queries["top_values"][0][:6] == "UPDATE"
    assert queries["top_values"][1][:6] == "INSERT"
    assert queries["top_values"][2][:6] == "INSERT"
    assert queries["top_values"][3][:6] == "INSERT"

    assert len(fill_rate_queries) == 3
    assert fill_rate_queries[0] == "INSERT INTO marketplace_datafeedfield (name, sequence, datafield_id, data_feed_id, fill_rate, unique_to_data_feed) " \
        "VALUES ('test_column_1', '0', '1', 'TEST_DF', '92.0', false) ON CONFLICT (datafield_id, data_feed_id) DO UPDATE SET fill_rate = '92.0';"
    assert fill_rate_queries[1] == "INSERT INTO marketplace_datafeedfield (name, sequence, datafield_id, data_feed_id, fill_rate, unique_to_data_feed) " \
        "VALUES ('test_column_2', '0', '2', 'TEST_DF', '0.0', false) ON CONFLICT (datafield_id, data_feed_id) DO UPDATE SET fill_rate = '0.0';"
    assert fill_rate_queries[2] == "INSERT INTO marketplace_datafeedfield (name, sequence, datafield_id, data_feed_id, fill_rate, unique_to_data_feed) " \
        "VALUES ('test_column_3', '0', '3', 'TEST_DF', '100.0', false) ON CONFLICT (datafield_id, data_feed_id) DO UPDATE SET fill_rate = '100.0';"

    assert len(version_queries) == 1
    # Long query, contains full JSON data layout of the three mock fields
    assert version_queries[0] == "INSERT INTO marketplace_datafeedversion (version, data_feed_id, data_layout) VALUES ('FAKE', 'TEST_DF', '[{'fill_rate': '92.0', 'description': 'Test description 1', 'sequence': 0, 'top_values': 'test_value_1 (1000:0.01), test_value_2 (2000:0.02)', 'datatable': {'description': 'Test table description 1', 'sequence': 0, 'id': 1, 'name': 'Test table 1'}, 'category': 'TODO', 'field_type_name': 'String', 'name': 'test_column_1', 'data_feed': 'TEST_DF', 'id': '1-TEST_DF'}, {'fill_rate': '0.0', 'description': 'Test description 2', 'sequence': 0, 'top_values': '0 (10000:0.1)', 'datatable': {'description': 'Test table description 1', 'sequence': 0, 'id': 1, 'name': 'Test table 1'}, 'category': 'TODO', 'field_type_name': 'Number', 'name': 'test_column_2', 'data_feed': 'TEST_DF', 'id': '2-TEST_DF'}, {'fill_rate': '100.0', 'description': 'Test description 3', 'sequence': 0, 'top_values': '99 (90918:0.90918)', 'datatable': {'description': 'Test table description 2', 'sequence': 1, 'id': 1, 'name': 'Test table 2'}, 'category': 'TODO', 'field_type_name': 'String', 'name': 'test_column_3', 'data_feed': 'TEST_DF', 'id': '3-TEST_DF'}]');"
