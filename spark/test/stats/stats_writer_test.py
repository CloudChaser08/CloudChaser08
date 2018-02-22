import spark.stats.stats_writer as stats_writer

TEST_DATAFEED_ID = 'TEST_DF'


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
        ]
    }

    queries = stats_writer._generate_queries(test_stats, TEST_DATAFEED_ID)

    key_stats_queries = queries['key_stats']
    longitudinality_queries = queries['longitudinality']
    year_over_year_queries = queries['year_over_year']

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
