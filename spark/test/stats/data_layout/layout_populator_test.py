from spark.stats.data_layout.layout_populator import populate_stat_values


class TestSingleTable(object):
    """ Class for testing stat population for data_layouts with one table """
    provider_config_input = {'datatype': 'not_emr'}

    data_layout_input = [
        {
            'name': 'field_1',
            'fill_rate': None,
            'top_values': None,
            'datatable': {
                'id': '1',
                'name': 'Table 1',
                'description': 'Table number 1',
                'sequence': 0,
            },
        },
        {
            'name': 'field_2',
            'fill_rate': None,
            'top_values': None,
            'datatable': {
                'id': '1',
                'name': 'Table 1',
                'description': 'Table number 1',
                'sequence': 0,
            },
        },
    ]

    stats_input = {
        'fill_rate': [
            {'field': 'field_1', 'fill': 1.0}, {'field': 'field_2', 'fill': 2.0},
        ],
        'top_values': [
            {
                'column': 'field_1',
                'value': 'val1',
                'count': '1000',
                'percentage': 0.01
            },
            {
                'column': 'field_1',
                'value': 'val2',
                'count': '60',
                'percentage': 5.05
            },
        ],
    }

    def test_stat_population(self):
        """ Test stat population for data_layouts with one table """
        data_layout = self.data_layout_input
        populate_stat_values(self.provider_config_input, data_layout, self.stats_input)

        field_asserted_count = 0
        for field_dict in data_layout:
            if field_dict['name'] == 'field_1':
                assert field_dict['fill_rate'] == 1.0
                assert field_dict['top_values'] == 'val1 (1000:0.01), val2 (60:5.05)'
                field_asserted_count += 1

            elif field_dict['name'] == 'field_2':
                assert field_dict['fill_rate'] == 2.0
                assert field_dict['top_values'] is None
                field_asserted_count += 1

        assert field_asserted_count == 2


class TestMultiTable(object):
    """ Class for testing stat population for data_layouts with more than one table (emr) """
    provider_config_input = {
        'datatype': 'emr',
        'models': [{'datatype' : 'emr_clin_obsn'}, {'datatype' : 'emr_medctn'}]
    }

    data_layout_input = [
        {
            'name': 'field_1',
            'fill_rate': None,
            'top_values': None,
            'datatable': {
                'id': '1',
                'name': 'Clinical Observation',  # map of emr_clin_obsn
                'description': 'Clin obs table',
                'sequence': 0,
            },
        },
        {
            'name': 'field_1',
            'fill_rate': None,
            'top_values': None,
            'datatable': {
                'id': '2',
                'name': 'Medication',  # map of emr_medctn
                'description': 'Med table',
                'sequence': 1,
            },
        },
        {
            'name': 'field_2',
            'fill_rate': None,
            'top_values': None,
            'datatable': {
                'id': '2',
                'name': 'Medication',  # map of emr_medctn
                'description': 'Med table',
                'sequence': 1,
            },
        },
    ]

    stats_input = {
        'emr_clin_obsn': {
            'fill_rate': [{'field': 'field_1', 'fill': 1.0}],
            'top_values': [
                {
                    'column': 'field_1',
                    'value': 'val1',
                    'count': '1000',
                    'percentage': 0.01
                },
                {
                    'column': 'field_1',
                    'value': 'val2',
                    'count': '60',
                    'percentage': 5.05
                },
            ],
        },
        'emr_medctn': {
            'fill_rate': [
                {'field': 'field_1', 'fill': 3.0}, {'field': 'field_2', 'fill': 2.0},
            ],
            'top_values': [
                {
                    'column': 'field_1',
                    'value': 'val1',
                    'count': '100',
                    'percentage': 0.12
                },
                {
                    'column': 'field_2',
                    'value': 'val1',
                    'count': '40',
                    'percentage': 0.02
                },
                {
                    'column': 'field_2',
                    'value': 'val2',
                    'count': '30',
                    'percentage': 4.02
                },
            ],
        }
    }

    def test_stat_population(self):
        """ Test stat population for data_layouts with more than one table (emr) """
        data_layout = self.data_layout_input
        populate_stat_values(self.provider_config_input, data_layout, self.stats_input)

        field_asserted_count = 0
        for field_dict in data_layout:
            if (
                field_dict['name'] == 'field_1' and
                field_dict['datatable']['name'] == 'Clinical Observation'
            ):
                assert field_dict['fill_rate'] == 1.0
                assert field_dict['top_values'] == 'val1 (1000:0.01), val2 (60:5.05)'
                field_asserted_count += 1

            if field_dict['name'] == 'field_1' and field_dict['datatable']['name'] == 'Medication':
                assert field_dict['fill_rate'] == 3.0
                assert field_dict['top_values'] == 'val1 (100:0.12)'
                field_asserted_count += 1

            if field_dict['name'] == 'field_2' and field_dict['datatable']['name'] == 'Medication':
                assert field_dict['fill_rate'] == 2.0
                assert field_dict['top_values'] == 'val1 (40:0.02), val2 (30:4.02)'
                field_asserted_count += 1

        assert field_asserted_count == 3
