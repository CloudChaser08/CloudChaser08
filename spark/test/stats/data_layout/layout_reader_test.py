"""
Test functions from spark.stats.data_layout.layout_reader that query a DB for
the base data_layout of a DataFeed, and then converts it to the format needed in Marketplace.
"""
from spark.stats.data_layout.layout_reader import _normalize_field_layout


def test_normalizing_basic_field():
    """ Test normalizing a standard field that is not supplemental """
    input_field = {
        'name': 'field_1',
        'field_id': '1',
        'category': 'Location',
        'description': 'Field number 1',
        'sequence': 0,
        'table_id': '1',
        'table_desc': 'Table number 1',
        'table_name': 'table_1',
        'table_seq': 0,
        'field_type_name': 'String',
        'is_supplemental': 'f',
        'datamodel_name': 'd_model_name'
    }

    normalized_field = _normalize_field_layout(input_field)
    assert(
        normalized_field == (
            {
                'name': 'field_1',
                'field_id': '1',
                'category': 'Location',
                'description': 'Field number 1',
                'sequence': 0,
                'datatable': {
                    'id': '1',
                    'description': 'Table number 1',
                    'name': 'table_1',
                    'sequence': 0,
                },
                'fill_rate': None,
                'top_values': None,
                'field_type_name': 'String',
                'is_supplemental': 'f',
                'supplemental_type_name': None
            }
        )
    )

def test_normalizing_supplemental_field():
    """ Test normalizing a field that is supplemental """
    input_field = {
        'name': 'field_1',
        'field_id': '1',
        'category': 'Location',
        'description': 'Field number 1',
        'sequence': 1,
        'table_id': '1',
        'table_desc': 'Table number 1',
        'table_name': 'table_1',
        'table_seq': 2,
        'field_type_name': 'String',
        'is_supplemental': 't',
        'datamodel_name': 'd_model_name'
    }

    normalized_field = _normalize_field_layout(input_field)
    assert(
        normalized_field == (
            {
                'name': 'field_1',
                'field_id': '1',
                'category': 'Location',
                'description': 'Field number 1',
                'sequence': 1,
                'datatable': {
                    'id': '1',
                    'description': 'Table number 1',
                    'name': 'table_1',
                    'sequence': 2,
                },
                'fill_rate': None,
                'top_values': None,
                'field_type_name': 'String',
                'is_supplemental': 't',
                'supplemental_type_name': 'd_model_name'
            }
        )
    )
